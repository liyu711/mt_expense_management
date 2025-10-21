from backend.connect_local import connect_local, select_all_from_table
import pandas as pd

def get_departments_display():
	"""Return a DataFrame for display of departments joined with POs.

	Behavior:
	- Reads `departments` and `pos` from the local database using select_all_from_table
	- Attempts a LEFT JOIN of departments -> pos on departments.category_id == pos.id
	  If departments does not have `category_id`, falls back to departments.po_id == pos.id
	- Returns a DataFrame with columns department_name and po_name (id columns dropped)
	"""
	conn = connect_local()
	cursor, cnxn = conn.connect_to_db()
	depts = select_all_from_table(cursor, cnxn, 'departments')
	pos = select_all_from_table(cursor, cnxn, 'pos')

	# if pos missing, return department names with po_name as None
	if pos is None or pos.empty:
		out = pd.DataFrame()
		if 'name' in depts.columns:
			out['department_name'] = depts['name']
		else:
			out['department_name'] = depts.iloc[:, 0]
		out['po_name'] = None
		return out.reset_index(drop=True)

	merged = pd.merge(depts, pos, how='left', left_on='po_id', right_on='id', suffixes=('_departments', '_po'))
	merged.drop(columns=['id_departments', 'po_id', 'id_po'], inplace=True)

	return merged.reset_index(drop=True)


def get_forecasts_display():
	"""Return a DataFrame for display of project_forecasts_nonpc.

	Behavior:
	- Reads `project_forecasts_nonpc` and reference tables (projects, departments, POs, IOs, project_categories)
	- Maps id columns (project_id, department_id, PO_id, io_id) to friendly display columns
	- Renames non_personnel_expense -> 'Non-personnel Expense' and fiscal_year -> 'Fiscal Year'
	- Reorders columns to place id, Department, IO, Project Category near the front similar to transform_table
	- Handles missing reference tables gracefully (leaves mapped columns as-is or None)
	"""
	conn = connect_local()
	cursor, cnxn = conn.connect_to_db()

	df = select_all_from_table(cursor, cnxn, 'project_forecasts_nonpc')
	if df is None or df.empty:
		return pd.DataFrame() if df is None else df

	# Map id -> name for department, PO, project, IO, project_category
	id_name_map = {
		'department_id': ('departments', 'id', 'name', 'Department Name'),
		'PO_id': ('pos', 'id', 'name', 'PO Name'),
		'project_id': ('projects', 'id', 'name', 'Project'),
		'io_id': ('IOs', 'id', 'IO_num', 'IO'),
		'project_category_id': ('project_categories', 'id', 'category', 'Project Category'),
	}

	for id_col, (ref_table, ref_id, ref_name, new_col_name) in id_name_map.items():
		if id_col in df.columns:
			try:
				ref_df = select_all_from_table(cursor, cnxn, ref_table)
				if ref_df is None or ref_df.empty or ref_id not in ref_df.columns or ref_name not in ref_df.columns:
					df[new_col_name] = None
				else:
					ref_dict = dict(zip(ref_df[ref_id], ref_df[ref_name]))
					df[new_col_name] = df[id_col].map(ref_dict)
			except Exception:
				df[new_col_name] = None

	# Drop the raw id columns where appropriate (keep if user may need them)
	drop_cols = [c for c in ['department_id', 'PO_id', 'project_id', 'io_id', 'project_category_id'] if c in df.columns]
	if drop_cols:
		try:
			df = df.drop(columns=drop_cols)
		except Exception:
			pass

	# Rename non_personnel_expense and fiscal_year if present
	if 'non_personnel_expense' in df.columns:
		df = df.rename(columns={'non_personnel_expense': 'Non-personnel Expense'})
	if 'fiscal_year' in df.columns:
		df = df.rename(columns={'fiscal_year': 'Fiscal Year'})

	# Reorder columns similar to transform_table special-case for project_forecasts_nonpc
	current_cols = list(df.columns)
	new_order = []
	if 'id' in current_cols:
		new_order.append('id')

	# Department candidates
	dept_candidates = ['Department Name', 'Department', 'department']
	dept = next((c for c in dept_candidates if c in current_cols), None)
	io_candidates = ['IO', 'IO number']
	io_col = next((c for c in io_candidates if c in current_cols), None)
	pc_col = 'Project Category' if 'Project Category' in current_cols else None

	if dept and dept not in new_order:
		new_order.append(dept)
	if io_col and io_col not in new_order:
		new_order.append(io_col)
	if pc_col and pc_col not in new_order:
		new_order.append(pc_col)

	# Append remaining columns preserving original order
	for c in current_cols:
		if c not in new_order:
			new_order.append(c)

	try:
		new_order = [c for c in new_order if c in df.columns]
		if new_order and new_order != current_cols:
			df = df.loc[:, new_order]
	except Exception:
		pass

	return df.reset_index(drop=True)


def get_pc_display():
	"""Return a DataFrame for display of project_forecasts_pc.

	Behavior:
	- Reads `project_forecasts_pc` and reference tables (projects, departments, POs, IOs, human_resource_categories, human_resource_cost)
	- Renames human_resource_fte -> 'Work Hours(FTE)', personnel_expense -> 'Personnel Expense'
	- Maps human_resource_category_id to 'Staff Category' (friendly name) and drops the id column
	- Computes 'Personnel Cost' by looking up unit cost in human_resource_cost (keyed by (category_id, year)) and multiplying by FTE
	- Maps other id columns (project_id, department_id, PO_id, io_id) to friendly names when present
	- Reorders columns to place Fiscal Year, IO, Project, Project Category, Staff Category near the front
	- Handles missing reference tables or columns gracefully
	"""
	conn = connect_local()
	cursor, cnxn = conn.connect_to_db()

	df = select_all_from_table(cursor, cnxn, 'project_forecasts_pc')
	if df is None or df.empty:
		return pd.DataFrame() if df is None else df

	# Renames
	if 'human_resource_fte' in df.columns:
		df = df.rename(columns={'human_resource_fte': 'Work Hours(FTE)'})
	if 'personnel_expense' in df.columns:
		df = df.rename(columns={'personnel_expense': 'Personnel Expense'})

	# Map human_resource_category_id -> Staff Category name
	if 'human_resource_category_id' in df.columns:
		try:
			hr_ref = select_all_from_table(cursor, cnxn, 'human_resource_categories')
			if hr_ref is None or hr_ref.empty or 'id' not in hr_ref.columns or 'name' not in hr_ref.columns:
				df['Staff Category'] = None
			else:
				ref_dict = dict(zip(hr_ref['id'], hr_ref['name']))
				df['Staff Category'] = df['human_resource_category_id'].map(ref_dict)
				# drop the id column to prefer friendly name display
				try:
					df = df.drop(columns=['human_resource_category_id'])
				except Exception:
					pass
		except Exception:
			df['Staff Category'] = None

	# Compute Personnel Cost using human_resource_cost table if possible
	try:
		hr_cost_df = select_all_from_table(cursor, cnxn, 'human_resource_cost')
		if hr_cost_df is not None and not hr_cost_df.empty and 'category_id' in hr_cost_df.columns and 'year' in hr_cost_df.columns and 'cost' in hr_cost_df.columns:
			# Build lookup dict keyed by (category_id, year)
			hr_cost_df_local = hr_cost_df[['category_id', 'year', 'cost']].copy()
			try:
				hr_cost_df_local['year'] = hr_cost_df_local['year'].astype(int)
			except Exception:
				pass
			cost_lookup = {(int(r['category_id']), int(r['year'])): float(r['cost']) for _, r in hr_cost_df_local.iterrows() if r['category_id'] is not None}

			# Determine columns
			fte_col = 'Work Hours(FTE)' if 'Work Hours(FTE)' in df.columns else ('human_resource_fte' if 'human_resource_fte' in df.columns else None)
			fy_col = 'Fiscal Year' if 'Fiscal Year' in df.columns else ('fiscal_year' if 'fiscal_year' in df.columns else None)

			personnel_costs = []
			# Build name->id map for staff categories if needed
			name_to_id = {}
			try:
				if 'Staff Category' in df.columns:
					hr_ref = select_all_from_table(cursor, cnxn, 'human_resource_categories')
					if hr_ref is not None and not hr_ref.empty and 'name' in hr_ref.columns and 'id' in hr_ref.columns:
						name_to_id = dict(zip(hr_ref['name'], hr_ref['id']))
			except Exception:
				name_to_id = {}

			for _, row in df.iterrows():
				try:
					staff_name = row.get('Staff Category') if 'Staff Category' in row.index else None
					cat_id = name_to_id.get(staff_name) if staff_name is not None else None
					fy = int(row.get(fy_col)) if fy_col and row.get(fy_col) not in (None, '') else None
					wh = row.get(fte_col) if fte_col else None
					try:
						wh = float(wh) if wh not in (None, '') else 0.0
					except Exception:
						wh = 0.0
					if cat_id is None or fy is None:
						personnel_costs.append(None)
						continue
					key = (int(cat_id), int(fy))
					hourly = cost_lookup.get(key)
					if hourly is None:
						personnel_costs.append(None)
					else:
						personnel_costs.append(hourly * wh)
				except Exception:
					personnel_costs.append(None)

			df['Personnel Cost'] = personnel_costs
	except Exception:
		# non-fatal
		pass

	# Map other id columns to friendly names (project, department, PO, IO, project_category)
	id_name_map = {
		'project_id': ('projects', 'id', 'name', 'Project'),
		'department_id': ('departments', 'id', 'name', 'Department Name'),
		'PO_id': ('pos', 'id', 'name', 'PO Name'),
		'io_id': ('IOs', 'id', 'IO_num', 'IO'),
		'project_category_id': ('project_categories', 'id', 'category', 'Project Category'),
	}
	for id_col, (ref_table, ref_id, ref_name, new_col_name) in id_name_map.items():
		if id_col in df.columns:
			try:
				ref_df = select_all_from_table(cursor, cnxn, ref_table)
				if ref_df is None or ref_df.empty or ref_id not in ref_df.columns or ref_name not in ref_df.columns:
					df[new_col_name] = None
				else:
					ref_dict = dict(zip(ref_df[ref_id], ref_df[ref_name]))
					df[new_col_name] = df[id_col].map(ref_dict)
			except Exception:
				df[new_col_name] = None

	# Drop raw id columns where appropriate
	drop_cols = [c for c in ['human_resource_category_id', 'project_id', 'department_id', 'PO_id', 'io_id', 'project_category_id'] if c in df.columns]
	if drop_cols:
		try:
			df = df.drop(columns=drop_cols)
		except Exception:
			pass

	# Global renames
	if 'fiscal_year' in df.columns:
		df = df.rename(columns={'fiscal_year': 'Fiscal Year'})
	if 'cap_year' in df.columns:
		df = df.rename(columns={'cap_year': 'Capital Year'})

	# Reorder to place Fiscal Year then IO/Project/Project Category/Staff Category front
	current_cols = list(df.columns)
	new_order = []
	# prefer fiscal year
	fy = 'Fiscal Year' if 'Fiscal Year' in current_cols else ('fiscal_year' if 'fiscal_year' in current_cols else None)
	if fy:
		# append up to and including fiscal year
		for c in current_cols:
			new_order.append(c)
			if c == fy:
				break

		# Ensure PO and Department are preserved before inserted block
		po_col = next((c for c in ['PO', 'PO_id', 'PO Name'] if c in current_cols), None)
		dept_col = next((c for c in ['Department Name', 'Department', 'department'] if c in current_cols), None)
		if po_col and po_col not in new_order:
			new_order.append(po_col)
		if dept_col and dept_col not in new_order:
			new_order.append(dept_col)

		io_col = next((c for c in ['IO', 'IO number'] if c in current_cols), None)
		if io_col and io_col not in new_order:
			new_order.append(io_col)
		if 'Project' in current_cols and 'Project' not in new_order:
			new_order.append('Project')
		if 'Project Category' in current_cols and 'Project Category' not in new_order:
			new_order.append('Project Category')
		if 'Staff Category' in current_cols and 'Staff Category' not in new_order:
			new_order.append('Staff Category')
	else:
		if 'id' in current_cols:
			new_order.append('id')

	# Append the rest preserving order
	for c in current_cols:
		if c not in new_order:
			new_order.append(c)

	try:
		new_order = [c for c in new_order if c in df.columns]
		if new_order and new_order != current_cols:
			df = df.loc[:, new_order]
	except Exception:
		pass

	return df.reset_index(drop=True)


def get_projects_display():
	"""Return a DataFrame of available projects based on project_forecasts_nonpc.

	Behavior:
	- Calls get_forecasts_display() to obtain a cleaned forecasts DataFrame
	- Drops 'Project Category' and 'id' columns if present
	- If a 'Project' column exists, returns a deduplicated DataFrame with one row per Project (preserving other display columns like PO Name, Department Name when available)
	- Otherwise returns the modified DataFrame as-is
	"""
	try:
		df = get_forecasts_display()
	except Exception:
		return pd.DataFrame()

	if df is None or df.empty:
		return pd.DataFrame() if df is None else df

	cols_to_drop = [c for c in ['Project Category', 'id'] if c in df.columns]
	if cols_to_drop:
		try:
			df = df.drop(columns=cols_to_drop)
		except Exception:
			pass

	# If Project column exists, deduplicate by Project (keep first occurrence)
	if 'Project' in df.columns:
		try:
			# keep first occurrence and preserve PO/Department if present
			subset_cols = ['Project', 'PO Name', 'Department Name']
			present_subset = [c for c in subset_cols if c in df.columns]
			projects_df = df.drop_duplicates(subset=['Project']).reset_index(drop=True)
			return projects_df
		except Exception:
			return df.reset_index(drop=True)

	return df.reset_index(drop=True)


# def get_pc_sum():
	