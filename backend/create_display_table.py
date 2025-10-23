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
	"""Return a DataFrame with projects joined to departments and project_categories.

	Behavior:
	- Reads the `projects` table and reference tables `departments` and `project_categories`.
	- Joins projects.department_id -> departments.id and projects.category_id -> project_categories.id.
	- Removes id columns and returns a DataFrame containing project name, department name, and category name.
	- Gracefully handles missing tables/columns.
	"""
	conn = connect_local()
	cursor, cnxn = conn.connect_to_db()

	proj_df = select_all_from_table(cursor, cnxn, 'projects')
	dept_df = select_all_from_table(cursor, cnxn, 'departments')
	cat_df = select_all_from_table(cursor, cnxn, 'project_categories')

	# If projects missing return empty
	if proj_df is None or proj_df.empty:
		return pd.DataFrame()

	# Normalize project name column
	proj_name_col = 'name' if 'name' in proj_df.columns else ('Project' if 'Project' in proj_df.columns else proj_df.columns[0])

	# Determine key columns on projects
	proj_dept_col = next((c for c in ('department_id', 'dept_id', 'department') if c in proj_df.columns), None)
	proj_cat_col = next((c for c in ('category_id', 'project_category_id', 'project_category') if c in proj_df.columns), None)

	left = proj_df.copy()
	# Join departments
	if proj_dept_col and dept_df is not None and not dept_df.empty and 'id' in dept_df.columns:
		try:
			# coerce keys to numeric where possible
			left['_dept_key'] = pd.to_numeric(left[proj_dept_col], errors='coerce')
			right = dept_df.copy()
			right['_dept_key'] = pd.to_numeric(right['id'], errors='coerce')
			merged = pd.merge(left, right, how='left', left_on='_dept_key', right_on='_dept_key', suffixes=('_proj','_dept'))
		except Exception:
			left['_dept_key'] = left[proj_dept_col].astype(str)
			right = dept_df.copy()
			right['_dept_key'] = right['id'].astype(str)
			merged = pd.merge(left, right, how='left', left_on='_dept_key', right_on='_dept_key', suffixes=('_proj','_dept'))
	else:
		# no department join performed
		merged = left.copy()

	# Now join project categories
	if proj_cat_col and cat_df is not None and not cat_df.empty and 'id' in cat_df.columns:
		try:
			merged['_cat_key'] = pd.to_numeric(merged[proj_cat_col], errors='coerce')
			right_cat = cat_df.copy()
			right_cat['_cat_key'] = pd.to_numeric(right_cat['id'] if 'right_cat' in locals() else right_cat['id'], errors='coerce')
		except Exception:
			# fallback: ensure string join
			merged['_cat_key'] = merged[proj_cat_col].astype(str)
			right_cat = cat_df.copy()
			right_cat['_cat_key'] = right_cat['id'].astype(str) if 'right_cat' in locals() else right_cat['id'].astype(str)
		# perform merge
		try:
			merged = pd.merge(merged, right_cat, how='left', left_on='_cat_key', right_on='_cat_key', suffixes=('','_cat'))
		except Exception:
			# if merge fails, leave as-is
			pass

	# Build output DataFrame
	out = pd.DataFrame()
	out['project_name'] = merged[proj_name_col].astype(object) if proj_name_col in merged.columns else merged.iloc[:,0].astype(object)

	# department name
	if 'name_dept' in merged.columns:
		out['department_name'] = merged['name_dept'].where(pd.notna(merged['name_dept']), None)
	elif 'name' in dept_df.columns if dept_df is not None and not dept_df.empty else False:
		# sometimes merge left the department name in column 'name'
		out['department_name'] = merged['name'].where(pd.notna(merged['name']), None) if 'name' in merged.columns else None
	else:
		out['department_name'] = None

	# category name
	if 'category' in merged.columns:
		out['category'] = merged['category'].where(pd.notna(merged['category']), None)
	elif 'category_cat' in merged.columns:
		out['category'] = merged['category_cat'].where(pd.notna(merged['category_cat']), None)
	else:
		out['category'] = None

	# Ensure columns are exactly as requested
	out = out[['project_name', 'department_name', 'category']]

	return out.reset_index(drop=True)


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
	"""Return a DataFrame of projects joined with department and project category.

	Behavior:
	- Reads `projects`, `departments`, and `project_categories` using select_all_from_table
	- Joins projects.department_id -> departments.id and projects.category_id -> project_categories.id
	- Returns a DataFrame with exactly three columns: `project_name`, `department_name`, `category`
	- Handles missing tables/columns gracefully (fills missing names with None)
	"""
	conn = connect_local()
	cursor, cnxn = conn.connect_to_db()

	proj_df = select_all_from_table(cursor, cnxn, 'projects')
	dept_df = select_all_from_table(cursor, cnxn, 'departments')
	cat_df = select_all_from_table(cursor, cnxn, 'project_categories')

	# If projects missing or empty return empty DataFrame
	if proj_df is None or proj_df.empty:
		return pd.DataFrame()

	# Determine project name column (prefer 'name')
	if 'name' in proj_df.columns:
		proj_name_col = 'name'
	elif 'Project' in proj_df.columns:
		proj_name_col = 'Project'
	else:
		proj_name_col = proj_df.columns[0]

	# Determine department id column on projects
	proj_dept_col = None
	for cand in ('department_id', 'dept_id', 'Department', 'department'):
		if cand in proj_df.columns:
			proj_dept_col = cand
			break

	# Determine project category id column on projects
	proj_cat_col = next((c for c in ('category_id', 'project_category_id', 'project_category') if c in proj_df.columns), None)

	# Work on a copy to avoid mutating original
	left = proj_df.copy()

	# Normalize project name column into a canonical 'project_name'
	left['project_name'] = left[proj_name_col].astype(object) if proj_name_col in left.columns else left.iloc[:, 0].astype(object)

	# Ensure fiscal_year is preserved when present
	if 'fiscal_year' in left.columns:
		left['fiscal_year'] = left['fiscal_year']
	else:
		left['fiscal_year'] = None

	# Prepare right-hand tables with canonical name columns
	# Departments -> department_name
	if dept_df is not None and not dept_df.empty and 'id' in dept_df.columns:
		dept_name_col = 'name' if 'name' in dept_df.columns else dept_df.columns[0]
		dept_right = dept_df.rename(columns={dept_name_col: 'department_name'})[['id', 'department_name']].copy()
	else:
		dept_right = None

	# Project categories -> category
	if cat_df is not None and not cat_df.empty and 'id' in cat_df.columns:
		cat_name_col = 'category' if 'category' in cat_df.columns else (cat_df.columns[1] if len(cat_df.columns) > 1 else cat_df.columns[0])
		cat_right = cat_df.rename(columns={cat_name_col: 'category'})[['id', 'category']].copy()
	else:
		cat_right = None

	# POs -> po_name (join via po_id on projects or via departments.po_id if projects lacks po_id)
	pos_df = select_all_from_table(cursor, cnxn, 'pos')
	if pos_df is not None and not pos_df.empty and 'id' in pos_df.columns:
		pos_name_col = 'name' if 'name' in pos_df.columns else pos_df.columns[0]
		pos_right = pos_df.rename(columns={pos_name_col: 'po_name'})[['id', 'po_name']].copy()
	else:
		pos_right = None

	# Start with left (projects) and merge departments on detected key
	merged = left
	if proj_dept_col and dept_right is not None:
		# Coerce comparable types where sensible by creating temporary numeric keys
		try:
			merged['_proj_dept_key'] = pd.to_numeric(merged[proj_dept_col], errors='coerce')
			dept_right['_dept_key'] = pd.to_numeric(dept_right['id'], errors='coerce')
			merged = pd.merge(merged, dept_right, how='left', left_on='_proj_dept_key', right_on='_dept_key', suffixes=('', '_dept'))
		except Exception:
			merged[proj_dept_col] = merged[proj_dept_col].astype(str)
			dept_right['id'] = dept_right['id'].astype(str)
			merged = pd.merge(merged, dept_right.rename(columns={'id': proj_dept_col}), how='left', left_on=proj_dept_col, right_on=proj_dept_col)
	else:
		# no department info available; ensure department_name column exists
		merged['department_name'] = None

	# Merge project categories
	if proj_cat_col and cat_right is not None:
		try:
			merged['_proj_cat_key'] = pd.to_numeric(merged[proj_cat_col], errors='coerce')
			cat_right['_cat_key'] = pd.to_numeric(cat_right['id'], errors='coerce')
			merged = pd.merge(merged, cat_right, how='left', left_on='_proj_cat_key', right_on='_cat_key', suffixes=('', '_cat'))
		except Exception:
			merged[proj_cat_col] = merged[proj_cat_col].astype(str)
			cat_right['id'] = cat_right['id'].astype(str)
			merged = pd.merge(merged, cat_right.rename(columns={'id': proj_cat_col}), how='left', left_on=proj_cat_col, right_on=proj_cat_col)
	else:
		merged['category'] = None

	# Merge POs. First try to join directly from projects.po_id -> pos.id if present
	po_joined = False
	if 'po_id' in merged.columns and pos_right is not None:
		try:
			merged['_proj_po_key'] = pd.to_numeric(merged['po_id'], errors='coerce')
			pos_right['_po_key'] = pd.to_numeric(pos_right['id'], errors='coerce')
			merged = pd.merge(merged, pos_right, how='left', left_on='_proj_po_key', right_on='_po_key', suffixes=('', '_po'))
			po_joined = True
		except Exception:
			merged['po_id'] = merged['po_id'].astype(str)
			pos_right['id'] = pos_right['id'].astype(str)
			merged = pd.merge(merged, pos_right.rename(columns={'id': 'po_id'}), how='left', left_on='po_id', right_on='po_id')
			po_joined = True

	# If not joined yet, try to get PO via department -> departments.po_id
	if not po_joined and dept_df is not None and not dept_df.empty and 'po_id' in dept_df.columns and pos_right is not None:
		# Make sure department id columns are comparable
		try:
			merged['_dept_id_for_po'] = pd.to_numeric(merged[proj_dept_col], errors='coerce') if proj_dept_col in merged.columns else None
			dept_local = dept_df.copy()
			dept_local['_dept_id_for_po'] = pd.to_numeric(dept_local['id'], errors='coerce')
			dept_local['_dept_po_key'] = pd.to_numeric(dept_local['po_id'], errors='coerce')
			# merge departments to expose po_id then merge pos
			merged = pd.merge(merged, dept_local[['_dept_id_for_po', '_dept_po_key']], how='left', left_on='_dept_id_for_po', right_on='_dept_id_for_po')
			merged = pd.merge(merged, pos_right.rename(columns={'id': '_dept_po_key'}), how='left', left_on='_dept_po_key', right_on='_dept_po_key')
		except Exception:
			# last-resort: set po_name to None
			merged['po_name'] = None

	# Build final output with canonical columns
	out = pd.DataFrame()
	out['project_name'] = merged['project_name'].where(pd.notna(merged['project_name']), None)

	# department_name might be present from dept_right merge, or as 'department_name'
	if 'department_name' in merged.columns:
		out['department_name'] = merged['department_name'].where(pd.notna(merged['department_name']), None)
	elif 'name' in dept_df.columns if dept_df is not None and not dept_df.empty else False:
		out['department_name'] = None
	else:
		out['department_name'] = None

	# category
	if 'category' in merged.columns:
		out['category'] = merged['category'].where(pd.notna(merged['category']), None)
	elif 'category_cat' in merged.columns:
		out['category'] = merged['category_cat'].where(pd.notna(merged['category_cat']), None)
	else:
		out['category'] = None

	# po_name
	if 'po_name' in merged.columns:
		out['po_name'] = merged['po_name'].where(pd.notna(merged['po_name']), None)
	else:
		out['po_name'] = None

	# fiscal_year already preserved
	out['fiscal_year'] = merged['fiscal_year'] if 'fiscal_year' in merged.columns else None

	# Ensure only the requested columns are present (keep the new po_name and fiscal_year as requested)
	cols = ['project_name', 'department_name', 'category', 'po_name', 'fiscal_year']
	out = out.loc[:, cols]
	

	return out.reset_index(drop=True)


def get_nonpc_display():
	"""Return a DataFrame for project_forecasts_nonpc joined with PO, Department and Project.

	Behavior:
	- Reads `project_forecasts_nonpc`, `pos`, `departments`, and `projects` from the local DB
	- Attempts to join on project_id -> projects.id, department_id -> departments.id, po_id -> pos.id
	- Returns a DataFrame with canonical columns: project_name, department_name, po_name, fiscal_year, and a non-personnel amount column
	- Handles missing tables/columns gracefully (fills missing names with None)
	"""
	conn = connect_local()
	cursor, cnxn = conn.connect_to_db()

	df = select_all_from_table(cursor, cnxn, 'project_forecasts_nonpc')
	if df is None or df.empty:
		return pd.DataFrame()

	left = df.copy()

	# detect candidate id columns on forecast table
	proj_id_col = next((c for c in ('project_id', 'proj_id', 'projects_id', 'project') if c in left.columns), None)
	dept_id_col = next((c for c in ('department_id', 'dept_id', 'department') if c in left.columns), None)
	po_id_col = next((c for c in ('PO_id', 'po_id', 'po', 'PO') if c in left.columns), None)
	fiscal_col = next((c for c in ('fiscal_year', 'Fiscal Year', 'fy', 'year') if c in left.columns), None)

	# load reference tables
	proj_df = select_all_from_table(cursor, cnxn, 'projects')
	dept_df = select_all_from_table(cursor, cnxn, 'departments')
	pos_df = select_all_from_table(cursor, cnxn, 'pos')

	merged = left

	# join projects -> provide project_name
	if proj_df is not None and not proj_df.empty and proj_id_col is not None:
		try:
			merged['_proj_key'] = pd.to_numeric(merged[proj_id_col], errors='coerce')
			right = proj_df.copy()
			right['_proj_key'] = pd.to_numeric(right['id'], errors='coerce')
			merged = pd.merge(merged, right, how='left', left_on='_proj_key', right_on='_proj_key', suffixes=('','_proj'))
		except Exception:
			try:
				merged[proj_id_col] = merged[proj_id_col].astype(str)
				right = proj_df.copy()
				right['id'] = right['id'].astype(str)
				merged = pd.merge(merged, right.rename(columns={'id': proj_id_col}), how='left', left_on=proj_id_col, right_on=proj_id_col)
			except Exception:
				pass
	else:
		# ensure project_name exists even if not joined
		merged['project_name'] = None

	# join departments -> provide department_name
	if dept_df is not None and not dept_df.empty and dept_id_col is not None:
		try:
			merged['_dept_key'] = pd.to_numeric(merged[dept_id_col], errors='coerce')
			right_dept = dept_df.copy()
			right_dept['_dept_key'] = pd.to_numeric(right_dept['id'], errors='coerce')
			merged = pd.merge(merged, right_dept, how='left', left_on='_dept_key', right_on='_dept_key', suffixes=('','_dept'))
		except Exception:
			try:
				merged[dept_id_col] = merged[dept_id_col].astype(str)
				right_dept = dept_df.copy()
				right_dept['id'] = right_dept['id'].astype(str)
				merged = pd.merge(merged, right_dept.rename(columns={'id': dept_id_col}), how='left', left_on=dept_id_col, right_on=dept_id_col)
			except Exception:
				pass
	else:
		merged['department_name'] = None

	# join POs -> provide po_name (direct join from po_id if present)
	if pos_df is not None and not pos_df.empty and po_id_col is not None:
		try:
			merged['_po_key'] = pd.to_numeric(merged[po_id_col], errors='coerce')
			right_po = pos_df.copy()
			right_po['_po_key'] = pd.to_numeric(right_po['id'], errors='coerce')
			merged = pd.merge(merged, right_po, how='left', left_on='_po_key', right_on='_po_key', suffixes=('','_po'))
		except Exception:
			try:
				merged[po_id_col] = merged[po_id_col].astype(str)
				right_po = pos_df.copy()
				right_po['id'] = right_po['id'].astype(str)
				merged = pd.merge(merged, right_po.rename(columns={'id': po_id_col}), how='left', left_on=po_id_col, right_on=po_id_col)
			except Exception:
				pass
	else:
		merged['po_name'] = None

	# Build canonical output
	out = pd.DataFrame()

	# project name candidates
	proj_name_col = 'name' if proj_df is not None and 'name' in proj_df.columns else ('Project' if proj_df is not None and 'Project' in proj_df.columns else None)
	if proj_name_col and proj_name_col in merged.columns:
		out['project_name'] = merged[proj_name_col].where(pd.notna(merged[proj_name_col]), None)
	elif 'project_name' in merged.columns:
		out['project_name'] = merged['project_name'].where(pd.notna(merged['project_name']), None)
	else:
		out['project_name'] = None

	# department name
	if 'name_dept' in merged.columns:
		out['department_name'] = merged['name_dept'].where(pd.notna(merged['name_dept']), None)
	elif 'department_name' in merged.columns:
		out['department_name'] = merged['department_name'].where(pd.notna(merged['department_name']), None)
	elif dept_df is not None and 'name' in dept_df.columns and 'name' in merged.columns:
		out['department_name'] = merged['name'].where(pd.notna(merged['name']), None)
	else:
		out['department_name'] = None

	# po name
	if 'po_name' in merged.columns:
		out['po_name'] = merged['po_name'].where(pd.notna(merged['po_name']), None)
	elif 'name_po' in merged.columns:
		out['po_name'] = merged['name_po'].where(pd.notna(merged['name_po']), None)
	elif pos_df is not None and 'name' in pos_df.columns and 'name' in merged.columns:
		out['po_name'] = merged['name'].where(pd.notna(merged['name']), None)
	else:
		out['po_name'] = None

	# fiscal year
	if fiscal_col and fiscal_col in merged.columns:
		out['fiscal_year'] = merged[fiscal_col].where(pd.notna(merged[fiscal_col]), None)
	elif 'fiscal_year' in merged.columns:
		out['fiscal_year'] = merged['fiscal_year'].where(pd.notna(merged['fiscal_year']), None)
	else:
		out['fiscal_year'] = None

	# non-personnel amount: detect candidate columns
	nonpc_candidates = ['non_personnel_expense', 'Non-personnel Expense', 'Non-personnel cost', 'non_personnel_cost', 'amount', 'expense', 'nonpersonnel_expense']
	nonpc_col = next((c for c in nonpc_candidates if c in merged.columns), None)
	if nonpc_col:
		try:
			out['non_personnel_expense'] = merged[nonpc_col].where(pd.notna(merged[nonpc_col]), 0.0).astype(float)
		except Exception:
			# fallback: coerce via to_numeric
			try:
				out['non_personnel_expense'] = pd.to_numeric(merged[nonpc_col], errors='coerce').fillna(0.0)
			except Exception:
				out['non_personnel_expense'] = 0.0
	else:
		out['non_personnel_expense'] = 0.0

	# Ensure column order
	cols = ['project_name', 'department_name', 'po_name', 'fiscal_year', 'non_personnel_expense']
	out = out.loc[:, cols]

	return out.reset_index(drop=True)

def get_project_cateogory_display():
	"""Return a DataFrame of projects with their project category.

	Behavior:
	- Reads `projects` and `project_categories` using select_all_from_table
	- Joins projects.category_id (or fallback project_category_id) to project_categories.id
	- Returns a DataFrame with exactly two columns: 'name' (from projects) and 'category' (from project_categories)
	- Handles missing tables/columns gracefully (fills missing category with None)
	"""
	conn = connect_local()
	cursor, cnxn = conn.connect_to_db()

	proj_df = select_all_from_table(cursor, cnxn, 'projects')
	cat_df = select_all_from_table(cursor, cnxn, 'project_categories')

	# If projects missing or empty return empty DataFrame
	if proj_df is None or proj_df.empty:
		return pd.DataFrame()

	# Normalize project name column
	# prefer 'name' but accept other common variants
	name_col = 'name' if 'name' in proj_df.columns else ( 'Project' if 'Project' in proj_df.columns else None )

	# Determine category id column on projects (common variants)
	proj_cat_col = None
	for candidate in ('category_id', 'project_category_id', 'project_category'):
		if candidate in proj_df.columns:
			proj_cat_col = candidate
			break

	# If category table missing, return projects with category None
	if cat_df is None or cat_df.empty or 'id' not in cat_df.columns or 'category' not in cat_df.columns:
		out = pd.DataFrame()
		# project name
		if name_col:
			out['name'] = proj_df[name_col].astype(object)
		else:
			# fallback to first column as name
			out['name'] = proj_df.iloc[:,0].astype(object)
		out['category'] = None
		return out.reset_index(drop=True)

	# Perform join
	try:
		# ensure id columns are comparable types
		left = proj_df.copy()
		right = cat_df.copy()
		# If project has no explicit category id column, return names with category None
		if proj_cat_col is None:
			# attempt to join on name->category? Not requested — return None categories
			out = pd.DataFrame()
			if name_col:
				out['name'] = left[name_col].astype(object)
			else:
				out['name'] = left.iloc[:,0].astype(object)
			out['category'] = None
			return out.reset_index(drop=True)

		# Coerce join keys to numeric where possible to avoid dtype mismatches
		try:
			left_key = pd.to_numeric(left[proj_cat_col], errors='coerce')
			right_key = pd.to_numeric(right['id'], errors='coerce')
			left['_join_key'] = left_key
			right['_join_key'] = right_key
			merged = pd.merge(left, right, how='left', left_on='_join_key', right_on='_join_key', suffixes=('_proj','_cat'))
		except Exception:
			# fallback to string-based join
			left['_join_key'] = left[proj_cat_col].astype(str)
			right['_join_key'] = right['id'].astype(str)
			merged = pd.merge(left, right, how='left', left_on='_join_key', right_on='_join_key', suffixes=('_proj','_cat'))

		# Extract project name and category columns
		out = pd.DataFrame()
		if name_col and name_col in merged.columns:
			out['name'] = merged[name_col].astype(object)
		else:
			out['name'] = merged.iloc[:,0].astype(object)

		# category column in right table is 'category'
		if 'category' in merged.columns:
			out['category'] = merged['category'].where(pd.notna(merged['category']), None)
		else:
			out['category'] = None

		return out.reset_index(drop=True)
	except Exception:
		# On error return a minimal DataFrame with names and None categories
		out = pd.DataFrame()
		if name_col:
			out['name'] = proj_df[name_col].astype(object)
		else:
			out['name'] = proj_df.iloc[:,0].astype(object)
		out['category'] = None
		return out.reset_index(drop=True)

def get_IO_display_table():
	"""Return a DataFrame with IO numbers and their associated project name.

	Behavior:
	- Reads `IOs` and `projects` using select_all_from_table
	- LEFT JOIN io_table.project_id -> project_table.id
	- Returns a DataFrame with exactly two columns: 'IO' (from IO_num) and 'project_name'
	- Handles missing tables/columns gracefully (fills missing project_name with None)
	"""
	conn = connect_local()
	cursor, cnxn = conn.connect_to_db()
	io_table = select_all_from_table(cursor, cnxn, "IOs")
	project_table = select_all_from_table(cursor, cnxn, "projects")

	# If IO table missing or empty, return empty DataFrame
	if io_table is None or io_table.empty:
		return pd.DataFrame()

	# Determine IO number column (common variants)
	io_num_col = next((c for c in ('IO_num', 'IO', 'io_num', 'io') if c in io_table.columns), io_table.columns[0])

	# Determine project id column on io_table (prefer 'project_id')
	proj_id_col = next((c for c in ('project_id', 'proj_id', 'project') if c in io_table.columns), None)

	# If project table missing, return IOs with None project_name
	if project_table is None or project_table.empty:
		out = pd.DataFrame()
		out['IO'] = io_table[io_num_col].astype(object) if io_num_col in io_table.columns else io_table.iloc[:,0].astype(object)
		out['project_name'] = None
		return out.reset_index(drop=True)

	# Determine project name column on projects table
	proj_name_col = 'name' if 'name' in project_table.columns else ('Project' if 'Project' in project_table.columns else project_table.columns[0])

	# Perform left join, attempting numeric coercion first then fallback to string join
	left = io_table.copy()
	right = project_table.copy()
	merged = None
	if proj_id_col is not None and 'id' in right.columns:
		try:
			left['_proj_key'] = pd.to_numeric(left[proj_id_col], errors='coerce')
			right['_proj_key'] = pd.to_numeric(right['id'], errors='coerce')
			merged = pd.merge(left, right, how='left', left_on='_proj_key', right_on='_proj_key', suffixes=('','_proj'))
		except Exception:
			# fallback to string-based join
			left[proj_id_col] = left[proj_id_col].astype(str)
			right['id'] = right['id'].astype(str)
			merged = pd.merge(left, right, how='left', left_on=proj_id_col, right_on='id', suffixes=('','_proj'))
	else:
		# no project id to join on; preserve IOs and set project_name None
		merged = left.copy()
		merged['project_name'] = None

	# Build output DataFrame with canonical columns
	out = pd.DataFrame()
	# IO column
	if io_num_col in merged.columns:
		out['IO'] = merged[io_num_col].where(pd.notna(merged[io_num_col]), None)
	else:
		out['IO'] = merged.iloc[:,0].where(pd.notna(merged.iloc[:,0]), None)

	# project_name column from joined projects table (use proj_name_col if available)
	if proj_name_col in merged.columns:
		out['project_name'] = merged[proj_name_col].where(pd.notna(merged[proj_name_col]), None)
	elif 'project_name' in merged.columns:
		out['project_name'] = merged['project_name'].where(pd.notna(merged['project_name']), None)
	else:
		# attempt to find a candidate name column from the right-hand table
		cand = next((c for c in ('name_proj', 'name', 'Project') if c in merged.columns), None)
		out['project_name'] = merged[cand].where(pd.notna(merged[cand]), None) if cand else None

	# Ensure exact column order
	out = out[['IO', 'project_name']]

	return out.reset_index(drop=True)
