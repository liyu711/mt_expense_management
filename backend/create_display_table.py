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
