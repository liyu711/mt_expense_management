# from backend.generate_data import generate_tables
from backend.connect_local import connect_local, drop_all_tables, initialize_database, clear_table, select_columns_from_table
from backend.upload_forecasts_nonpc import upload_nonpc_forecasts_local
from backend.upload_forecasts_pc import upload_pc_forecasts_local
from backend.upload_expenses import upload_expenses_local
from backend.upload_budgets import upload_budgets_local, upload_fundings_local
from backend.upload_capex_forecast import upload_capex_forecasts_local
from backend.upload_capex_budgets import upload_capex_budget_local
from backend.upload_capex_expenses import upload_capex_expense_local
from backend.modify_table_local import add_entry
from backend import \
    upload_nonpc_forecasts_local_m, upload_pc_forecasts_local_m, \
    upload_budgets_local_m, upload_capex_forecast_m, upload_capex_budgets_local_m, upload_human_resource
import pandas as pd
import numpy as np
import os

funding_path = 'processed_data/budgets/fundings_.csv'
filepath_nonpc = 'processed_data/forecasts/forecasts_nonpc_.csv'
filepath_pc = 'processed_data/forecasts/forecasts_pc_.csv'
file_path_expenses = "processed_data/expenses/expenses_.csv"
budget_path = 'processed_data/budgets/budgets_.csv'
funding_path = 'processed_data/budgets/fundings_.csv'

capex_forecast_path = "processed_data/forecasts/capex_forecasts_.csv"
capex_budgets_path = "processed_data/budgets/capex_budgets_.csv"
capex_expense_path = "processed_data/expenses/capex_expenses_.csv"
hr_path = "processed_data/info/personnel_categories.csv"
department_path = 'processed_data/info/departments.csv'

df_nonpc = pd.read_csv(filepath_nonpc)
df_pc = pd.read_csv(filepath_pc)
df_pc = df_pc.drop(['Personnel cost'],axis=1)
df_expense = pd.read_csv(file_path_expenses)
df_budgets = pd.read_csv(budget_path)
df_funding = pd.read_csv(funding_path)
df_departments = pd.read_csv(department_path)

df_capex_forecast = pd.read_csv(capex_forecast_path)
df_capex_budget = pd.read_csv(capex_budgets_path)
df_capex_expense = pd.read_csv(capex_expense_path)
df_hr = pd.read_csv(hr_path)


def generate_staff_cost(df_pc, df_hr, out_path='processed_data/info/staff_cost_.csv', seed=2):
    """Generate staff cost table from df_pc and df_hr.

    - Extract unique (staff category name, year) pairs from df_pc.
    - Assign a random float cost in range [0, 100) for each pair (dtype=float).
    - Join with df_hr on category_name == name to obtain category_id.
    - Return DataFrame with columns ['category_id', 'year', 'cost'] and save to out_path.

    Notes: function is tolerant to different column names. If staff category is provided
    as an id in df_pc, it will map it to name via df_hr. Year column candidates are
    'Fiscal Year', 'fiscal_year', 'year'.
    """
    # Detect year column in df_pc
    year_candidates = ['Fiscal Year', 'fiscal_year', 'year', 'cap_year']
    year_col = next((c for c in year_candidates if c in df_pc.columns), None)

    # Detect staff category column in df_pc (robust: case-insensitive and partial matches)
    staff_candidates = ['Staff Category', 'staff_category', 'human_resource_category', 'Human Resource Category', 'category_name', 'category']
    # first try exact column name match (case-sensitive)
    staff_col = next((c for c in df_pc.columns if c in staff_candidates), None)
    if staff_col is None:
        # try case-insensitive exact matches
        lower_candidates = [s.lower() for s in staff_candidates]
        staff_col = next((c for c in df_pc.columns if c.lower() in lower_candidates), None)
    if staff_col is None:
        # try partial matches (e.g., 'Human resource category')
        def is_staff_like(name):
            n = name.lower()
            return (('human' in n and 'category' in n) or ('resource' in n and 'category' in n) or ('staff' in n and 'category' in n))

        staff_col = next((c for c in df_pc.columns if is_staff_like(c)), None)

    # If staff column is an id, try to map to name using df_hr
    if staff_col is None:
        id_candidates = ['human_resource_category_id', 'category_id', 'staff_category_id']
        for ic in id_candidates:
            if ic in df_pc.columns:
                if 'id' in df_hr.columns and 'name' in df_hr.columns:
                    id_to_name = dict(zip(df_hr['id'], df_hr['name']))
                    df_pc = df_pc.copy()
                    df_pc['Staff Category'] = df_pc[ic].map(id_to_name)
                    staff_col = 'Staff Category'
                    break

    if year_col is None or staff_col is None:
        print('generate_staff_cost: could not detect year or staff category column in df_pc')
        return pd.DataFrame(columns=['category_id', 'year', 'cost'])

    # Extract unique pairs present in df_pc
    pairs = df_pc[[staff_col, year_col]].dropna().drop_duplicates().copy()
    pairs = pairs.rename(columns={staff_col: 'category_name', year_col: 'year'})
    # Ensure year is integer where possible
    try:
        pairs['year'] = pairs['year'].astype(int)
    except Exception:
        pass

    # Assign random cost in [0, 100)
    rng = np.random.default_rng(seed)
    pairs['cost'] = rng.random(len(pairs)) * 100.0
    pairs['cost'] = pairs['cost'].astype(float)

    # Normalize df_hr: many input files are single-column lists with a header like
    # 'Personnel Categories'. Ensure df_hr has 'id' and 'name' columns for merging.
    hr = df_hr.copy()
    if 'name' not in hr.columns and 'Personnel Categories' in hr.columns:
        hr = hr.rename(columns={'Personnel Categories': 'name'})
    if 'name' not in hr.columns and 'Personnel Categories' not in hr.columns and hr.shape[1] == 1:
        # fallback: take the only column as name
        hr = hr.rename(columns={hr.columns[0]: 'name'})
    if 'id' not in hr.columns:
        hr = hr.reset_index().rename(columns={'index': 'id'})

    # Normalize names for case-insensitive matching
    hr['name_norm'] = hr['name'].astype(str).str.strip().str.lower()
    pairs['category_name_norm'] = pairs['category_name'].astype(str).str.strip().str.lower()

    # Join on normalized name
    merged = pairs.merge(hr[['id', 'name', 'name_norm']], left_on='category_name_norm', right_on='name_norm', how='left')
    merged = merged.rename(columns={'id': 'category_id'})
    result = merged[['category_id', 'year', 'cost']].copy()

    # Report matching stats for debugging
    matched = result['category_id'].notna().sum()
    total = len(result)
    print(f"generate_staff_cost: matched {matched}/{total} category_name -> category_id")

    # Fallback: for any unmatched category_name, try fuzzy matching against hr names
    try:
        import difflib
        name_to_id = dict(zip(hr['name_norm'], hr['id']))
        unmatched_mask = result['category_id'].isna()
        if unmatched_mask.any():
            hr_names = list(name_to_id.keys())
            for idx in result[unmatched_mask].index:
                cat_norm = merged.at[idx, 'category_name_norm']
                # find closest match
                matches = difflib.get_close_matches(cat_norm, hr_names, n=1, cutoff=0.75)
                if matches:
                    matched_name = matches[0]
                    result.at[idx, 'category_id'] = name_to_id.get(matched_name)
    except Exception:
        pass

    # Recompute matched stats after fallback
    try:
        matched_after = result['category_id'].notna().sum()
        print(f"generate_staff_cost: matched after fallback {matched_after}/{total}")
    except Exception:
        pass

    # Increment category_id by 1 for all non-null entries (per request)
    try:
        result.loc[result['category_id'].notna(), 'category_id'] = result.loc[result['category_id'].notna(), 'category_id'].astype(int) + 1
    except Exception:
        pass

    # Ensure correct dtypes
    try:
        result['year'] = result['year'].astype(pd.Int64Dtype())
    except Exception:
        pass
    result['cost'] = result['cost'].astype(float)

    # Ensure output directory exists
    out_dir = os.path.dirname(out_path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    result.to_csv(out_path, index=False)
    print(f"Saved staff cost table to {out_path} ({len(result)} rows)")
    return result


# connect to local DB
connect_obj = connect_local()

engine, cursor, cnxn = connect_obj.connect_to_db(engine=True)
# drop_all_tables(cursor, cnxn)
initialize_database(cursor, cnxn, initial_values=False)

# df_test = pd.read_csv("df_test.csv")

# res = select_columns_from_table(cursor, "project_forecasts_nonpc", ['fiscal_year', 'department_id', 'project_id','project_category_id', 'io_id', 'po_id'])
# print(res)

# df_po = df_nonpc[['PO']]

df_io = df_nonpc[['IO', 'Project Name']]
# df_department = df_nonpc[['Department']]
df_project_category = df_nonpc[['Project Category']]
df_projects = df_nonpc[['Project Name', 'Project Category', 'Department']]

# df_po.to_csv('processed_data/info/po_.csv', index= False)
df_po = pd.read_csv('processed_data/info/po_.csv')
# df_department = df_department.drop_duplicates()
# df_department.to_csv('processed_data/info/departments_.csv', index= False)
# df_department = pd.read_csv('processed_data/info/departments_.csv')
po_row = add_entry(df_po, "pos", ['name'], 'name')
department_row = add_entry(df_departments, 'departments',['category'], 'name')
category_row = add_entry(df_project_category, "project_categories", ['category'], 'category')
projects_row = add_entry(df_projects, "projects", ['name', 'category_id', 'department_id'], 'name')
add_entry(df_hr, "human_resource_categories", ['name'], 'name')
# Now generate staff cost CSV (after human_resource_categories inserted) and insert into DB
try:
    staff_cost_df = generate_staff_cost(df_pc, df_hr)
    print('Generated staff_cost_df rows:', len(staff_cost_df))
    if staff_cost_df is not None and not staff_cost_df.empty:
        # add_entry expects merge columns and a merge_on key; use category_id, year, cost
        add_entry(staff_cost_df, 'human_resource_cost', ['category_id', 'year', 'cost'], 'category_id')
except Exception as e:
    print('Failed to generate/insert staff_cost_df into human_resource_cost:', e)
add_entry(df_io, "ios", ['IO_num', 'project_id'], 'IO_num')


upload_nonpc_forecasts_local(df_nonpc)
upload_pc_forecasts_local(df_pc)

# cnxn.commit()

upload_expenses_local(df_expense)
upload_budgets_local(df_budgets)

upload_fundings_local(df_funding)
upload_capex_forecasts_local(df_capex_forecast)

upload_capex_budget_local(df_capex_budget)
upload_capex_expense_local(df_capex_expense)

def temp_get_all_table_columns():
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    for table_name_tuple in tables:
        table_name = table_name_tuple[0]
        print(f"\nTable: {table_name}")

        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()

        for col_info in columns:
            # col_info structure: (cid, name, type, notnull, dflt_value, pk)
            print(f"  Column Name: {col_info[1]}, Type: {col_info[2]}")
