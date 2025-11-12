
from flask import Flask, flash, render_template, request, redirect, url_for, Blueprint, jsonify
from werkzeug.utils import secure_filename
from backend.login import valid_login
from backend.connect_local import connect_local, select_columns_from_table, select_all_from_table
from app_local.select_data import transform_table

from backend import \
    upload_nonpc_forecasts_local_m, upload_pc_forecasts_local_m,\
    upload_budgets_local_m, upload_expenses_local, upload_fundings_local, \
    upload_capex_forecast_m, upload_capex_budgets_local_m, upload_capex_expense_local, get_projects_display,\
    get_project_cateogory_display, get_IO_display_table, get_hr_category_display
from backend.upload_forecasts_nonpc import upload_nonpc_forecasts_df, upload_nonpc_forecasts_local_m
import pandas as pd

manual_upload = Blueprint('manual_upload', __name__, template_folder='templates')
# module-level selected PO for manual input page
selected_po = None
# module-level selected Department for manual input page
selected_department = None
# module-level selected Fiscal Year for manual input page
selected_fiscal_year = None
# module-level selected Project for manual input page
selected_project = None
input_types = [
        "forecast_nonpc",
        "forecast_pc" ,
        "budgets",
        "fundings",
        "expenses",
        "capex_forecast",
        "capex_budget",
        "capex_expense"
]

display_names = {
    "forecast_nonpc": ['PO', 'IO','Department', "Project Category", "Project Name", "Fiscal Year", "Non-personnel Expense"],
    "forecast_pc" : ['PO', 'IO','Department', "Project Category", "Project Name", "Fiscal Year", 'Human resource category', "Human resource FTE", "Personnel Expense"],
    "budgets": ['PO', 'Department', "Fiscal Year", "Personnel Budget (k CNY)", "Non-personnel Budget (k CNY)"],
    "fundings": ['PO', 'Department', "Fiscal Year", "Funding (k CNY)", "Funding From", "Funding For"],
    "expenses": ['Department', "Fiscal Year", "From Period", "Order(IO)", "CO Object Name", "Cost Element", "Cost Element Name", "Val.in rep.cur.", "Name"],
    "capex_forecast" : ['PO', 'Department', 'CapYear', 'For Project', 'CapEx Description', 'CapEx Forecast', 'Cost Center'],
    "capex_budget" : ['PO',	'Department', 'CapYear', 'For Project',	'Capex Description', 'Approved Budget (k CNY)', 'Comments from finance'],
    "capex_expense":['PO', 'Department', 'CapYear', 'For Project', 'Capex Description', 'Project number', 'Actual (k CNY)', 'Date']
}

upload_columns = {
    "forecast_nonpc": ["PO","IO","Department","Project Category","Project Name","fiscal_year","Non-personnel cost"],
    "forecast_pc" : ["PO","IO","Department","Project Category","Project Name","fiscal_year","Human resource category","Human resource FTE","Personnel cost"],
    "budgets": ["PO", "Department", "fiscal_year", "Human Resources Budget", "Non-Human Resources Budget"],
    "fundings": ["PO", "Department", "fiscal_year", "funding", "funding_from", "funding_for"],
    "expenses": ["Department", "fiscal_year", "from_period", "Order", "CO object name", "Cost element", "Cost element name", "Val.in rep.cur", "Name"],
    "capex_forecast" : ["PO", "Department", "cap_year", "Project name", "capex_description", "Forecast", "cost_center"],
    "capex_budget" : ["PO", "Department", "fiscal_year", "for_project", "capex_description", "budget"],
    "capex_expense": ["PO", "Department", "fiscal_year", "Project Name", "capex_description", "Project number", "Expense"]
}


titles = {
    "forecast_nonpc": "None-Personnel Forecast",
    "forecast_pc": "Personnel Forecast",
    "budgets": "Budgets",
    "fundings": "Fundings",
    "expenses": "Expenses",
    "capex_forecast": "CapEx Forecast",
    "capex_budget": "CapEx Budget",
    "capex_expense": "CapEx Expense"
}

@manual_upload.route("/manual_input")
def render_mannual_input():
    conn = connect_local()
    engine, cursor, cnxn = conn.connect_to_db(engine=True)
    # load POs first so we can resolve selected_po -> po_id
    pos_df = select_all_from_table(cursor, cnxn, 'pos')
    if pos_df is None:
        po = []
    else:
        po = []
        for _, r in pos_df.iterrows():
            pname = r['name'] if 'name' in r.index and pd.notna(r['name']) else (r.get('Name') if 'Name' in r.index else None)
            pid = None
            if 'id' in r.index and pd.notna(r['id']):
                try:
                    pid = int(r['id'])
                except Exception:
                    pid = None
            po.append({'id': pid, 'name': pname})

    # Resolve selected_po (name) -> selected_po_id using the POs table
    selected_po_id = None
    try:
        if pos_df is not None and selected_po:
            # normalize both sides for robust match
            name_series = None
            if 'name' in pos_df.columns:
                name_series = pos_df['name']
            elif 'Name' in pos_df.columns:
                name_series = pos_df['Name']
            if name_series is not None:
                mask = name_series.astype(str).str.strip().str.lower() == str(selected_po).strip().lower()
                match = pos_df[mask]
                if not match.empty and 'id' in match.columns and pd.notna(match.iloc[0]['id']):
                    try:
                        selected_po_id = int(match.iloc[0]['id'])
                    except Exception:
                        selected_po_id = None
    except Exception:
        selected_po_id = None
    # load departments with their po_id so the UI can filter departments by selected PO
    depts_df = select_all_from_table(cursor, cnxn, 'departments')
    if depts_df is None:
        departments_all = []
        departments = []
    else:
        # Build complete department list for client-side dynamic filtering
        departments_all = []
        for _, r in depts_df.iterrows():
            name = None
            if 'name' in r.index and pd.notna(r['name']):
                name = r['name']
            elif 'Department' in r.index and pd.notna(r['Department']):
                name = r['Department']
            elif 'Name' in r.index and pd.notna(r['Name']):
                name = r['Name']

            # try several possible column names for po id
            po_all_id = None
            for key in ('po_id', 'PO_id', 'poId', 'po', 'PO'):
                if key in r.index and pd.notna(r[key]):
                    try:
                        po_all_id = int(r[key])
                    except Exception:
                        try:
                            po_all_id = int(str(r[key]))
                        except Exception:
                            po_all_id = None
                    break

            dept_all_id = None
            if 'id' in r.index and pd.notna(r['id']):
                try:
                    dept_all_id = int(r['id'])
                except Exception:
                    dept_all_id = None

            departments_all.append({'id': dept_all_id, 'name': name, 'po_id': po_all_id})

        # If a PO has been selected, filter departments to those linked to that PO id
        try:
            if selected_po_id is not None:
                possible_cols = [c for c in ('po_id', 'PO_id', 'poId', 'po', 'PO') if c in depts_df.columns]
                if possible_cols:
                    mask_total = None
                    for c in possible_cols:
                        try:
                            coerced = pd.to_numeric(depts_df[c], errors='coerce')
                        except Exception:
                            coerced = pd.Series([pd.NA] * len(depts_df))
                        mask_c = (coerced == selected_po_id)
                        mask_total = mask_c if mask_total is None else (mask_total | mask_c)
                    if mask_total is not None:
                        depts_df = depts_df[mask_total.fillna(False)]
        except Exception:
            # If anything goes wrong during filtering, fall back to unfiltered departments
            pass
        # departments = []
        # # iterate defensively over rows and normalize keys
        # for _, r in depts_df.iterrows():
        #     name = None
        #     if 'name' in r.index and pd.notna(r['name']):
        #         name = r['name']
        #     elif 'Department' in r.index and pd.notna(r['Department']):
        #         name = r['Department']
        #     elif 'Name' in r.index and pd.notna(r['Name']):
        #         name = r['Name']

        #     # try several possible column names for po id
        #     po_id = None
        #     for key in ('po_id', 'PO_id', 'poId', 'po', 'PO'):
        #         if key in r.index and pd.notna(r[key]):
        #             try:
        #                 po_id = int(r[key])
        #             except Exception:
        #                 try:
        #                     po_id = int(str(r[key]))
        #                 except Exception:
        #                     po_id = None
        #             break

        #     dept_id = None
        #     if 'id' in r.index and pd.notna(r['id']):
        #         try:
        #             dept_id = int(r['id'])
        #         except Exception:
        #             dept_id = None

        #     departments.append({'id': dept_id, 'name': name, 'po_id': po_id})

    # POs already loaded above
    io_df = get_IO_display_table()

    # Build initial IO list optionally filtered by module-level selected_project
    io = []
    try:
        if io_df is None or io_df.empty:
            io = []
        else:
            try:
                records = io_df.to_dict(orient='records')
            except Exception:
                records = []

            seen_io = set()
            for r in records:
                io_val = r.get('IO') or r.get('IO_num') or r.get('io') or r.get('io_num')
                proj_val = r.get('project_name') or r.get('Project') or r.get('project') or r.get('name')

                # if a project is selected at module level, only include IOs for that project
                if selected_project and str(selected_project).strip() not in ('', 'All'):
                    if not proj_val or str(proj_val).strip() != str(selected_project).strip():
                        continue

                if io_val is None:
                    continue
                # try to coerce to int for display where possible
                try:
                    candidate = int(io_val)
                except Exception:
                    candidate = str(io_val).strip()
                if candidate not in seen_io:
                    seen_io.add(candidate)
                    io.append(candidate)
    except Exception:
        io = []

    # Build initial projects list from the display helper and apply any module-level filters
    proj_df = get_projects_display()

    human_resource_categories = select_all_from_table(cursor, cnxn, "human_resource_categories")['name']
    # Fetch forecast table contents to show below the Add button
    try:
        df_nonpc = select_all_from_table(cursor, cnxn, 'project_forecasts_nonpc')
    except Exception:
        df_nonpc = None
    try:
        df_pc = select_all_from_table(cursor, cnxn, 'project_forecasts_pc')
    except Exception:
        df_pc = None

    # map id columns to readable names (same mapping used elsewhere)
    id_name_map = {
        'department_id': ('departments', 'id', 'name', 'Department'),
        'po_id': ('POs', 'id', 'name', 'PO'),
        'PO_id': ('POs', 'id', 'name', 'PO'),
        'project_id': ('projects', 'id', 'name', 'Project'),
        'io_id': ('IOs', 'id', 'IO_num', 'IO'),
        'project_category_id': ('project_categories', 'id', 'category', 'Project Category'),
        'human_resource_category_id': ('human_resource_categories', 'id', 'name', 'Human resource category'),
    }
    def map_and_prepare(df, table_name=None):
        if df is None:
            return [], []
        try:
            for id_col, (ref_table, ref_id, ref_name, new_col_name) in id_name_map.items():
                if id_col in df.columns:
                    ref_df = select_all_from_table(cursor, cnxn, ref_table)
                    ref_dict = dict(zip(ref_df[ref_id], ref_df[ref_name]))
                    df[new_col_name] = df[id_col].map(ref_dict)
            drop_cols = [col for col in ['department_id', 'po_id', 'PO_id', 'project_id', 'io_id', 'project_category_id'] if col in df.columns]
            df2 = df.drop(columns=drop_cols)
            # Apply the same transforms used in the select view so naming and ordering match
            try:
                df2 = transform_table(df2, table_name or '', cursor, cnxn)
            except Exception:
                pass
            return df2.columns.tolist(), df2.values.tolist()
        except Exception:
            return [], []

    pf_nonpc_columns, pf_nonpc_data = map_and_prepare(df_nonpc, 'project_forecasts_nonpc')
    pf_pc_columns, pf_pc_data = map_and_prepare(df_pc, 'project_forecasts_pc')

    # Build combined forecast table pivoting each Human Resource Category into its own column with its cost value.
    # 1. Determine base columns (union without personnel-specific columns). These remain unchanged across pivot.
    base_cols_order = []
    seen = set()
    personnel_specific = {'Human resource category', 'Staff Category', 'Work Hours(FTE)', 'Personnel Expense', 'Personnel Cost', 'Personnel cost'}
    for c in pf_nonpc_columns + pf_pc_columns:
        if c not in seen and c not in personnel_specific:
            seen.add(c)
            base_cols_order.append(c)

    # 2. Helper to convert list rows to dicts.
    def to_dicts(cols, rows):
        out = []
        for r in rows:
            d = {}
            for i, col in enumerate(cols):
                d[col] = r[i] if i < len(r) else None
            out.append(d)
        return out

    nonpc_dicts = to_dicts(pf_nonpc_columns, pf_nonpc_data)
    pc_dicts = to_dicts(pf_pc_columns, pf_pc_data)

    # 3. Get full list of HR categories (column names for pivot)
    try:
        hr_cat_df = select_all_from_table(cursor, cnxn, 'human_resource_categories')
        if hr_cat_df is not None and 'name' in hr_cat_df.columns:
            hr_category_list = [str(v).strip() for v in hr_cat_df['name'].dropna().tolist() if str(v).strip()]
        else:
            # fallback: scan pc_dicts
            hr_category_list = []
    except Exception:
        hr_category_list = []
    # fallback from pc rows if DB call failed or empty
    if not hr_category_list:
        seen_cat = set()
        for d in pc_dicts:
            cval = d.get('Human resource category') or d.get('Staff Category')
            if cval:
                sc = str(cval).strip()
                if sc and sc not in seen_cat:
                    seen_cat.add(sc)
                    hr_category_list.append(sc)

    # Ensure deterministic ordering
    hr_category_list = sorted(hr_category_list, key=lambda x: x.lower())

    # 4. Index personnel rows by composite key for grouping and build per-key category->cost mapping.
    def norm(v):
        return '' if v is None else str(v).strip()

    def make_key(d):
        return (
            norm(d.get('PO') or d.get('PO_id')),
            norm(d.get('Department') or d.get('Department Name')),
            norm(d.get('Project Category')),
            norm(d.get('Project Name') or d.get('Project')),
            norm(d.get('IO')),
            norm(d.get('Fiscal Year'))
        )

    pc_group = {}
    for d in pc_dicts:
        key = make_key(d)
        pc_group.setdefault(key, []).append(d)

    # 5. Build combined rows: start from nonpc rows, then add pc-only keys.
    combined_rows_dicts = []

    # Build base cost lookup with priority keys:
    # (po_name_lower|None, dept_name_lower|None, category_name_lower, fiscal_year) -> base_cost
    # We'll fall back progressively: (PO,Dept,Cat,Year) -> (PO,None,Cat,Year) -> (None,Dept,Cat,Year) -> (None,None,Cat,Year)
    base_cost_lookup = {}
    try:
        hr_cost_df = select_all_from_table(cursor, cnxn, 'human_resource_cost')
        hr_cat_df_full = select_all_from_table(cursor, cnxn, 'human_resource_categories')
        if hr_cost_df is not None and hr_cat_df_full is not None and 'category_id' in hr_cost_df.columns and 'year' in hr_cost_df.columns:
            # Map ids -> names for category/PO/Department
            cat_id_to_name = {}
            try:
                if 'id' in hr_cat_df_full.columns and 'name' in hr_cat_df_full.columns:
                    cat_id_to_name = dict(zip(hr_cat_df_full['id'], hr_cat_df_full['name']))
            except Exception:
                cat_id_to_name = {}
            # POs and Departments for name resolution
            pos_df2 = select_all_from_table(cursor, cnxn, 'pos')
            depts_df2 = select_all_from_table(cursor, cnxn, 'departments')
            po_id_to_name = {}
            dept_id_to_name = {}
            try:
                if pos_df2 is not None and 'id' in pos_df2.columns and 'name' in pos_df2.columns:
                    po_id_to_name = dict(zip(pos_df2['id'], pos_df2['name']))
            except Exception:
                po_id_to_name = {}
            try:
                if depts_df2 is not None and 'id' in depts_df2.columns and 'name' in depts_df2.columns:
                    dept_id_to_name = dict(zip(depts_df2['id'], depts_df2['name']))
            except Exception:
                dept_id_to_name = {}

            for _, r in hr_cost_df.iterrows():
                try:
                    cid = r.get('category_id')
                    year_val = r.get('year')
                    cost_val = r.get('cost')
                    po_id_row = r.get('po_id') if 'po_id' in hr_cost_df.columns else None
                    dept_id_row = r.get('department_id') if 'department_id' in hr_cost_df.columns else None
                    cat_name = cat_id_to_name.get(cid) or (cid if isinstance(cid, str) else None)
                    if cat_name is None or year_val is None:
                        continue
                    po_name = po_id_to_name.get(po_id_row) if po_id_row is not None else None
                    dept_name = dept_id_to_name.get(dept_id_row) if dept_id_row is not None else None
                    key = (
                        (str(po_name).strip().lower() if po_name not in (None, '') else None),
                        (str(dept_name).strip().lower() if dept_name not in (None, '') else None),
                        str(cat_name).strip().lower(),
                        int(year_val)
                    )
                    if cost_val is not None:
                        base_cost_lookup[key] = cost_val
                    # Also store fallbacks without PO/Dept to ease lookup
                    key_po_only = (key[0], None, key[2], key[3])
                    key_dept_only = (None, key[1], key[2], key[3])
                    key_none = (None, None, key[2], key[3])
                    if cost_val is not None:
                        # only set if not already present to preserve more specific entries
                        base_cost_lookup.setdefault(key_po_only, cost_val)
                        base_cost_lookup.setdefault(key_dept_only, cost_val)
                        base_cost_lookup.setdefault(key_none, cost_val)
                except Exception:
                    continue
    except Exception:
        base_cost_lookup = {}

    def compute_personnel_cost(pr):
        """Derive personnel cost = FTE * base_cost(category, fiscal_year). Falls back to stored Personnel Expense if present."""
        # Attempt to read previously stored cost columns (legacy) first
        legacy_cost = pr.get('Personnel Expense') or pr.get('Personnel Cost') or pr.get('personnel_expense')
        # Determine FTE and category
        cat = pr.get('Human resource category') or pr.get('Staff Category')
        fte_raw = pr.get('Human resource FTE') or pr.get('Work Hours(FTE)') or pr.get('human_resource_fte')
        fy = pr.get('Fiscal Year') or pr.get('fiscal_year')
        po_name = pr.get('PO') or pr.get('PO_id')
        dept_name = pr.get('Department') or pr.get('Department Name')
        try:
            fte_val = float(fte_raw)
        except Exception:
            fte_val = None
        try:
            fy_val = int(fy)
        except Exception:
            fy_val = None
        if cat and fte_val is not None and fy_val is not None:
            # try most specific -> least specific
            cat_key = str(cat).strip().lower()
            po_key = str(po_name).strip().lower() if po_name not in (None, '') else None
            dept_key = str(dept_name).strip().lower() if dept_name not in (None, '') else None
            base_cost = None
            for k in [
                (po_key, dept_key, cat_key, fy_val),
                (po_key, None, cat_key, fy_val),
                (None, dept_key, cat_key, fy_val),
                (None, None, cat_key, fy_val)
            ]:
                if k in base_cost_lookup:
                    base_cost = base_cost_lookup.get(k)
                    if base_cost is not None:
                        break
            try:
                base_num = float(base_cost) if base_cost is not None else None
            except Exception:
                base_num = None
            if base_num is not None:
                return base_num * fte_val
        # fallback to legacy stored cost value if available
        return legacy_cost

    def build_row_base(source_dict):
        return {c: source_dict.get(c) for c in base_cols_order}

    # For each non-personnel row, add HR category columns.
    for d in nonpc_dicts:
        key = make_key(d)
        row = build_row_base(d)
        # Initialize all hr category columns as None
        for cat in hr_category_list:
            row[cat] = None
        # Fill with derived personnel cost values from matching personnel rows
        for pr in pc_group.get(key, []):
            cat = pr.get('Human resource category') or pr.get('Staff Category')
            if cat:
                cat_norm = str(cat).strip()
                if cat_norm in row:
                    cost_val = compute_personnel_cost(pr)
                    # If multiple rows for same category, sum them
                    try:
                        if row[cat_norm] is None:
                            row[cat_norm] = cost_val
                        else:
                            # convert to float if possible and sum
                            current = row[cat_norm]
                            cv = float(current) if current not in (None, '') else 0.0
                            nv = float(cost_val) if cost_val not in (None, '') else 0.0
                            row[cat_norm] = cv + nv
                    except Exception:
                        # fallback: keep first value
                        if row[cat_norm] is None:
                            row[cat_norm] = cost_val
        combined_rows_dicts.append(row)

    # Add personnel-only keys (those without a nonpc counterpart)
    existing_keys = {make_key(d) for d in nonpc_dicts}
    for key, pc_list in pc_group.items():
        if key in existing_keys:
            continue
        # use first pc row for base info
        rep = pc_list[0]
        row = build_row_base(rep)
        for cat in hr_category_list:
            row[cat] = None
        for pr in pc_list:
            cat = pr.get('Human resource category') or pr.get('Staff Category')
            if not cat:
                continue
            cat_norm = str(cat).strip()
            if cat_norm in row:
                cost_val = compute_personnel_cost(pr)
                try:
                    if row[cat_norm] is None:
                        row[cat_norm] = cost_val
                    else:
                        current = row[cat_norm]
                        cv = float(current) if current not in (None, '') else 0.0
                        nv = float(cost_val) if cost_val not in (None, '') else 0.0
                        row[cat_norm] = cv + nv
                except Exception:
                    if row[cat_norm] is None:
                        row[cat_norm] = cost_val
        combined_rows_dicts.append(row)

    # 6. Compute Forecast Sum per row (sum of Non-personnel Expense + all HR category costs).
    def to_float(x):
        try:
            return float(x)
        except Exception:
            return 0.0

    for idx, d in enumerate(combined_rows_dicts, start=1):
        d['id'] = idx
        # Sum Non-personnel Expense (if present) + all hr category columns
        total = 0.0
        npexp = d.get('Non-personnel Expense')
        total += to_float(npexp)
        for cat in hr_category_list:
            total += to_float(d.get(cat))
        d['Forecast Sum'] = total

    # 7. Final columns in required order:
    #    Project, PO, Department, Fiscal Year, IO, Project Category, Non-personnel Expense,
    #    [HR category columns...], Forecast Sum
    project_label = 'Project' if 'Project' in base_cols_order else ('Project Name' if 'Project Name' in base_cols_order else None)
    po_label = 'PO' if 'PO' in base_cols_order else None
    department_label = 'Department' if 'Department' in base_cols_order else ('Department Name' if 'Department Name' in base_cols_order else None)
    fy_label = 'Fiscal Year' if 'Fiscal Year' in base_cols_order else ('fiscal_year' if 'fiscal_year' in base_cols_order else None)
    io_label = 'IO' if 'IO' in base_cols_order else None
    pc_label = 'Project Category' if 'Project Category' in base_cols_order else None
    npexp_label = 'Non-personnel Expense' if 'Non-personnel Expense' in base_cols_order else None

    ordered_prefix = [lbl for lbl in ['id', project_label, po_label, department_label, fy_label, io_label, pc_label, npexp_label] if lbl]

    # Any other base columns not explicitly listed should follow after the prefix
    remaining_base = [c for c in base_cols_order if c not in set(ordered_prefix)]

    combined_columns = ordered_prefix + remaining_base + hr_category_list + ['Forecast Sum']
    # For display, rename 'Non-personnel Expense' to 'Non-personnel Forecast' without changing values
    # For display purposes append unit suffix to monetary columns (labels-only, no numeric scaling)
    combined_columns_display = []
    for c in combined_columns:
        if c == 'Non-personnel Expense':
            combined_columns_display.append('Non-personnel Forecast (k CNY)')
        elif c == 'Forecast Sum':
            combined_columns_display.append('Forecast Sum (k CNY)')
        else:
            combined_columns_display.append(c)
    combined_data = []
    for d in combined_rows_dicts:
        # Use source names for value lookup; only headers are renamed for display
        combined_data.append([d.get(c, None) for c in combined_columns])

    # Remove IO from display table entirely (do not send IO as a visible column).
    # This strips the IO column from both the displayed headers and from the data rows.
    try:
        # possible IO column names (prefer 'IO')
        io_names = ['IO', 'IO Number', 'IO_num']
        io_idx = None
        for name in io_names:
            if name in combined_columns:
                io_idx = combined_columns.index(name)
                break
        if io_idx is not None:
            # remove from combined_columns and display headers
            combined_columns.pop(io_idx)
            combined_columns_display.pop(io_idx)
            # remove the corresponding value from each data row
            for row in combined_data:
                try:
                    if len(row) > io_idx:
                        row.pop(io_idx)
                except Exception:
                    pass
    except Exception:
        pass

    return render_template("pages/manual_input.html", 
                           input_types = input_types, 
                           titles = titles, 
                           display_names=display_names, 
                           upload_columns=upload_columns,
                           departments = [],
                           departments_all = departments_all,
                           pos = po,
                           ios = io,
                           projects = [],
                           project_categories=[],
                           human_resource_categories= human_resource_categories,
                           personnel_group_columns= hr_category_list,  # for grouped header 'Personnel Cost'
                           combined_forecast_columns=combined_columns_display,
                           combined_forecast_data=combined_data
                           )


@manual_upload.route('/api/hr_cost', methods=['GET'])
def api_hr_cost():
    """Return hierarchical unit cost for a human resource category.
    Query params:
      category (required) - HR category name
      year (required) - fiscal year
      po (optional) - PO name for most specific match
      department (optional) - Department/BU name for most specific match
    Specificity order:
      (po, department, category, year)
      (department, category, year)
      (po, category, year)
      (category, year)
    Returns JSON: {"cost": <float|null>}
    """
    category = request.args.get('category')
    year = request.args.get('year')
    po_name = request.args.get('po') or selected_po
    dept_name = request.args.get('department') or selected_department
    if not category or not year:
        return jsonify({'cost': None}), 200
    try:
        y = int(year)
    except Exception:
        return jsonify({'cost': None}), 200
    try:
        db = connect_local()
        cursor, cnxn = db.connect_to_db()
        # Resolve category id
        cat_id = None
        try:
            cursor.execute("SELECT id FROM human_resource_categories WHERE name = ?", (category,))
            r = cursor.fetchone()
            if r: cat_id = r[0]
        except Exception:
            cat_id = None
        # Resolve po_id
        po_id = None
        if po_name:
            try:
                cursor.execute("SELECT id FROM pos WHERE name = ?", (po_name,))
                rp = cursor.fetchone()
                if rp: po_id = rp[0]
            except Exception:
                po_id = None
        # Resolve department_id
        dept_id = None
        if dept_name:
            try:
                cursor.execute("SELECT id FROM departments WHERE name = ?", (dept_name,))
                rd = cursor.fetchone()
                if rd: dept_id = rd[0]
            except Exception:
                dept_id = None

        def fetch(q, params):
            try:
                cursor.execute(q, params)
                rr = cursor.fetchone()
                return rr[0] if rr else None
            except Exception:
                return None

        cost = None
        if cost is None and all(v is not None for v in (po_id, dept_id, cat_id)):
            cost = fetch("SELECT cost FROM human_resource_cost WHERE po_id = ? AND department_id = ? AND category_id = ? AND year = ? LIMIT 1", (po_id, dept_id, cat_id, y))
        if cost is None and all(v is not None for v in (dept_id, cat_id)):
            cost = fetch("SELECT cost FROM human_resource_cost WHERE department_id = ? AND category_id = ? AND year = ? LIMIT 1", (dept_id, cat_id, y))
        if cost is None and all(v is not None for v in (po_id, cat_id)):
            cost = fetch("SELECT cost FROM human_resource_cost WHERE po_id = ? AND category_id = ? AND year = ? LIMIT 1", (po_id, cat_id, y))
        if cost is None and cat_id is not None:
            cost = fetch("SELECT cost FROM human_resource_cost WHERE category_id = ? AND year = ? LIMIT 1", (cat_id, y))
        if cost is None:
            # fallback where category stored as text
            cost = fetch("SELECT cost FROM human_resource_cost WHERE category_id = ? AND year = ? LIMIT 1", (category, y))
        return jsonify({'cost': cost}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        try:
            cursor.close(); cnxn.close()
        except Exception:
            pass


@manual_upload.route('/manual_input/po_selection', methods=['POST'])
def manual_po_selection():
    """Endpoint to receive PO selection from client for manual input page and update module-level selected_po."""
    global selected_po
    try:
        if request.is_json:
            payload = request.get_json()
            val = payload.get('po')
        else:
            val = request.form.get('po')
        selected_po = val
        return {'status': 'ok', 'selected_po': selected_po}, 200
    except Exception as e:
        return {'status': 'error', 'message': str(e)}, 500


@manual_upload.route('/manual_input/department_selection', methods=['POST'])
def manual_department_selection():
    """Endpoint to receive Department selection from client for manual input page and update module-level selected_department."""
    global selected_department
    try:
        if request.is_json:
            payload = request.get_json()
            val = payload.get('department')
        else:
            val = request.form.get('department')
        selected_department = val
        return {'status': 'ok', 'selected_department': selected_department}, 200
    except Exception as e:
        return {'status': 'error', 'message': str(e)}, 500


@manual_upload.route('/manual_input/fiscal_year_selection', methods=['POST'])
def manual_fiscal_year_selection():
    """Endpoint to receive Fiscal Year selection from client for manual input page and update module-level selected_fiscal_year."""
    global selected_fiscal_year
    try:
        if request.is_json:
            payload = request.get_json()
            val = payload.get('fiscal_year')
        else:
            val = request.form.get('fiscal_year')
        selected_fiscal_year = val
        return {'status': 'ok', 'selected_fiscal_year': selected_fiscal_year}, 200
    except Exception as e:
        return {'status': 'error', 'message': str(e)}, 500


@manual_upload.route('/manual_input/departments', methods=['GET'])
def manual_departments():
    """Return departments filtered by the provided PO name (query param 'po')
    or by the current module-level selected_po if no query param is provided.
    Response JSON: {"departments": [{id, name, po_id}, ...]}
    """
    po_name = request.args.get('po') or selected_po
    # Hierarchy: if no PO selected, department options should be empty
    if not po_name or str(po_name).strip() == '' or str(po_name) == 'All':
        return jsonify({'departments': []}), 200
    try:
        db = connect_local()
        # No need for SQLAlchemy engine here
        cursor, cnxn = db.connect_to_db()

        # Load POs and resolve po_name -> po_id
        pos_df = select_all_from_table(cursor, cnxn, 'pos')
        po_id = None
        if pos_df is not None and po_name:
            # Choose name column
            name_col = 'name' if 'name' in pos_df.columns else ('Name' if 'Name' in pos_df.columns else None)
            if name_col:
                mask = pos_df[name_col].astype(str).str.strip().str.lower() == str(po_name).strip().lower()
                match = pos_df[mask]
                if not match.empty and 'id' in match.columns and pd.notna(match.iloc[0]['id']):
                    try:
                        po_id = int(match.iloc[0]['id'])
                    except Exception:
                        po_id = None

        # Load departments
        depts_df = select_all_from_table(cursor, cnxn, 'departments')
        result = []
        if depts_df is not None:
            df = depts_df
            # If po_id resolved, filter rows by any possible column variant
            if po_id is not None:
                possible_cols = [c for c in ('po_id', 'PO_id', 'poId', 'po', 'PO') if c in df.columns]
                if possible_cols:
                    mask_total = None
                    for c in possible_cols:
                        try:
                            coerced = pd.to_numeric(df[c], errors='coerce')
                        except Exception:
                            coerced = pd.Series([pd.NA] * len(df))
                        mask_c = (coerced == po_id)
                        mask_total = mask_c if mask_total is None else (mask_total | mask_c)
                    if mask_total is not None:
                        df = df[mask_total.fillna(False)]
            # Build output list
            for _, r in df.iterrows():
                name = None
                if 'name' in r.index and pd.notna(r['name']):
                    name = r['name']
                elif 'Department' in r.index and pd.notna(r['Department']):
                    name = r['Department']
                elif 'Name' in r.index and pd.notna(r['Name']):
                    name = r['Name']

                dept_id = None
                if 'id' in r.index and pd.notna(r['id']):
                    try:
                        dept_id = int(r['id'])
                    except Exception:
                        dept_id = None

                d_po_id = None
                for key in ('po_id', 'PO_id', 'poId', 'po', 'PO'):
                    if key in r.index and pd.notna(r[key]):
                        try:
                            d_po_id = int(r[key])
                        except Exception:
                            try:
                                d_po_id = int(str(r[key]))
                            except Exception:
                                d_po_id = None
                        break

                result.append({'id': dept_id, 'name': name, 'po_id': d_po_id})

        return jsonify({'departments': result}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        try:
            cursor.close()
            cnxn.close()
        except Exception:
            pass


@manual_upload.route('/manual_input/projects', methods=['GET'])
def manual_projects():
    """Return projects filtered by provided query params: po (name), department (name), fiscal_year.
    If a param is not provided, fall back to module-level selected_* variables.
    Response JSON: {'projects': [{'name': <str>} , ...]}
    """
    print("projects route")
    po_name = request.args.get('po') or selected_po
    dept_name = request.args.get('department') or selected_department
    fy = request.args.get('fiscal_year') or selected_fiscal_year
    try:
        proj_df = get_projects_display()
    except Exception:
        proj_df = None
    # Hierarchy: if no fiscal year selected, project options should be empty
    if not fy or str(fy).strip() == '' or str(fy) == 'All':
        return jsonify({'projects': []}), 200
    result = []
    if proj_df is None:
        return jsonify({'projects': []}), 200
    try:
        raw = proj_df.to_dict(orient='records')
    except Exception:
        raw = []
    try:
        # Use same normalization/filtering approach as data_summary.data_summary
        for p in raw:
            # normalize project name candidates (include project_name)
            proj_name = p.get('project_name') or p.get('Project') or p.get('project') or p.get('name')
            # normalize PO name candidates
            po_val = p.get('po_name') or p.get('PO Name') or p.get('PO') or p.get('po')
            # normalize department name candidates
            dept_val = p.get('department_name') or p.get('Department Name') or p.get('Department') or p.get('department')
            # normalize fiscal year candidates
            fy_val = p.get('fiscal_year') or p.get('Fiscal Year') or p.get('fy') or p.get('fiscal')

            if not proj_name:
                continue

            ok = True
            if po_name and po_name != '' and po_name != 'All':
                ok = ok and (po_val == po_name)
            if dept_name and dept_name != '' and dept_name != 'All':
                ok = ok and (dept_val == dept_name)
            if fy and fy != '' and fy != 'All':
                ok = ok and (str(fy_val) == str(fy))
            if ok:
                result.append({'name': proj_name})
    except Exception:
        result = []
    # deduplicate preserving order
    seen = set()
    deduped = []
    for r in result:
        n = r.get('name')
        if n not in seen:
            seen.add(n)
            deduped.append({'name': n})
    print(result)
    print(deduped)
    return jsonify({'projects': deduped}), 200


@manual_upload.route('/manual_input/hr_categories', methods=['GET'])
def manual_hr_categories():
    """Return ALL human resource categories without filtering by PO.
    Response JSON: {"human_resource_categories": [<str>, ...]}
    """
    try:
        df = get_hr_category_display()
    except Exception as e:
        return jsonify({'human_resource_categories': [], 'error': str(e)}), 500

    if df is None or getattr(df, 'empty', True):
        return jsonify({'human_resource_categories': []}), 200

    # Try to find the HR category column with several fallbacks
    def find_hr_col(cols):
        candidates = ['Human Resource Category', 'Human resource category', 'Staff Category', 'Staff category', 'name', 'Category']
        for cand in candidates:
            for c in cols:
                if c.lower() == cand.lower():
                    return c
        return None

    hr_col = find_hr_col(list(df.columns))

    cats = []
    if hr_col:
        try:
            values = df[hr_col].dropna().astype(str).str.strip().tolist()
        except Exception:
            try:
                values = [str(v).strip() for v in list(df[hr_col]) if v is not None]
            except Exception:
                values = []
        seen = set()
        for v in values:
            if v and v not in seen:
                seen.add(v)
                cats.append(v)
    else:
        # Column not found; try record-wise fallbacks
        try:
            records = df.to_dict(orient='records')
        except Exception:
            records = []
        seen = set()
        for r in records:
            h = r.get('Human Resource Category') or r.get('Human resource category') or r.get('Staff Category') or r.get('Staff category') or r.get('name') or r.get('Category')
            if not h:
                continue
            hv = str(h).strip()
            if hv and hv not in seen:
                seen.add(hv)
                cats.append(hv)

    return jsonify({'human_resource_categories': cats}), 200


@manual_upload.route('/manual_input/fiscal_years', methods=['GET'])
def manual_fiscal_years():
    """Return fiscal year options filtered by provided query params: po (name), department (name).
    If department is not provided, return empty list to enforce hierarchy.
    Response JSON: {'fiscal_years': ['2022', '2023', ...]}
    """
    po_name = request.args.get('po') or selected_po
    dept_name = request.args.get('department') or selected_department
    # If no department selected, fiscal year options should be empty
    if not dept_name or str(dept_name).strip() == '' or str(dept_name) == 'All':
        return jsonify({'fiscal_years': []}), 200

    years = set()
    
    years_list = sorted(list(years))
    return jsonify({'fiscal_years': years_list}), 200


@manual_upload.route('/manual_input/project_selection', methods=['POST'])
def manual_project_selection():
    """Receive Project selection from client for manual input page and update module-level selected_project."""
    global selected_project
    try:
        if request.is_json:
            payload = request.get_json()
            val = payload.get('project')
        else:
            val = request.form.get('project')
        selected_project = val
        return {'status': 'ok', 'selected_project': selected_project}, 200
    except Exception as e:
        return {'status': 'error', 'message': str(e)}, 500


@manual_upload.route('/manual_input/project_categories', methods=['GET'])
def manual_project_categories():
    """Return project categories filtered by provided query param 'project' or by module-level selected_project.
    Response JSON: {'project_categories': [<str>, ...]}
    """
    proj_name = request.args.get('project') or selected_project
    # If no project selected, return empty list
    if not proj_name or str(proj_name).strip() == '' or str(proj_name) == 'All':
        return jsonify({'project_categories': []}), 200

    try:
        # get full project-category display table from backend helper
        try:
            pc_df = get_project_cateogory_display()
        except Exception:
            pc_df = None

        if pc_df is None:
            return jsonify({'project_categories': []}), 200

        # normalize column names and filter by project name defensively
        try:
            records = pc_df.to_dict(orient='records')
        except Exception:
            records = []

        cats = []
        seen = set()
        for r in records:
            # possible project name columns
            name_val = r.get('project_name') or r.get('Project') or r.get('project') or r.get('name')
            # possible category columns
            cat_val = r.get('category') or r.get('Project Category') or r.get('project_category') or r.get('Category')
            if not name_val or not cat_val:
                continue
            try:
                if str(name_val).strip() == str(proj_name).strip():
                    cv = str(cat_val).strip()
                    if cv not in seen:
                        seen.add(cv)
                        cats.append(cv)
            except Exception:
                continue

        return jsonify({'project_categories': cats}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@manual_upload.route('/manual_input/ios', methods=['GET'])
def manual_ios():
    """Return IO options filtered by provided query param 'project' or by module-level selected_project.
    Response JSON: {'ios': [<int or str>, ...]}
    """
    proj_name = request.args.get('project') or selected_project
    try:
        io_df = get_IO_display_table()
    except Exception:
        io_df = None

    if io_df is None or io_df.empty:
        return jsonify({'ios': []}), 200

    try:
        records = io_df.to_dict(orient='records')
    except Exception:
        records = []

    ios = []
    seen = set()
    for r in records:
        io_val = r.get('IO') or r.get('IO_num') or r.get('io') or r.get('io_num')
        proj_val = r.get('project_name') or r.get('Project') or r.get('project') or r.get('name')

        if proj_name and str(proj_name).strip() not in ('', 'All'):
            try:
                if not proj_val or str(proj_val).strip() != str(proj_name).strip():
                    continue
            except Exception:
                continue

        if io_val is None:
            continue
        try:
            cand = int(io_val)
        except Exception:
            cand = str(io_val).strip()
        if cand not in seen:
            seen.add(cand)
            ios.append(cand)

    return jsonify({'ios': ios}), 200


@manual_upload.route("/upload_forecast_nonpc", methods=['POST'])
def upload_forecast_nonpc_r():
    df = pd.DataFrame(columns = ["PO","IO","Department","Project Category","Project Name","fiscal_year","Non-personnel cost"])
    row = {}
    for fieldname, value in request.form.items():
        # print(fieldname, value)
        row[fieldname] = value
    df.loc[len(df)] = row
    df["IO"] = df["IO"].astype(int)
    try:
        columns_changed = upload_nonpc_forecasts_local_m(df)
        if columns_changed > 0:
            return "Upload successful"
        else:
            return "Value already exist"
    except:
        return "Upload failed. Please check your input values."

    # return "Upload successful"

@manual_upload.route("/upload_forecast_pc", methods=['POST'])
def upload_forecast_pc_r():
    df = pd.DataFrame(columns=upload_columns["forecast_pc"])
    row = {}
    for fieldname, value in request.form.items():
        # print(fieldname, value)
        row[fieldname] = value
    df.loc[len(df)] = row
    df["IO"] = df["IO"].astype(int)
    # project_forecasts_pc table does not store personnel_expense - drop Personnel cost before upload
    if 'Personnel cost' in df.columns:
        df = df.drop(columns=['Personnel cost'])
    try:
        columns_changed = upload_pc_forecasts_local_m(df)
        if columns_changed > 0:
            return "Upload successful"
        else:
            return "Value already exist"
    except:
        return "Upload failed. Please check your input values."
    # return "Upload successful"

@manual_upload.route("/upload_budgets", methods=['POST'])
def upload_budgets_r():
    df = pd.DataFrame(columns=upload_columns["budgets"])
    row = {}
    for fieldname, value in request.form.items():
        # print(fieldname, value)
        row[fieldname] = value
    df.loc[len(df)] = row
    print(df)
    res = upload_budgets_local_m(df)
    return "Upload successful"

@manual_upload.route("/upload_expenses", methods=['POST'])
def upload_expenses_r():
    df = pd.DataFrame(columns=upload_columns["expenses"])
    row = {}
    for fieldname, value in request.form.items():
        # print(fieldname, value)
        row[fieldname] = value
    df.loc[len(df)] = row
    # try:
    res = upload_expenses_local(df)
    return "successful"

@manual_upload.route("/upload_fundings", methods=['POST'])
def upload_fundings_r():
    df = pd.DataFrame(columns=upload_columns["fundings"])
    row = {}
    for fieldname, value in request.form.items():
        # print(fieldname, value)
        row[fieldname] = value
    df.loc[len(df)] = row
    # try:
    res = upload_fundings_local(df)
    return "successful"

@manual_upload.route("/upload_capex_forecast", methods=['POST'])
def upload_capex_forecast_r():
    df = pd.DataFrame(columns=upload_columns["capex_forecast"])
    row = {}
    for fieldname, value in request.form.items():
        # print(fieldname, value)
        row[fieldname] = value
    df.loc[len(df)] = row
    print(df)
    res = upload_capex_forecast_m(df)
    return "Upload successful"

@manual_upload.route("/upload_capex_budget", methods=['POST'])
def upload_capex_budget_r():
    df = pd.DataFrame(columns=upload_columns["capex_budget"])
    row = {}
    for fieldname, value in request.form.items():
        # print(fieldname, value)
        row[fieldname] = value
    df.loc[len(df)] = row
    print(df)
    res = upload_capex_budgets_local_m(df)
    return "Upload successful"

@manual_upload.route("/upload_capex_expense", methods=['POST'])
def upload_capex_expense_r():
    df = pd.DataFrame(columns=upload_columns["capex_expense"])
    row = {}
    for fieldname, value in request.form.items():
        # print(fieldname, value)
        row[fieldname] = value
    df.loc[len(df)] = row
    # try:
    res = upload_capex_expense_local(df, clear=False)
    return "successful"


@manual_upload.route('/manual_input/change_forecast', methods=['POST'])
def manual_change_forecast():
    """Modify existing forecasts:
    - Update project_forecasts_nonpc.non_personnel_expense when Non_personnel_cost is provided
    - Update project_forecasts_pc.human_resource_fte when Human_resource_FTE is provided
    Matching keys: PO, Department, fiscal_year, Project_Name, Project_Category, IO
    For personnel, also match Human_resource_category.
    """
    form = request.form
    po_name = form.get('PO')
    dept_name = form.get('Department')
    fiscal_year = form.get('fiscal_year')
    project_name = form.get('Project_Name')
    project_category = form.get('Project_Category')
    io_value = form.get('IO')
    # Legacy single-category fields removed from UI; still read for backward compatibility
    hr_category = form.get('Human_resource_category')
    fte = form.get('Human_resource_FTE')
    nonpc = form.get('Non_personnel_cost')

    try:
        conn = connect_local()
        cursor, cnxn = conn.connect_to_db()

        # Resolve IDs
        pos_df = select_all_from_table(cursor, cnxn, 'pos')
        depts_df = select_all_from_table(cursor, cnxn, 'departments')
        projs_df = select_all_from_table(cursor, cnxn, 'projects')
        pcats_df = select_all_from_table(cursor, cnxn, 'project_categories')
        ios_df = select_all_from_table(cursor, cnxn, 'IOs')
        hrc_df = select_all_from_table(cursor, cnxn, 'human_resource_categories')

        po_id = None
        if pos_df is not None and 'name' in pos_df.columns and 'id' in pos_df.columns:
            po_id = dict(zip(pos_df['name'], pos_df['id'])).get(po_name)

        dept_id = None
        if depts_df is not None and 'name' in depts_df.columns and 'id' in depts_df.columns:
            dept_id = dict(zip(depts_df['name'], depts_df['id'])).get(dept_name)

        # Filter projects by name (and optionally department/fiscal_year if duplicates exist)
        proj_id = None
        if projs_df is not None:
            try:
                dfp = projs_df
                if 'name' in dfp.columns:
                    mask = dfp['name'].astype(str) == str(project_name)
                    if dept_id is not None and 'department_id' in dfp.columns:
                        mask &= (dfp['department_id'] == int(dept_id))
                    if fiscal_year and 'fiscal_year' in dfp.columns:
                        try:
                            mask &= (dfp['fiscal_year'].astype(str) == str(int(fiscal_year)))
                        except Exception:
                            mask &= (dfp['fiscal_year'].astype(str) == str(fiscal_year))
                    rows = dfp[mask]
                    if not rows.empty and 'id' in rows.columns:
                        proj_id = rows.iloc[0]['id']
            except Exception:
                proj_id = None

        # Project category id
        pc_id = None
        if pcats_df is not None and 'category' in pcats_df.columns and 'id' in pcats_df.columns:
            pc_id = dict(zip(pcats_df['category'], pcats_df['id'])).get(project_category)

        # IO id by IO_num and project_id when possible
        io_id = None
        if ios_df is not None and 'IO_num' in ios_df.columns and 'id' in ios_df.columns:
            try:
                io_num_val = int(io_value) if io_value not in (None, '') else None
            except Exception:
                io_num_val = None
            if io_num_val is not None:
                try:
                    dfi = ios_df[ios_df['IO_num'].astype(int) == int(io_num_val)]
                except Exception:
                    dfi = ios_df[ios_df['IO_num'].astype(str) == str(io_num_val)]
                if proj_id is not None and 'project_id' in ios_df.columns:
                    try:
                        dfi = dfi[dfi['project_id'] == int(proj_id)]
                    except Exception:
                        pass
                if not dfi.empty:
                    io_id = dfi.iloc[0]['id']

        # Helper to infer IO when it's not provided in the form (IO removed from UI)
        def infer_io_id_for_category(cat_id_local):
            try:
                if None in (po_id, dept_id, proj_id, pc_id, fy_val) or cat_id_local is None:
                    return None
                # Find distinct io_id values for the matching key in project_forecasts_pc
                cursor.execute(
                    """
                    SELECT DISTINCT io_id
                      FROM project_forecasts_pc
                     WHERE PO_id = ? AND department_id = ? AND project_id = ?
                       AND project_category_id = ? AND fiscal_year = ? AND human_resource_category_id = ?
                    """,
                    (int(po_id), int(dept_id), int(proj_id), int(pc_id), int(fy_val), int(cat_id_local))
                )
                rows = cursor.fetchall()
                if not rows:
                    return None
                # If exactly one IO matches, use it; otherwise ambiguous
                distinct_ios = list({r[0] for r in rows if r and r[0] is not None})
                if len(distinct_ios) == 1:
                    return distinct_ios[0]
                return None
            except Exception:
                return None

        # HR category id
        hr_cat_id = None
        if hrc_df is not None and 'name' in hrc_df.columns and 'id' in hrc_df.columns:
            try:
                # exact match
                hr_cat_id = dict(zip(hrc_df['name'], hrc_df['id'])).get(hr_category)
                if hr_cat_id is None and hr_category:
                    # case-insensitive fallback
                    name_lower_map = {str(n).strip().lower(): i for n, i in zip(hrc_df['name'], hrc_df['id'])}
                    hr_cat_id = name_lower_map.get(str(hr_category).strip().lower())
            except Exception:
                hr_cat_id = None

        # Coerce numbers
        fy_val = None
        try:
            fy_val = int(fiscal_year) if fiscal_year not in (None, '') else None
        except Exception:
            fy_val = None
        fte_val = None
        try:
            fte_val = float(fte) if fte not in (None, '') else None
        except Exception:
            fte_val = None
        nonpc_val = None
        try:
            nonpc_val = float(nonpc) if nonpc not in (None, '') else None
        except Exception:
            nonpc_val = None

        # Update non-personnel expense if base keys are available
        if all(v is not None for v in (po_id, dept_id, proj_id, pc_id, fy_val)) and nonpc_val is not None:
            try:
                if io_id is not None:
                    cursor.execute(
                        """
                        UPDATE project_forecasts_nonpc
                           SET non_personnel_expense = ?
                         WHERE PO_id = ? AND department_id = ? AND project_id = ?
                           AND io_id = ? AND project_category_id = ? AND fiscal_year = ?
                        """,
                        (nonpc_val, int(po_id), int(dept_id), int(proj_id), int(io_id), int(pc_id), int(fy_val))
                    )
                else:
                    # IO not specified/known: update all matching rows across IOs for this key
                    cursor.execute(
                        """
                        UPDATE project_forecasts_nonpc
                           SET non_personnel_expense = ?
                         WHERE PO_id = ? AND department_id = ? AND project_id = ?
                           AND project_category_id = ? AND fiscal_year = ?
                        """,
                        (nonpc_val, int(po_id), int(dept_id), int(proj_id), int(pc_id), int(fy_val))
                    )
            except Exception:
                pass

        # Multi-category personnel updates: support forward (FTE->cost) and reverse (cost->FTE)
        total_personnel_cost = 0.0
        def hierarchical_unit_cost(cat_name_local):
            if not cat_name_local or fy_val is None:
                return None
            try:
                cursor.execute("SELECT id FROM human_resource_categories WHERE name = ?", (cat_name_local,))
                rr = cursor.fetchone(); cat_id_lookup = rr[0] if rr else None
            except Exception:
                cat_id_lookup = None
            def fetch(q, params):
                try:
                    cursor.execute(q, params)
                    fr = cursor.fetchone(); return fr[0] if fr else None
                except Exception:
                    return None
            cost_local = None
            if cost_local is None and all(v is not None for v in (po_id, dept_id, cat_id_lookup, fy_val)):
                cost_local = fetch("SELECT cost FROM human_resource_cost WHERE po_id = ? AND department_id = ? AND category_id = ? AND year = ? LIMIT 1", (int(po_id), int(dept_id), int(cat_id_lookup), int(fy_val)))
            if cost_local is None and all(v is not None for v in (dept_id, cat_id_lookup, fy_val)):
                cost_local = fetch("SELECT cost FROM human_resource_cost WHERE department_id = ? AND category_id = ? AND year = ? LIMIT 1", (int(dept_id), int(cat_id_lookup), int(fy_val)))
            if cost_local is None and all(v is not None for v in (po_id, cat_id_lookup, fy_val)):
                cost_local = fetch("SELECT cost FROM human_resource_cost WHERE po_id = ? AND category_id = ? AND year = ? LIMIT 1", (int(po_id), int(cat_id_lookup), int(fy_val)))
            if cost_local is None and all(v is not None for v in (cat_id_lookup, fy_val)):
                cost_local = fetch("SELECT cost FROM human_resource_cost WHERE category_id = ? AND year = ? LIMIT 1", (int(cat_id_lookup), int(fy_val)))
            if cost_local is None:
                cost_local = fetch("SELECT cost FROM human_resource_cost WHERE category_id = ? AND year = ? LIMIT 1", (cat_name_local, int(fy_val)))
            return cost_local
        for key in form.keys():
            # Accept either fte__slug (forward) or cost__slug (reverse)
            if key.startswith('fte__') or key.startswith('cost__'):
                reverse_mode = key.startswith('cost__')
                slug = key.split('__',1)[1]
                fte_key = 'fte__' + slug
                cost_key = 'cost__' + slug
                cat_key = 'cat__' + slug
                cat_name = form.get(cat_key)
                if not cat_name:
                    continue
                unit_rate = hierarchical_unit_cost(cat_name)
                # Parse provided values
                fte_raw = form.get(fte_key)
                cost_raw = form.get(cost_key)
                fte_local = None
                cost_local = None
                try:
                    if fte_raw not in (None, ''):
                        fte_local = float(fte_raw)
                except Exception:
                    fte_local = None
                try:
                    if cost_raw not in (None, ''):
                        cost_local = float(cost_raw)
                except Exception:
                    cost_local = None
                # Determine operation direction
                if reverse_mode and cost_local is not None and unit_rate and unit_rate != 0:
                    # Derive FTE from cost
                    fte_local = cost_local / unit_rate
                elif not reverse_mode and fte_local is not None and unit_rate is not None:
                    cost_local = unit_rate * fte_local
                # Accumulate total personnel cost
                if cost_local is not None:
                    total_personnel_cost += cost_local
                # Resolve hr category id for update
                hr_cat_id_local = None
                if hrc_df is not None and 'name' in hrc_df.columns and 'id' in hrc_df.columns:
                    try:
                        hr_cat_id_local = dict(zip(hrc_df['name'], hrc_df['id'])).get(cat_name)
                        if hr_cat_id_local is None:
                            name_lower_map = {str(n).strip().lower(): i for n, i in zip(hrc_df['name'], hrc_df['id'])}
                            hr_cat_id_local = name_lower_map.get(str(cat_name).strip().lower())
                    except Exception:
                        hr_cat_id_local = None
                # Determine IO for update
                candidate_io_id = io_id if io_id is not None else infer_io_id_for_category(hr_cat_id_local)
                if fte_local is not None and all(v is not None for v in (po_id, dept_id, proj_id, candidate_io_id, pc_id, fy_val, hr_cat_id_local)):
                    try:
                        cursor.execute(
                            """
                            UPDATE project_forecasts_pc
                               SET human_resource_fte = ?
                             WHERE PO_id = ? AND department_id = ? AND project_id = ? AND io_id = ?
                               AND project_category_id = ? AND fiscal_year = ? AND human_resource_category_id = ?
                            """,
                            (fte_local, int(po_id), int(dept_id), int(proj_id), int(candidate_io_id), int(pc_id), int(fy_val), int(hr_cat_id_local))
                        )
                    except Exception:
                        pass

        try:
            cnxn.commit()
        except Exception:
            pass
        # Flash a summary of personnel cost change for visibility (optional)
        try:
            flash(f"Personnel cost (calculated): {total_personnel_cost:.2f}", 'info')
        except Exception:
            pass
    except Exception:
        # swallow and continue to redirect
        pass

    # Redirect back to manual input page so the tables refresh
    return redirect(url_for('manual_upload.render_mannual_input'))


@manual_upload.route("/upload_forecast", methods=['POST'])
def upload_forecast_merged():
    form = request.form
    results = []

    # Shared keys
    base_context = {
        "PO": form.get("PO"),
        "IO": form.get("IO"),
        "Department": form.get("Department"),
        "Project Category": form.get("Project_Category"),
        "Project Name": form.get("Project_Name"),
        "fiscal_year": form.get("fiscal_year")
    }

    # Non-personnel upload (single value)
    nonpc_val = form.get("Non_personnel_cost")
    if nonpc_val not in (None, ''):
        nonpc_row = dict(base_context)
        nonpc_row["Non-personnel cost"] = nonpc_val
        df_nonpc = pd.DataFrame([nonpc_row])
        try:
            df_nonpc["IO"] = df_nonpc["IO"].astype(int)
        except Exception:
            pass
        try:
            changed = upload_nonpc_forecasts_local_m(df_nonpc)
            if changed > 0:
                results.append("Non-personnel forecast uploaded successfully.")
            else:
                results.append("Non-personnel forecast already exists.")
        except Exception as e:
            results.append(f"Non-personnel upload failed: {e}")

    # Personnel uploads (multiple categories)
    # Expect pairs: cat__<slug>=<Category Name>, fte__<slug>=<FTE>
    pc_rows = []
    for key in form.keys():
        if key.startswith('fte__'):
            slug = key[len('fte__'):]
            fte_raw = form.get(key)
            cat_key = 'cat__' + slug
            category_name = form.get(cat_key)
            if category_name is None:
                continue
            # Skip empty or zero FTE values
            try:
                fte_val = float(fte_raw)
            except Exception:
                fte_val = 0.0
            if fte_val == 0.0:
                continue
            pc_row = dict(base_context)
            pc_row['Human resource category'] = category_name
            pc_row['Human resource FTE'] = fte_val
            # Personnel cost not stored; omit
            pc_rows.append(pc_row)

    if pc_rows:
        df_pc = pd.DataFrame(pc_rows)
        try:
            df_pc['IO'] = df_pc['IO'].astype(int)
        except Exception:
            pass
        try:
            from backend.upload_forecasts_pc import upload_pc_forecasts_local_m
            changed = upload_pc_forecasts_local_m(df_pc)
            if changed > 0:
                results.append(f"{len(pc_rows)} personnel forecast row(s) uploaded successfully.")
            else:
                results.append("Personnel forecast row(s) already exist.")
        except Exception as e:
            results.append(f"Personnel upload failed: {e}")

    if not results:
        try:
            flash('No forecast data provided. Please fill at least one forecast field.', 'warning')
        except Exception:
            pass
        return redirect(url_for('manual_upload.render_mannual_input'))

    try:
        for msg in results:
            flash(msg, 'success')
    except Exception:
        pass
    return redirect(url_for('manual_upload.render_mannual_input'))


@manual_upload.route('/manual_input/delete_forecast', methods=['POST'])
def manual_delete_forecast():
    """Delete forecast rows (both non-personnel and personnel) matching the visible composite key.
    Expected JSON body with the following visible keys from the display table:
      {
        "PO": str,
        "Department": str,
        "Project": str | null,          # either Project or Project Name will be provided
        "Project Name": str | null,
        "Project Category": str,
        "Fiscal Year": int | str
      }
    Notes:
    - IO is intentionally NOT required (and removed from display). This endpoint deletes all matching rows across IOs.
    - Personnel deletion removes all HR categories for the matching key.
    Returns JSON: { status, deleted_nonpc, deleted_pc }
    """
    try:
        payload = request.get_json(silent=True) or {}
        po_name = payload.get('PO')
        dept_name = payload.get('Department')
        # project name could be under 'Project' or 'Project Name'
        project_name = payload.get('Project') or payload.get('Project Name')
        project_category = payload.get('Project Category')
        fiscal_year = payload.get('Fiscal Year') or payload.get('fiscal_year')

        if not all([po_name, dept_name, project_name, project_category, fiscal_year]):
            return jsonify({
                'status': 'error',
                'message': 'Missing required fields (PO, Department, Project, Project Category, Fiscal Year)'
            }), 400

        # Coerce FY to int when possible
        try:
            fy_val = int(fiscal_year)
        except Exception:
            return jsonify({'status': 'error', 'message': 'Invalid Fiscal Year'}), 400

        conn = connect_local()
        cursor, cnxn = conn.connect_to_db()

        # Resolve IDs robustly
        pos_df = select_all_from_table(cursor, cnxn, 'pos')
        depts_df = select_all_from_table(cursor, cnxn, 'departments')
        projs_df = select_all_from_table(cursor, cnxn, 'projects')
        pcats_df = select_all_from_table(cursor, cnxn, 'project_categories')

        po_id = None
        if pos_df is not None:
            name_col = 'name' if 'name' in pos_df.columns else ('Name' if 'Name' in pos_df.columns else None)
            if name_col and 'id' in pos_df.columns:
                try:
                    match = pos_df[pos_df[name_col].astype(str).str.strip().str.lower() == str(po_name).strip().lower()]
                    if not match.empty:
                        po_id = int(match.iloc[0]['id'])
                except Exception:
                    po_id = None

        dept_id = None
        if depts_df is not None and 'name' in depts_df.columns and 'id' in depts_df.columns:
            try:
                match = depts_df[depts_df['name'].astype(str).str.strip().str.lower() == str(dept_name).strip().lower()]
                if not match.empty:
                    dept_id = int(match.iloc[0]['id'])
            except Exception:
                dept_id = None

        proj_id = None
        if projs_df is not None:
            try:
                dfp = projs_df
                if 'name' in dfp.columns:
                    mask = dfp['name'].astype(str).str.strip().str.lower() == str(project_name).strip().lower()
                    # If duplicates exist, prefer those matching department and/or fiscal year if such columns exist
                    if dept_id is not None and 'department_id' in dfp.columns:
                        mask &= (dfp['department_id'] == int(dept_id))
                    if 'fiscal_year' in dfp.columns:
                        try:
                            mask &= (dfp['fiscal_year'].astype(int) == int(fy_val))
                        except Exception:
                            mask &= (dfp['fiscal_year'].astype(str) == str(fy_val))
                    rows = dfp[mask]
                    if not rows.empty and 'id' in rows.columns:
                        proj_id = int(rows.iloc[0]['id'])
            except Exception:
                proj_id = None

        pc_id = None
        if pcats_df is not None and 'category' in pcats_df.columns and 'id' in pcats_df.columns:
            try:
                match = pcats_df[pcats_df['category'].astype(str).str.strip().str.lower() == str(project_category).strip().lower()]
                if not match.empty:
                    pc_id = int(match.iloc[0]['id'])
            except Exception:
                pc_id = None

        if None in (po_id, dept_id, proj_id, pc_id):
            return jsonify({'status': 'error', 'message': 'Unable to resolve identifiers for deletion'}), 400

        deleted_nonpc = 0
        deleted_pc = 0
        # Delete Non-Personnel forecasts (all IOs for the key)
        try:
            cursor.execute(
                """
                DELETE FROM project_forecasts_nonpc
                 WHERE PO_id = ? AND department_id = ? AND project_id = ?
                   AND project_category_id = ? AND fiscal_year = ?
                """,
                (int(po_id), int(dept_id), int(proj_id), int(pc_id), int(fy_val))
            )
            try:
                deleted_nonpc = cursor.rowcount if cursor.rowcount is not None else 0
            except Exception:
                deleted_nonpc = 0
        except Exception:
            deleted_nonpc = 0

        # Delete Personnel forecasts (all categories and IOs for the key)
        try:
            cursor.execute(
                """
                DELETE FROM project_forecasts_pc
                 WHERE PO_id = ? AND department_id = ? AND project_id = ?
                   AND project_category_id = ? AND fiscal_year = ?
                """,
                (int(po_id), int(dept_id), int(proj_id), int(pc_id), int(fy_val))
            )
            try:
                deleted_pc = cursor.rowcount if cursor.rowcount is not None else 0
            except Exception:
                deleted_pc = 0
        except Exception:
            deleted_pc = 0

        try:
            cnxn.commit()
        except Exception:
            pass
        try:
            cursor.close(); cnxn.close()
        except Exception:
            pass

        return jsonify({'status': 'ok', 'deleted_nonpc': int(deleted_nonpc), 'deleted_pc': int(deleted_pc)}), 200
    except Exception as e:
        try:
            return jsonify({'status': 'error', 'message': str(e)}), 500
        except Exception:
            return ('', 500)