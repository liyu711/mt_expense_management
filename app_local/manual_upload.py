
from flask import Flask, flash, render_template, request, redirect, url_for, Blueprint, jsonify
from werkzeug.utils import secure_filename
from backend.login import valid_login
from backend.connect_local import connect_local, select_columns_from_table, select_all_from_table
from app_local.select_data import transform_table

from backend import \
    upload_nonpc_forecasts_local_m, upload_pc_forecasts_local_m,\
    upload_budgets_local_m, upload_expenses_local, upload_fundings_local, \
    upload_capex_forecast_m, upload_capex_budgets_local_m, upload_capex_expense_local, get_projects_display,\
    get_project_cateogory_display, get_IO_display_table
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
    "budgets": ['PO', 'Department', "Fiscal Year", "Personnel Budget", "Non-personnel Budget"],
    "fundings": ['PO', 'Department', "Fiscal Year", "Funding", "Funding From", "Funding For"],
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
                           pf_nonpc_columns=pf_nonpc_columns,
                           pf_nonpc_data=pf_nonpc_data,
                           pf_pc_columns=pf_pc_columns,
                           pf_pc_data=pf_pc_data
                           )


@manual_upload.route('/api/hr_cost', methods=['GET'])
def api_hr_cost():
    """Return the cost for a given human resource category name and year.
    Query params: category (name), year (int)
    Returns JSON: {"cost": <float>|null}
    """
    category = request.args.get('category')
    year = request.args.get('year')
    if not category or not year:
        return jsonify({'cost': None}), 200
    try:
        db = connect_local()
        cursor, cnxn = db.connect_to_db()
        # find category id
        cursor.execute("SELECT id FROM human_resource_categories WHERE name = ?", (category,))
        row = cursor.fetchone()
        if not row:
            return jsonify({'cost': None}), 200
        category_id = row[0]
        # find cost for that category and year
        try:
            cursor.execute("SELECT cost FROM human_resource_cost WHERE category_id = ? AND year = ?", (category_id, int(year)))
            r2 = cursor.fetchone()
        except Exception:
            r2 = None

        # Fallback: some rows may have category_id stored as the category name (string). Try that too.
        if not r2:
            try:
                cursor.execute("SELECT cost FROM human_resource_cost WHERE category_id = ? AND year = ?", (category, int(year)))
                r2 = cursor.fetchone()
            except Exception:
                r2 = None

        if not r2:
            return jsonify({'cost': None}), 200
        return jsonify({'cost': r2[0]}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        try:
            cursor.close()
            cnxn.close()
        except:
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


@manual_upload.route("/upload_forecast", methods=['POST'])
def upload_forecast_merged():
    # Extract form data
    form = request.form
    # Prepare for both forecast types
    # Non-personnel
    nonpc_fields = ["PO","IO","Department","Project_Category","Project_Name","fiscal_year","Non_personnel_cost"]
    nonpc_row = {
        "PO": form.get("PO"),
        "IO": form.get("IO"),
        "Department": form.get("Department"),
        "Project Category": form.get("Project_Category"),
        "Project Name": form.get("Project_Name"),
        "fiscal_year": form.get("fiscal_year"),
        "Non-personnel cost": form.get("Non_personnel_cost")
    }
    # Personnel
    pc_fields = ["PO","IO","Department","Project_Category","Project_Name","fiscal_year","Human_resource_category","Human_resource_FTE","Personnel_cost"]
    pc_row = {
        "PO": form.get("PO"),
        "IO": form.get("IO"),
        "Department": form.get("Department"),
        "Project Category": form.get("Project_Category"),
        "Project Name": form.get("Project_Name"),
        "fiscal_year": form.get("fiscal_year"),
        "Human resource category": form.get("Human_resource_category"),
        "Human resource FTE": form.get("Human_resource_FTE"),
        "Personnel cost": form.get("Personnel_cost")
    }
    # Track results
    results = []
    # Only upload if at least one relevant field is filled
    # Non-personnel
    if form.get("Non_personnel_cost"):
        df_nonpc = pd.DataFrame([nonpc_row])
        print(df_nonpc)
        try:
            df_nonpc["IO"] = df_nonpc["IO"].astype(int)
        except:
            pass
        # try:
            # from backend.upload_forecasts_nonpc import upload_nonpc_forecasts_local_m
        changed = upload_nonpc_forecasts_local_m(df_nonpc)
        if changed > 0:
            results.append("Non-personnel forecast uploaded successfully.")
        else:
            results.append("Non-personnel forecast already exists.")
        # except Exception as e:
        #     results.append(f"Non-personnel upload failed: {e}")
    # Personnel
    if form.get("Personnel_cost"):
        df_pc = pd.DataFrame([pc_row])
        try:
            df_pc["IO"] = df_pc["IO"].astype(int)
        except:
            pass
        # project_forecasts_pc table does not store personnel_expense - drop Personnel cost before upload
        if 'Personnel cost' in df_pc.columns:
            df_pc = df_pc.drop(columns=['Personnel cost'])
        try:
            from backend.upload_forecasts_pc import upload_pc_forecasts_local_m
            changed = upload_pc_forecasts_local_m(df_pc)
            if changed > 0:
                results.append("Personnel forecast uploaded successfully.")
            else:
                results.append("Personnel forecast already exists.")
        except Exception as e:
            results.append(f"Personnel upload failed: {e}")
    if not results:
        # No data provided: flash a message and redirect back to the manual input page
        try:
            flash('No forecast data provided. Please fill at least one forecast field.', 'warning')
        except Exception:
            pass
        return redirect(url_for('manual_upload.render_mannual_input'))

    # On success, flash each result message and redirect back to the manual input page
    try:
        for msg in results:
            flash(msg, 'success')
    except Exception:
        pass
    return redirect(url_for('manual_upload.render_mannual_input'))