from flask import render_template, request, Blueprint
from backend.connect_local import connect_local, select_all_from_table
from backend.display_names import DISPLAY_NAMES
import pandas as pd

data_vis = Blueprint('data_vis', __name__, template_folder='templates')


@data_vis.route('/data_vis', methods=['GET', 'POST'])
def data_vis_page():
    conn = connect_local()
    data = []
    columns = []
    selected_option = None
    x_col = ''
    y_col = ''
    plot_type = 'bar'

    options = [
        'projects', 'departments', 'POs', 'cost_elements', 'budgets', 'expenses', 'fundings',
        'project_categories', 'co_object_names', 'IOs', 'IO_CE_connection', 'human_resource_categories',
        'human_resource_expense', 'project_forecasts_nonpc', 'project_forecasts_pc',
        'capex_forecasts', 'capex_budgets', 'capex_expenses'
    ]

    if request.method == 'POST':
        selected_option = request.form.get('table_name')
        x_col = request.form.get('x_col')
        y_col = request.form.get('y_col')
        plot_type = request.form.get('plot_type')
        if selected_option:
            cursor, cnxn = conn.connect_to_db()
            df = select_all_from_table(cursor, cnxn, selected_option)
            # same id -> name mapping logic as select_data
            id_name_map = {
                'department_id': ('departments', 'id', 'name', 'Department'),
                'po_id': ('POs', 'id', 'name', 'PO'),
                'PO_id': ('POs', 'id', 'name', 'PO'),
                'project_id': ('projects', 'id', 'name', 'Project'),
                'io_id': ('IOs', 'id', 'IO_num', 'IO'),
                'project_category_id': ('project_categories', 'id', 'category', 'Project Category'),
            }
            for id_col, (ref_table, ref_id, ref_name, new_col_name) in id_name_map.items():
                if id_col in df.columns:
                    ref_df = select_all_from_table(cursor, cnxn, ref_table)
                    ref_dict = dict(zip(ref_df[ref_id], ref_df[ref_name]))
                    df[new_col_name] = df[id_col].map(ref_dict)
            drop_cols = [col for col in ['department_id', 'po_id', 'PO_id', 'project_id', 'io_id', 'project_category_id'] if col in df.columns]
            df = df.drop(columns=drop_cols)
            columns = df.columns.tolist() if df is not None else []
            data = df.values.tolist() if df is not None else []

    else:
        selected_option = request.args.get('table_name')
        if selected_option:
            cursor, cnxn = conn.connect_to_db()
            df = select_all_from_table(cursor, cnxn, selected_option)
            id_name_map = {
                'department_id': ('departments', 'id', 'name', 'Department'),
                'po_id': ('POs', 'id', 'name', 'PO'),
                'PO_id': ('POs', 'id', 'name', 'PO'),
                'project_id': ('projects', 'id', 'name', 'Project'),
                'io_id': ('IOs', 'id', 'IO_num', 'IO'),
                'project_category_id': ('project_categories', 'id', 'category', 'Project Category'),
            }
            for id_col, (ref_table, ref_id, ref_name, new_col_name) in id_name_map.items():
                if id_col in df.columns:
                    ref_df = select_all_from_table(cursor, cnxn, ref_table)
                    ref_dict = dict(zip(ref_df[ref_id], ref_df[ref_name]))
                    df[new_col_name] = df[id_col].map(ref_dict)
            drop_cols = [col for col in ['department_id', 'po_id', 'PO_id', 'project_id', 'io_id', 'project_category_id'] if col in df.columns]
            df = df.drop(columns=drop_cols)
            columns = df.columns.tolist() if df is not None else []
            data = df.values.tolist() if df is not None else []

            
    return render_template('pages/data_vis.html', options=options, selected_option=selected_option, data=data, columns=columns, x_col=x_col, y_col=y_col, plot_type=plot_type, display_names=DISPLAY_NAMES)


@data_vis.route('/data_summary', methods=['GET', 'POST'])
def data_summary():
    conn = connect_local()
    cursor, cnxn = conn.connect_to_db()

    # list of tables we care about for summary
    tables = [
        'projects', 'departments', 'POs', 'budgets', 'expenses', 'fundings',
        'project_categories', 'IOs', 'human_resource_categories', 'project_forecasts_nonpc',
        'project_forecasts_pc', 'human_resource_cost', 'capex_forecasts', 'capex_budgets', 'capex_expenses'
    ]

    # load available POs so the template can show a PO dropdown
    pos_df = select_all_from_table(cursor, cnxn, 'pos')
    pos = []
    if pos_df is not None:
        for _, r in pos_df.iterrows():
            pname = r['name'] if 'name' in r.index and r['name'] is not None else (r.get('Name') if 'Name' in r.index else None)
            pid = None
            if 'id' in r.index and r['id'] is not None:
                try:
                    pid = int(r['id'])
                except Exception:
                    pid = None
            pos.append({'id': pid, 'name': pname})

    # load departments with po mapping (for client-side filtering)
    depts_df = select_all_from_table(cursor, cnxn, 'departments')
    departments = []
    if depts_df is not None:
        for _, r in depts_df.iterrows():
            dname = None
            if 'name' in r.index and r['name'] is not None:
                dname = r['name']
            elif 'Department' in r.index and r['Department'] is not None:
                dname = r['Department']
            elif 'Name' in r.index and r['Name'] is not None:
                dname = r['Name']

            dept_id = None
            if 'id' in r.index and r['id'] not in (None, ''):
                try:
                    dept_id = int(r['id'])
                except Exception:
                    try:
                        dept_id = int(str(r['id']))
                    except Exception:
                        dept_id = None

            po_id = None
            for key in ('po_id', 'PO_id', 'poId', 'po', 'PO'):
                if key in r.index and r[key] not in (None, ''):
                    try:
                        po_id = int(r[key])
                    except Exception:
                        try:
                            po_id = int(str(r[key]))
                        except Exception:
                            po_id = None
                    break

            departments.append({'id': dept_id, 'name': dname, 'po_id': po_id})

    # load IOs and attach po mapping when available
    ios_df = select_all_from_table(cursor, cnxn, 'ios')
    ios = []
    if ios_df is not None:
        # determine IO value column
        if 'IO_num' in ios_df.columns:
            val_col = 'IO_num'
        elif 'IO' in ios_df.columns:
            val_col = 'IO'
        else:
            val_col = ios_df.columns[0] if len(ios_df.columns) > 0 else None

        for _, r in ios_df.iterrows():
            if val_col is None:
                continue
            ival = r[val_col] if val_col in r.index else None
            if ival is None:
                continue
            # try find po id mapping in ios table
            po_id = None
            for key in ('po_id', 'PO_id', 'po', 'PO'):
                if key in r.index and r[key] not in (None, ''):
                    try:
                        po_id = int(r[key])
                    except Exception:
                        try:
                            po_id = int(str(r[key]))
                        except Exception:
                            po_id = None
                    break
            # try to detect project_id relationship
            project_id = None
            for key in ('project_id', 'Project_id', 'project', 'projectId'):
                if key in r.index and r[key] not in (None, ''):
                    try:
                        project_id = int(r[key])
                    except Exception:
                        try:
                            project_id = int(str(r[key]))
                        except Exception:
                            project_id = None
                    break
            ios.append({'value': str(ival), 'po_id': po_id, 'project_id': project_id})

    # load projects with PO mapping if present
    projects_df = select_all_from_table(cursor, cnxn, 'projects')
    projects = []
    if projects_df is not None:
        name_col = 'name' if 'name' in projects_df.columns else (projects_df.columns[0] if len(projects_df.columns) > 0 else None)
        if name_col:
            # build a helper map from department id -> department name for resolving project department
            dept_id_to_name = {d['id']: d['name'] for d in departments if d.get('id') is not None}
            for _, r in projects_df.iterrows():
                pname = r[name_col] if name_col in r.index and r[name_col] not in (None, '') else None
                if pname is None:
                    continue
                proj_id = None
                if 'id' in r.index and r['id'] not in (None, ''):
                    try:
                        proj_id = int(r['id'])
                    except Exception:
                        try:
                            proj_id = int(str(r['id']))
                        except Exception:
                            proj_id = None
                po_id = None
                for key in ('po_id', 'PO_id', 'po', 'PO'):
                    if key in r.index and r[key] not in (None, ''):
                        try:
                            po_id = int(r[key])
                        except Exception:
                            try:
                                po_id = int(str(r[key]))
                            except Exception:
                                po_id = None
                        break
                dept_id = None
                for key in ('department_id', 'dept_id', 'departmentId'):
                    if key in r.index and r[key] not in (None, ''):
                        try:
                            dept_id = int(r[key])
                        except Exception:
                            try:
                                dept_id = int(str(r[key]))
                            except Exception:
                                dept_id = None
                        break
                dept_name = dept_id_to_name.get(dept_id) if dept_id is not None else None
                projects.append({'id': proj_id, 'name': pname, 'po_id': po_id, 'dept_id': dept_id, 'dept_name': dept_name})

    # gather fiscal years from common tables; fallback to a default range
    fiscal_years = []
    candidate_tables = ['budgets', 'project_forecasts_nonpc', 'project_forecasts_pc', 'fundings', 'expenses', 'capex_budgets', 'capex_forecasts']
    for tbl in candidate_tables:
        try:
            df_tbl = select_all_from_table(cursor, cnxn, tbl)
            if df_tbl is None:
                continue
            for col in ('fiscal_year', 'Fiscal Year', 'fiscalYear', 'cap_year', 'cap_year'):
                if col in df_tbl.columns:
                    vals = df_tbl[col].dropna().unique().tolist()
                    # convert to int if possible, otherwise keep strings
                    cleaned = []
                    for v in vals:
                        try:
                            cleaned.append(int(v))
                        except Exception:
                            try:
                                cleaned.append(int(str(v)))
                            except Exception:
                                cleaned.append(v)
                    fiscal_years.extend(cleaned)
                    break
        except Exception:
            continue
    # dedupe and sort fiscal years
    if fiscal_years:
        # convert to strings for template display
        unique_years = sorted(set(fiscal_years))
        fiscal_years = [str(y) for y in unique_years]
    else:
        fiscal_years = [str(y) for y in range(2020, 2036)]

    # preserve selected filters if provided via form/args
    selected_po = None
    selected_department = None
    selected_io = None
    selected_project = None
    selected_year = None
    if request.method == 'POST':
        selected_po = request.form.get('po')
        selected_department = request.form.get('department')
        selected_io = request.form.get('io')
        selected_project = request.form.get('project')
        selected_year = request.form.get('fiscal_year')
    else:
        selected_po = request.args.get('po')
        selected_department = request.args.get('department')
        selected_io = request.args.get('io')
        selected_project = request.args.get('project')
        selected_year = request.args.get('fiscal_year')

    # Compute three aggregated metrics when the form is submitted
    summary_row = None
    if request.method == 'POST':
        # mapping for id -> friendly names (used to create columns like 'PO', 'Department', etc.)
        id_name_map = {
            'department_id': ('departments', 'id', 'name', 'Department'),
            'po_id': ('pos', 'id', 'name', 'PO'),
            'PO_id': ('pos', 'id', 'name', 'PO'),
            'project_id': ('projects', 'id', 'name', 'Project'),
            'io_id': ('ios', 'id', 'IO_num', 'IO'),
            'project_category_id': ('project_categories', 'id', 'category', 'Project Category'),
        }

        # helper to filter a dataframe by selected filters (matching friendly-name columns).
        # This also creates friendly-name columns from id columns when needed so filters work.
        def apply_filters(df):
            if df is None or df.empty:
                return df
            df_local = df.copy()

            # try to build friendly-name columns from id columns if they don't exist
            for id_col, (ref_table, ref_id, ref_name, new_col_name) in id_name_map.items():
                if id_col in df_local.columns and new_col_name not in df_local.columns:
                    try:
                        ref_df = select_all_from_table(cursor, cnxn, ref_table)
                        if ref_df is not None:
                            ref_dict = dict(zip(ref_df[ref_id], ref_df[ref_name]))
                            # map values; tolerate string ids stored in source
                            df_local[new_col_name] = df_local[id_col].map(ref_dict)
                    except Exception:
                        # ignore mapping errors and continue
                        pass

            # normalize fiscal year / cap_year to 'Fiscal Year'
            if 'fiscal_year' in df_local.columns and 'Fiscal Year' not in df_local.columns:
                df_local['Fiscal Year'] = df_local['fiscal_year']
            if 'cap_year' in df_local.columns and 'Fiscal Year' not in df_local.columns:
                df_local['Fiscal Year'] = df_local['cap_year']

            # apply filters against friendly-name columns where available
            if selected_po and selected_po != '' and selected_po != 'All' and 'PO' in df_local.columns:
                df_local = df_local[df_local['PO'].astype(str) == str(selected_po)]
            if selected_department and selected_department != '' and selected_department != 'All' and 'Department' in df_local.columns:
                df_local = df_local[df_local['Department'].astype(str) == str(selected_department)]
            if selected_io and selected_io != '' and selected_io != 'All' and 'IO' in df_local.columns:
                df_local = df_local[df_local['IO'].astype(str) == str(selected_io)]
            if selected_project and selected_project != '' and selected_project != 'All' and 'Project' in df_local.columns:
                df_local = df_local[df_local['Project'].astype(str) == str(selected_project)]
            if selected_year and selected_year != '' and selected_year != 'All' and 'Fiscal Year' in df_local.columns:
                df_local = df_local[df_local['Fiscal Year'].astype(str) == str(selected_year)]
            return df_local
        
        # 1) Forecasted Expense: sum(non-personnel in project_forecasts_nonpc) + sum(personnel_expense in project_forecasts_pc)
        forecast_nonpc = select_all_from_table(cursor, cnxn, 'project_forecasts_nonpc')
        forecast_pc = select_all_from_table(cursor, cnxn, 'project_forecasts_pc')
        f_nonpc_sum = 0.0
        f_pc_sum = 0.0
        if forecast_nonpc is not None and not forecast_nonpc.empty:
            df1 = apply_filters(forecast_nonpc)
            # possible columns for non-personnel cost
            for col in ['non_personnel_expense']:
                if col in df1.columns:
                    f_nonpc_sum = float(pd.to_numeric(df1[col], errors='coerce').sum())
        
        if forecast_pc is not None and not forecast_pc.empty:
            df2 = apply_filters(forecast_pc)
            # possible columns for personnel cost
            # Build human resource cost lookup (category_id, year) -> cost
            f_pc_sum = 0.0
            try:
                hr_ref = select_all_from_table(cursor, cnxn, 'human_resource_categories')
                name_to_id = dict(zip(hr_ref['name'], hr_ref['id'])) if hr_ref is not None and 'name' in hr_ref.columns and 'id' in hr_ref.columns else {}
            except Exception:
                name_to_id = {}

            try:
                hr_cost_df = select_all_from_table(cursor, cnxn, 'human_resource_cost')
            except Exception:
                hr_cost_df = None

            cost_lookup = {}
            if hr_cost_df is not None and not hr_cost_df.empty and 'category_id' in hr_cost_df.columns and 'year' in hr_cost_df.columns and 'cost' in hr_cost_df.columns:
                for _, r in hr_cost_df[['category_id', 'year', 'cost']].iterrows():
                    try:
                        key = (int(r['category_id']), int(r['year']))
                        cost_lookup[key] = float(r['cost'])
                    except Exception:
                        continue

            if df2 is not None and not df2.empty:
                # find FTE/work-hours column
                fte_candidates = ['Work Hours(FTE)', 'human_resource_fte', 'Human_resource_FTE', 'Human resource FTE', 'Human_resource_fte', 'Human_resource_FTE', 'Human_resource_FTE']
                fte_col = next((c for c in fte_candidates if c in df2.columns), None)
                # fiscal year column
                fy_col = next((c for c in ['Fiscal Year', 'fiscal_year', 'year'] if c in df2.columns), None)

                for _, row in df2.iterrows():
                    try:
                        # determine category id: prefer numeric id column if present
                        cat_id = None
                        if 'human_resource_category_id' in row.index and row.get('human_resource_category_id') not in (None, ''):
                            try:
                                cat_id = int(row.get('human_resource_category_id'))
                            except Exception:
                                try:
                                    cat_id = int(str(row.get('human_resource_category_id')))
                                except Exception:
                                    cat_id = None
                        elif 'Staff Category' in row.index and row.get('Staff Category') not in (None, ''):
                            cat_id = name_to_id.get(row.get('Staff Category'))
                        elif 'human_resource_category' in row.index and row.get('human_resource_category') not in (None, ''):
                            cat_id = name_to_id.get(row.get('human_resource_category'))

                        # fiscal year
                        if fy_col is None or row.get(fy_col) in (None, ''):
                            continue
                        try:
                            year = int(row.get(fy_col))
                        except Exception:
                            try:
                                year = int(str(row.get(fy_col)))
                            except Exception:
                                continue

                        # work hours / FTE
                        wh = 0.0
                        if fte_col and row.get(fte_col) not in (None, ''):
                            try:
                                wh = float(row.get(fte_col))
                            except Exception:
                                try:
                                    wh = float(str(row.get(fte_col)))
                                except Exception:
                                    wh = 0.0

                        if cat_id is None:
                            continue
                        key = (int(cat_id), int(year))
                        hourly = cost_lookup.get(key)
                        if hourly is None:
                            continue
                        f_pc_sum += float(hourly) * float(wh)
                    except Exception:
                        continue

            # ensure f_pc_sum variable exists for later aggregation
            try:
                f_pc_sum
            except NameError:
                f_pc_sum = 0.0

        forecasted_expense = f_nonpc_sum + f_pc_sum

        # 2) Budget and Funding: sum(human_resource_expense + non_personnel_expense in budgets) + sum(funding in fundings)
        budgets_df = select_all_from_table(cursor, cnxn, 'budgets')
        fundings_df = select_all_from_table(cursor, cnxn, 'fundings')
        budgets_sum = 0.0
        fundings_sum = 0.0
        if budgets_df is not None and not budgets_df.empty:
            dbud = apply_filters(budgets_df)
            # look for columns for HR and non-HR budgets
            hr_cols = [c for c in ['Human Resources Budget', 'Human Resources Budget', 'Human Resources', 'Human_Resources_Budget', 'Human Resources Budget'] if c in dbud.columns]
            nonhr_cols = [c for c in ['Non-Human Resources Budget', 'Non-Human Resources', 'Non_personnel_budget', 'Non-Human Resources Budget'] if c in dbud.columns]
            s = 0.0
            for c in hr_cols:
                try:
                    s += float(pd.to_numeric(dbud[c], errors='coerce').sum())
                except Exception:
                    pass
            for c in nonhr_cols:
                try:
                    s += float(pd.to_numeric(dbud[c], errors='coerce').sum())
                except Exception:
                    pass
            budgets_sum = s

        if fundings_df is not None and not fundings_df.empty:
            dfund = apply_filters(fundings_df)
            # possible funding column names
            for col in ['funding', 'Funding', 'funding_amount']:
                if col in dfund.columns:
                    try:
                        fundings_sum = float(pd.to_numeric(dfund[col], errors='coerce').sum())
                    except Exception:
                        fundings_sum = 0.0
                    break

        budget_and_funding = budgets_sum + fundings_sum

        # 3) Actual Expenditure: sum of expense value in expenses table
        expenses_df = select_all_from_table(cursor, cnxn, 'expenses')
        expenses_sum = 0.0
        if expenses_df is not None and not expenses_df.empty:
            de = apply_filters(expenses_df)
            # try common expense columns
            for col in ['expense_value', 'Val.in rep.cur', 'Val.in rep.cur.', 'Expense', 'Actual (k CNY)', 'Expense']:
                if col in de.columns:
                    try:
                        expenses_sum = float(pd.to_numeric(de[col], errors='coerce').sum())
                    except Exception:
                        expenses_sum = 0.0
                    break

        # final summary row
        summary_row = {
            'Forecasted Expense': round(float(forecasted_expense or 0.0), 2),
            'Budget and Funding': round(float(budget_and_funding or 0.0), 2),
            'Actual Expenditure': round(float(expenses_sum or 0.0), 2)
        }

    return render_template('pages/data_summary.html', tables=tables, display_names=DISPLAY_NAMES, pos=pos, selected_po=selected_po, departments=departments, ios=ios, projects=projects, fiscal_years=fiscal_years, selected_department=selected_department, selected_io=selected_io, selected_project=selected_project, selected_year=selected_year, summary_row=summary_row)
