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

    # load departments
    depts_df = select_all_from_table(cursor, cnxn, 'departments')
    departments = []
    if depts_df is not None:
        for _, r in depts_df.iterrows():
            dname = r['name'] if 'name' in r.index and r['name'] is not None else (r.get('Name') if 'Name' in r.index else None)
            departments.append(dname)

    # load IOs
    ios_df = select_all_from_table(cursor, cnxn, 'ios')
    ios = []
    if ios_df is not None:
        # prefer column 'IO_num' or 'IO'
        if 'IO_num' in ios_df.columns:
            ios = ios_df['IO_num'].dropna().astype(str).unique().tolist()
        elif 'IO' in ios_df.columns:
            ios = ios_df['IO'].dropna().astype(str).unique().tolist()
        else:
            # fallback: any first column
            first_col = ios_df.columns[0] if len(ios_df.columns) > 0 else None
            if first_col:
                ios = ios_df[first_col].dropna().astype(str).unique().tolist()

    # load projects
    projects_df = select_all_from_table(cursor, cnxn, 'projects')
    projects = []
    if projects_df is not None and 'name' in projects_df.columns:
        projects = projects_df['name'].dropna().unique().tolist()

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

    # If the form was submitted, fetch and filter the target tables
    results_list = []
    if request.method == 'POST':
        # target tables to display
        target_tables = {
            'project_forecasts_pc': 'Project Forecasts (personnel)',
            'project_forecasts_nonpc': 'Project Forecasts (non-personnel)',
            'expenses': 'Expenses',
            'budgets': 'Budgets',
            'fundings': 'Fundings'
        }

        # mapping for id -> friendly names (same as elsewhere)
        id_name_map = {
            'department_id': ('departments', 'id', 'name', 'Department'),
            'po_id': ('POs', 'id', 'name', 'PO'),
            'PO_id': ('POs', 'id', 'name', 'PO'),
            'project_id': ('projects', 'id', 'name', 'Project'),
            'io_id': ('IOs', 'id', 'IO_num', 'IO'),
            'project_category_id': ('project_categories', 'id', 'category', 'Project Category'),
        }

        for tbl, title in target_tables.items():
            try:
                df = select_all_from_table(cursor, cnxn, tbl)
                if df is None or df.empty:
                    continue
                df2 = df.copy()

                # map id columns to friendly names where possible
                for id_col, (ref_table, ref_id, ref_name, new_col_name) in id_name_map.items():
                    if id_col in df2.columns:
                        try:
                            ref_df = select_all_from_table(cursor, cnxn, ref_table)
                            ref_dict = dict(zip(ref_df[ref_id], ref_df[ref_name]))
                            df2[new_col_name] = df2[id_col].map(ref_dict)
                        except Exception:
                            # skip mapping if something goes wrong
                            pass

                # ensure Fiscal Year column is present for filtering
                if 'fiscal_year' in df2.columns and 'Fiscal Year' not in df2.columns:
                    df2['Fiscal Year'] = df2['fiscal_year']
                if 'cap_year' in df2.columns and 'Fiscal Year' not in df2.columns:
                    df2['Fiscal Year'] = df2['cap_year']

                # apply filters
                if selected_po and selected_po != '' and selected_po != 'All' and 'PO' in df2.columns:
                    df2 = df2[df2['PO'].astype(str) == str(selected_po)]
                if selected_department and selected_department != '' and selected_department != 'All' and 'Department' in df2.columns:
                    df2 = df2[df2['Department'].astype(str) == str(selected_department)]
                if selected_io and selected_io != '' and selected_io != 'All' and 'IO' in df2.columns:
                    df2 = df2[df2['IO'].astype(str) == str(selected_io)]
                if selected_project and selected_project != '' and selected_project != 'All' and 'Project' in df2.columns:
                    df2 = df2[df2['Project'].astype(str) == str(selected_project)]
                if selected_year and selected_year != '' and selected_year != 'All' and 'Fiscal Year' in df2.columns:
                    df2 = df2[df2['Fiscal Year'].astype(str) == str(selected_year)]

                # drop internal id columns for display
                drop_cols = [c for c in df2.columns if (c.lower().endswith('_id') or c.lower() == 'id')]
                try:
                    df_out = df2.drop(columns=list(set(drop_cols)))
                except Exception:
                    df_out = df2

                results_list.append({'title': title, 'columns': df_out.columns.tolist(), 'data': df_out.values.tolist()})
            except Exception:
                continue

    return render_template('pages/data_summary.html', tables=tables, display_names=DISPLAY_NAMES, pos=pos, selected_po=selected_po, departments=departments, ios=ios, projects=projects, fiscal_years=fiscal_years, selected_department=selected_department, selected_io=selected_io, selected_project=selected_project, selected_year=selected_year, results_list=results_list)
