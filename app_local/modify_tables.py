
from flask import Flask, render_template, Blueprint, request, redirect, url_for
from backend.modify_table_local import add_entry
from backend import upload_budgets_local
import pandas as pd
import os
from backend.connect_local import connect_local, select_all_from_table
from app_local.select_data import transform_table
from backend.create_display_table import get_departments_display, get_projects_display

modify_tables = Blueprint('modify_tables', __name__, template_folder='templates')

# module-level selected PO for modify forms (capex and others)
selected_po = None
selected_department = None
selected_cap_year = None
selected_project = None

# Map route to table and form fields
modify_table_config = {
    'modify_department': {
        'title': 'Modify Department',
        'table_name': 'departments',
        'fields': [
            {'name': 'Department', 'type': 'text', 'label': 'Department Name'},
            {'name': 'po', 'type': 'select', 'label': 'PO', 'options': []}
        ],
        # We'll merge on Department name and include po_id when present
        'merge_on': 'Department',
        'columns': ['Department', 'po_id']
    },
    'modify_po': {
        'title': 'Modify PO',
        'table_name': 'pos',
        'fields': [
            {'name': 'PO', 'type': 'text', 'label': 'PO Name'}
        ],
        'merge_on': 'name',
        'columns': ['name']
    },
    'modify_porject_category': {
        'title': 'Modify Project Category',
        'table_name': 'project_categories',
        'fields': [
            {'name': 'category', 'type': 'text', 'label': 'Project Category'}
        ],
        'merge_on': 'category',
        'columns': ['category']
    },
    'modify_project': {
        'title': 'Modify Project',
        'table_name': 'projects',
        'fields': [
            {'name': 'name', 'type': 'text', 'label': 'Project Name'},
            {'name': 'category', 'type': 'select', 'label': 'Category Name', 'options': []},
            {'name': 'department', 'type': 'select', 'label': 'Department', 'options': []}
            ,{'name': 'fiscal_year', 'type': 'select', 'label': 'Fiscal Year', 'options': []}
        ],
        'merge_on': 'name',
        'columns': ['name', 'category_id', 'department_id', 'fiscal_year']
    },
    'modify_io': {
        'title': 'Modify IO',
        'table_name': 'ios',
        'fields': [
            {'name': 'IO', 'type': 'number', 'label': 'IO Number'},
            {'name': 'project_name', 'type': 'select', 'label': 'Project Name', 'options': []}
        ],
        'merge_on': 'IO_num',
        'columns': ['IO_num', 'project_id']
    },
    'modify_staff_categories': {
        'title': 'Modify Staff Categories',
        'table_name': 'human_resource_categories',
        'fields': [
            {'name': 'category', 'type': 'text', 'label': 'Staff Category'},
        ],
        'merge_on': ['name'],
        'columns': ['name']
    },
    'modify_staff_cost': {
        'title': 'Modify Staff Cost',
        'table_name': 'human_resource_cost',
        'fields': [
            {'name': 'staff_category', 'type': 'select', 'label': 'Staff Category', 'options': []},
            {'name': 'year', 'type': 'select', 'label': 'Year', 'options': []},
            {'name': 'cost', 'type': 'number', 'label': 'Cost'}
        ],
        'merge_on': 'category_id',
        'columns': ['category_id', 'year', 'cost']
    },
    'add_user': {
        'title': 'Add User',
        'table_name': 'users',
        'fields': [
            {'name': 'username', 'type': 'text', 'label': 'Username'},
            {'name': 'password', 'type': 'password', 'label': 'Password'},
            {'name': 'user_type', 'type': 'text', 'label': 'User Type'}
        ],
        'merge_on': 'name'
    },
    'delete_user': {
        'title': 'Delete User',
        'table_name': 'users',
        'fields': [
            {'name': 'username', 'type': 'text', 'label': 'Username'}
        ],
        'merge_on': 'name'
    },
    'modify_user_type': {
        'title': 'Modify User Type',
        'table_name': 'users',
        'fields': [
            {'name': 'username', 'type': 'text', 'label': 'Username'},
            {'name': 'user_type', 'type': 'text', 'label': 'New User Type'}
        ],
        'merge_on': 'name'
    },
    'upload_budget':{
        'title': 'Upload budget',
        'table_name': 'budgets',
        'fields': [
            {'name': 'po', 'type': 'select', 'label': 'PO', 'options': []},
            {'name': 'department', 'type': 'select', 'label': 'Department', 'options': []},
            {'name': 'fiscal_year', 'type': 'select', 'label': 'Fiscal Year', 'options': []},
            {'name': 'human_resource_expense', 'type':'number', 'label': 'Personnel budget'},
            {'name': 'non_personnel_expense', 'type':'number', 'label': 'Non-personnel budget'}
        ],
        'merge_on': 'IO_num',
        'columns': ['IO_num', 'project_id']
    },
    'modify_funding': {
        'title': 'Modify Funding',
        'table_name': 'fundings',
        'fields': [
            {'name': 'po', 'type': 'select', 'label': 'PO', 'options': []},
            {'name': 'department', 'type': 'select', 'label': 'Department', 'options': []},
            {'name': 'fiscal_year', 'type': 'select', 'label': 'Fiscal Year', 'options': []},
            {'name': 'funding', 'type': 'number', 'label': 'Funding (K CNY)'},
            {'name': 'funding_from', 'type': 'text', 'label': 'Funding From'},
            {'name': 'funding_for', 'type': 'text', 'label': 'Funding For'}
        ],
        'merge_on': 'funding',
        'columns': ['po_id', 'department_id', 'fiscal_year', 'funding', 'funding_from', 'funding_for']
    },
    'capex_forecast': {
        'title': 'Capex Forecast',
        'table_name': 'capex_forecasts',
        'fields': [
            {'name': 'po', 'type': 'select', 'label': 'PO', 'options': []},
            {'name': 'department', 'type': 'select', 'label': 'Department', 'options': []},
            {'name': 'cap_year', 'type': 'select', 'label': 'Capex Year', 'options': []},
            {'name': 'project_name', 'type': 'select', 'label': 'Project Name', 'options': []},
            {'name': 'capex_description', 'type': 'text', 'label': 'Capex Description'},
            {'name': 'capex_forecast', 'type': 'number', 'label': 'Capex Forecast'},
            {'name': 'cost_center', 'type': 'text', 'label': 'Cost Center'}
        ],
        'merge_on': 'capex_description',
        'columns': ['po_id', 'department_id', 'cap_year', 'project_id', 'capex_description', 'capex_forecast', 'cost_center']
    },
    'capex_budget': {
        'title': 'Capex Budget',
        'table_name': 'capex_budgets',
        'fields': [
            {'name': 'po', 'type': 'select', 'label': 'PO', 'options': []},
            {'name': 'department', 'type': 'select', 'label': 'Department', 'options': []},
            {'name': 'cap_year', 'type': 'select', 'label': 'Capex Year', 'options': []},
            {'name': 'project_name', 'type': 'select', 'label': 'Project Name', 'options': []},
            {'name': 'capex_description', 'type': 'text', 'label': 'Capex Description'},
            {'name': 'approved_budget', 'type': 'number', 'label': 'Approved Budget (K CNY)'}
        ],
        'merge_on': 'capex_description',
        'columns': ['po_id', 'department_id',  'project_id', 'cap_year','capex_description', 'budget']
    },
}

# Generic route for all admin table modifications


@modify_tables.route('/<action>', methods=['GET', 'POST'])
def modify_table_router(action):

    config = modify_table_config.get(action)
    if not config:
        return f"Unknown modification: {action}", 404


    # Helper: fetch options from DB
    def fetch_options(table, col='name'):
        try:
            db = connect_local()
            cursor, cnxn = db.connect_to_db()
            df = select_all_from_table(cursor, cnxn, table)
            return df[col].dropna().unique().tolist() if col in df.columns else []
        except Exception:
            return []

    # Populate dropdowns for upload_budget
    if action == 'upload_budget':
        po_options = fetch_options('pos', 'name')
        dept_options = fetch_options('departments', 'name')
        year_options = [str(y) for y in range(2020, 2031)]
        for field in config['fields']:
            if field['name'] == 'po':
                field['options'] = po_options
            if field['name'] == 'department':
                field['options'] = dept_options
            if field['name'] == 'fiscal_year':
                field['options'] = year_options

    # Populate dropdowns for modify_funding (PO, Department, Fiscal Year)
    if action == 'modify_funding':
        po_options = fetch_options('pos', 'name')
        dept_options = fetch_options('departments', 'name')
        year_options = [str(y) for y in range(2020, 2031)]
        for field in config['fields']:
            if field['name'] == 'po' and field.get('type') == 'select':
                field['options'] = po_options
            if field['name'] == 'department' and field.get('type') == 'select':
                field['options'] = dept_options
            if field['name'] == 'fiscal_year' and field.get('type') == 'select':
                field['options'] = year_options

    # Populate dropdowns for capex_forecast and capex_budget
    if action in ['capex_forecast', 'capex_budget']:
        po_options = fetch_options('pos', 'name')
        dept_options = fetch_options('departments', 'name')
        project_options = fetch_options('projects', 'name')
        year_options = [str(y) for y in range(2020, 2031)]
        for field in config['fields']:
            if field['name'] == 'po':
                field['options'] = po_options
            if field['name'] == 'department':
                field['options'] = dept_options
            if field['name'] == 'project_name':
                field['options'] = project_options
            if field['name'] == 'cap_year':
                field['options'] = year_options

    # If this is the modify_project form, fetch category options from DB
    if action == 'modify_project':
        options = []
        try:
            db = connect_local()
            cursor, cnxn = db.connect_to_db()
            df = select_all_from_table(cursor, cnxn, 'project_categories')
            options = df['category'].tolist() if 'category' in df.columns else []
        except Exception as e:
            options = []
        # Update the config's field options
        for field in config['fields']:
            if field['name'] == 'category' and field['type'] == 'select':
                field['options'] = options
            if field['name'] == 'department' and field['type'] == 'select':
                try:
                    db = connect_local()
                    cursor, cnxn = db.connect_to_db()
                    ddf = select_all_from_table(cursor, cnxn, 'departments')
                    dept_options = ddf['name'].tolist() if 'name' in ddf.columns else []
                except Exception:
                    dept_options = []
                field['options'] = dept_options
            if field['name'] == 'fiscal_year' and field['type'] == 'select':
                field['options'] = [str(y) for y in range(2020, 2031)]

    # If this is the modify_department form, fetch PO options for dropdown
    if action == 'modify_department':
        po_options = fetch_options('pos', 'name')
        for field in config['fields']:
            if field['name'] == 'po' and field['type'] == 'select':
                field['options'] = po_options

    # If this is the modify_staff_cost form, populate staff category and year options
    if action == 'modify_staff_cost':
        staff_options = fetch_options('human_resource_categories', 'name')
        year_options = [str(y) for y in range(2020, 2031)]
        for field in config['fields']:
            if field['name'] == 'staff_category' and field['type'] == 'select':
                field['options'] = staff_options
            if field['name'] == 'year' and field['type'] == 'select':
                field['options'] = year_options

    # If this is the modify_io form, fetch project names for dropdown
    if action == 'modify_io':
        project_options = []
        try:
            db = connect_local()
            cursor, cnxn = db.connect_to_db()
            df = select_all_from_table(cursor, cnxn, 'projects')
            project_options = df['name'].tolist() if 'name' in df.columns else []
        except Exception as e:
            project_options = []
        for field in config['fields']:
            if field['name'] == 'project_name' and field['type'] == 'select':
                field['options'] = project_options

    if request.method == 'POST':
        form_data = dict(request.form)
        table_name = form_data.pop('table_name', config['table_name'])
        # Only use fields defined in config
        field_names = [f['name'] for f in config['fields']]
        row = {k: v for k, v in form_data.items() if k in field_names}
        if action == 'upload_budget':
            # Custom upload_budget logic goes here
            # You can add your own database logic here
            df_upload = pd.DataFrame([row])
            df_upload.columns = ['PO', 'Department', 'fiscal_year', 'human_resource_expense', 'non_personnel_expense']
            upload_budgets_local(df_upload)

            msg = f"Budget uploaded: {row}"
            return msg
        else:
            # Default: use add_entry for other actions
            df_upload = pd.DataFrame([row])
            # Special handling for modify_funding: map PO and Department names to their local ids
            if action == 'modify_io':
                df_upload['IO'] = df_upload['IO'].astype(int)

            if action == 'modify_funding':
                try:
                    db = connect_local()
                    cursor, cnxn = db.connect_to_db()
                    pos_df = select_all_from_table(cursor, cnxn, 'pos')
                    dept_df = select_all_from_table(cursor, cnxn, 'departments')
                    po_map = dict(zip(pos_df['name'], pos_df['id'])) if 'name' in pos_df.columns and 'id' in pos_df.columns else {}
                    dept_map = dict(zip(dept_df['name'], dept_df['id'])) if 'name' in dept_df.columns and 'id' in dept_df.columns else {}
                    # create id columns
                    df_upload['po_id'] = df_upload.get('po').map(po_map) if 'po' in df_upload.columns else None
                    df_upload['department_id'] = df_upload.get('department').map(dept_map) if 'department' in df_upload.columns else None
                    # ensure fiscal_year is int when possible
                    if 'fiscal_year' in df_upload.columns:
                        try:
                            df_upload['fiscal_year'] = df_upload['fiscal_year'].astype(int)
                        except Exception:
                            pass
                    # reorder/keep only the expected columns for the fundings table
                    expected_cols = modify_table_config.get(action).get('columns', [])
                    # Only keep columns that exist in df_upload to avoid KeyError
                    keep_cols = [c for c in expected_cols if c in df_upload.columns]
                    if keep_cols:
                        df_upload = df_upload[keep_cols]
                except Exception:
                    # Fallback: try a simple rename if mapping failed
                    try:
                        df_upload.rename(columns={'po': 'po_id', 'department': 'department_id'}, inplace=True)
                        if 'fiscal_year' in df_upload.columns:
                            try:
                                df_upload['fiscal_year'] = df_upload['fiscal_year'].astype(int)
                            except Exception:
                                pass
                    except Exception:
                        pass

            # Special handling for modify_staff_cost: map staff_category name to category_id
            if action == 'modify_staff_cost':
                try:
                    db = connect_local()
                    cursor, cnxn = db.connect_to_db()
                    hr_df = select_all_from_table(cursor, cnxn, 'human_resource_categories')
                    hr_map = dict(zip(hr_df['name'], hr_df['id'])) if 'name' in hr_df.columns and 'id' in hr_df.columns else {}
                    # create category_id column by mapping the provided staff_category name
                    df_upload['category_id'] = df_upload.get('staff_category').map(hr_map) if 'staff_category' in df_upload.columns else None
                    # ensure year is int when possible
                    if 'year' in df_upload.columns:
                        try:
                            df_upload['year'] = df_upload['year'].astype(int)
                        except Exception:
                            pass
                    # reorder/keep only the expected columns for the human_resource_cost table
                    expected_cols = modify_table_config.get(action).get('columns', [])
                    keep_cols = [c for c in expected_cols if c in df_upload.columns]
                    if keep_cols:
                        df_upload = df_upload[keep_cols]
                    df_upload['cost'] = df_upload['cost'].astype(float)
                    
                except Exception:
                    # Fallback: try a simple rename if mapping failed
                    try:
                        df_upload.rename(columns={'staff_category': 'category_id'}, inplace=True)
                        if 'year' in df_upload.columns:
                            try:
                                df_upload['year'] = df_upload['year'].astype(int)
                            except Exception:
                                pass
                    except Exception:
                        pass

            if action == 'modify_department':
                db = connect_local()
                cursor, cnxn = db.connect_to_db()
                pos_df = select_all_from_table(cursor, cnxn, 'pos')
                po_map = dict(zip(pos_df['name'], pos_df['id'])) if 'name' in pos_df.columns and 'id' in pos_df.columns else {}
                df_upload['po_id'] = df_upload.get('po').map(po_map) if 'po' in df_upload.columns else None
                # Keep only expected columns
                expected_cols = modify_table_config.get(action).get('columns', [])
                keep_cols = [c for c in expected_cols if c in df_upload.columns]
                if keep_cols:
                    df_upload = df_upload[keep_cols]
                df_upload = df_upload.rename(columns={'Department': 'name'})
                
            if action == 'modify_project':
                try:
                    print(df_upload)
                    db = connect_local()
                    cursor, cnxn = db.connect_to_db()
                    cat_df = select_all_from_table(cursor, cnxn, 'project_categories')
                    dept_df = select_all_from_table(cursor, cnxn, 'departments')
                    cat_map = dict(zip(cat_df['category'], cat_df['id'])) if 'category' in cat_df.columns and 'id' in cat_df.columns else {}
                    dept_map = dict(zip(dept_df['name'], dept_df['id'])) if 'name' in dept_df.columns and 'id' in dept_df.columns else {}
                    df_upload['category_id'] = df_upload.get('category').map(cat_map) if 'category' in df_upload.columns else None
                    df_upload['department_id'] = df_upload.get('department').map(dept_map) if 'department' in df_upload.columns else None
                    
                    # Keep only expected columns
                    expected_cols = modify_table_config.get(action).get('columns', [])
                    keep_cols = [c for c in expected_cols if c in df_upload.columns]
                    print(expected_cols)
                    print(df_upload.columns)
                    print(keep_cols)
                    if keep_cols:
                        df_upload = df_upload[keep_cols]
                except Exception:
                    try:
                        df_upload.rename(columns={'category': 'category_id', 'department': 'department_id'}, inplace=True)
                    except Exception:
                        pass
            

            merge_columns = modify_table_config.get(action).get('columns', list(row.keys()))
            merge_on = modify_table_config.get(action).get('merge_on', list(row.keys())[0] if row else None)
            if action == 'modify_project':
                conn = connect_local()
                engine, cursor, cnxn = conn.connect_to_db(engine=True)
                df_upload.to_sql(table_name, con=engine, if_exists='append', index=False)
            else:
                res = add_entry(df_upload, table_name, merge_columns, merge_on)
            

    # After handling POST (or on GET), fetch table contents to display below the form
    try:
        db = connect_local()
        cursor, cnxn = db.connect_to_db()
        df_table = select_all_from_table(cursor, cnxn, config['table_name'])
        # Map id columns to names similarly to select_data (do this BEFORE transform_table
        # so the select/modify views match the same column names and ordering)
        id_name_map = {
            'department_id': ('departments', 'id', 'name', 'Department'),
            'po_id': ('POs', 'id', 'name', 'PO'),
            'PO_id': ('POs', 'id', 'name', 'PO'),
            'project_id': ('projects', 'id', 'name', 'Project'),
            'io_id': ('IOs', 'id', 'IO_num', 'IO'),
            'project_category_id': ('project_categories', 'id', 'category', 'Project Category'),
        }
        for id_col, (ref_table, ref_id, ref_name, new_col_name) in id_name_map.items():
            if id_col in df_table.columns:
                ref_df = select_all_from_table(cursor, cnxn, ref_table)
                ref_dict = dict(zip(ref_df[ref_id], ref_df[ref_name]))
                df_table[new_col_name] = df_table[id_col].map(ref_dict)
        drop_cols = [col for col in ['department_id', 'po_id', 'PO_id', 'project_id', 'io_id', 'project_category_id'] if col in df_table.columns]
        if drop_cols:
            df_table = df_table.drop(columns=drop_cols)

        # Now apply the same table-specific transforms as select view so projects
        # show project category name instead of category_id, and other renames
        try:
            df_table = transform_table(df_table, config['table_name'], cursor, cnxn)
        except Exception:
            # non-fatal: if transform fails, continue with raw df_table
            pass

        columns = df_table.columns.tolist()
        data = df_table.values.tolist()
    except Exception:
        columns = []
        data = []

    return render_template(
        'pages/modify_table.html',
        title=config['title'],
        table_name=config['table_name'],
        fields=config['fields'],
        columns=columns if 'columns' in locals() else [],
        data=data if 'data' in locals() else []
    )


@modify_tables.route('/change_staff_cost', methods=['POST'])
def change_staff_cost():
    """Handle modify action to change an existing staff cost entry.

    Expects form fields: staff_category, year, cost
    Updates human_resource_cost.cost WHERE category_id = mapped id AND year = provided year.
    """
    form = dict(request.form)
    staff_category = form.get('staff_category')
    year = form.get('year')
    cost = form.get('cost')

    try:
        db = connect_local()
        cursor, cnxn = db.connect_to_db()
        # map staff category name to id
        hr_df = select_all_from_table(cursor, cnxn, 'human_resource_categories')
        hr_map = dict(zip(hr_df['name'], hr_df['id'])) if 'name' in hr_df.columns and 'id' in hr_df.columns else {}
        cat_id = hr_map.get(staff_category)
        # coerce types
        try:
            year_val = int(year) if year not in (None, '') else None
        except Exception:
            year_val = None
        try:
            cost_val = float(cost) if cost not in (None, '') else None
        except Exception:
            cost_val = None

        if cat_id is None or year_val is None or cost_val is None:
            # invalid request; redirect back with no change
            return redirect(url_for('modify_tables.modify_table_router', action='modify_staff_cost'))

        # perform update
        cursor.execute("UPDATE human_resource_cost SET cost = ? WHERE category_id = ? AND year = ?", (cost_val, int(cat_id), int(year_val)))
        cnxn.commit()
    except Exception:
        # ignore errors and redirect back
        pass

    return redirect(url_for('modify_tables.modify_table_router', action='modify_staff_cost'))


@modify_tables.route('/capex_forecast/po_selection', methods=['POST'])
def modify_po_selection():
    """Receive PO selection from client for modify forms and update module-level selected_po."""
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


@modify_tables.route('/capex_forecast/department_selection', methods=['POST'])
def modify_department_selection():
    """Receive Department selection from client for modify forms and update module-level selected_department."""
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


@modify_tables.route('/capex_forecast/cap_year_selection', methods=['POST'])
def modify_cap_year_selection():
    """Receive Capex year selection from client for modify forms and update module-level selected_cap_year."""
    global selected_cap_year
    try:
        if request.is_json:
            payload = request.get_json()
            val = payload.get('cap_year')
        else:
            val = request.form.get('cap_year')
        selected_cap_year = val
        return {'status': 'ok', 'selected_cap_year': selected_cap_year}, 200
    except Exception as e:
        return {'status': 'error', 'message': str(e)}, 500


@modify_tables.route('/capex_forecast/project_selection', methods=['POST'])
def modify_project_selection():
    """Receive Project selection from client for modify forms and update module-level selected_project."""
    global selected_project
    try:
        if request.is_json:
            payload = request.get_json()
            val = payload.get('project') or payload.get('project_name')
        else:
            val = request.form.get('project') or request.form.get('project_name')
        selected_project = val
        return {'status': 'ok', 'selected_project': selected_project}, 200
    except Exception as e:
        return {'status': 'error', 'message': str(e)}, 500


@modify_tables.route('/capex_forecast/department_update', methods=['GET'])
def capex_department_update():
    """Return a JSON list of departments filtered by PO.

    Query params:
    - po: optional PO name to filter by. If not provided, falls back to module-level selected_po.
    Response: { 'departments': [ ... ] }
    """
        # prefer explicit query parameter, fall back to module-level selected_po
    po = request.args.get('po') or selected_po
    df = get_departments_display()
    if df is None or df.empty:
        return {'departments': []}, 200
    df_dept = df[df['name_po'] == po]
    depts = df_dept['name_departments'].to_list()
                
    return {'departments': depts}, 200


@modify_tables.route('/capex_forecast/project_update', methods=['GET'])
def capex_project_update():
    """Return a JSON list of projects filtered by PO, Department and Fiscal Year.

    Query params:
    - po: optional PO name to filter by. Falls back to module-level selected_po.
    - department: optional department name to filter by. Falls back to selected_department.
    - fiscal_year or cap_year: optional fiscal year to filter by. Falls back to selected_cap_year.
    Response: { 'projects': [ ... ] }
    """
    try:
        po = request.args.get('po') or selected_po
        department = request.args.get('department') or selected_department
        fiscal_year = request.args.get('fiscal_year') or request.args.get('cap_year') or selected_cap_year

        df = get_projects_display()
        if df is None or df.empty:
            return {'projects': []}, 200

        proj_col = 'project_name' if 'project_name' in df.columns else ( 'name' if 'name' in df.columns else (df.columns[0] if len(df.columns)>0 else None))
        dept_col = next((c for c in ('department_name','department','name') if c in df.columns), None)
        po_col = next((c for c in ('po_name','po','name_po') if c in df.columns), None)
        fy_col = next((c for c in ('fiscal_year','Fiscal Year') if c in df.columns), None)

        filt = df
        if po and po_col in filt.columns:
            filt = filt[filt[po_col].astype(str) == str(po)]
        if department and dept_col in filt.columns:
            filt = filt[filt[dept_col].astype(str) == str(department)]
        if fiscal_year and fy_col in filt.columns:
            filt = filt[filt[fy_col].astype(str) == str(fiscal_year)]

        try:
            projects = list(dict.fromkeys([p for p in filt[proj_col].dropna().astype(str).tolist()])) if proj_col in filt.columns else []
        except Exception:
            projects = []

        return {'projects': projects}, 200
    except Exception as e:
        return {'projects': [], 'error': str(e)}, 500
