
from flask import Flask, render_template, Blueprint, request, redirect, url_for
from backend.modify_table_local import add_entry
from backend import upload_budgets_local, upload_budgets_local_m
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
            {'name': 'po', 'type': 'select', 'label': 'PO', 'options': []},
            {'name': 'department', 'type': 'select', 'label': 'BU', 'options': []}
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
            {'name': 'po', 'type': 'select', 'label': 'PO', 'options': []},
        ],
        'merge_on': ['name'],
        'columns': ['name', 'po_id']
    },
    'modify_staff_cost': {
        'title': 'Modify Staff Cost',
        'table_name': 'human_resource_cost',
        'fields': [
            {'name': 'po', 'type': 'select', 'label': 'PO', 'options': []},
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
            if field['name'] == 'po' and field['type'] == 'select':
                try:
                    db = connect_local()
                    cursor, cnxn = db.connect_to_db()
                    pdf = select_all_from_table(cursor, cnxn, 'pos')
                    po_options = pdf['name'].tolist() if pdf is not None and 'name' in pdf.columns else []
                except Exception:
                    po_options = []
                field['options'] = po_options
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

    # If this is the modify_staff_categories form, fetch PO options for dropdown
    if action == 'modify_staff_categories':
        po_options = fetch_options('pos', 'name')
        for field in config['fields']:
            if field.get('name') == 'po' and field.get('type') == 'select':
                field['options'] = po_options

    # If this is the modify_staff_cost form, populate staff category and year options
    if action == 'modify_staff_cost':
        staff_options = fetch_options('human_resource_categories', 'name')
        po_options = fetch_options('pos', 'name')
        year_options = [str(y) for y in range(2020, 2031)]
        for field in config['fields']:
            if field['name'] == 'po' and field['type'] == 'select':
                field['options'] = po_options
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
        # Support combined Project+IO page: a hidden field 'form_kind' can override handling
        form_kind = form_data.get('form_kind')
        table_name = form_data.pop('table_name', config['table_name'])

        # If we're on the modify_project route but the form_kind indicates IO,
        # handle this POST as an IO submission.
        if action == 'modify_project' and form_kind == 'io':
            # Build df_upload using IO config fields
            try:
                io_fields_cfg = modify_table_config.get('modify_io', {}).get('fields', [])
                io_field_names = [f['name'] for f in io_fields_cfg]
                io_row = {k: v for k, v in form_data.items() if k in io_field_names}
                df_upload = pd.DataFrame([io_row])

                # Accept empty IO input; default to -1 when missing/empty
                try:
                    if 'IO' in df_upload.columns:
                        df_upload['IO'] = pd.to_numeric(df_upload['IO'], errors='coerce').fillna(-1).astype(int)
                except Exception:
                    try:
                        df_upload['IO'] = -1
                    except Exception:
                        pass

                # Prevent duplicate IOs for the same project_id
                skip_io_insert = False
                try:
                    db = connect_local()
                    cursor, cnxn = db.connect_to_db()
                    proj_df = select_all_from_table(cursor, cnxn, 'projects')
                    ios_df = select_all_from_table(cursor, cnxn, 'ios')
                    proj_map = dict(zip(proj_df['name'], proj_df['id'])) if 'name' in proj_df.columns and 'id' in proj_df.columns else {}
                    proj_name_val = None
                    if 'project_name' in df_upload.columns and not df_upload.empty:
                        try:
                            proj_name_val = df_upload.iloc[0]['project_name']
                        except Exception:
                            proj_name_val = None
                    project_id_val = proj_map.get(proj_name_val)
                    if project_id_val is not None and 'project_id' in ios_df.columns:
                        try:
                            existing_ids = set(ios_df['project_id'].dropna().astype(int).tolist())
                        except Exception:
                            existing_ids = set(ios_df['project_id'].dropna().tolist())
                        if int(project_id_val) in existing_ids:
                            skip_io_insert = True
                except Exception:
                    skip_io_insert = False

                # Insert IO only if not duplicate
                try:
                    if not skip_io_insert:
                        merge_columns = modify_table_config.get('modify_io', {}).get('columns', list(io_row.keys()))
                        merge_on = modify_table_config.get('modify_io', {}).get('merge_on', list(io_row.keys())[0] if io_row else None)
                        res = add_entry(df_upload, 'ios', merge_columns, merge_on)
                except Exception:
                    pass
            except Exception:
                pass

            # redirect back to combined page
            return redirect(url_for('modify_tables.modify_table_router', action='modify_project'))

        # Only use fields defined in config for normal handling
        field_names = [f['name'] for f in config['fields']]
        row = {k: v for k, v in form_data.items() if k in field_names}
        if action == 'upload_budget':
            # Custom upload_budget logic goes here
            # You can add your own database logic here
            df_upload = pd.DataFrame([row])
            df_upload.columns = ['PO', 'Department', 'fiscal_year', 'human_resource_expense', 'non_personnel_expense']
            # Use the de-duplicating uploader to avoid duplicates on (PO, Department, Fiscal Year)
            # This function maps PO/Department to ids and appends only non-existing keys
            try:
                upload_budgets_local_m(df_upload)
            except Exception:
                # Fallback to original uploader if de-dup fails (should be rare)
                upload_budgets_local(df_upload)

            # msg = f"Budget uploaded: {row}"
            # return msg
        else:
            # Default: use add_entry for other actions
            df_upload = pd.DataFrame([row])
            # Special handling for modify_staff_categories: map PO to po_id and rename to expected columns
            if action == 'modify_staff_categories':
                try:
                    db = connect_local()
                    cursor, cnxn = db.connect_to_db()
                    pos_df = select_all_from_table(cursor, cnxn, 'pos')
                    po_map = dict(zip(pos_df['name'], pos_df['id'])) if 'name' in pos_df.columns and 'id' in pos_df.columns else {}
                    # Map selected PO name to po_id
                    df_upload['po_id'] = df_upload.get('po').map(po_map) if 'po' in df_upload.columns else None
                except Exception:
                    # Fallback: leave po_id as None if mapping fails
                    pass
                # Rename UI field to backend column
                try:
                    df_upload = df_upload.rename(columns={'category': 'name'})
                except Exception:
                    pass
                # Keep only expected columns
                expected_cols = modify_table_config.get(action, {}).get('columns', [])
                keep_cols = [c for c in expected_cols if c in df_upload.columns]
                if keep_cols:
                    df_upload = df_upload[keep_cols]
            # Special handling for modify_funding: map PO and Department names to their local ids
            if action == 'modify_io':
                # Accept empty IO input; default to -1 when missing/empty
                try:
                    if 'IO' in df_upload.columns:
                        df_upload['IO'] = pd.to_numeric(df_upload['IO'], errors='coerce').fillna(-1).astype(int)
                except Exception:
                    # last-resort fallback
                    try:
                        df_upload['IO'] = -1
                    except Exception:
                        pass

                # Prevent duplicate IOs for the same project_id: if an IO already exists for this project, skip insert
                skip_io_insert = False
                try:
                    db = connect_local()
                    cursor, cnxn = db.connect_to_db()
                    proj_df = select_all_from_table(cursor, cnxn, 'projects')
                    ios_df = select_all_from_table(cursor, cnxn, 'ios')
                    proj_map = dict(zip(proj_df['name'], proj_df['id'])) if 'name' in proj_df.columns and 'id' in proj_df.columns else {}
                    proj_name_val = None
                    if 'project_name' in df_upload.columns and not df_upload.empty:
                        try:
                            proj_name_val = df_upload.iloc[0]['project_name']
                        except Exception:
                            proj_name_val = None
                    project_id_val = proj_map.get(proj_name_val)
                    if project_id_val is not None and 'project_id' in ios_df.columns:
                        try:
                            existing_ids = set(ios_df['project_id'].dropna().astype(int).tolist())
                        except Exception:
                            existing_ids = set(ios_df['project_id'].dropna().tolist())
                        if int(project_id_val) in existing_ids:
                            skip_io_insert = True
                except Exception:
                    # If duplicate check fails, don't block; proceed to attempt insert
                    skip_io_insert = False

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
                df_upload['funding'] = df_upload['funding'].astype(float)

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
                # Prevent duplicate projects by (name, department_id)
                try:
                    db = connect_local()
                    cursor, cnxn = db.connect_to_db()
                    existing = select_all_from_table(cursor, cnxn, 'projects')
                except Exception:
                    existing = None

                insert_rows = []
                try:
                    # Normalize name for compare
                    if existing is not None and not existing.empty:
                        existing = existing.copy()
                        if 'name' in existing.columns:
                            existing['__name_norm__'] = existing['name'].astype(str).str.strip().str.lower()
                        if 'department_id' in existing.columns:
                            try:
                                existing['__dept_id__'] = existing['department_id'].astype('Int64')
                            except Exception:
                                existing['__dept_id__'] = existing['department_id']

                    for _, r in df_upload.iterrows():
                        name_val = str(r.get('name')) if 'name' in df_upload.columns else None
                        dept_val = r.get('department_id') if 'department_id' in df_upload.columns else None
                        name_norm = (name_val or '').strip().lower()
                        try:
                            dept_norm = int(dept_val) if dept_val not in (None, '') else None
                        except Exception:
                            dept_norm = None

                        is_dup = False
                        if existing is not None and not existing.empty and name_norm and dept_norm is not None:
                            try:
                                dup_mask = (existing.get('__name_norm__') == name_norm) & (existing.get('__dept_id__') == dept_norm)
                                is_dup = bool(dup_mask.any())
                            except Exception:
                                is_dup = False

                        if not is_dup:
                            insert_rows.append(r)
                except Exception:
                    # If duplicate check fails, fall back to inserting
                    insert_rows = [r for _, r in df_upload.iterrows()]

                if insert_rows:
                    to_insert = pd.DataFrame(insert_rows)
                    conn = connect_local()
                    engine, _cursor, _cnxn = conn.connect_to_db(engine=True)
                    to_insert.to_sql(table_name, con=engine, if_exists='append', index=False)
            elif action == 'capex_forecast':
                # Custom handling for capex_forecast: map names to IDs, check duplicates, and upsert accordingly
                try:
                    db = connect_local()
                    cursor, cnxn = db.connect_to_db()

                    pos_df = select_all_from_table(cursor, cnxn, 'pos')
                    dept_df = select_all_from_table(cursor, cnxn, 'departments')
                    proj_df = select_all_from_table(cursor, cnxn, 'projects')

                    po_map = dict(zip(pos_df['name'], pos_df['id'])) if 'name' in pos_df.columns and 'id' in pos_df.columns else {}
                    dept_map = dict(zip(dept_df['name'], dept_df['id'])) if 'name' in dept_df.columns and 'id' in dept_df.columns else {}
                    proj_map = dict(zip(proj_df['name'], proj_df['id'])) if 'name' in proj_df.columns and 'id' in proj_df.columns else {}

                    po_name = row.get('po')
                    dept_name = row.get('department')
                    proj_name = row.get('project_name')
                    cap_year_val = None
                    try:
                        cap_year_val = int(row.get('cap_year')) if row.get('cap_year') not in (None, '') else None
                    except Exception:
                        cap_year_val = None
                    try:
                        forecast_val = float(row.get('capex_forecast')) if row.get('capex_forecast') not in (None, '') else None
                    except Exception:
                        forecast_val = None

                    po_id = po_map.get(po_name)
                    department_id = dept_map.get(dept_name)
                    project_id = proj_map.get(proj_name)
                    capex_description = row.get('capex_description')
                    cost_center = row.get('cost_center')

                    if None in (po_id, department_id, project_id, cap_year_val):
                        # Missing required identifiers; skip insert/update
                        pass
                    else:
                        # Check duplicate by (po_id, department_id, project_id, cap_year)
                        cursor.execute(
                            """
                            SELECT 1
                              FROM capex_forecasts
                             WHERE po_id = ?
                               AND department_id = ?
                               AND project_id = ?
                               AND cap_year = ?
                             LIMIT 1
                            """,
                            (int(po_id), int(department_id), int(project_id), int(cap_year_val))
                        )
                        exists = cursor.fetchone() is not None
                        if exists:
                            # Update existing entry
                            cursor.execute(
                                """
                                UPDATE capex_forecasts
                                   SET capex_description = ?,
                                       capex_forecast = ?,
                                       cost_center = ?
                                 WHERE po_id = ?
                                   AND department_id = ?
                                   AND project_id = ?
                                   AND cap_year = ?
                                """,
                                (
                                    capex_description,
                                    forecast_val,
                                    cost_center,
                                    int(po_id),
                                    int(department_id),
                                    int(project_id),
                                    int(cap_year_val),
                                ),
                            )
                        else:
                            # Insert new entry
                            cursor.execute(
                                """
                                INSERT INTO capex_forecasts (
                                    po_id, department_id, project_id, cap_year,
                                    capex_description, capex_forecast, cost_center
                                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    int(po_id),
                                    int(department_id),
                                    int(project_id),
                                    int(cap_year_val),
                                    capex_description,
                                    forecast_val,
                                    cost_center,
                                ),
                            )
                        cnxn.commit()
                except Exception:
                    # On error, fall back to no-op to avoid breaking the request
                    pass
            elif action == 'modify_io':
                # Only insert when no existing IO for the same project_id
                try:
                    if not locals().get('skip_io_insert', False):
                        res = add_entry(df_upload, table_name, merge_columns, merge_on)
                except Exception:
                    # fall back silently
                    pass
            else:
                res = add_entry(df_upload, table_name, merge_columns, merge_on)
            

    # After handling POST (or on GET), prepare table contents to display
    try:
        db = connect_local()
        cursor, cnxn = db.connect_to_db()

        # Helper to load and transform a table for display
        def load_table(tbl_name):
            try:
                df_tbl = select_all_from_table(cursor, cnxn, tbl_name)
            except Exception:
                df_tbl = pd.DataFrame()
            # Map id columns to names
            id_name_map = {
                'department_id': ('departments', 'id', 'name', 'Department'),
                'po_id': ('POs', 'id', 'name', 'PO'),
                'PO_id': ('POs', 'id', 'name', 'PO'),
                'project_id': ('projects', 'id', 'name', 'Project'),
                'io_id': ('IOs', 'id', 'IO_num', 'IO'),
                'project_category_id': ('project_categories', 'id', 'category', 'Project Category'),
            }
            for id_col, (ref_table, ref_id, ref_name, new_col_name) in id_name_map.items():
                if id_col in getattr(df_tbl, 'columns', []):
                    try:
                        ref_df = select_all_from_table(cursor, cnxn, ref_table)
                        ref_dict = dict(zip(ref_df[ref_id], ref_df[ref_name]))
                        df_tbl[new_col_name] = df_tbl[id_col].map(ref_dict)
                    except Exception:
                        pass
            drop_cols = [col for col in ['department_id', 'po_id', 'PO_id', 'project_id', 'io_id', 'project_category_id'] if col in getattr(df_tbl, 'columns', [])]
            if drop_cols:
                try:
                    df_tbl = df_tbl.drop(columns=drop_cols)
                except Exception:
                    pass
            # Transform for display
            try:
                df_tbl = transform_table(df_tbl, tbl_name, cursor, cnxn)
            except Exception:
                pass
            try:
                return df_tbl.columns.tolist(), df_tbl.values.tolist()
            except Exception:
                return [], []

        # If action is modify_project, render combined page with both Projects and IOs
        if action == 'modify_project':
            columns_project, data_project = load_table('projects')
            columns_io, data_io = load_table('ios')

            # Prepare fields for both forms
            project_fields = modify_table_config.get('modify_project', {}).get('fields', [])
            # Ensure options are populated (already done earlier for modify_project)
            project_fields = config['fields']

            # IO fields: populate project_name options
            io_fields = [dict(f) for f in modify_table_config.get('modify_io', {}).get('fields', [])]
            try:
                proj_df = select_all_from_table(cursor, cnxn, 'projects')
                proj_options = proj_df['name'].dropna().astype(str).tolist() if 'name' in proj_df.columns else []
            except Exception:
                proj_options = []
            for f in io_fields:
                if f.get('name') == 'project_name' and f.get('type') == 'select':
                    f['options'] = proj_options

            # Provide project list (id + name) for edit dropdown
            project_names = []
            try:
                if 'id' in proj_df.columns and 'name' in proj_df.columns:
                    project_names = [{ 'id': int(r['id']), 'name': str(r['name']) } for _, r in proj_df[['id','name']].dropna().iterrows()]
                else:
                    project_names = [{ 'id': None, 'name': n } for n in proj_options]
            except Exception:
                project_names = [{ 'id': None, 'name': n } for n in proj_options]

            # Provide IO entries (id, IO number, project name) for IO edit dropdown
            io_entries = []
            try:
                ios_df = select_all_from_table(cursor, cnxn, 'ios')
                if ios_df is None:
                    ios_df = pd.DataFrame()
                # Map project_id to name
                pmap = {}
                try:
                    if 'id' in proj_df.columns and 'name' in proj_df.columns:
                        pmap = dict(zip(proj_df['id'], proj_df['name']))
                except Exception:
                    pmap = {}
                for _, r in ios_df.iterrows():
                    io_id = int(r['id']) if 'id' in ios_df.columns and pd.notna(r.get('id')) else None
                    io_num = int(r['IO_num']) if 'IO_num' in ios_df.columns and pd.notna(r.get('IO_num')) else None
                    pname = None
                    try:
                        if 'project_id' in ios_df.columns and pd.notna(r.get('project_id')):
                            pname = pmap.get(int(r['project_id']))
                    except Exception:
                        pname = None
                    io_entries.append({'id': io_id, 'io': io_num, 'project_name': pname})
            except Exception:
                io_entries = []

            return render_template(
                'pages/modify_project_io.html',
                title='Modify Project & IO',
                project_fields=project_fields,
                io_fields=io_fields,
                columns_project=columns_project,
                data_project=data_project,
                columns_io=columns_io,
                data_io=data_io,
                project_names=project_names,
                io_entries=io_entries,
            )
        else:
            # Default: render single table view
            columns, data = load_table(config['table_name'])
            return render_template(
                'pages/modify_table.html',
                title=config['title'],
                table_name=config['table_name'],
                fields=config['fields'],
                columns=columns,
                data=data,
            )
    except Exception:
        # Fallback to generic empty render
        return render_template(
            'pages/modify_table.html',
            title=config['title'],
            table_name=config['table_name'],
            fields=config['fields'],
            columns=[],
            data=[],
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


@modify_tables.route('/capex_forecast/change_capex_forecast', methods=['POST'])
def change_capex_forecast():
    """Handle modify action to change an existing capex forecast entry.

    Expects form fields: po, department, cap_year, project_name, capex_description, capex_forecast, cost_center
    Updates capex_forecasts.capex_forecast and cost_center WHERE po_id, department_id, project_id, cap_year and capex_description match.
    """
    form = dict(request.form)
    po = form.get('po')
    department = form.get('department')
    cap_year = form.get('cap_year')
    project_name = form.get('project_name')
    capex_description = form.get('capex_description')
    capex_forecast = form.get('capex_forecast')
    cost_center = form.get('cost_center')

    try:
        # Map names to IDs
        db = connect_local()
        cursor, cnxn = db.connect_to_db()

        pos_df = select_all_from_table(cursor, cnxn, 'pos')
        dept_df = select_all_from_table(cursor, cnxn, 'departments')
        proj_df = select_all_from_table(cursor, cnxn, 'projects')

        po_map = dict(zip(pos_df['name'], pos_df['id'])) if 'name' in pos_df.columns and 'id' in pos_df.columns else {}
        dept_map = dict(zip(dept_df['name'], dept_df['id'])) if 'name' in dept_df.columns and 'id' in dept_df.columns else {}
        proj_map = dict(zip(proj_df['name'], proj_df['id'])) if 'name' in proj_df.columns and 'id' in proj_df.columns else {}

        po_id = po_map.get(po)
        department_id = dept_map.get(department)
        project_id = proj_map.get(project_name)

        # Coerce numeric types
        try:
            cap_year_val = int(cap_year) if cap_year not in (None, '') else None
        except Exception:
            cap_year_val = None
        try:
            forecast_val = float(capex_forecast) if capex_forecast not in (None, '') else None
        except Exception:
            forecast_val = None

        # Basic validation: required mapping fields must exist
        if po_id is None or department_id is None or project_id is None or cap_year_val is None:
            return redirect(url_for('modify_tables.modify_table_router', action='capex_forecast'))

        # Perform update. Per requirement, identify rows by PO/Department/Project/Cap Year
        # and update capex_description, capex_forecast and cost_center.
        cursor.execute(
            """
            UPDATE capex_forecasts
               SET capex_description = ?,
                   capex_forecast = ?,
                   cost_center = ?
             WHERE po_id = ?
               AND department_id = ?
               AND project_id = ?
               AND cap_year = ?
            """,
            (
                capex_description,
                forecast_val,
                cost_center,
                int(po_id),
                int(department_id),
                int(project_id),
                int(cap_year_val),
            ),
        )
        cnxn.commit()
    except Exception:
        # Swallow errors and redirect back for now
        pass

    return redirect(url_for('modify_tables.modify_table_router', action='capex_forecast'))


@modify_tables.route('/modify_project/details', methods=['GET'])
def get_project_details():
    """Return JSON details for a given project by name or id.

    Query params:
    - name: project name (preferred)
    - id: project id (optional)
    Response: { id, name, category, department, fiscal_year, po }
    """
    try:
        name = request.args.get('name')
        pid = request.args.get('id')

        db = connect_local()
        cursor, cnxn = db.connect_to_db()
        proj_df = select_all_from_table(cursor, cnxn, 'projects')
        if proj_df is None or proj_df.empty:
            return {'error': 'no projects'}, 404

        row = None
        if pid is not None and 'id' in proj_df.columns:
            try:
                pid_int = int(pid)
                row = proj_df[proj_df['id'] == pid_int].head(1)
            except Exception:
                row = None
        if (row is None or row.empty) and name is not None and 'name' in proj_df.columns:
            row = proj_df[proj_df['name'].astype(str) == str(name)].head(1)
        if row is None or row.empty:
            return {'error': 'not found'}, 404

        row = row.iloc[0]
        out = {
            'id': int(row['id']) if 'id' in proj_df.columns else None,
            'name': row['name'] if 'name' in proj_df.columns else None,
            'fiscal_year': int(row['fiscal_year']) if 'fiscal_year' in proj_df.columns and pd.notna(row['fiscal_year']) else None,
        }

        # Map ids to names
        cat_name = None
        try:
            if 'category_id' in proj_df.columns and pd.notna(row.get('category_id')):
                cats = select_all_from_table(cursor, cnxn, 'project_categories')
                cdict = dict(zip(cats['id'], cats['category'])) if 'id' in cats.columns and 'category' in cats.columns else {}
                cat_name = cdict.get(int(row['category_id']))
        except Exception:
            pass

        dept_name, po_name = None, None
        try:
            if 'department_id' in proj_df.columns and pd.notna(row.get('department_id')):
                depts = select_all_from_table(cursor, cnxn, 'departments')
                ddict = dict(zip(depts['id'], depts['name'])) if 'id' in depts.columns and 'name' in depts.columns else {}
                dept_id = int(row['department_id'])
                dept_name = ddict.get(dept_id)
                # get PO from departments.po_id -> POs.name
                if 'po_id' in depts.columns:
                    try:
                        po_id = int(depts[depts['id'] == dept_id]['po_id'].iloc[0])
                        pos = select_all_from_table(cursor, cnxn, 'POs')
                        pdict = dict(zip(pos['id'], pos['name'])) if 'id' in pos.columns and 'name' in pos.columns else {}
                        po_name = pdict.get(po_id)
                    except Exception:
                        po_name = None
        except Exception:
            pass

        out.update({'category': cat_name, 'department': dept_name, 'po': po_name})
        return out, 200
    except Exception as e:
        return {'error': str(e)}, 500


@modify_tables.route('/modify_project/change_project', methods=['POST'])
def change_project():
    """Update an existing project row by id or name."""
    form = dict(request.form)
    project_id = form.get('project_id')
    existing_name = form.get('existing_project') or form.get('existing_name')
    name = form.get('name')
    category = form.get('category')
    department = form.get('department')
    fiscal_year = form.get('fiscal_year')

    try:
        db = connect_local()
        cursor, cnxn = db.connect_to_db()

        # Map category and department to ids
        cat_id = None
        dept_id = None
        try:
            cats = select_all_from_table(cursor, cnxn, 'project_categories')
            cdict = dict(zip(cats['category'], cats['id'])) if 'category' in cats.columns and 'id' in cats.columns else {}
            if category in cdict:
                cat_id = int(cdict[category])
        except Exception:
            pass
        try:
            depts = select_all_from_table(cursor, cnxn, 'departments')
            ddict = dict(zip(depts['name'], depts['id'])) if 'name' in depts.columns and 'id' in depts.columns else {}
            if department in ddict:
                dept_id = int(ddict[department])
        except Exception:
            pass

        # Coerce fiscal year
        fy_val = None
        try:
            fy_val = int(fiscal_year) if fiscal_year not in (None, '') else None
        except Exception:
            fy_val = None

        # Resolve target project id
        pid_val = None
        if project_id not in (None, ''):
            try:
                pid_val = int(project_id)
            except Exception:
                pid_val = None
        if pid_val is None and existing_name:
            try:
                proj_df = select_all_from_table(cursor, cnxn, 'projects')
                if 'name' in proj_df.columns and 'id' in proj_df.columns:
                    match = proj_df[proj_df['name'].astype(str) == str(existing_name)].head(1)
                    if not match.empty:
                        pid_val = int(match.iloc[0]['id'])
            except Exception:
                pid_val = None

        if pid_val is None:
            return redirect(url_for('modify_tables.modify_table_router', action='modify_project'))

        # Build update fields and execute
        sets = []
        params = []
        if name not in (None, ''):
            sets.append('name = ?')
            params.append(name)
        if cat_id is not None:
            sets.append('category_id = ?')
            params.append(cat_id)
        if dept_id is not None:
            sets.append('department_id = ?')
            params.append(dept_id)
        if fy_val is not None:
            sets.append('fiscal_year = ?')
            params.append(fy_val)

        if sets:
            q = f"UPDATE projects SET {', '.join(sets)} WHERE id = ?"
            params.append(pid_val)
            cursor.execute(q, tuple(params))
            cnxn.commit()
    except Exception:
        # swallow errors for now
        pass

    return redirect(url_for('modify_tables.modify_table_router', action='modify_project'))


@modify_tables.route('/modify_io/details', methods=['GET'])
def get_io_details():
    """Return JSON details for a given IO by id, io number, or project name.

    Query params: id (preferred) or io or project_name
    Response: { id, io, project_name }
    """
    try:
        q_id = request.args.get('id')
        q_io = request.args.get('io')
        q_project = request.args.get('project_name')

        db = connect_local()
        cursor, cnxn = db.connect_to_db()
        ios_df = select_all_from_table(cursor, cnxn, 'ios')
        if ios_df is None or ios_df.empty:
            return {'error': 'no ios'}, 404

        row = None
        if q_id is not None and 'id' in ios_df.columns:
            try:
                qi = int(q_id)
                row = ios_df[ios_df['id'] == qi].head(1)
            except Exception:
                row = None
        if (row is None or row.empty) and q_io is not None and 'IO_num' in ios_df.columns:
            try:
                qin = int(q_io)
                row = ios_df[ios_df['IO_num'].astype(int) == qin].head(1)
            except Exception:
                row = ios_df[ios_df['IO_num'].astype(str) == str(q_io)].head(1)
        if (row is None or row.empty) and q_project is not None and 'project_id' in ios_df.columns:
            # resolve project name -> id
            proj_df = select_all_from_table(cursor, cnxn, 'projects')
            pmap = dict(zip(proj_df['name'], proj_df['id'])) if 'name' in proj_df.columns and 'id' in proj_df.columns else {}
            pid = pmap.get(q_project)
            if pid is not None:
                row = ios_df[ios_df['project_id'] == pid].head(1)
        if row is None or row.empty:
            return {'error': 'not found'}, 404

        row = row.iloc[0]
        io_id = int(row['id']) if 'id' in ios_df.columns else None
        io_num = int(row['IO_num']) if 'IO_num' in ios_df.columns and pd.notna(row.get('IO_num')) else None
        project_name = None
        try:
            if 'project_id' in ios_df.columns:
                proj_df = select_all_from_table(cursor, cnxn, 'projects')
                pmap2 = dict(zip(proj_df['id'], proj_df['name'])) if 'id' in proj_df.columns and 'name' in proj_df.columns else {}
                project_name = pmap2.get(int(row['project_id']))
        except Exception:
            pass
        return {'id': io_id, 'io': io_num, 'project_name': project_name}, 200
    except Exception as e:
        return {'error': str(e)}, 500


@modify_tables.route('/modify_io/change_io', methods=['POST'])
def change_io():
    """Update an existing IO row by id or project."""
    form = dict(request.form)
    io_id = form.get('io_id') or form.get('existing_io_id')
    existing_project = form.get('existing_project') or form.get('existing_project_name')
    new_io = form.get('IO') or form.get('io')
    new_project = form.get('project_name')

    try:
        db = connect_local()
        cursor, cnxn = db.connect_to_db()
        ios_df = select_all_from_table(cursor, cnxn, 'ios')
        proj_df = select_all_from_table(cursor, cnxn, 'projects')

        # Resolve target IO row id
        target_id = None
        if io_id not in (None, ''):
            try:
                target_id = int(io_id)
            except Exception:
                target_id = None
        if target_id is None and existing_project:
            try:
                pmap = dict(zip(proj_df['name'], proj_df['id'])) if 'name' in proj_df.columns and 'id' in proj_df.columns else {}
                pid = pmap.get(existing_project)
                if pid is not None and 'project_id' in ios_df.columns and 'id' in ios_df.columns:
                    match = ios_df[ios_df['project_id'] == int(pid)].head(1)
                    if not match.empty:
                        target_id = int(match.iloc[0]['id'])
            except Exception:
                target_id = None

        if target_id is None:
            return redirect(url_for('modify_tables.modify_table_router', action='modify_project'))

        # Build new values
        set_parts = []
        params = []
        # New IO number (optional; default remains unchanged if not provided)
        if new_io not in (None, ''):
            try:
                new_io_val = int(float(new_io))
            except Exception:
                # keep as is; if cannot coerce, skip update to IO
                new_io_val = None
            if new_io_val is not None:
                set_parts.append('IO_num = ?')
                params.append(new_io_val)

        # New project mapping (respect duplicate guard: only allow if the new project has no existing IO)
        if new_project not in (None, ''):
            try:
                pmap2 = dict(zip(proj_df['name'], proj_df['id'])) if 'name' in proj_df.columns and 'id' in proj_df.columns else {}
                new_pid = pmap2.get(new_project)
            except Exception:
                new_pid = None
            if new_pid is not None:
                # Check current row's project_id
                current_row = ios_df[ios_df['id'] == target_id].head(1)
                current_pid = int(current_row.iloc[0]['project_id']) if not current_row.empty and 'project_id' in current_row.columns and pd.notna(current_row.iloc[0]['project_id']) else None
                if current_pid is None or current_pid != int(new_pid):
                    # ensure no other IO exists for the target project
                    try:
                        existing_ids = set(ios_df['project_id'].dropna().astype(int).tolist()) if 'project_id' in ios_df.columns else set()
                    except Exception:
                        existing_ids = set(ios_df['project_id'].dropna().tolist()) if 'project_id' in ios_df.columns else set()
                    if int(new_pid) in existing_ids:
                        # duplicate exists; do not switch project
                        pass
                    else:
                        set_parts.append('project_id = ?')
                        params.append(int(new_pid))

        if set_parts:
            q = f"UPDATE ios SET {', '.join(set_parts)} WHERE id = ?"
            params.append(target_id)
            cursor.execute(q, tuple(params))
            cnxn.commit()
    except Exception:
        pass

    return redirect(url_for('modify_tables.modify_table_router', action='modify_project'))

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


@modify_tables.route('/modify_project/departments', methods=['GET'])
def modify_project_department_update():
    """Return a JSON list of departments filtered by PO for Modify Project modal.

    Query params:
    - po: PO name to filter by
    Response: { 'departments': [ ... ] }
    """
    try:
        po = request.args.get('po')
        df = get_departments_display()
        if df is None or df.empty:
            return {'departments': []}, 200
        # get_departments_display returns columns name_departments and name_po
        filt = df
        if po and 'name_po' in filt.columns:
            filt = filt[filt['name_po'].astype(str) == str(po)]
        depts = []
        if 'name_departments' in filt.columns:
            try:
                depts = filt['name_departments'].dropna().astype(str).tolist()
            except Exception:
                depts = filt['name_departments'].tolist()
        return {'departments': list(dict.fromkeys(depts))}, 200
    except Exception as e:
        return {'departments': [], 'error': str(e)}, 500


@modify_tables.route('/modify_staff_cost/categories', methods=['GET'])
def staff_cost_categories_update():
    """Return a JSON list of staff categories filtered by PO.

    Query params:
    - po: optional PO name to filter by; if omitted, returns all categories
    Response: { 'categories': [ ... ] }
    """
    try:
        po_name = request.args.get('po')
        db = connect_local()
        cursor, cnxn = db.connect_to_db()

        # Map PO name -> id
        po_id = None
        if po_name:
            try:
                pos_df = select_all_from_table(cursor, cnxn, 'pos')
                if pos_df is not None and not pos_df.empty and 'name' in pos_df.columns and 'id' in pos_df.columns:
                    pmap = dict(zip(pos_df['name'], pos_df['id']))
                    pid = pmap.get(po_name)
                    po_id = int(pid) if pid is not None else None
            except Exception:
                po_id = None

        # Load human_resource_categories and filter by po_id if provided
        cats_df = select_all_from_table(cursor, cnxn, 'human_resource_categories')
        if cats_df is None or cats_df.empty:
            return {'categories': []}, 200
        filt = cats_df
        if po_id is not None and 'po_id' in filt.columns:
            try:
                filt = filt[filt['po_id'].astype('Int64') == po_id]
            except Exception:
                filt = filt[filt['po_id'] == po_id]

        names = []
        if 'name' in filt.columns:
            try:
                names = filt['name'].dropna().astype(str).tolist()
            except Exception:
                names = filt['name'].tolist()
        # de-duplicate while preserving order
        names = list(dict.fromkeys(names))
        return {'categories': names}, 200
    except Exception as e:
        return {'categories': [], 'error': str(e)}, 500
