
from flask import Flask, render_template, Blueprint, request, redirect, url_for
from backend.modify_table_local import add_entry
from backend import upload_budgets_local, upload_budgets_local_m
import pandas as pd
import os
from backend.connect_local import connect_local, select_all_from_table
from app_local.select_data import transform_table
from backend.create_display_table import get_departments_display, get_projects_display

# Shared column ordering helper
def standardize_columns_order(columns, table_name=None):
    """Reorder columns to a standard order across tables.

    Priority (front to back):
    1) PO
    2) BU
    3) Fiscal Year / Capex Year (whichever exists; if both, Fiscal Year first)
    4) Project Category
    5) Project (Project Name)
    6) IO
    Then: all remaining columns in their original order.

    We match common synonyms but do not rename columns here.
    """
    if not columns:
        return columns

    cols = list(columns)

    # Define synonym groups for detection
    group_ID = ['id', 'ID']
    group_PO = ['PO', 'po', 'po_name', 'name_po']
    group_BU = ['BU', 'Department', 'department', 'department_name', 'name_departments']
    group_FY = ['Fiscal Year', 'fiscal_year']
    group_CapY = ['Capex Year', 'cap_year', 'Capital Year']
    group_ProjCat = ['Project Category', 'project_category', 'category', 'category_name']
    # For project name, prefer friendly 'Project'; only treat raw 'name' as project name for projects table
    group_Project = ['Project'] + (['name', 'project_name'] if (table_name == 'projects') else ['project_name'])
    # include display variants used in the projects display
    group_IO = ['IOs', 'IO', 'IO_num', 'io']

    # Default ordering; but for projects we want Project and IOs immediately after id
    if table_name == 'projects':
        priority_groups = [
            group_ID,
            group_Project,
            group_IO,
            group_PO,
            group_BU,
            group_FY,
            group_CapY,
            group_ProjCat,
        ]
    else:
        priority_groups = [
            group_ID,
            group_PO,
            group_BU,
            group_FY,
            group_CapY,
            group_ProjCat,
            group_Project,
            group_IO,
        ]

    picked = []
    for group in priority_groups:
        for alias in group:
            if alias in cols and alias not in picked:
                picked.append(alias)

    # Append remaining columns preserving original order
    remaining = [c for c in cols if c not in picked]
    return picked + remaining

modify_tables = Blueprint('modify_tables', __name__, template_folder='templates')

# module-level selected PO for modify forms (capex and others)
selected_po = None
selected_department = None
selected_cap_year = None
selected_project = None

# Map route to table and form fields
modify_table_config = {
    'modify_department': {
        'title': 'Modify BU',
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
            {'name': 'category', 'type': 'text', 'label': 'Staff Category'}
        ],
        'merge_on': ['name'],
        'columns': ['name']
    },
    'modify_staff_cost': {
        'title': 'Modify Staff Cost',
        'table_name': 'human_resource_cost',
        'fields': [
            {'name': 'po', 'type': 'select', 'label': 'PO', 'options': []},
            {'name': 'department', 'type': 'select', 'label': 'BU', 'options': []},
            {'name': 'staff_category', 'type': 'select', 'label': 'Staff Category', 'options': []},
            {'name': 'year', 'type': 'select', 'label': 'Year', 'options': []},
            {'name': 'cost', 'type': 'number', 'label': 'Cost'}
        ],
        # Merge and identify rows by (po_id, department_id, category_id, year)
        'merge_on': ['po_id', 'department_id', 'category_id', 'year'],
        'columns': ['po_id', 'department_id', 'category_id', 'year', 'cost']
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
            {'name': 'human_resource_expense', 'type':'number', 'label': 'Personnel Budget (k CNY)'},
            {'name': 'non_personnel_expense', 'type':'number', 'label': 'Non-personnel Budget (k CNY)'}
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
            {'name': 'funding', 'type': 'number', 'label': 'Funding (k CNY)'},
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
            {'name': 'approved_budget', 'type': 'number', 'label': 'Approved Budget (k CNY)'}
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

    # DRY helper utilities for populating select options
    def set_field_options(fields, field_name, options):
        for f in fields:
            if f.get('name') == field_name and f.get('type') == 'select':
                f['options'] = options or []

    def get_years(start=2020, end=2031):
        return [str(y) for y in range(start, end)]

    def populate_for_action(action_key, fields):
        sources = {
            'upload_budget': {
                'po': ('table', 'pos', 'name'),
                'department': ('table', 'departments', 'name'),
                'fiscal_year': ('years', 2020, 2031),
            },
            'modify_funding': {
                'po': ('table', 'pos', 'name'),
                'department': ('table', 'departments', 'name'),
                'fiscal_year': ('years', 2020, 2031),
            },
            'capex_forecast': {
                'po': ('table', 'pos', 'name'),
                'department': ('table', 'departments', 'name'),
                'project_name': ('table', 'projects', 'name'),
                'cap_year': ('years', 2020, 2031),
            },
            'capex_budget': {
                'po': ('table', 'pos', 'name'),
                'department': ('table', 'departments', 'name'),
                'project_name': ('table', 'projects', 'name'),
                'cap_year': ('years', 2020, 2031),
            },
            'modify_project': {
                'category': ('table', 'project_categories', 'category'),
                'po': ('table', 'pos', 'name'),
                'department': ('table', 'departments', 'name'),
                'fiscal_year': ('years', 2020, 2031),
            },
            'modify_department': {
                'po': ('table', 'pos', 'name'),
            },
            'modify_staff_categories': {
                'po': ('table', 'pos', 'name'),
                'department': ('table', 'departments', 'name'),
            },
            'modify_staff_cost': {
                'po': ('table', 'pos', 'name'),
                'department': ('table', 'departments', 'name'),
                'staff_category': ('table', 'human_resource_categories', 'name'),
                'year': ('years', 2020, 2031),
            },
            'modify_io': {
                'project_name': ('table', 'projects', 'name'),
            },
        }

        spec = sources.get(action_key)
        if not spec:
            return
        for fname, src in spec.items():
            try:
                if not isinstance(src, tuple):
                    continue
                kind = src[0]
                if kind == 'table':
                    _, table, col = src
                    opts = fetch_options(table, col)
                    set_field_options(fields, fname, opts)
                elif kind == 'years':
                    _, start, end = src
                    set_field_options(fields, fname, get_years(start, end))
            except Exception:
                set_field_options(fields, fname, [])

    populate_for_action(action, config['fields'])

    if request.method == 'POST':
        form_data = dict(request.form)
        # Support combined Project+IO page: a hidden field 'form_kind' can override handling
        form_kind = form_data.get('form_kind')
        table_name = form_data.pop('table_name', config['table_name'])

        # If we're on the modify_project route but the form_kind indicates IO,
        # handle this POST as an IO submission.
        if action == 'modify_project' and form_kind == 'io':
            # Build df_upload using IO config fields and insert only if IO number not used globally
            try:
                io_fields_cfg = modify_table_config.get('modify_io', {}).get('fields', [])
                io_field_names = [f['name'] for f in io_fields_cfg]
                io_row = {k: v for k, v in form_data.items() if k in io_field_names}
                df_upload = pd.DataFrame([io_row])

                # Normalize IO number
                io_val = None
                try:
                    if 'IO' in df_upload.columns:
                        io_val = int(float(df_upload.iloc[0]['IO']))
                except Exception:
                    io_val = None

                # Map project_name to project_id
                pid_val = None
                try:
                    db = connect_local()
                    cursor, cnxn = db.connect_to_db()
                    proj_df = select_all_from_table(cursor, cnxn, 'projects')
                    if proj_df is not None and not proj_df.empty and 'name' in proj_df.columns and 'id' in proj_df.columns:
                        pmap = dict(zip(proj_df['name'], proj_df['id']))
                        pname = df_upload.iloc[0].get('project_name') if not df_upload.empty else None
                        if pname in pmap:
                            pid_val = int(pmap[pname])
                except Exception:
                    pid_val = None

                # Insert a new IO row only if IO_num not already used globally
                try:
                    if io_val is not None and pid_val is not None:
                        db = connect_local()
                        cursor, cnxn = db.connect_to_db()
                        # check global uniqueness on IO_num
                        try:
                            cursor.execute("SELECT 1 FROM ios WHERE IO_num = ? LIMIT 1", (int(io_val),))
                            exists = cursor.fetchone() is not None
                        except Exception:
                            exists = False
                        if not exists:
                            cursor.execute("INSERT INTO ios (IO_num, project_id) VALUES (?, ?)", (int(io_val), int(pid_val)))
                            cnxn.commit()
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
                    # Map selected Department name to department_id
                    try:
                        dept_df = select_all_from_table(cursor, cnxn, 'departments')
                        dept_map = dict(zip(dept_df['name'], dept_df['id'])) if 'name' in dept_df.columns and 'id' in dept_df.columns else {}
                    except Exception:
                        dept_map = {}
                    df_upload['department_id'] = df_upload.get('department').map(dept_map) if 'department' in df_upload.columns else None
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
                # Insert only if IO number is unique globally; allow multiple IO rows per project
                try:
                    db = connect_local()
                    cursor, cnxn = db.connect_to_db()
                    # Normalize IO number
                    io_val = None
                    try:
                        if 'IO' in df_upload.columns:
                            io_val = int(float(df_upload.iloc[0]['IO']))
                    except Exception:
                        io_val = None
                    # Map project name to id
                    pid_val = None
                    try:
                        proj_df = select_all_from_table(cursor, cnxn, 'projects')
                        if proj_df is not None and not proj_df.empty and 'name' in proj_df.columns and 'id' in proj_df.columns:
                            pmap = dict(zip(proj_df['name'], proj_df['id']))
                            pname = df_upload.iloc[0].get('project_name') if not df_upload.empty else None
                            if pname in pmap:
                                pid_val = int(pmap[pname])
                    except Exception:
                        pid_val = None
                    # Insert only if unique IO_num
                    try:
                        if io_val is not None and pid_val is not None:
                            cursor.execute("SELECT 1 FROM ios WHERE IO_num = ? LIMIT 1", (int(io_val),))
                            exists = cursor.fetchone() is not None
                            if not exists:
                                cursor.execute("INSERT INTO ios (IO_num, project_id) VALUES (?, ?)", (int(io_val), int(pid_val)))
                                cnxn.commit()
                    except Exception:
                        pass
                except Exception:
                    pass

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

                    # Map PO name -> po_id
                    try:
                        pos_df = select_all_from_table(cursor, cnxn, 'pos')
                        po_map = dict(zip(pos_df['name'], pos_df['id'])) if 'name' in pos_df.columns and 'id' in pos_df.columns else {}
                        if 'po' in df_upload.columns:
                            df_upload['po_id'] = df_upload['po'].map(po_map)
                    except Exception:
                        df_upload['po_id'] = None

                    # Map Department name -> department_id
                    try:
                        dept_df = select_all_from_table(cursor, cnxn, 'departments')
                        dept_map = dict(zip(dept_df['name'], dept_df['id'])) if 'name' in dept_df.columns and 'id' in dept_df.columns else {}
                        if 'department' in df_upload.columns:
                            df_upload['department_id'] = df_upload['department'].map(dept_map)
                    except Exception:
                        df_upload['department_id'] = None

                    # Map Staff Category name -> category_id
                    hr_df = select_all_from_table(cursor, cnxn, 'human_resource_categories')
                    hr_map = dict(zip(hr_df['name'], hr_df['id'])) if 'name' in hr_df.columns and 'id' in hr_df.columns else {}
                    df_upload['category_id'] = df_upload.get('staff_category').map(hr_map) if 'staff_category' in df_upload.columns else None

                    # Types
                    if 'year' in df_upload.columns:
                        try:
                            df_upload['year'] = df_upload['year'].astype(int)
                        except Exception:
                            pass
                    if 'cost' in df_upload.columns:
                        try:
                            df_upload['cost'] = df_upload['cost'].astype(float)
                        except Exception:
                            pass

                    # Keep only expected columns for upload
                    expected_cols = modify_table_config.get(action).get('columns', [])
                    keep_cols = [c for c in expected_cols if c in df_upload.columns]
                    if keep_cols:
                        df_upload = df_upload[keep_cols]
                except Exception:
                    # Fallback minimal mapping
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

                # Also handle optional IO creation in the same submit (merged add flow)
                # New behavior: Insert only if IO_num doesn't already exist globally; allow multiple IOs per project
                try:
                    # Gather all IO values from potentially multiple inputs named 'IO'
                    raw_list = []
                    try:
                        raw_list = request.form.getlist('IO')
                    except Exception:
                        # Fallback to single value if getlist isn't available
                        val = form_data.get('IO') or form_data.get('io')
                        raw_list = [val] if val not in (None, '') else []

                    # Resolve project id by (name, department_id)
                    proj_name = row.get('name')
                    dept_id_val = None
                    try:
                        if 'department_id' in df_upload.columns and not df_upload.empty:
                            dept_id_val = int(df_upload.iloc[0]['department_id']) if pd.notna(df_upload.iloc[0]['department_id']) else None
                    except Exception:
                        dept_id_val = None

                    pid_val = None
                    try:
                        proj_tbl = select_all_from_table(cursor, cnxn, 'projects')
                        if proj_tbl is not None and not proj_tbl.empty:
                            cand = proj_tbl[proj_tbl.get('name', proj_tbl.columns[0]).astype(str).str.strip() == str(proj_name).strip()]
                            if dept_id_val is not None and 'department_id' in proj_tbl.columns:
                                try:
                                    cand = cand[cand['department_id'].astype(int) == int(dept_id_val)]
                                except Exception:
                                    cand = cand[cand['department_id'] == dept_id_val]
                            if not cand.empty and 'id' in cand.columns:
                                pid_val = int(cand.iloc[0]['id'])
                    except Exception:
                        pid_val = None

                    if pid_val is not None and raw_list:
                        # Normalize and de-duplicate IO values
                        io_vals = []
                        for io_raw in raw_list:
                            if io_raw in (None, ''):
                                continue
                            try:
                                f = float(io_raw)
                                io_vals.append(int(f))
                            except Exception:
                                # skip values that cannot be coerced
                                continue
                        seen = set()
                        io_unique = []
                        for v in io_vals:
                            if v not in seen:
                                seen.add(v)
                                io_unique.append(v)

                        for io_val in io_unique:
                            try:
                                cursor.execute("SELECT 1 FROM ios WHERE IO_num = ? LIMIT 1", (int(io_val),))
                                exists = cursor.fetchone() is not None
                            except Exception:
                                exists = False
                            if not exists:
                                try:
                                    cursor.execute("INSERT INTO ios (IO_num, project_id) VALUES (?, ?)", (int(io_val), int(pid_val)))
                                    cnxn.commit()
                                except Exception:
                                    # best-effort; do not break project add
                                    pass
                except Exception:
                    # swallow IO upsert errors to not break project add
                    pass
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
                # handled above (manual insert with global uniqueness check)
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
            # If select_all_from_table returned no columns (empty DF), try to obtain column names from the DB schema
            try:
                cols = list(df_tbl.columns) if (df_tbl is not None and hasattr(df_tbl, 'columns')) else []
                if not cols:
                    # Try SQLite PRAGMA first
                    try:
                        cursor.execute(f"PRAGMA table_info({tbl_name})")
                        info = cursor.fetchall()
                        cols = [row[1] for row in info] if info else []
                    except Exception:
                        cols = []
                    # Fallback: do a LIMIT 0 SELECT to get cursor.description (works for many DBs)
                    if not cols:
                        try:
                            cursor.execute(f"SELECT * FROM {tbl_name} LIMIT 0")
                            desc = cursor.description
                            cols = [d[0] for d in desc] if desc else []
                        except Exception:
                            cols = []
                    if cols:
                        df_tbl = pd.DataFrame(columns=cols)
            except Exception:
                # best-effort; leave df_tbl as-is
                pass
            # Map id columns to names
            id_name_map = {
                'department_id': ('departments', 'id', 'name', 'Department'),
                'po_id': ('POs', 'id', 'name', 'PO'),
                'PO_id': ('POs', 'id', 'name', 'PO'),
                'project_id': ('projects', 'id', 'name', 'Project'),
                'io_id': ('IOs', 'id', 'IO_num', 'IO'),
                'project_category_id': ('project_categories', 'id', 'category', 'Project Category'),
                # Map HR category id to friendly name for staff cost table
                'category_id': ('human_resource_categories', 'id', 'name', 'Staff Category'),
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
            # Reorder columns to standardized order
            try:
                ordered_cols = standardize_columns_order(df_tbl.columns.tolist(), table_name=tbl_name)
                df_tbl = df_tbl[ordered_cols]
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


# moved to app_local/staff_cost_routes.py: /change_staff_cost


# moved to app_local/capex_forecast_routes.py: /capex_forecast/change_capex_forecast


@modify_tables.route('/upload_budget/change_budget', methods=['POST'])
def change_budget():
    """Handle modify action to change an existing budget entry.

    Expects form fields: po, department, fiscal_year, human_resource_expense, non_personnel_expense
    Updates budgets row identified by (po_id, department_id, fiscal_year).
    """
    form = dict(request.form)
    # New & potentially changed values
    po = form.get('po')
    department = form.get('department')
    fiscal_year = form.get('fiscal_year')
    hr_exp = form.get('human_resource_expense')
    nonpc_exp = form.get('non_personnel_expense')

    # Original key fields (hidden inputs) to locate the existing row before updating keys
    orig_po = form.get('original_po') or po
    orig_department = form.get('original_department') or department
    orig_fiscal_year = form.get('original_fiscal_year') or fiscal_year

    try:
        db = connect_local()
        cursor, cnxn = db.connect_to_db()

        # Map names to IDs
        pos_df = select_all_from_table(cursor, cnxn, 'pos')
        dept_df = select_all_from_table(cursor, cnxn, 'departments')

        po_map = dict(zip(pos_df['name'], pos_df['id'])) if 'name' in pos_df.columns and 'id' in pos_df.columns else {}
        dept_map = dict(zip(dept_df['name'], dept_df['id'])) if 'name' in dept_df.columns and 'id' in dept_df.columns else {}

        # Map new identifiers
        po_id = po_map.get(po)
        department_id = dept_map.get(department)

        # Map original identifiers for WHERE clause
        orig_po_id = po_map.get(orig_po)
        orig_department_id = dept_map.get(orig_department)

        # Coerce numeric types
        try:
            fy_val = int(fiscal_year) if fiscal_year not in (None, '') else None
        except Exception:
            fy_val = None
        try:
            hr_val = float(hr_exp) if hr_exp not in (None, '') else None
        except Exception:
            hr_val = None
        try:
            nonpc_val = float(nonpc_exp) if nonpc_exp not in (None, '') else None
        except Exception:
            nonpc_val = None

        if None in (po_id, department_id, fy_val, hr_val, nonpc_val, orig_po_id, orig_department_id):
            return redirect(url_for('modify_tables.modify_table_router', action='upload_budget'))

        # Original fiscal year coercion
        try:
            orig_fy_val = int(orig_fiscal_year) if orig_fiscal_year not in (None, '') else None
        except Exception:
            orig_fy_val = fy_val  # fallback

        if orig_fy_val is None:
            return redirect(url_for('modify_tables.modify_table_router', action='upload_budget'))

        # Update both key columns and value columns
        cursor.execute(
            """
            UPDATE budgets
               SET po_id = ?,
                   department_id = ?,
                   fiscal_year = ?,
                   human_resource_expense = ?,
                   non_personnel_expense = ?
             WHERE po_id = ?
               AND department_id = ?
               AND fiscal_year = ?
            """,
            (
                int(po_id),
                int(department_id),
                fy_val,
                hr_val,
                nonpc_val,
                int(orig_po_id),
                int(orig_department_id),
                orig_fy_val,
            ),
        )
        cnxn.commit()
    except Exception:
        pass

    return redirect(url_for('modify_tables.modify_table_router', action='upload_budget'))


@modify_tables.route('/modify_funding/change_funding', methods=['POST'])
def change_funding():
    """Handle modify action to change an existing funding entry.

    Expects form fields: po, department, fiscal_year, funding, funding_from, funding_for
    Updates fundings row identified by (po_id, department_id, fiscal_year).
    """
    form = dict(request.form)
    # New (possibly changed) identifiers
    po = form.get('po')
    department = form.get('department')
    fiscal_year = form.get('fiscal_year')
    funding = form.get('funding')
    funding_from = form.get('funding_from')
    funding_for = form.get('funding_for')

    # Original key fields (hidden inputs)
    orig_po = form.get('original_po') or po
    orig_department = form.get('original_department') or department
    orig_fiscal_year = form.get('original_fiscal_year') or fiscal_year

    try:
        db = connect_local()
        cursor, cnxn = db.connect_to_db()

        # Map names to IDs
        pos_df = select_all_from_table(cursor, cnxn, 'pos')
        dept_df = select_all_from_table(cursor, cnxn, 'departments')

        po_map = dict(zip(pos_df['name'], pos_df['id'])) if 'name' in pos_df.columns and 'id' in pos_df.columns else {}
        dept_map = dict(zip(dept_df['name'], dept_df['id'])) if 'name' in dept_df.columns and 'id' in dept_df.columns else {}

        # New identifiers
        po_id = po_map.get(po)
        department_id = dept_map.get(department)
        # Original identifiers for WHERE clause
        orig_po_id = po_map.get(orig_po)
        orig_department_id = dept_map.get(orig_department)

        # Coerce numeric types
        try:
            fy_val = int(fiscal_year) if fiscal_year not in (None, '') else None
        except Exception:
            fy_val = None
        try:
            fund_val = float(funding) if funding not in (None, '') else None
        except Exception:
            fund_val = None

        if None in (po_id, department_id, fy_val, fund_val, orig_po_id, orig_department_id):
            return redirect(url_for('modify_tables.modify_table_router', action='modify_funding'))

        # Original fiscal year coercion
        try:
            orig_fy_val = int(orig_fiscal_year) if orig_fiscal_year not in (None, '') else None
        except Exception:
            orig_fy_val = fy_val

        if orig_fy_val is None:
            return redirect(url_for('modify_tables.modify_table_router', action='modify_funding'))

        cursor.execute(
            """
            UPDATE fundings
               SET po_id = ?,
                   department_id = ?,
                   fiscal_year = ?,
                   funding = ?,
                   funding_from = ?,
                   funding_for = ?
             WHERE po_id = ?
               AND department_id = ?
               AND fiscal_year = ?
            """,
            (
                int(po_id),
                int(department_id),
                fy_val,
                fund_val,
                funding_from,
                funding_for,
                int(orig_po_id),
                int(orig_department_id),
                orig_fy_val,
            ),
        )
        cnxn.commit()
    except Exception:
        pass

    return redirect(url_for('modify_tables.modify_table_router', action='modify_funding'))


@modify_tables.route('/upload_budget/delete_budget', methods=['POST'])
def delete_budget():
    """Delete a budget row identified by (po, department, fiscal_year).

    Accepts JSON or form fields: po, department, fiscal_year
    """
    try:
        # Accept payload from JSON or form
        if request.is_json:
            form = request.get_json() or {}
        else:
            form = dict(request.form)

        po = form.get('po')
        department = form.get('department')
        fiscal_year = form.get('fiscal_year') or form.get('year')

        if not (po and department and fiscal_year):
            return {'status': 'error', 'message': 'Missing required fields'}, 400

        db = connect_local()
        cursor, cnxn = db.connect_to_db()

        pos_df = select_all_from_table(cursor, cnxn, 'pos')
        dept_df = select_all_from_table(cursor, cnxn, 'departments')

        po_map = dict(zip(pos_df['name'], pos_df['id'])) if pos_df is not None and not pos_df.empty and 'name' in pos_df.columns and 'id' in pos_df.columns else {}
        dept_map = dict(zip(dept_df['name'], dept_df['id'])) if dept_df is not None and not dept_df.empty and 'name' in dept_df.columns and 'id' in dept_df.columns else {}

        po_id = po_map.get(po)
        department_id = dept_map.get(department)
        try:
            fy = int(fiscal_year)
        except Exception:
            fy = None

        if po_id is None or department_id is None or fy is None:
            return {'status': 'error', 'message': 'Unable to resolve identifiers'}, 400

        cursor.execute(
            """
            DELETE FROM budgets
             WHERE po_id = ?
               AND department_id = ?
               AND fiscal_year = ?
            """,
            (int(po_id), int(department_id), int(fy))
        )
        cnxn.commit()
        return {'status': 'ok'}, 200
    except Exception as e:
        return {'status': 'error', 'message': str(e)}, 500


@modify_tables.route('/modify_funding/delete_funding', methods=['POST'])
def delete_funding():
    """Delete a funding row identified by (po, department, fiscal_year).

    Accepts JSON or form fields: po, department, fiscal_year
    """
    try:
        if request.is_json:
            form = request.get_json() or {}
        else:
            form = dict(request.form)

        po = form.get('po')
        department = form.get('department')
        fiscal_year = form.get('fiscal_year') or form.get('year')

        if not (po and department and fiscal_year):
            return {'status': 'error', 'message': 'Missing required fields'}, 400

        db = connect_local()
        cursor, cnxn = db.connect_to_db()

        pos_df = select_all_from_table(cursor, cnxn, 'pos')
        dept_df = select_all_from_table(cursor, cnxn, 'departments')

        po_map = dict(zip(pos_df['name'], pos_df['id'])) if pos_df is not None and not pos_df.empty and 'name' in pos_df.columns and 'id' in pos_df.columns else {}
        dept_map = dict(zip(dept_df['name'], dept_df['id'])) if dept_df is not None and not dept_df.empty and 'name' in dept_df.columns and 'id' in dept_df.columns else {}

        po_id = po_map.get(po)
        department_id = dept_map.get(department)
        try:
            fy = int(fiscal_year)
        except Exception:
            fy = None

        if po_id is None or department_id is None or fy is None:
            return {'status': 'error', 'message': 'Unable to resolve identifiers'}, 400

        cursor.execute(
            """
            DELETE FROM fundings
             WHERE po_id = ?
               AND department_id = ?
               AND fiscal_year = ?
            """,
            (int(po_id), int(department_id), int(fy))
        )
        cnxn.commit()
        return {'status': 'ok'}, 200
    except Exception as e:
        return {'status': 'error', 'message': str(e)}, 500




# moved to app_local/project_routes.py: /modify_project/details


# moved to app_local/project_routes.py: /modify_project/change_project


# moved to app_local/io_routes.py: /modify_io/details


# moved to app_local/io_routes.py: /modify_io/change_io

# moved to app_local/project_routes.py: /modify_project/project_ios

# moved to app_local/capex_forecast_routes.py: /capex_forecast/po_selection


# moved to app_local/capex_forecast_routes.py: /capex_forecast/department_selection


# moved to app_local/capex_forecast_routes.py: /capex_forecast/cap_year_selection


# moved to app_local/capex_forecast_routes.py: /capex_forecast/project_selection


# moved to app_local/capex_forecast_routes.py: /capex_forecast/department_update


# moved to app_local/capex_forecast_routes.py: /capex_forecast/project_update


# moved to app_local/project_routes.py: /modify_project/departments


# moved to app_local/staff_cost_routes.py: /modify_staff_cost/categories


# moved to app_local/project_routes.py: /modify_project/list


# moved to app_local/io_routes.py: /modify_io/list
