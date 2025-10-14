
from flask import Flask, flash, render_template, request, redirect, url_for, Blueprint
from backend.connect_local import connect_local, select_all_from_table
from backend.display_names import DISPLAY_NAMES


select_data = Blueprint('select_data', __name__, template_folder='templates')


def transform_table(df, table_name, cursor, cnxn):
    """Apply table-specific joins, renames and reorder columns according to UI rules.

    - projects: join project_categories on category_id=id -> 'Project Category'
    - departments: rename 'name' -> 'Department Name'
    - budgets: rename human_resource_expense -> 'Personnel Budget', non_personnel_expense -> 'Non-personnel Budget'
    - expenses: join co_object_names on co_object_id=id -> 'Cost Element Name'; drop cost_element_id; rename co_element_name -> 'Cost Element Name'
    - project_categories: rename 'category' -> 'Project Category'
    - IOs: rename 'IO_num' -> 'IO number'
    - global: rename fiscal_year -> 'Fiscal Year', cap_year -> 'Capital Year'
    - reorder: if 'id' exists, move PO and Department-related columns to directly after id
    """
    if df is None:
        return df

    # Make a copy of column order
    cols = list(df.columns)

    try:
        # projects: category_id -> Project Category
        if table_name == 'projects' and 'category_id' in df.columns:
            ref_df = select_all_from_table(cursor, cnxn, 'project_categories')
            ref_dict = dict(zip(ref_df['id'], ref_df['category']))
            df['Project Category'] = df['category_id'].map(ref_dict)
            if 'category_id' in df.columns:
                df = df.drop(columns=['category_id'])

        # departments: rename name -> Department Name
        if table_name == 'departments' and 'name' in df.columns:
            df = df.rename(columns={'name': 'Department Name'})

        # budgets renames
        if table_name == 'budgets':
            if 'human_resource_expense' in df.columns:
                df = df.rename(columns={'human_resource_expense': 'Personnel Budget'})
            if 'non_personnel_expense' in df.columns:
                df = df.rename(columns={'non_personnel_expense': 'Non-personnel Budget'})

        # expenses: map co_object_id -> Cost Element Name, drop cost_element_id, rename co_element_name
        if table_name == 'expenses':
            if 'co_object_id' in df.columns:
                ref_df = select_all_from_table(cursor, cnxn, 'co_object_names')
                # expect ref_df has 'id' and 'name'
                ref_dict = dict(zip(ref_df['id'], ref_df['name']))
                df['Cost Element Name'] = df['co_object_id'].map(ref_dict)
                df = df.drop(columns=['co_object_id'])
            if 'cost_element_id' in df.columns:
                df = df.drop(columns=['cost_element_id'])
            if 'co_element_name' in df.columns:
                df = df.rename(columns={'co_element_name': 'Cost Element Name'})

        # project_categories: category -> Project Category
        if table_name == 'project_categories' and 'category' in df.columns:
            df = df.rename(columns={'category': 'Project Category'})

        # IOs: rename IO_num -> IO number
        if table_name == 'IOs' and 'IO_num' in df.columns:
            df = df.rename(columns={'IO_num': 'IO number'})

        # Global renames
        if 'fiscal_year' in df.columns:
            df = df.rename(columns={'fiscal_year': 'Fiscal Year'})
        if 'cap_year' in df.columns:
            df = df.rename(columns={'cap_year': 'Capital Year'})

        # Reorder: place PO and Department-related columns after 'id' when present
        current_cols = list(df.columns)
        new_order = []
        if 'id' in current_cols:
            new_order.append('id')
        # Candidate names for PO/Department columns (after id)
        candidates = ['PO', 'PO_id', 'Department', 'Department Name', 'department']
        for c in candidates:
            if c in current_cols and c not in new_order:
                new_order.append(c)
        # Append the rest of the columns preserving order
        for c in current_cols:
            if c not in new_order:
                new_order.append(c)

        # Reindex dataframe columns if any change
        if new_order and new_order != current_cols:
            # Only keep columns that exist (defensive)
            new_order = [c for c in new_order if c in df.columns]
            df = df.loc[:, new_order]

    except Exception:
        # Non-fatal: if any mapping fails, return df as-is
        return df

    return df


@select_data.route('/select', methods=['GET', 'POST'])
def select():
    conn = connect_local()
    data = None
    columns = None
    selected_option = None
    x_col = None
    y_col = None
    plot_type = None
    page = 1
    per_page = 50  # You can adjust this value for more/less rows per page
    total_pages = 1

    # all the avaliable options
    # options = [
    #     'projects', 'departments', 'POs', 'cost_elements', 'budgets', 'expenses', 'fundings',
    #     'project_categories', 'co_object_names', 'IOs', 'IO_CE_connection', 'human_resource_categories',
    #     'human_resource_expense', 'project_forecasts_nonpc', 'project_forecasts_pc',
    #     'capex_forecasts', 'capex_budgets', 'capex_expenses'
    # ]

    options = [
        'projects', 'departments', 'POs', 'budgets', 'expenses', 'fundings',
        'project_categories', 'IOs', 'human_resource_categories',
        'project_forecasts_nonpc', 'project_forecasts_pc',
        'capex_forecasts', 'capex_budgets', 'capex_expenses'
    ]

    if request.method == 'POST':
        table_name = request.form.get('table_name')
        selected_option = table_name
        x_col = request.form.get('x_col')
        y_col = request.form.get('y_col')
        plot_type = request.form.get('plot_type')
        # For pagination, get page from form if present
        try:
            page = int(request.form.get('page', 1))
        except Exception:
            page = 1

        if selected_option:
            cursor, cnxn = conn.connect_to_db()
            df = select_all_from_table(cursor, cnxn, selected_option)
            # Replace IDs with names for department_id, PO_id, project_id if present
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
            # Prefer to show names instead of IDs in columns
            drop_cols = [col for col in ['department_id', 'po_id', 'PO_id', 'project_id', 'io_id', 'project_category_id'] if col in df.columns]
            df = df.drop(columns=drop_cols)
            # Apply table-specific transforms (joins, renames, reorder)
            df = transform_table(df, selected_option, cursor, cnxn)
            columns = df.columns.tolist()
            # Pagination
            total_rows = len(df)
            total_pages = max(1, (total_rows + per_page - 1) // per_page)
            if page < 1:
                page = 1
            if page > total_pages:
                page = total_pages
            start = (page - 1) * per_page
            end = start + per_page
            data = df.iloc[start:end].values.tolist()
    elif request.method == 'GET':
        # For GET, allow page navigation via query string
        selected_option = request.args.get('table_name')
        try:
            page = int(request.args.get('page', 1))
        except Exception:
            page = 1
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
            # Apply table-specific transforms (joins, renames, reorder)
            df = transform_table(df, selected_option, cursor, cnxn)
            columns = df.columns.tolist()
            total_rows = len(df)
            total_pages = max(1, (total_rows + per_page - 1) // per_page)
            if page < 1:
                page = 1
            if page > total_pages:
                page = total_pages
            start = (page - 1) * per_page
            end = start + per_page
            data = df.iloc[start:end].values.tolist()

    return render_template(
        'pages/select.html',
        options=options,
        selected_option=selected_option,
        data=data,
        columns=columns,
        x_col=x_col,
        y_col=y_col,
        plot_type=plot_type,
        page=page,
        total_pages=total_pages,
        per_page=per_page
        , display_names=DISPLAY_NAMES
    )
