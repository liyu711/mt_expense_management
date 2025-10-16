
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
        # For projects table, rename 'name' -> 'Project Name' for display consistency
        if table_name == 'projects' and 'name' in df.columns:
            df = df.rename(columns={'name': 'Project Name'})

        # departments: rename name -> Department Name
        if table_name == 'departments' and 'name' in df.columns:
            df = df.rename(columns={'name': 'Department Name'})

        # budgets renames
        if table_name == 'budgets':
            if 'human_resource_expense' in df.columns:
                df = df.rename(columns={'human_resource_expense': 'Personnel Budget'})
            if 'non_personnel_expense' in df.columns:
                df = df.rename(columns={'non_personnel_expense': 'Non-personnel Budget'})

        # fundings: friendly column names
        if table_name == 'fundings':
            if 'funding' in df.columns:
                df = df.rename(columns={'funding': 'Funding'})
            if 'funding_from' in df.columns:
                df = df.rename(columns={'funding_from': 'Funding From'})
            if 'funding_for' in df.columns:
                df = df.rename(columns={'funding_for': 'Funding For'})

        # capex_forecasts: friendly display names
        if table_name == 'capex_forecasts':
            if 'capex_description' in df.columns:
                df = df.rename(columns={'capex_description': 'Description'})
            if 'capex_forecast' in df.columns:
                df = df.rename(columns={'capex_forecast': 'CapEx Forecast'})
            if 'cost_center' in df.columns:
                df = df.rename(columns={'cost_center': 'Cost Center'})

        # capex_budgets: friendly display names
        if table_name == 'capex_budgets':
            if 'capex_description' in df.columns:
                df = df.rename(columns={'capex_description': 'Description'})
            if 'budget' in df.columns:
                df = df.rename(columns={'budget': 'Budget'})

        # project_forecasts_nonpc: rename non_personnel_expense -> Non-personnel Expense
        if table_name == 'project_forecasts_nonpc' and 'non_personnel_expense' in df.columns:
            df = df.rename(columns={'non_personnel_expense': 'Non-personnel Expense'})

        # project_forecasts_pc: map human_resource_category_id -> Staff Category (name)
        if table_name == 'project_forecasts_pc':
            # rename FTE and personnel expense columns for display
            # handle both the correct column and any historical typo
            if 'human_resource_fte' in df.columns:
                df = df.rename(columns={'human_resource_fte': 'Work Hours(FTE)'})
            if 'personnel_expense' in df.columns:
                df = df.rename(columns={'personnel_expense': 'Personnel Expense'})

            if 'human_resource_category_id' in df.columns:
                ref_df = select_all_from_table(cursor, cnxn, 'human_resource_categories')
                # expect ref_df has 'id' and 'name'
                ref_dict = dict(zip(ref_df['id'], ref_df['name']))
                df['Staff Category'] = df['human_resource_category_id'].map(ref_dict)
                # drop the id column to prefer friendly name display
                df = df.drop(columns=['human_resource_category_id'])

            # Compute Personnel Cost by looking up hourly/unit cost from human_resource_cost
            # We need mapping from Staff Category name -> id to query human_resource_cost table
            try:
                # build name->id map from human_resource_categories
                hr_ref = select_all_from_table(cursor, cnxn, 'human_resource_categories')
                name_to_id = dict(zip(hr_ref['name'], hr_ref['id'])) if 'name' in hr_ref.columns and 'id' in hr_ref.columns else {}
                # load human_resource_cost table once and pivot for fast lookup
                hr_cost_df = select_all_from_table(cursor, cnxn, 'human_resource_cost')
                # ensure expected columns exist
                if not hr_cost_df.empty and 'category_id' in hr_cost_df.columns and 'year' in hr_cost_df.columns and 'cost' in hr_cost_df.columns:
                    # create a lookup dict keyed by (category_id, year)
                    hr_cost_df_local = hr_cost_df[['category_id', 'year', 'cost']].copy()
                    # force types for reliable lookup
                    try:
                        hr_cost_df_local['year'] = hr_cost_df_local['year'].astype(int)
                    except Exception:
                        pass
                    cost_lookup = {(int(r['category_id']), int(r['year'])): float(r['cost']) for _, r in hr_cost_df_local.iterrows() if r['category_id'] is not None}

                    # Determine which column holds work-hours (FTE) after renames
                    fte_col = None
                    if 'Work Hours(FTE)' in df.columns:
                        fte_col = 'Work Hours(FTE)'
                    elif 'human_resource_fte' in df.columns:
                        fte_col = 'human_resource_fte'

                    # Determine fiscal year column after transform
                    fy_col = 'Fiscal Year' if 'Fiscal Year' in df.columns else ('fiscal_year' if 'fiscal_year' in df.columns else None)

                    personnel_costs = []
                    for _, row in df.iterrows():
                        try:
                            # map Staff Category name back to id
                            staff_name = row.get('Staff Category') if 'Staff Category' in row.index else None
                            cat_id = name_to_id.get(staff_name) if staff_name is not None else None
                            fy = int(row.get(fy_col)) if fy_col and row.get(fy_col) not in (None, '') else None
                            # get work hours
                            wh = row.get(fte_col) if fte_col else None
                            # coerce work hours to float
                            try:
                                wh = float(wh) if wh not in (None, '') else 0.0
                            except Exception:
                                wh = 0.0
                            if cat_id is None or fy is None:
                                personnel_costs.append(None)
                                continue
                            key = (int(cat_id), int(fy))
                            hourly = cost_lookup.get(key)
                            if hourly is None:
                                personnel_costs.append(None)
                            else:
                                personnel_costs.append(hourly * wh)
                        except Exception:
                            personnel_costs.append(None)

                    # attach the new column
                    df['Personnel Cost'] = personnel_costs
            except Exception:
                # non-fatal: if cost lookup fails, continue without Personnel Cost
                pass
            # If the DataFrame itself has a 'name' column (ambiguous), prefer Staff Category label
            if 'name' in df.columns:
                if 'Staff Category' in df.columns:
                    df = df.drop(columns=['name'])
                else:
                    df = df.rename(columns={'name': 'StaffW Category'})

        # expenses: map co_object_id -> CO Object Name, drop cost_element_id, rename co_element_name
        if table_name == 'expenses':
            if 'co_object_id' in df.columns:
                ref_df = select_all_from_table(cursor, cnxn, 'co_object_names')
                # expect ref_df has 'id' and 'name'
                ref_dict = dict(zip(ref_df['id'], ref_df['name']))
                df['CO Object Name'] = df['co_object_id'].map(ref_dict)
                df = df.drop(columns=['co_object_id'])
            if 'cost_element_id' in df.columns:
                df = df.drop(columns=['cost_element_id'])
            if 'co_element_name' in df.columns:
                df = df.rename(columns={'co_element_name': 'CO Object Name'})
            # Display-friendly names for expenses table
            if 'from_period' in df.columns:
                # rename the period column to Month for display
                df = df.rename(columns={'from_period': 'Month'})
            if 'expense_value' in df.columns:
                # rename expense value to Expenditure
                df = df.rename(columns={'expense_value': 'Expenditure'})

        # project_categories: category -> Project Category
        if table_name == 'project_categories' and 'category' in df.columns:
            df = df.rename(columns={'category': 'Project Category'})

        # human_resource_categories: rename name -> Staff Category
        if table_name == 'human_resource_categories' and 'name' in df.columns:
            df = df.rename(columns={'name': 'Staff Category'})

        # human_resource_cost: map category_id -> Staff Category (name) and rename year
        if table_name == 'human_resource_cost':
            if 'category_id' in df.columns:
                ref_df = select_all_from_table(cursor, cnxn, 'human_resource_categories')
                ref_dict = dict(zip(ref_df['id'], ref_df['name'])) if 'id' in ref_df.columns and 'name' in ref_df.columns else {}
                df['Staff Category'] = df['category_id'].map(ref_dict)
                # drop the numeric id column to prefer friendly name display
                df = df.drop(columns=['category_id'])
            # If fiscal year or year appears, prefer 'Year' or keep as-is
            if 'year' in df.columns:
                df = df.rename(columns={'year': 'Year'})
            # Rename cost -> Unit Expense (k CNY) for clearer display
            if 'cost' in df.columns:
                df = df.rename(columns={'cost': 'Unit Expense (k CNY)'})

        # pos/POs: rename name -> PO Name for PO display
        if table_name in ('pos', 'POs') and 'name' in df.columns:
            df = df.rename(columns={'name': 'PO Name'})

        # IOs: rename IO_num -> IO Number (apply for 'IOs' and 'ios')
        if table_name in ('IOs', 'ios') and 'IO_num' in df.columns:
            df = df.rename(columns={'IO_num': 'IO Number'})

        # Global renames
        if 'fiscal_year' in df.columns:
            df = df.rename(columns={'fiscal_year': 'Fiscal Year'})
        if 'cap_year' in df.columns:
            df = df.rename(columns={'cap_year': 'Capital Year'})
        # Reorder: place PO and Department-related columns after 'id' when present
        current_cols = list(df.columns)

        # Special-case reorder for project_forecasts_nonpc:
        # Put Department (or Department Name), then IO, then Project Category directly after id
        if table_name == 'project_forecasts_nonpc':
            new_order = []
            if 'id' in current_cols:
                new_order.append('id')

            # Prefer the display name if present
            dept_candidates = ['Department Name', 'Department', 'department']
            dept = next((c for c in dept_candidates if c in current_cols), None)
            io_candidates = ['IO', 'IO number']
            io_col = next((c for c in io_candidates if c in current_cols), None)
            pc_col = 'Project Category' if 'Project Category' in current_cols else None

            if dept and dept not in new_order:
                new_order.append(dept)
            if io_col and io_col not in new_order:
                new_order.append(io_col)
            if pc_col and pc_col not in new_order:
                new_order.append(pc_col)

            # Append remaining columns preserving original order
            for c in current_cols:
                if c not in new_order:
                    new_order.append(c)

            if new_order and new_order != current_cols:
                new_order = [c for c in new_order if c in df.columns]
                df = df.loc[:, new_order]
        # Special-case reorder for project_forecasts_pc: place IO, Project, Project Category, Staff Category after Fiscal Year
        elif table_name == 'project_forecasts_pc':
            # We'll keep original order up to Fiscal Year (inclusive), then inject the desired columns,
            # then append the remaining columns in original order.
            desired_after_fy = ['IO', 'IO number', 'Project', 'Project Category', 'Staff Category']
            # prefer display name 'Fiscal Year' but fall back to raw 'fiscal_year' if necessary
            fy_name = 'Fiscal Year' if 'Fiscal Year' in current_cols else ('fiscal_year' if 'fiscal_year' in current_cols else None)
            new_order = []
            if fy_name:
                # append columns up to and including Fiscal Year
                for c in current_cols:
                    new_order.append(c)
                    if c == fy_name:
                        break
                # Before inserting IO/Project/etc, ensure PO and Department (if present)
                # are preserved and placed before the injected block so they are not moved behind.
                po_col = next((c for c in ['PO', 'PO_id'] if c in current_cols), None)
                dept_col = next((c for c in ['Department Name', 'Department', 'department'] if c in current_cols), None)
                if po_col and po_col not in new_order:
                    new_order.append(po_col)
                if dept_col and dept_col not in new_order:
                    new_order.append(dept_col)

                # then append desired cols (choose the first matching variant for IO)
                io_col = next((c for c in ['IO', 'IO number'] if c in current_cols), None)
                if io_col and io_col not in new_order:
                    new_order.append(io_col)
                if 'Project' in current_cols and 'Project' not in new_order:
                    new_order.append('Project')
                if 'Project Category' in current_cols and 'Project Category' not in new_order:
                    new_order.append('Project Category')
                if 'Staff Category' in current_cols and 'Staff Category' not in new_order:
                    new_order.append('Staff Category')
            else:
                # if no Fiscal Year present, fall back to generic behavior
                if 'id' in current_cols:
                    new_order.append('id')
            # Append the rest preserving order
            for c in current_cols:
                if c not in new_order:
                    new_order.append(c)
            if new_order and new_order != current_cols:
                new_order = [c for c in new_order if c in df.columns]
                df = df.loc[:, new_order]
        else:
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
    #     'human_resource_cost', 'project_forecasts_nonpc', 'project_forecasts_pc',
    #     'capex_forecasts', 'capex_budgets', 'capex_expenses'
    # ]

    options = [
        'projects', 'departments', 'POs', 'budgets', 'expenses', 'fundings',
        'project_categories', 'IOs', 'human_resource_categories',
        'project_forecasts_nonpc', 'project_forecasts_pc', 'human_resource_cost',
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
