from flask import render_template, request, Blueprint
from backend.connect_local import connect_local, select_all_from_table
from backend.display_names import DISPLAY_NAMES
from backend import get_departments_display
import pandas as pd

data_vis = Blueprint('data_vis', __name__, template_folder='templates')

# module-level selection state for data_summary page


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


# data_summary route handlers moved to app_local/data_summary.py
