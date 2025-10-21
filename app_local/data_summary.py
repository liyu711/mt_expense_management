from flask import render_template, request, Blueprint
from backend.connect_local import connect_local, select_all_from_table
from backend.display_names import DISPLAY_NAMES
from backend import get_departments_display
import pandas as pd

data_summary_bp = Blueprint('data_summary', __name__, template_folder='templates')

# module-level selection state for data_summary page
selected_po = None


@data_summary_bp.route('/data_summary', methods=['GET', 'POST'])
def data_summary():
    conn = connect_local()
    cursor, cnxn = conn.connect_to_db()

    # Prepare DataFrame variables from database tables for use in summary and filtering
    po_df = select_all_from_table(cursor, cnxn, 'pos')
    departments_df = get_departments_display()
    project_df = select_all_from_table(cursor, cnxn, 'projects')
    IO_df = select_all_from_table(cursor, cnxn, 'ios')
    pc_forecast_df = select_all_from_table(cursor, cnxn, 'project_forecasts_pc')
    non_pc_forecast_df = select_all_from_table(cursor, cnxn, 'project_forecasts_nonpc')
    expense_df = select_all_from_table(cursor, cnxn, 'expenses')
    budget_df = select_all_from_table(cursor, cnxn, 'budgets')
    funding_df = select_all_from_table(cursor, cnxn, 'fundings')
    fiscal_years = [str(y) for y in range(2020, 2036)]

    # build simple list of PO dicts (id, name) from po_df for template iteration
    pos = []
    if po_df is not None:
        try:
            for _, r in po_df.iterrows():
                pname = None
                if 'name' in r.index and r['name'] not in (None, ''):
                    pname = r['name']
                elif 'Name' in r.index and r['Name'] not in (None, ''):
                    pname = r['Name']
                pid = None
                if 'id' in r.index and r['id'] not in (None, ''):
                    try:
                        pid = int(r['id'])
                    except Exception:
                        try:
                            pid = int(str(r['id']))
                        except Exception:
                            pid = None
                pos.append({'id': pid, 'name': pname})
        except Exception:
            pos = []

    # convert departments_df (DataFrame) to list of dicts for template
    departments = []
    if departments_df is not None:
        try:
            raw_departments = departments_df.to_dict(orient='records')
        except Exception:
            raw_departments = []

        # normalize record keys to provide 'name' for template and filter by selected_po
        try:
            for d in raw_departments:
                # department name candidates
                dept_name = d.get('department_name') or d.get('name') or d.get('name_dept') or d.get('name_departments')
                po_name = d.get('po_name') or d.get('name_po') or d.get('po')
                rec = {'id': d.get('id'), 'name': dept_name, 'po_name': po_name}
                # apply server-side filter by selected_po if provided
                if selected_po and selected_po != '' and selected_po != 'All':
                    if po_name == selected_po:
                        departments.append(rec)
                else:
                    departments.append(rec)
        except Exception:
            departments = []

    # selected filter placeholders (dropdowns removed)
    selected_department = None
    return render_template('pages/data_summary.html', summary_row=None, pos=pos, selected_po=selected_po, departments=departments, selected_department=selected_department)


@data_summary_bp.route('/data_summary/po_selection', methods=['POST'])
def po_selection():
    """Endpoint to receive PO selection from client and update server-side selected_po."""
    global selected_po
    try:
        # accept JSON or form-encoded
        if request.is_json:
            payload = request.get_json()
            val = payload.get('po')
        else:
            val = request.form.get('po')
        # set global selection
        selected_po = val
        return {'status': 'ok', 'selected_po': selected_po}, 200
    except Exception as e:
        return {'status': 'error', 'message': str(e)}, 500
