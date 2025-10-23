from flask import render_template, request, Blueprint
from backend.connect_local import connect_local, select_all_from_table
from backend.display_names import DISPLAY_NAMES
from backend import \
    get_departments_display, get_forecasts_display, get_pc_display, get_projects_display,\
    get_nonpc_display
import pandas as pd

data_summary_bp = Blueprint('data_summary', __name__, template_folder='templates')

# module-level selection state for data_summary page
selected_po = None
selected_department = None
selected_fiscal_year = None
selected_project = None


@data_summary_bp.route('/data_summary', methods=['GET', 'POST'])
def data_summary():
    conn = connect_local()
    cursor, cnxn = conn.connect_to_db()

    # Prepare DataFrame variables from database tables for use in summary and filtering
    po_df = select_all_from_table(cursor, cnxn, 'pos')
    departments_df = get_departments_display()
    project_df = get_projects_display()
    IO_df = select_all_from_table(cursor, cnxn, 'ios')
    pc_forecast_df = get_pc_display()
    non_pc_forecast_df = get_forecasts_display()
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

    # return template with current module-level selections
    # Build projects list filtered by server-side selections
    projects = []
    if project_df is not None:
        try:
            raw_projects = project_df.to_dict(orient='records')
        except Exception:
            raw_projects = []

        try:
            print('test')
            for p in raw_projects:
                print(raw_projects)
                # normalize project name candidates (include project_name)
                proj_name = p.get('project_name')
                # normalize PO name candidates
                po_name = p.get('po_name') or p.get('PO Name') or p.get('PO') or p.get('po')
                # normalize department name candidates
                dept_name = p.get('department_name') or p.get('Department Name') or p.get('Department') or p.get('department')
                # normalize fiscal year candidates
                fy = p.get('fiscal_year') or p.get('Fiscal Year') or p.get('fy') or p.get('fiscal')

                rec = {'name': proj_name, 'po_name': po_name, 'department_name': dept_name, 'fiscal_year': fy}

                # apply server-side filters if selections exist (treat 'All' as wildcard)
                ok = True
                if selected_po and selected_po != '' and selected_po != 'All':
                    # if project record lacks po_name, treat as non-matching
                    ok = ok and (po_name == selected_po)
                if selected_department and selected_department != '' and selected_department != 'All':
                    ok = ok and (dept_name == selected_department)
                if selected_fiscal_year and selected_fiscal_year != '' and selected_fiscal_year != 'All':
                    ok = ok and (str(fy) == str(selected_fiscal_year))
                if ok:
                    projects.append(rec)
                
        except Exception:
            print('test2')
            projects = []

    return render_template('pages/data_summary.html', summary_row=None, pos=pos, selected_po=selected_po, departments=departments, selected_department=selected_department, fiscal_years=fiscal_years, selected_fiscal_year=selected_fiscal_year, projects=projects, selected_project=selected_project)


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


@data_summary_bp.route('/data_summary/department_selection', methods=['POST'])
def department_selection():
    """Endpoint to receive Department selection from client and update server-side selected_department."""
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


@data_summary_bp.route('/data_summary/fiscal_year_selection', methods=['POST'])
def fiscal_year_selection():
    """Endpoint to receive Fiscal Year selection and update server-side selected_fiscal_year."""
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



@data_summary_bp.route('/data_summary/project_selection', methods=['POST'])
def project_selection():
    """Endpoint to receive Project selection and update server-side selected_project."""
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


@data_summary_bp.route('/data_summary/get_statistics', methods=['GET'])
def get_statistics():
    """Compute aggregated statistics filtered by current module-level selections.

    Returns JSON with keys:
    - non_personnel_forecast
    - personnel_forecast
    - total_forecast
    - budget
    - funding
    - total_budget
    - actual_expense
    """
    try:
        conn = connect_local()
        cursor, cnxn = conn.connect_to_db()

        # load dataframes
        # project_forecasts_nonpc: non-personnel forecasts
        nonpc = get_nonpc_display(xw)
        # project_forecasts_pc: personnel forecasts (personnel_expense)
        pc = get_pc_display()
        # budgetsxw
        budgets = select_all_from_table(cursor, cnxn, 'budgets')
        # fundings
        fundings = select_all_from_table(cursor, cnxn, 'fundings')
        # expenses
        expenses = select_all_from_table(cursor, cnxn, 'expenses')

        def apply_filters(df):
            if df is None or df.empty:
                return pd.DataFrame()
            out = df.copy()
            # helper to match selection keys across variants
            # PO
            if selected_po and selected_po != '' and selected_po != 'All':
                po_cols = [c for c in out.columns if c.lower() in ('po', 'po_name', 'po_id')]
                if po_cols:
                    col = po_cols[0]
                    out = out[out[col].astype(str) == str(selected_po)]
            # Department
            if selected_department and selected_department != '' and selected_department != 'All':
                dept_cols = [c for c in out.columns if c.lower() in ('department', 'department_name')]
                if dept_cols:
                    col = dept_cols[0]
                    out = out[out[col].astype(str) == str(selected_department)]
            # Fiscal year
            if selected_fiscal_year and selected_fiscal_year != '' and selected_fiscal_year != 'All':
                fy_cols = [c for c in out.columns if c.lower() in ('fiscal_year', 'fiscal year', 'fy')]
                if fy_cols:
                    col = fy_cols[0]
                    out = out[out[col].astype(str) == str(selected_fiscal_year)]
            # Project
            if selected_project and selected_project != '' and selected_project != 'All':
                proj_cols = [c for c in out.columns if c.lower() in ('project_name', 'project', 'project name', 'name')]
                if proj_cols:
                    col = proj_cols[0]
                    out = out[out[col].astype(str) == str(selected_project)]
            return out

        # apply filters and sum
        nonpc_f = apply_filters(nonpc)
        pc_f = apply_filters(pc)
        budgets_f = apply_filters(budgets)
        fundings_f = apply_filters(fundings)
        expenses_f = apply_filters(expenses)

        def sum_column(df, candidates):
            if df is None or df.empty:
                return 0.0
            for c in candidates:
                if c in df.columns:
                    try:
                        return float(df[c].dropna().astype(float).sum())
                    except Exception:
                        continue
            return 0.0

        non_personnel_forecast = sum_column(nonpc_f, ['non_personnel_expense', 'Non-personnel Expense', 'Non-personnel cost', 'non_personnel_cost'])
        personnel_forecast = sum_column(pc_f, ['personnel_expense', 'Personnel Expense', 'personnel_cost'])
        total_forecast = non_personnel_forecast + personnel_forecast

        budget_sum = sum_column(budgets_f, ['non_personnel_expense', 'human_resource_expense', 'Non-personnel Budget', 'Personnel Budget'])
        funding_sum = sum_column(fundings_f, ['funding', 'Funding'])
        total_budget = budget_sum + funding_sum

        actual_expense = sum_column(expenses_f, ['expenses', 'expense', 'amount', 'Actual Expenditure'])

        result = {
            'non_personnel_forecast': round(non_personnel_forecast, 2),
            'personnel_forecast': round(personnel_forecast, 2),
            'total_forecast': round(total_forecast, 2),
            'budget': round(budget_sum, 2),
            'funding': round(funding_sum, 2),
            'total_budget': round(total_budget, 2),
            'actual_expense': round(actual_expense, 2)
        }
        print(result)
        return result, 200
    except Exception as e:
        return {'error': str(e)}, 500

