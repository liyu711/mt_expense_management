from flask import render_template, request, Blueprint
from backend.connect_local import connect_local, select_all_from_table
from backend.display_names import DISPLAY_NAMES
from backend import \
    get_departments_display, get_forecasts_display, get_pc_display, \
    get_nonpc_display, get_budget_display_table, create_funding_display, get_capex_expenses_display, get_expenses_display, get_projects_display, \
    create_capex_forecast_display, create_capex_budgets_dispaly
import pandas as pd

data_summary_bp = Blueprint('data_summary', __name__, template_folder='templates')

# module-level selection state for data_summary page
selected_po = None
selected_department = None
selected_fiscal_year = None
selected_project = None  # deprecated: project filter removed
selected_project = None  # reused for project_summary page


@data_summary_bp.route('/data_summary', methods=['GET', 'POST'])
def data_summary():
    conn = connect_local()
    cursor, cnxn = conn.connect_to_db()

    # Prepare DataFrame variables from database tables for use in summary and filtering
    po_df = select_all_from_table(cursor, cnxn, 'pos')
    departments_df = get_departments_display()
    # project filter removed; no need to load projects or IOs
    pc_forecast_df = get_pc_display()
    non_pc_forecast_df = get_forecasts_display()
    expense_df = get_expenses_display()
    budget_df = get_budget_display_table()
    funding_df = select_all_from_table(cursor, cnxn, 'fundings')
    fiscal_years = [str(y) for y in range(2020, 2036)]
    capex_expenses = get_capex_expenses_display()

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

    # return template with current module-level selections (project filter removed)
    return render_template('pages/data_summary.html', summary_row=None, pos=pos, selected_po=selected_po, departments=departments, selected_department=selected_department, fiscal_years=fiscal_years, selected_fiscal_year=selected_fiscal_year)


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



# Project filter endpoint removed


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
        nonpc = get_nonpc_display()
        # project_forecasts_pc: personnel forecasts (personnel_expense)
        pc = get_pc_display()
        # budgetsxw
        budgets = get_budget_display_table()
        # fundings
        fundings = create_funding_display()
        # expenses
        expenses = get_expenses_display()
        # capex-related displays
        capex_forecast_df = create_capex_forecast_display()
        capex_budget_df = create_capex_budgets_dispaly()
        capex_expense_df = get_capex_expenses_display()

        def apply_filters(df):
            if df is None or df.empty:
                return pd.DataFrame()
            out = df.copy()

            # Use pandas boolean masking for clear, fast filtering
            mask = pd.Series(True, index=out.index)

            def find_col(candidates):
                # case-insensitive lookup of first matching column
                col_map = {c.lower(): c for c in out.columns}
                for cand in candidates:
                    key = cand.lower()
                    if key in col_map:
                        return col_map[key]
                return None

            # PO
            if selected_po and selected_po != '' and selected_po != 'All':
                po_col = find_col(['po', 'po_name', 'po name', 'po_id', 'poid', 'PO Name'])
                if po_col is not None:
                    mask &= out[po_col].astype(str) == str(selected_po)
                else:
                    mask &= False

            # Department
            if selected_department and selected_department != '' and selected_department != 'All':
                dept_col = find_col(['department', 'department_name', 'department name', 'dept', 'dept_name', 'BU Name'])
                if dept_col is not None:
                    mask &= out[dept_col].astype(str) == str(selected_department)
                else:
                    mask &= False

            # Fiscal year
            if selected_fiscal_year and selected_fiscal_year != '' and selected_fiscal_year != 'All':
                fy_col = find_col(['fiscal_year', 'fiscal year', 'fy', 'year', 'cap_year'])
                if fy_col is not None:
                    mask &= out[fy_col].astype(str) == str(selected_fiscal_year)
                else:
                    mask &= False

            # Project filter removed

            return out.loc[mask]

        # apply filters and sum
        nonpc_f = apply_filters(nonpc)
        pc_f = apply_filters(pc)
        budgets_f = apply_filters(budgets)
        fundings_f = apply_filters(fundings)
        expenses_f = apply_filters(expenses)
        capex_forecast_f = apply_filters(capex_forecast_df)
        capex_budget_f = apply_filters(capex_budget_df)
        capex_expense_f = apply_filters(capex_expense_df)


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
        personnel_forecast = sum_column(pc_f, ['Personnel Cost'])
        total_forecast = non_personnel_forecast + personnel_forecast

        # Sum both personnel and non-personnel budgets (after filters)
        personnel_budget_sum = sum_column(budgets_f, ['personnel_budget'])
        non_personnel_budget_sum = sum_column(budgets_f, ['non_personnel_budget'])
        budget_sum = personnel_budget_sum + non_personnel_budget_sum
        funding_sum = sum_column(fundings_f, ['funding', 'Funding'])
        total_budget = budget_sum + funding_sum

        # Total actual expense across all cost elements (after filters)
        actual_expense = sum_column(expenses_f, ['expenses', 'expense', 'amount', 'Actual Expenditure'])

        # Capex sums
        capex_forecast_sum = sum_column(capex_forecast_f, ['capex_forecast'])
        capex_budget_sum = sum_column(capex_budget_f, ['budget'])
        capex_expense_sum = sum_column(capex_expense_f, ['expense'])

        # Derive Personnel Expense and Non-personnel Expense from expenses based on cost_element prefix '94'
        def sum_expenses_by_cost_element_prefix(df, prefix, value_candidates):
            if df is None or df.empty:
                return 0.0
            # locate value column
            val_col = next((c for c in value_candidates if c in df.columns), None)
            if val_col is None:
                return 0.0
            # locate cost element column
            cost_col = None
            for cand in ('cost_element', 'co_id', 'cost element'):
                if cand in df.columns:
                    cost_col = cand
                    break
            if cost_col is None:
                return 0.0
            try:
                mask = df[cost_col].astype(str).str.startswith(str(prefix))
                values = pd.to_numeric(df.loc[mask, val_col], errors='coerce').fillna(0.0)
                return float(values.sum())
            except Exception:
                return 0.0

        personnel_expense_actual = sum_expenses_by_cost_element_prefix(expenses_f, '94', ['expenses', 'expense', 'amount', 'Actual Expenditure'])
        non_personnel_expense_actual = max(0.0, float(actual_expense) - float(personnel_expense_actual))

        result = {
            'non_personnel_forecast': round(non_personnel_forecast, 2),
            'personnel_forecast': round(personnel_forecast, 2),
            'total_forecast': round(total_forecast, 2),
            'personnel budget': round(personnel_budget_sum, 2),
            'non personnel budget': round(non_personnel_budget_sum, 2),
            'budget': round(budget_sum, 2),
            'funding': round(funding_sum, 2),
            'total_budget and funding': round(total_budget, 2),
            'Total Expense': round(actual_expense, 2),
            'Personnel Expense': round(personnel_expense_actual, 2),
            'Non-personnel Expense': round(non_personnel_expense_actual, 2)
        }
        # include Capex Statistics
        result.update({
            'capex_forecast': round(capex_forecast_sum, 2),
            'capex_budget': round(capex_budget_sum, 2),
            'capex_expense': round(capex_expense_sum, 2),
        })
        print(result)
        return result, 200
    except Exception as e:
        return {'error': str(e)}, 500







