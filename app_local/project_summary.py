from flask import Blueprint, request, render_template
import pandas as pd

# Import data_summary module for shared selection state (PO, Department, Fiscal Year, Project)
import app_local.data_summary as data_summary_module
from backend.connect_local import connect_local, select_all_from_table
from backend import (
    get_departments_display, get_projects_display, get_nonpc_display,
    get_pc_display, get_budget_display_table, create_funding_display,
    get_expenses_display
)

# Dedicated blueprint for Project Summary page and related endpoints
project_summary_bp = Blueprint('project_summary', __name__)

@project_summary_bp.route('/project_summary', methods=['GET', 'POST'])
def project_summary():
    """Render the Project Summary page with PO, Department, Fiscal Year, and Project dropdowns.

    Projects dropdown options are filtered by the selected PO, Department, and Fiscal Year maintained
    in data_summary_module. Only the render route has been migrated here; other selection endpoints
    remain in the data_summary blueprint.
    """
    conn = connect_local()
    cursor, cnxn = conn.connect_to_db()

    # Load base tables
    po_df = select_all_from_table(cursor, cnxn, 'pos')
    departments_df = get_departments_display()
    projects_df = get_projects_display()

    fiscal_years = [str(y) for y in range(2020, 2036)]

    # Build PO list
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

    # Departments filtered by selected PO
    departments = []
    if departments_df is not None:
        try:
            raw_departments = departments_df.to_dict(orient='records')
        except Exception:
            raw_departments = []
        try:
            for d in raw_departments:
                dept_name = d.get('department_name') or d.get('name') or d.get('name_dept') or d.get('name_departments')
                po_name = d.get('po_name') or d.get('name_po') or d.get('po')
                rec = {'id': d.get('id'), 'name': dept_name, 'po_name': po_name}
                if data_summary_module.selected_po and data_summary_module.selected_po not in ('', 'All'):
                    if po_name == data_summary_module.selected_po:
                        departments.append(rec)
                else:
                    departments.append(rec)
        except Exception:
            departments = []

    # Filter projects based on selected PO, Department, Fiscal Year
    projects = []
    if projects_df is not None and not projects_df.empty:
        filtered = projects_df.copy()
        try:
            colmap = {c.lower(): c for c in filtered.columns}
            def getcol(*names):
                for n in names:
                    if n.lower() in colmap:
                        return colmap[n.lower()]
                return None

            po_col = getcol('po_name', 'po name')
            dept_col = getcol('department_name', 'department name')
            fy_col = getcol('fiscal_year', 'fiscal year', 'fy', 'year')
            proj_col = getcol('project_name', 'project name')

            mask = pd.Series(True, index=filtered.index)
            if data_summary_module.selected_po and data_summary_module.selected_po not in ('', 'All') and po_col:
                mask &= filtered[po_col].astype(str) == str(data_summary_module.selected_po)
            if data_summary_module.selected_department and data_summary_module.selected_department not in ('', 'All') and dept_col:
                mask &= filtered[dept_col].astype(str) == str(data_summary_module.selected_department)
            if data_summary_module.selected_fiscal_year and data_summary_module.selected_fiscal_year not in ('', 'All') and fy_col:
                mask &= filtered[fy_col].astype(str) == str(data_summary_module.selected_fiscal_year)

            filtered = filtered.loc[mask]

            if proj_col:
                for val in filtered[proj_col].dropna().unique():
                    projects.append({'name': val})
        except Exception:
            projects = []

    return render_template(
        'pages/project_summary.html',
        pos=pos,
        departments=departments,
        fiscal_years=fiscal_years,
        projects=projects,
        selected_po=data_summary_module.selected_po,
        selected_department=data_summary_module.selected_department,
        selected_fiscal_year=data_summary_module.selected_fiscal_year,
        selected_project=data_summary_module.selected_project
    )

@project_summary_bp.route('/project_summary/project_selection', methods=['POST'])
def project_selection():
    """Endpoint to receive Project selection for Project Summary page.

    Updates shared selection state in data_summary_module.selected_project.
    """
    try:
        if request.is_json:
            payload = request.get_json()
            val = payload.get('project')
        else:
            val = request.form.get('project')
        data_summary_module.selected_project = val
        return {'status': 'ok', 'selected_project': data_summary_module.selected_project}, 200
    except Exception as e:
        return {'status': 'error', 'message': str(e)}, 500


@project_summary_bp.route('/project_summary/get_statistics', methods=['GET'])
def get_project_statistics():
    """Compute aggregated statistics filtered by current selections including selected_project.

    Returns JSON with keys mirroring data_summary/get_statistics but scoped to project when selected:
    - non_personnel_forecast
    - personnel_forecast
    - total_forecast
    - personnel budget
    - non personnel budget
    - budget
    - funding
    - total_budget and funding
    - Total Expense
    - Personnel Expense
    - Non-personnel Expense
    """
    try:
        # Load display DataFrames
        nonpc = get_nonpc_display()
        pc = get_pc_display()
        budgets = get_budget_display_table()
        fundings = create_funding_display()
        expenses = get_expenses_display()

        def apply_filters(df):
            if df is None or df.empty:
                return pd.DataFrame()
            out = df.copy()
            mask = pd.Series(True, index=out.index)

            def find_col(candidates):
                col_map = {c.lower(): c for c in out.columns}
                for cand in candidates:
                    key = cand.lower()
                    if key in col_map:
                        return col_map[key]
                return None

            # PO filter
            if data_summary_module.selected_po and data_summary_module.selected_po not in ('', 'All'):
                po_col = find_col(['po', 'po_name', 'po name', 'po_id', 'poid', 'PO Name'])
                if po_col is not None:
                    mask &= out[po_col].astype(str) == str(data_summary_module.selected_po)
                else:
                    mask &= False

            # Department filter
            if data_summary_module.selected_department and data_summary_module.selected_department not in ('', 'All'):
                dept_col = find_col(['department', 'department_name', 'department name', 'dept', 'dept_name', 'BU Name'])
                if dept_col is not None:
                    mask &= out[dept_col].astype(str) == str(data_summary_module.selected_department)
                else:
                    mask &= False

            # Fiscal year filter
            if data_summary_module.selected_fiscal_year and data_summary_module.selected_fiscal_year not in ('', 'All'):
                fy_col = find_col(['fiscal_year', 'fiscal year', 'fy', 'year', 'cap_year'])
                if fy_col is not None:
                    mask &= out[fy_col].astype(str) == str(data_summary_module.selected_fiscal_year)
                else:
                    mask &= False

            # Project filter
            if data_summary_module.selected_project and data_summary_module.selected_project not in ('', 'All'):
                proj_col = find_col(['project', 'project_name', 'project name', 'Project Name'])
                if proj_col is not None:
                    mask &= out[proj_col].astype(str) == str(data_summary_module.selected_project)
                else:
                    mask &= False

            return out.loc[mask]

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
        personnel_forecast = sum_column(pc_f, ['Personnel Cost'])
        total_forecast = non_personnel_forecast + personnel_forecast

        personnel_budget_sum = sum_column(budgets_f, ['personnel_budget'])
        non_personnel_budget_sum = sum_column(budgets_f, ['non_personnel_budget'])
        budget_sum = personnel_budget_sum + non_personnel_budget_sum
        funding_sum = sum_column(fundings_f, ['funding', 'Funding'])
        total_budget = budget_sum + funding_sum

        actual_expense = sum_column(expenses_f, ['expenses', 'expense', 'amount', 'Actual Expenditure'])

        def sum_expenses_by_cost_element_prefix(df, prefix, value_candidates):
            if df is None or df.empty:
                return 0.0
            val_col = next((c for c in value_candidates if c in df.columns), None)
            if val_col is None:
                return 0.0
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
            'Non-personnel Expense': round(non_personnel_expense_actual, 2),
            'selected_project': data_summary_module.selected_project or ''
        }
        return result, 200
    except Exception as e:
        return {'error': str(e)}, 500
