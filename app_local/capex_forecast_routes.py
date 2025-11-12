from flask import Blueprint, request
from backend.connect_local import connect_local, select_all_from_table
from backend.create_display_table import get_departments_display, get_projects_display

capex_forecast_routes = Blueprint('capex_forecast_routes', __name__)

@capex_forecast_routes.route('/capex_forecast/po_selection', methods=['POST'])
def modify_po_selection():
    """Receive PO selection from client and acknowledge (no server-side state needed here)."""
    try:
        if request.is_json:
            payload = request.get_json()
            val = payload.get('po')
        else:
            val = request.form.get('po')
        return {'status': 'ok', 'selected_po': val}, 200
    except Exception as e:
        return {'status': 'error', 'message': str(e)}, 500


@capex_forecast_routes.route('/capex_forecast/department_selection', methods=['POST'])
def modify_department_selection():
    """Receive Department selection from client and acknowledge."""
    try:
        if request.is_json:
            payload = request.get_json()
            val = payload.get('department')
        else:
            val = request.form.get('department')
        return {'status': 'ok', 'selected_department': val}, 200
    except Exception as e:
        return {'status': 'error', 'message': str(e)}, 500


@capex_forecast_routes.route('/capex_forecast/cap_year_selection', methods=['POST'])
def modify_cap_year_selection():
    """Receive Capex year selection from client and acknowledge."""
    try:
        if request.is_json:
            payload = request.get_json()
            val = payload.get('cap_year')
        else:
            val = request.form.get('cap_year')
        return {'status': 'ok', 'selected_cap_year': val}, 200
    except Exception as e:
        return {'status': 'error', 'message': str(e)}, 500


@capex_forecast_routes.route('/capex_forecast/project_selection', methods=['POST'])
def modify_project_selection():
    """Receive Project selection from client and acknowledge."""
    try:
        if request.is_json:
            payload = request.get_json()
            val = payload.get('project') or payload.get('project_name')
        else:
            val = request.form.get('project') or request.form.get('project_name')
        return {'status': 'ok', 'selected_project': val}, 200
    except Exception as e:
        return {'status': 'error', 'message': str(e)}, 500


@capex_forecast_routes.route('/capex_forecast/department_update', methods=['GET'])
def capex_department_update():
    """Return a JSON list of departments filtered by PO.

    Query params:
    - po: optional PO name to filter by.
    Response: { 'departments': [ ... ] }
    """
    try:
        po = request.args.get('po')
        df = get_departments_display()
        if df is None or df.empty:
            return {'departments': []}, 200
        if po and 'name_po' in df.columns:
            df = df[df['name_po'].astype(str) == str(po)]
        depts = []
        if 'name_departments' in df.columns:
            try:
                depts = df['name_departments'].dropna().astype(str).tolist()
            except Exception:
                depts = df['name_departments'].tolist()
        # de-duplicate
        depts = list(dict.fromkeys(depts))
        return {'departments': depts}, 200
    except Exception as e:
        return {'departments': [], 'error': str(e)}, 500


@capex_forecast_routes.route('/capex_forecast/project_update', methods=['GET'])
def capex_project_update():
    """Return a JSON list of projects filtered by PO, Department and Fiscal Year.

    Query params:
    - po: PO name
    - department: department name
    - fiscal_year or cap_year: year
    """
    try:
        po = request.args.get('po')
        department = request.args.get('department')
        fiscal_year = request.args.get('fiscal_year') or request.args.get('cap_year')

        df = get_projects_display()
        if df is None or df.empty:
            return {'projects': []}, 200

        proj_col = 'project_name' if 'project_name' in df.columns else ('name' if 'name' in df.columns else (df.columns[0] if len(df.columns)>0 else None))
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


@capex_forecast_routes.route('/capex_forecast/change_capex_forecast', methods=['POST'])
def change_capex_forecast():
    """Handle modify action to change an existing capex forecast entry.

    Expects form fields: po, department, cap_year, project_name, capex_description, capex_forecast, cost_center
    Updates capex_forecasts.capex_forecast and cost_center WHERE po_id, department_id, project_id, cap_year and capex_description match.
    """
    form = dict(request.form)
    # New values (can be changed by user)
    po = form.get('po')
    department = form.get('department')
    cap_year = form.get('cap_year')
    project_name = form.get('project_name')
    capex_description = form.get('capex_description')
    capex_forecast = form.get('capex_forecast')
    cost_center = form.get('cost_center')

    # Original key fields to locate existing row
    orig_po = form.get('original_po') or po
    orig_department = form.get('original_department') or department
    orig_cap_year = form.get('original_cap_year') or cap_year
    orig_project_name = form.get('original_project_name') or project_name
    orig_description = form.get('original_capex_description')

    try:
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

        # originals
        orig_po_id = po_map.get(orig_po)
        orig_department_id = dept_map.get(orig_department)
        orig_project_id = proj_map.get(orig_project_name)

        try:
            cap_year_val = int(cap_year) if cap_year not in (None, '') else None
        except Exception:
            cap_year_val = None
        try:
            forecast_val = float(capex_forecast) if capex_forecast not in (None, '') else None
        except Exception:
            forecast_val = None

        # Coerce originals
        try:
            orig_cap_year_val = int(orig_cap_year) if orig_cap_year not in (None, '') else None
        except Exception:
            orig_cap_year_val = cap_year_val

        if None in (po_id, department_id, project_id, cap_year_val, orig_po_id, orig_department_id, orig_project_id, orig_cap_year_val):
            return {'status': 'error', 'message': 'Missing identifiers'}, 400

        # Build SQL with optional original description clause to narrow update if available
        base_sql = (
            "UPDATE capex_forecasts\n"
            "   SET po_id = ?,\n"
            "       department_id = ?,\n"
            "       project_id = ?,\n"
            "       cap_year = ?,\n"
            "       capex_description = ?,\n"
            "       capex_forecast = ?,\n"
            "       cost_center = ?\n"
            " WHERE po_id = ?\n"
            "   AND department_id = ?\n"
            "   AND project_id = ?\n"
            "   AND cap_year = ?"
        )
        params = [
            int(po_id),
            int(department_id),
            int(project_id),
            int(cap_year_val),
            capex_description,
            forecast_val,
            cost_center,
            int(orig_po_id),
            int(orig_department_id),
            int(orig_project_id),
            int(orig_cap_year_val),
        ]
        if orig_description:
            base_sql += "\n   AND capex_description = ?"
            params.append(orig_description)

        cursor.execute(base_sql, tuple(params))
        cnxn.commit()
        return {'status': 'ok'}, 200
    except Exception as e:
        return {'status': 'error', 'message': str(e)}, 500


@capex_forecast_routes.route('/capex_forecast/delete_capex_forecast', methods=['POST'])
def delete_capex_forecast():
    """Delete a CapEx forecast row identified by (po, department, cap_year, project_name).

    Accepts either JSON or form data with fields:
    - po, department, cap_year, project_name
    Optional: capex_description (ignored for keying; deletion uses the 4-key to match uniqueness)
    """
    try:
        # Accept payload from JSON or form
        if request.is_json:
            form = request.get_json() or {}
        else:
            form = dict(request.form)

        po = form.get('po')
        department = form.get('department')
        cap_year = form.get('cap_year') or form.get('fiscal_year')
        project_name = form.get('project_name') or form.get('project')

        # Basic validation
        if not (po and department and cap_year and project_name):
            return {'status': 'error', 'message': 'Missing required fields'}, 400

        # Map names to ids
        db = connect_local()
        cursor, cnxn = db.connect_to_db()

        pos_df = select_all_from_table(cursor, cnxn, 'pos')
        dept_df = select_all_from_table(cursor, cnxn, 'departments')
        proj_df = select_all_from_table(cursor, cnxn, 'projects')

        po_map = dict(zip(pos_df['name'], pos_df['id'])) if pos_df is not None and not pos_df.empty and 'name' in pos_df.columns and 'id' in pos_df.columns else {}
        dept_map = dict(zip(dept_df['name'], dept_df['id'])) if dept_df is not None and not dept_df.empty and 'name' in dept_df.columns and 'id' in dept_df.columns else {}
        proj_map = dict(zip(proj_df['name'], proj_df['id'])) if proj_df is not None and not proj_df.empty and 'name' in proj_df.columns and 'id' in proj_df.columns else {}

        po_id = po_map.get(po)
        department_id = dept_map.get(department)
        project_id = proj_map.get(project_name)

        try:
            cap_year_val = int(cap_year)
        except Exception:
            cap_year_val = None

        if po_id is None or department_id is None or project_id is None or cap_year_val is None:
            return {'status': 'error', 'message': 'Unable to resolve identifiers'}, 400

        # Optional description to further narrow deletion if provided
        capex_description = form.get('capex_description') or form.get('description')

        if capex_description:
            cursor.execute(
                """
                DELETE FROM capex_forecasts
                 WHERE po_id = ?
                   AND department_id = ?
                   AND project_id = ?
                   AND cap_year = ?
                   AND capex_description = ?
                """,
                (int(po_id), int(department_id), int(project_id), int(cap_year_val), capex_description)
            )
        else:
            # Perform delete using the unique 4-key
            cursor.execute(
                """
                DELETE FROM capex_forecasts
                 WHERE po_id = ?
                   AND department_id = ?
                   AND project_id = ?
                   AND cap_year = ?
                """,
                (int(po_id), int(department_id), int(project_id), int(cap_year_val))
            )
        cnxn.commit()

        return {'status': 'ok'}, 200
    except Exception as e:
        return {'status': 'error', 'message': str(e)}, 500


@capex_forecast_routes.route('/capex_budget/change_capex_budget', methods=['POST'])
def change_capex_budget():
    """Handle modify action to change an existing capex budget entry.

    Expects form fields: po, department, cap_year, project_name, capex_description, approved_budget
    Updates capex_budgets.budget and capex_description WHERE po_id, department_id, project_id, cap_year match.
    """
    form = dict(request.form)
    # New values
    po = form.get('po')
    department = form.get('department')
    cap_year = form.get('cap_year') or form.get('fiscal_year')
    project_name = form.get('project_name') or form.get('project')
    capex_description = form.get('capex_description')
    approved_budget = form.get('approved_budget') or form.get('budget')

    # Original key fields
    orig_po = form.get('original_po') or po
    orig_department = form.get('original_department') or department
    orig_cap_year = form.get('original_cap_year') or cap_year
    orig_project_name = form.get('original_project_name') or project_name

    try:
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

        # original ids for where clause
        orig_po_id = po_map.get(orig_po)
        orig_department_id = dept_map.get(orig_department)
        orig_project_id = proj_map.get(orig_project_name)

        try:
            cap_year_val = int(cap_year) if cap_year not in (None, '') else None
        except Exception:
            cap_year_val = None
        try:
            budget_val = float(approved_budget) if approved_budget not in (None, '') else None
        except Exception:
            budget_val = None
        try:
            orig_cap_year_val = int(orig_cap_year) if orig_cap_year not in (None, '') else None
        except Exception:
            orig_cap_year_val = cap_year_val

        if None in (po_id, department_id, project_id, cap_year_val, budget_val, orig_po_id, orig_department_id, orig_project_id, orig_cap_year_val):
            return {'status': 'error', 'message': 'Missing identifiers'}, 400

        cursor.execute(
            """
            UPDATE capex_budgets
               SET po_id = ?,
                   department_id = ?,
                   project_id = ?,
                   cap_year = ?,
                   capex_description = ?,
                   budget = ?
             WHERE po_id = ?
               AND department_id = ?
               AND project_id = ?
               AND cap_year = ?
            """,
            (
                int(po_id),
                int(department_id),
                int(project_id),
                int(cap_year_val),
                capex_description,
                budget_val,
                int(orig_po_id),
                int(orig_department_id),
                int(orig_project_id),
                int(orig_cap_year_val),
            ),
        )
        cnxn.commit()
        return {'status': 'ok'}, 200
    except Exception as e:
        return {'status': 'error', 'message': str(e)}, 500
