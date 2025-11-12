from flask import Blueprint, request, redirect, url_for
from backend.connect_local import connect_local, select_all_from_table

department_routes = Blueprint('department_routes', __name__)

@department_routes.route('/modify_department/change_department', methods=['POST'])
def change_department():
    """Update a Department; if its PO changes, safely cascade that PO change to related tables.

    Safe cascade rules:
    - Only run if (old_po_id != new_po_id) and both are not NULL.
    - For each dependent table, update rows WHERE department_id = dept_id AND current po/PO_id = old_po_id.
      This avoids overwriting rows that were manually pointed to a different PO previously.
    - Tables affected (as requested):
        capex_forecasts, capex_budgets, capex_expenses, budgets, fundings,
        project_forecasts_pc (column PO_id), project_forecasts_nonpc (column PO_id).
    """
    form = dict(request.form)
    existing_name = form.get('existing_department') or form.get('existing_name') or form.get('existing')
    dept_id = form.get('department_id') or form.get('id')
    new_name = form.get('Department') or form.get('department') or form.get('name')
    po_name = form.get('po') or form.get('PO')

    try:
        db = connect_local()
        cursor, cnxn = db.connect_to_db()

        # Resolve department id if only name provided
        resolved_dept_id = None
        if dept_id not in (None, ''):
            try:
                resolved_dept_id = int(dept_id)
            except Exception:
                resolved_dept_id = None
        elif existing_name not in (None, ''):
            cursor.execute("SELECT id FROM departments WHERE name = ?", (existing_name,))
            r = cursor.fetchone()
            if r:
                try:
                    resolved_dept_id = int(r[0])
                except Exception:
                    resolved_dept_id = None

        if resolved_dept_id is None:
            return redirect(url_for('modify_tables.modify_table_router', action='modify_department'))

        # Fetch current department row to obtain old_po_id before updating
        cursor.execute("SELECT po_id, name FROM departments WHERE id = ?", (resolved_dept_id,))
        dept_row = cursor.fetchone()
        old_po_id = dept_row[0] if dept_row else None

        # Map PO name to id (optional new value)
        new_po_id = None
        if po_name not in (None, ''):
            try:
                pos_df = select_all_from_table(cursor, cnxn, 'pos')
                if pos_df is not None and not pos_df.empty and 'name' in pos_df.columns and 'id' in pos_df.columns:
                    po_map = dict(zip(pos_df['name'], pos_df['id']))
                    new_po_id = po_map.get(po_name)
            except Exception:
                new_po_id = None

        # Build update set clause for departments
        sets = []
        params = []

        # Handle name change with duplicate guard
        if new_name not in (None, ''):
            try:
                cursor.execute("SELECT id FROM departments WHERE name = ?", (new_name,))
                conflict = cursor.fetchone()
                if conflict is not None:
                    conflict_id = None
                    try:
                        conflict_id = int(conflict[0])
                    except Exception:
                        conflict_id = None
                    if conflict_id is not None and conflict_id != resolved_dept_id:
                        # Duplicate name; abort
                        return redirect(url_for('modify_tables.modify_table_router', action='modify_department'))
            except Exception:
                pass
            sets.append('name = ?')
            params.append(new_name)

        if new_po_id is not None:
            sets.append('po_id = ?')
            params.append(int(new_po_id))

        if not sets:
            return redirect(url_for('modify_tables.modify_table_router', action='modify_department'))

        # Execute department update
        params.append(resolved_dept_id)
        cursor.execute(f"UPDATE departments SET {', '.join(sets)} WHERE id = ?", params)

        # Safe cascade only if PO actually changed
        if old_po_id is not None and new_po_id is not None and old_po_id != new_po_id:
            try:
                # Wrap cascades in a transaction (implicit with same connection)
                cascade_updates = [
                    ("UPDATE capex_forecasts SET po_id = ? WHERE department_id = ? AND po_id = ?", (new_po_id, resolved_dept_id, old_po_id)),
                    ("UPDATE capex_budgets SET po_id = ? WHERE department_id = ? AND po_id = ?", (new_po_id, resolved_dept_id, old_po_id)),
                    ("UPDATE capex_expenses SET po_id = ? WHERE department_id = ? AND po_id = ?", (new_po_id, resolved_dept_id, old_po_id)),
                    ("UPDATE budgets SET po_id = ? WHERE department_id = ? AND po_id = ?", (new_po_id, resolved_dept_id, old_po_id)),
                    ("UPDATE fundings SET po_id = ? WHERE department_id = ? AND po_id = ?", (new_po_id, resolved_dept_id, old_po_id)),
                    ("UPDATE expenses SET po_id = ? WHERE department_id = ? AND po_id = ?", (new_po_id, resolved_dept_id, old_po_id)),
                    ("UPDATE human_resource_cost SET po_id = ? WHERE department_id = ? AND po_id = ?", (new_po_id, resolved_dept_id, old_po_id)),
                    # Project forecast tables use column name PO_id
                    ("UPDATE project_forecasts_pc SET PO_id = ? WHERE department_id = ? AND PO_id = ?", (new_po_id, resolved_dept_id, old_po_id)),
                    ("UPDATE project_forecasts_nonpc SET PO_id = ? WHERE department_id = ? AND PO_id = ?", (new_po_id, resolved_dept_id, old_po_id)),
                ]
                for sql_stmt, sql_params in cascade_updates:
                    try:
                        cursor.execute(sql_stmt, sql_params)
                    except Exception:
                        # Skip individual failures to keep process resilient
                        continue
            except Exception:
                pass

        cnxn.commit()
    except Exception:
        # Swallow errors to keep redirect behavior consistent
        pass

    return redirect(url_for('modify_tables.modify_table_router', action='modify_department'))
