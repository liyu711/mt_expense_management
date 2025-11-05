from flask import Blueprint, request, redirect, url_for
from backend.connect_local import connect_local, select_all_from_table

department_routes = Blueprint('department_routes', __name__)

@department_routes.route('/modify_department/change_department', methods=['POST'])
def change_department():
    """Update an existing Department row with optional PO reassignment and duplicate guard."""
    form = dict(request.form)
    existing_name = form.get('existing_department') or form.get('existing_name') or form.get('existing')
    dept_id = form.get('department_id') or form.get('id')
    new_name = form.get('Department') or form.get('department') or form.get('name')
    po_name = form.get('po') or form.get('PO')

    try:
        db = connect_local()
        cursor, cnxn = db.connect_to_db()

        # Map PO name to id (optional)
        po_id = None
        try:
            pos_df = select_all_from_table(cursor, cnxn, 'pos')
            if pos_df is not None and not pos_df.empty and 'name' in pos_df.columns and 'id' in pos_df.columns:
                po_map = dict(zip(pos_df['name'], pos_df['id']))
                po_id = po_map.get(po_name)
        except Exception:
            po_id = None

        # Build update set clause
        sets = []
        params = []
        if new_name not in (None, ''):
            # Duplicate name guard if changing name
            try:
                target_id = None
                if dept_id not in (None, ''):
                    try:
                        target_id = int(dept_id)
                    except Exception:
                        target_id = None
                elif existing_name not in (None, ''):
                    cursor.execute("SELECT id FROM departments WHERE name = ?", (existing_name,))
                    r = cursor.fetchone()
                    if r is not None:
                        try:
                            target_id = int(r[0])
                        except Exception:
                            target_id = None
                cursor.execute("SELECT id FROM departments WHERE name = ?", (new_name,))
                conflict = cursor.fetchone()
                if conflict is not None:
                    try:
                        conflict_id = int(conflict[0])
                    except Exception:
                        conflict_id = None
                    if target_id is None or (conflict_id is not None and conflict_id != target_id):
                        return redirect(url_for('modify_tables.modify_table_router', action='modify_department'))
            except Exception:
                pass
            sets.append('name = ?')
            params.append(new_name)
        if po_id is not None:
            sets.append('po_id = ?')
            params.append(int(po_id))

        if not sets:
            return redirect(url_for('modify_tables.modify_table_router', action='modify_department'))

        # Prefer id-based update when available
        if dept_id not in (None, ''):
            params.append(int(dept_id))
            cursor.execute(f"UPDATE departments SET {', '.join(sets)} WHERE id = ?", params)
        elif existing_name not in (None, ''):
            params.append(existing_name)
            cursor.execute(f"UPDATE departments SET {', '.join(sets)} WHERE name = ?", params)
        else:
            return redirect(url_for('modify_tables.modify_table_router', action='modify_department'))

        cnxn.commit()
    except Exception:
        pass

    return redirect(url_for('modify_tables.modify_table_router', action='modify_department'))
