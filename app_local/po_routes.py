from flask import Blueprint, request, redirect, url_for
from backend.connect_local import connect_local

po_routes = Blueprint('po_routes', __name__)

@po_routes.route('/modify_po/change_po', methods=['POST'])
def change_po():
    """Update an existing PO row name with duplicate-name guard."""
    form = dict(request.form)
    existing_name = form.get('existing_name') or form.get('existing_PO') or form.get('existing')
    new_name = form.get('PO') or form.get('name')
    po_id = form.get('po_id') or form.get('id')

    try:
        if not new_name:
            return redirect(url_for('modify_tables.modify_table_router', action='modify_po'))

        db = connect_local()
        cursor, cnxn = db.connect_to_db()

        # Duplicate name guard
        try:
            target_id = None
            if po_id not in (None, ''):
                try:
                    target_id = int(po_id)
                except Exception:
                    target_id = None
            elif existing_name not in (None, ''):
                cursor.execute("SELECT id FROM pos WHERE name = ?", (existing_name,))
                r = cursor.fetchone()
                if r is not None:
                    try:
                        target_id = int(r[0])
                    except Exception:
                        target_id = None

            cursor.execute("SELECT id FROM pos WHERE name = ?", (new_name,))
            r2 = cursor.fetchone()
            if r2 is not None:
                try:
                    conflict_id = int(r2[0])
                except Exception:
                    conflict_id = None
                if target_id is None or (conflict_id is not None and conflict_id != target_id):
                    return redirect(url_for('modify_tables.modify_table_router', action='modify_po'))
        except Exception:
            pass

        # Perform update
        if po_id not in (None, ''):
            try:
                pid = int(po_id)
                cursor.execute("UPDATE pos SET name = ? WHERE id = ?", (str(new_name), pid))
            except Exception:
                cursor.execute("UPDATE pos SET name = ? WHERE name = ?", (str(new_name), str(existing_name or '')))
        else:
            cursor.execute("UPDATE pos SET name = ? WHERE name = ?", (str(new_name), str(existing_name or '')))
        cnxn.commit()
    except Exception:
        pass

    return redirect(url_for('modify_tables.modify_table_router', action='modify_po'))
