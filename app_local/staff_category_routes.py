from flask import Blueprint, request, redirect, url_for
from backend.connect_local import connect_local

staff_category_routes = Blueprint('staff_category_routes', __name__)

@staff_category_routes.route('/modify_staff_categories/change_staff_category', methods=['POST'])
def change_staff_category():
    """Rename an existing staff (human resource) category with duplicate-name guard.

    Accepts form fields produced by the Modify modal in `modify_table.html`:
    - existing_category: original name (hidden input) – optional if category_id provided
    - category_id: numeric id of the category row (hidden input) – preferred for precise updates
    - category (or Category): new desired name

    Behavior:
    - If neither a category_id nor existing_category is supplied, redirect without change.
    - If new_name is empty, redirect without change.
    - Duplicate guard: prevent changing to a name already used by a *different* id.
    - Updates only the targeted row.
    - Redirects back to the staff categories modify page.
    """
    form = dict(request.form)
    existing_name = form.get('existing_category') or form.get('existing_name') or form.get('existing')
    cat_id = form.get('category_id') or form.get('id')
    new_name = form.get('category') or form.get('Category') or form.get('name')

    try:
        if new_name in (None, ''):
            return redirect(url_for('modify_tables.modify_table_router', action='modify_staff_categories'))

        db = connect_local()
        cursor, cnxn = db.connect_to_db()

        # Resolve category id if only name provided
        resolved_cat_id = None
        if cat_id not in (None, ''):
            try:
                resolved_cat_id = int(cat_id)
            except Exception:
                resolved_cat_id = None
        elif existing_name not in (None, ''):
            try:
                cursor.execute("SELECT id FROM human_resource_categories WHERE name = ?", (existing_name,))
                r = cursor.fetchone()
                if r is not None:
                    try:
                        resolved_cat_id = int(r[0])
                    except Exception:
                        resolved_cat_id = None
            except Exception:
                resolved_cat_id = None

        if resolved_cat_id is None:
            return redirect(url_for('modify_tables.modify_table_router', action='modify_staff_categories'))

        # Duplicate name guard – allow if the duplicate is the same id we're updating
        try:
            cursor.execute("SELECT id FROM human_resource_categories WHERE name = ?", (new_name,))
            rdup = cursor.fetchone()
            if rdup is not None:
                try:
                    dup_id = int(rdup[0])
                except Exception:
                    dup_id = None
                if dup_id is not None and dup_id != resolved_cat_id:
                    # Different existing row already has this name
                    return redirect(url_for('modify_tables.modify_table_router', action='modify_staff_categories'))
        except Exception:
            return redirect(url_for('modify_tables.modify_table_router', action='modify_staff_categories'))

        # Perform update
        try:
            cursor.execute("UPDATE human_resource_categories SET name = ? WHERE id = ?", (str(new_name), int(resolved_cat_id)))
            cnxn.commit()
        except Exception:
            pass
    except Exception:
        pass

    return redirect(url_for('modify_tables.modify_table_router', action='modify_staff_categories'))
