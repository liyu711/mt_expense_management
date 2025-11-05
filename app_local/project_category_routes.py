from flask import Blueprint, request, redirect, url_for
from backend.connect_local import connect_local

project_category_routes = Blueprint('project_category_routes', __name__)

@project_category_routes.route('/modify_porject_category/change_porject_category', methods=['POST'])
def change_porject_category():
    """Update a Project Category name with duplicate guard (route retains original typo)."""
    form = dict(request.form)
    existing_name = form.get('existing_category') or form.get('existing_name') or form.get('existing')
    cat_id = form.get('category_id') or form.get('id')
    new_name = form.get('category') or form.get('Category') or form.get('name')

    try:
        if new_name in (None, ''):
            return redirect(url_for('modify_tables.modify_table_router', action='modify_porject_category'))

        db = connect_local()
        cursor, cnxn = db.connect_to_db()

        # Duplicate name guard
        try:
            cursor.execute("SELECT id FROM project_categories WHERE category = ?", (new_name,))
            row = cursor.fetchone()
            if row is not None:
                existing_id_for_name = row[0]
                if cat_id not in (None, ''):
                    try:
                        if int(existing_id_for_name) != int(cat_id):
                            return redirect(url_for('modify_tables.modify_table_router', action='modify_porject_category'))
                    except Exception:
                        return redirect(url_for('modify_tables.modify_table_router', action='modify_porject_category'))
                else:
                    if (existing_name or '').strip() != (new_name or '').strip():
                        return redirect(url_for('modify_tables.modify_table_router', action='modify_porject_category'))
        except Exception:
            return redirect(url_for('modify_tables.modify_table_router', action='modify_porject_category'))

        # Perform update
        if cat_id not in (None, ''):
            cursor.execute("UPDATE project_categories SET category = ? WHERE id = ?", (new_name, int(cat_id)))
        elif existing_name not in (None, ''):
            cursor.execute("UPDATE project_categories SET category = ? WHERE category = ?", (new_name, existing_name))
        else:
            return redirect(url_for('modify_tables.modify_table_router', action='modify_porject_category'))

        cnxn.commit()
    except Exception:
        pass

    return redirect(url_for('modify_tables.modify_table_router', action='modify_porject_category'))
