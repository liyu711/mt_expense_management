from flask import Blueprint, request, redirect, url_for
from backend.connect_local import connect_local, select_all_from_table

staff_cost_routes = Blueprint('staff_cost_routes', __name__)

@staff_cost_routes.route('/change_staff_cost', methods=['POST'])
def change_staff_cost():
    """Update an existing staff cost entry keyed by (po_id, department_id, category_id, year)."""
    form = dict(request.form)
    po_name = form.get('po')
    dept_name = form.get('department')
    staff_category = form.get('staff_category')
    year = form.get('year')
    cost = form.get('cost')

    try:
        db = connect_local()
        cursor, cnxn = db.connect_to_db()

        # Map names to IDs
        pos_df = select_all_from_table(cursor, cnxn, 'pos')
        dept_df = select_all_from_table(cursor, cnxn, 'departments')
        hr_df = select_all_from_table(cursor, cnxn, 'human_resource_categories')
        po_map = dict(zip(pos_df['name'], pos_df['id'])) if 'name' in pos_df.columns and 'id' in pos_df.columns else {}
        dept_map = dict(zip(dept_df['name'], dept_df['id'])) if 'name' in dept_df.columns and 'id' in dept_df.columns else {}
        cat_map = dict(zip(hr_df['name'], hr_df['id'])) if 'name' in hr_df.columns and 'id' in hr_df.columns else {}
        po_id = po_map.get(po_name)
        department_id = dept_map.get(dept_name)
        category_id = cat_map.get(staff_category)

        # Types
        try:
            year_val = int(year) if year not in (None, '') else None
        except Exception:
            year_val = None
        try:
            cost_val = float(cost) if cost not in (None, '') else None
        except Exception:
            cost_val = None

        # Validate
        if None in (po_id, department_id, category_id, year_val, cost_val):
            return redirect(url_for('modify_tables.modify_table_router', action='modify_staff_cost'))

        # Update exactly the targeted row
        cursor.execute(
            """
            UPDATE human_resource_cost
               SET cost = ?
             WHERE po_id = ?
               AND department_id = ?
               AND category_id = ?
               AND year = ?
            """,
            (cost_val, int(po_id), int(department_id), int(category_id), int(year_val))
        )
        cnxn.commit()
    except Exception:
        pass

    return redirect(url_for('modify_tables.modify_table_router', action='modify_staff_cost'))


@staff_cost_routes.route('/modify_staff_cost/categories', methods=['GET'])
def staff_cost_categories_update():
    """Return a JSON list of all staff categories (unfiltered)."""
    try:
        db = connect_local()
        cursor, cnxn = db.connect_to_db()
        cats_df = select_all_from_table(cursor, cnxn, 'human_resource_categories')
        if cats_df is None or cats_df.empty:
            return {'categories': []}, 200
        if 'name' in cats_df.columns:
            try:
                names = cats_df['name'].dropna().astype(str).tolist()
            except Exception:
                names = cats_df['name'].tolist()
        else:
            names = []
        # de-duplicate while preserving order
        names = list(dict.fromkeys(names))
        return {'categories': names}, 200
    except Exception as e:
        return {'categories': [], 'error': str(e)}, 500
