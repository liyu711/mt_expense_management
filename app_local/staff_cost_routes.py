from flask import Blueprint, request, redirect, url_for
from backend.connect_local import connect_local, select_all_from_table

staff_cost_routes = Blueprint('staff_cost_routes', __name__)

@staff_cost_routes.route('/change_staff_cost', methods=['POST'])
def change_staff_cost():
    """Simplified update: ONLY update cost for an existing staff cost row.

    Original identifying fields (po, department, staff_category, year) are treated as immutable keys.
    Any attempt to change them is ignored; we always look up by the original_* hidden fields if present.
    This removes previous logic that allowed cascading key changes.
    """
    form = dict(request.form)
    # Immutable key fields supplied via hidden inputs (fallback to visible inputs if hidden missing)
    orig_po = form.get('original_po') or form.get('po')
    orig_department = form.get('original_department') or form.get('department')
    orig_staff_category = form.get('original_staff_category') or form.get('staff_category')
    orig_year = form.get('original_year') or form.get('year')
    # New cost value (required)
    cost = form.get('cost')

    try:
        db = connect_local()
        cursor, cnxn = db.connect_to_db()

        # Maps for key resolution
        pos_df = select_all_from_table(cursor, cnxn, 'pos')
        dept_df = select_all_from_table(cursor, cnxn, 'departments')
        hr_df = select_all_from_table(cursor, cnxn, 'human_resource_categories')
        po_map = dict(zip(pos_df['name'], pos_df['id'])) if 'name' in pos_df.columns and 'id' in pos_df.columns else {}
        dept_map = dict(zip(dept_df['name'], dept_df['id'])) if 'name' in dept_df.columns and 'id' in dept_df.columns else {}
        cat_map = dict(zip(hr_df['name'], hr_df['id'])) if 'name' in hr_df.columns and 'id' in hr_df.columns else {}

        po_id = po_map.get(orig_po)
        department_id = dept_map.get(orig_department)
        category_id = cat_map.get(orig_staff_category)
        try:
            year_val = int(orig_year) if orig_year not in (None, '') else None
        except Exception:
            year_val = None
        try:
            cost_val = float(cost) if cost not in (None, '') else None
        except Exception:
            cost_val = None

        # Validate essentials
        if None in (po_id, department_id, category_id, year_val, cost_val):
            return redirect(url_for('modify_tables.modify_table_router', action='modify_staff_cost'))

        # Only update cost
        cursor.execute(
            """
            UPDATE human_resource_cost
               SET cost = ?
             WHERE po_id = ?
               AND department_id = ?
               AND category_id = ?
               AND year = ?
            """,
            (
                cost_val,
                int(po_id),
                int(department_id),
                int(category_id),
                int(year_val),
            )
        )
        cnxn.commit()
    except Exception:
        pass

    return redirect(url_for('modify_tables.modify_table_router', action='modify_staff_cost'))


@staff_cost_routes.route('/modify_staff_cost/delete_staff_cost', methods=['POST'])
def delete_staff_cost():
    """Delete a staff cost row identified by (po, department, staff_category, year)."""
    try:
        if request.is_json:
            payload = request.get_json(silent=True) or {}
            po_name = payload.get('po')
            dept_name = payload.get('department') or payload.get('bu')
            staff_category = payload.get('staff_category') or payload.get('category')
            year = payload.get('year')
        else:
            form = dict(request.form)
            po_name = form.get('po')
            dept_name = form.get('department') or form.get('bu')
            staff_category = form.get('staff_category') or form.get('category')
            year = form.get('year')

        # Basic validation
        if not (po_name and dept_name and staff_category and year):
            return { 'status': 'error', 'message': 'Missing key fields' }, 400

        # Connect and map names to IDs
        db = connect_local()
        cursor, cnxn = db.connect_to_db()
        pos_df = select_all_from_table(cursor, cnxn, 'pos')
        dept_df = select_all_from_table(cursor, cnxn, 'departments')
        hr_df = select_all_from_table(cursor, cnxn, 'human_resource_categories')

        po_map = dict(zip(pos_df['name'], pos_df['id'])) if pos_df is not None and not pos_df.empty and 'name' in pos_df.columns and 'id' in pos_df.columns else {}
        dept_map = dict(zip(dept_df['name'], dept_df['id'])) if dept_df is not None and not dept_df.empty and 'name' in dept_df.columns and 'id' in dept_df.columns else {}
        cat_map = dict(zip(hr_df['name'], hr_df['id'])) if hr_df is not None and not hr_df.empty and 'name' in hr_df.columns and 'id' in hr_df.columns else {}

        po_id = po_map.get(po_name)
        department_id = dept_map.get(dept_name)
        category_id = cat_map.get(staff_category)
        try:
            year_val = int(year) if year not in (None, '') else None
        except Exception:
            year_val = None

        if None in (po_id, department_id, category_id, year_val):
            return { 'status': 'error', 'message': 'Invalid identifiers' }, 400

        cursor.execute(
            """
            DELETE FROM human_resource_cost
             WHERE po_id = ?
               AND department_id = ?
               AND category_id = ?
               AND year = ?
            """,
            (int(po_id), int(department_id), int(category_id), int(year_val))
        )
        cnxn.commit()
        return { 'status': 'ok' }, 200
    except Exception as e:
        return { 'status': 'error', 'message': str(e) }, 500


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
