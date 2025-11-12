from flask import Blueprint, request, redirect, url_for
import pandas as pd
from backend.connect_local import connect_local, select_all_from_table
from backend.create_display_table import get_departments_display, get_projects_display
from app_local.modify_tables import standardize_columns_order

project_routes = Blueprint('project_routes', __name__)

@project_routes.route('/modify_project/details', methods=['GET'])
def get_project_details():
    """Return JSON details for a given project by name or id."""
    try:
        name = request.args.get('name')
        pid = request.args.get('id')

        db = connect_local()
        cursor, cnxn = db.connect_to_db()
        proj_df = select_all_from_table(cursor, cnxn, 'projects')
        if proj_df is None or proj_df.empty:
            return {'error': 'no projects'}, 404

        row = None
        if pid is not None and 'id' in proj_df.columns:
            try:
                pid_int = int(pid)
                row = proj_df[proj_df['id'] == pid_int].head(1)
            except Exception:
                row = None
        if (row is None or row.empty) and name is not None and 'name' in proj_df.columns:
            row = proj_df[proj_df['name'].astype(str) == str(name)].head(1)
        if row is None or row.empty:
            return {'error': 'not found'}, 404

        row = row.iloc[0]
        out = {
            'id': int(row['id']) if 'id' in proj_df.columns else None,
            'name': row['name'] if 'name' in proj_df.columns else None,
            'fiscal_year': int(row['fiscal_year']) if 'fiscal_year' in proj_df.columns and pd.notna(row['fiscal_year']) else None,
        }

        # Map ids to names
        cat_name = None
        try:
            if 'category_id' in proj_df.columns and pd.notna(row.get('category_id')):
                cats = select_all_from_table(cursor, cnxn, 'project_categories')
                cdict = dict(zip(cats['id'], cats['category'])) if 'id' in cats.columns and 'category' in cats.columns else {}
                cat_name = cdict.get(int(row['category_id']))
        except Exception:
            pass

        dept_name, po_name = None, None
        try:
            if 'department_id' in proj_df.columns and pd.notna(row.get('department_id')):
                depts = select_all_from_table(cursor, cnxn, 'departments')
                ddict = dict(zip(depts['id'], depts['name'])) if 'id' in depts.columns and 'name' in depts.columns else {}
                dept_id = int(row['department_id'])
                dept_name = ddict.get(dept_id)
                # get PO from departments.po_id -> POs.name
                if 'po_id' in depts.columns:
                    try:
                        po_id = int(depts[depts['id'] == dept_id]['po_id'].iloc[0])
                        pos = select_all_from_table(cursor, cnxn, 'POs')
                        pdict = dict(zip(pos['id'], pos['name'])) if 'id' in pos.columns and 'name' in pos.columns else {}
                        po_name = pdict.get(po_id)
                    except Exception:
                        po_name = None
        except Exception:
            pass

        out.update({'category': cat_name, 'department': dept_name, 'po': po_name})
        return out, 200
    except Exception as e:
        return {'error': str(e)}, 500


@project_routes.route('/modify_project/change_project', methods=['POST'])
def change_project():
    """Update an existing project row by id or name; also sync IOs if provided."""
    form = dict(request.form)
    project_id = form.get('project_id')
    existing_name = form.get('existing_project') or form.get('existing_name')
    name = form.get('name')
    category = form.get('category')
    department = form.get('department')
    fiscal_year = form.get('fiscal_year')

    try:
        db = connect_local()
        cursor, cnxn = db.connect_to_db()

        # Map category and department to ids
        cat_id = None
        dept_id = None
        try:
            cats = select_all_from_table(cursor, cnxn, 'project_categories')
            cdict = dict(zip(cats['category'], cats['id'])) if 'category' in cats.columns and 'id' in cats.columns else {}
            if category in cdict:
                cat_id = int(cdict[category])
        except Exception:
            pass
        try:
            depts = select_all_from_table(cursor, cnxn, 'departments')
            ddict = dict(zip(depts['name'], depts['id'])) if 'name' in depts.columns and 'id' in depts.columns else {}
            if department in ddict:
                dept_id = int(ddict[department])
        except Exception:
            pass

        # Coerce fiscal year
        fy_val = None
        try:
            fy_val = int(fiscal_year) if fiscal_year not in (None, '') else None
        except Exception:
            fy_val = None

        # Resolve target project id
        pid_val = None
        if project_id not in (None, ''):
            try:
                pid_val = int(project_id)
            except Exception:
                pid_val = None
        if pid_val is None and existing_name:
            try:
                proj_df = select_all_from_table(cursor, cnxn, 'projects')
                if 'name' in proj_df.columns and 'id' in proj_df.columns:
                    match = proj_df[proj_df['name'].astype(str) == str(existing_name)].head(1)
                    if not match.empty:
                        pid_val = int(match.iloc[0]['id'])
            except Exception:
                pid_val = None

        if pid_val is None:
            return redirect(url_for('modify_tables.modify_table_router', action='modify_project'))

        # Build update fields and execute
        sets = []
        params = []
        if name not in (None, ''):
            sets.append('name = ?')
            params.append(name)
        if cat_id is not None:
            sets.append('category_id = ?')
            params.append(cat_id)
        if dept_id is not None:
            sets.append('department_id = ?')
            params.append(dept_id)
        if fy_val is not None:
            sets.append('fiscal_year = ?')
            params.append(fy_val)

        # Determine existing department_id for cascade comparison
        old_dept_id = None
        try:
            proj_df_current = select_all_from_table(cursor, cnxn, 'projects')
            if proj_df_current is not None and not proj_df_current.empty and 'id' in proj_df_current.columns and 'department_id' in proj_df_current.columns:
                match_old = proj_df_current[proj_df_current['id'].astype(int) == int(pid_val)].head(1)
                if not match_old.empty:
                    try:
                        old_dept_id = int(match_old.iloc[0]['department_id']) if pd.notna(match_old.iloc[0]['department_id']) else None
                    except Exception:
                        old_dept_id = None
        except Exception:
            old_dept_id = None

        # Determine old and new PO ids via departments table to support PO cascade
        old_po_id = None
        new_po_id = None
        try:
            depts_tbl = select_all_from_table(cursor, cnxn, 'departments')
            if depts_tbl is not None and not depts_tbl.empty and 'id' in depts_tbl.columns:
                if old_dept_id is not None and 'po_id' in depts_tbl.columns:
                    try:
                        old_po_id = int(depts_tbl[depts_tbl['id'].astype(int) == int(old_dept_id)]['po_id'].iloc[0])
                    except Exception:
                        old_po_id = None
                if dept_id is not None and 'po_id' in depts_tbl.columns:
                    try:
                        new_po_id = int(depts_tbl[depts_tbl['id'].astype(int) == int(dept_id)]['po_id'].iloc[0])
                    except Exception:
                        new_po_id = None
        except Exception:
            old_po_id = None
            new_po_id = None

        dept_changed = (dept_id is not None and dept_id != old_dept_id)

        if sets:
            q = f"UPDATE projects SET {', '.join(sets)} WHERE id = ?"
            params.append(pid_val)
            cursor.execute(q, tuple(params))
            cnxn.commit()

        # Cascade department (and implied PO) change to dependent tables if department actually changed
        if dept_changed:
            try:
                # Update non-PC forecasts
                cursor.execute(
                    "UPDATE project_forecasts_nonpc SET department_id = ? WHERE project_id = ?",
                    (int(dept_id), int(pid_val))
                )
            except Exception:
                pass
            try:
                # Update PC forecasts
                cursor.execute(
                    "UPDATE project_forecasts_pc SET department_id = ? WHERE project_id = ?",
                    (int(dept_id), int(pid_val))
                )
            except Exception:
                pass
            # If the new department is under a different PO, cascade PO_id as well
            try:
                if new_po_id is not None:
                    # Only overwrite rows that matched the old PO (or had NULL)
                    if old_po_id is not None:
                        cursor.execute(
                            "UPDATE project_forecasts_nonpc SET PO_id = ? WHERE project_id = ? AND (PO_id = ? OR PO_id IS NULL)",
                            (int(new_po_id), int(pid_val), int(old_po_id))
                        )
                        cursor.execute(
                            "UPDATE project_forecasts_pc SET PO_id = ? WHERE project_id = ? AND (PO_id = ? OR PO_id IS NULL)",
                            (int(new_po_id), int(pid_val), int(old_po_id))
                        )
                    else:
                        cursor.execute(
                            "UPDATE project_forecasts_nonpc SET PO_id = ? WHERE project_id = ? AND PO_id IS NULL",
                            (int(new_po_id), int(pid_val))
                        )
                        cursor.execute(
                            "UPDATE project_forecasts_pc SET PO_id = ? WHERE project_id = ? AND PO_id IS NULL",
                            (int(new_po_id), int(pid_val))
                        )
            except Exception:
                pass
            # Update CapEx tables tied to project
            try:
                cursor.execute(
                    "UPDATE capex_forecasts SET department_id = ? WHERE project_id = ? AND (department_id = ? OR department_id IS NULL)",
                    (int(dept_id), int(pid_val), int(old_dept_id) if old_dept_id is not None else -1)
                )
            except Exception:
                pass
            try:
                cursor.execute(
                    "UPDATE capex_budgets SET department_id = ? WHERE project_id = ? AND (department_id = ? OR department_id IS NULL)",
                    (int(dept_id), int(pid_val), int(old_dept_id) if old_dept_id is not None else -1)
                )
            except Exception:
                pass
            try:
                cursor.execute(
                    "UPDATE capex_expenses SET department_id = ? WHERE project_id = ? AND (department_id = ? OR department_id IS NULL)",
                    (int(dept_id), int(pid_val), int(old_dept_id) if old_dept_id is not None else -1)
                )
            except Exception:
                pass
            # Also cascade PO to CapEx tables when department's PO changes
            try:
                if new_po_id is not None:
                    if old_po_id is not None:
                        cursor.execute(
                            "UPDATE capex_forecasts SET po_id = ? WHERE project_id = ? AND (po_id = ? OR po_id IS NULL)",
                            (int(new_po_id), int(pid_val), int(old_po_id))
                        )
                        cursor.execute(
                            "UPDATE capex_budgets SET po_id = ? WHERE project_id = ? AND (po_id = ? OR po_id IS NULL)",
                            (int(new_po_id), int(pid_val), int(old_po_id))
                        )
                        cursor.execute(
                            "UPDATE capex_expenses SET po_id = ? WHERE project_id = ? AND (po_id = ? OR po_id IS NULL)",
                            (int(new_po_id), int(pid_val), int(old_po_id))
                        )
                    else:
                        cursor.execute(
                            "UPDATE capex_forecasts SET po_id = ? WHERE project_id = ? AND po_id IS NULL",
                            (int(new_po_id), int(pid_val))
                        )
                        cursor.execute(
                            "UPDATE capex_budgets SET po_id = ? WHERE project_id = ? AND po_id IS NULL",
                            (int(new_po_id), int(pid_val))
                        )
                        cursor.execute(
                            "UPDATE capex_expenses SET po_id = ? WHERE project_id = ? AND po_id IS NULL",
                            (int(new_po_id), int(pid_val))
                        )
            except Exception:
                pass
            # Update operating expenses where IOs belong to this project
            try:
                cursor.execute(
                    "UPDATE expenses SET department_id = ? WHERE (department_id = ? OR department_id IS NULL) AND io_id IN (SELECT id FROM IOs WHERE project_id = ?)",
                    (int(dept_id), int(old_dept_id) if old_dept_id is not None else -1, int(pid_val))
                )
            except Exception:
                pass
            # No explicit PO on expenses; PO linkage is via department for display
            try:
                cnxn.commit()
            except Exception:
                pass

        # Handle IO updates as part of merged modify form
        try:
            # Submitted IO values (may be multiple inputs named 'IO')
            try:
                submitted_raw = request.form.getlist('IO')
            except Exception:
                val = form.get('IO') or form.get('io')
                submitted_raw = [val] if val not in (None, '') else []
            # Normalize to integers; de-duplicate
            submitted = []
            for v in submitted_raw:
                if v in (None, ''):
                    continue
                try:
                    iv = int(float(v))
                except Exception:
                    continue
                if iv not in submitted:
                    submitted.append(iv)

            # Current IOs for this project
            current = []
            try:
                ios_df = select_all_from_table(cursor, cnxn, 'ios')
                if ios_df is not None and not ios_df.empty:
                    cur = ios_df[ios_df['project_id'].astype(int) == int(pid_val)] if 'project_id' in ios_df.columns else ios_df.iloc[0:0]
                    if not cur.empty and 'IO_num' in cur.columns:
                        try:
                            current = cur['IO_num'].dropna().astype(int).tolist()
                        except Exception:
                            current = [int(float(x)) for x in cur['IO_num'].dropna().tolist()]
            except Exception:
                current = []

            to_delete = [x for x in current if x not in submitted]
            to_insert = [x for x in submitted if x not in current]

            # Delete removed IOs
            for io_val in to_delete:
                try:
                    cursor.execute("DELETE FROM ios WHERE project_id = ? AND IO_num = ?", (int(pid_val), int(io_val)))
                except Exception:
                    pass

            # Insert new IOs if globally unique
            for io_val in to_insert:
                try:
                    cursor.execute("SELECT 1 FROM ios WHERE IO_num = ? LIMIT 1", (int(io_val),))
                    exists = cursor.fetchone() is not None
                except Exception:
                    exists = False
                if not exists:
                    try:
                        cursor.execute("INSERT INTO ios (IO_num, project_id) VALUES (?, ?)", (int(io_val), int(pid_val)))
                    except Exception:
                        pass
            cnxn.commit()
        except Exception:
            pass
    except Exception:
        pass

    return redirect(url_for('modify_tables.modify_table_router', action='modify_project'))


@project_routes.route('/modify_project/project_ios', methods=['GET'])
def get_project_ios():
    """Return all IO numbers for a given project."""
    try:
        q_pid = request.args.get('project_id')
        q_name = request.args.get('name') or request.args.get('project_name')

        db = connect_local()
        cursor, cnxn = db.connect_to_db()
        proj_df = select_all_from_table(cursor, cnxn, 'projects')
        if proj_df is None or proj_df.empty:
            return {'ios': []}, 200

        pid = None
        if q_pid not in (None, ''):
            try:
                pid = int(q_pid)
            except Exception:
                pid = None
        if pid is None and q_name not in (None, ''):
            try:
                if 'name' in proj_df.columns and 'id' in proj_df.columns:
                    match = proj_df[proj_df['name'].astype(str) == str(q_name)].head(1)
                    if not match.empty:
                        pid = int(match.iloc[0]['id'])
            except Exception:
                pid = None

        if pid is None:
            return {'ios': []}, 200

        ios_df = select_all_from_table(cursor, cnxn, 'ios')
        if ios_df is None or ios_df.empty or 'project_id' not in ios_df.columns or 'IO_num' not in ios_df.columns:
            return {'ios': []}, 200

        try:
            subset = ios_df[ios_df['project_id'].astype(int) == int(pid)]
        except Exception:
            subset = ios_df[ios_df['project_id'] == pid]
        out = []
        if not subset.empty:
            try:
                out = subset['IO_num'].dropna().astype(int).tolist()
            except Exception:
                out = []
                for v in subset['IO_num'].dropna().tolist():
                    try:
                        out.append(int(float(v)))
                    except Exception:
                        pass
        return {'ios': out}, 200
    except Exception as e:
        return {'ios': [], 'error': str(e)}, 500


@project_routes.route('/modify_project/departments', methods=['GET'])
def modify_project_department_update():
    """Return a JSON list of departments filtered by PO for Modify Project modal."""
    try:
        po = request.args.get('po')
        df = get_departments_display()
        if df is None or df.empty:
            return {'departments': []}, 200
        # get_departments_display returns columns name_departments and name_po
        filt = df
        if po and 'name_po' in filt.columns:
            filt = filt[filt['name_po'].astype(str) == str(po)]
        depts = []
        if 'name_departments' in filt.columns:
            try:
                depts = filt['name_departments'].dropna().astype(str).tolist()
            except Exception:
                depts = filt['name_departments'].tolist()
        return {'departments': list(dict.fromkeys(depts))}, 200
    except Exception as e:
        return {'departments': [], 'error': str(e)}, 500


@project_routes.route('/modify_project/list', methods=['GET'])
def list_projects():
    """Return a JSON list of projects for the generic table."""
    try:
        # Base display (friendly names)
        df = get_projects_display()
        if df is None or df.empty:
            empty_cols = standardize_columns_order(['id', 'Project', 'PO', 'BU', 'Fiscal Year', 'IOs'], table_name='projects')
            return {'columns': empty_cols, 'rows': []}, 200

        # Normalize for frontend (friendly column headers)
        rename_map = {}
        for cand_in, cand_out in [
            ('project_name','Project'), ('name','Project'),
            ('po_name','PO'), ('name_po','PO'), ('po','PO'),
            ('department_name','BU'), ('name_departments','BU'), ('department','BU'),
            ('fiscal_year','Fiscal Year'), ('Fiscal Year','Fiscal Year')
        ]:
            if cand_in in df.columns:
                rename_map[cand_in] = cand_out
        out = df.rename(columns=rename_map).copy()

        # Try to attach the underlying project id from local DB
        try:
            db = connect_local()
            cursor, cnxn = db.connect_to_db()
            proj_tbl = select_all_from_table(cursor, cnxn, 'projects')
            ios_tbl = select_all_from_table(cursor, cnxn, 'ios')
            dept_tbl = select_all_from_table(cursor, cnxn, 'departments')

            # Build department id -> name map
            dept_map = {}
            if dept_tbl is not None and not dept_tbl.empty and 'id' in dept_tbl.columns and 'name' in dept_tbl.columns:
                dept_map = dict(zip(dept_tbl['id'], dept_tbl['name']))

            # Prepare a key in projects table: (name, dept_name, fiscal_year)
            key_series = None
            if proj_tbl is not None and not proj_tbl.empty:
                p = proj_tbl.copy()
                # map department_id -> dept name for keying
                if 'department_id' in p.columns:
                    try:
                        p['__dept_name__'] = p['department_id'].map(dept_map).fillna('')
                    except Exception:
                        p['__dept_name__'] = ''
                else:
                    p['__dept_name__'] = ''

                def mkkey_p(row):
                    n = str(row.get('name','')).strip().lower()
                    d = str(row.get('__dept_name__','')).strip().lower()
                    fy = str(row.get('fiscal_year','')).strip()
                    return f"{n}|||{d}|||{fy}"

                p['__key__'] = p.apply(mkkey_p, axis=1)
                id_by_key = dict(zip(p['__key__'], p['id'])) if 'id' in p.columns else {}
            else:
                id_by_key = {}

            # Build same key on the display table and map id
            def mkkey_out(row):
                n = str(row.get('Project','')).strip().lower()
                d = str(row.get('BU','')).strip().lower()
                fy = str(row.get('Fiscal Year','')).strip()
                return f"{n}|||{d}|||{fy}"

            out['id'] = out.apply(lambda r: id_by_key.get(mkkey_out(r)), axis=1)

            # Attach grouped IOs per project (comma-separated string)
            io_by_pid = {}
            try:
                if ios_tbl is not None and not ios_tbl.empty and 'project_id' in ios_tbl.columns and 'IO_num' in ios_tbl.columns:
                    # Normalize IO numbers to int-like strings and group
                    tmp = ios_tbl[['project_id', 'IO_num']].dropna()
                    vals = {}
                    for _, rr in tmp.iterrows():
                        try:
                            pid = int(rr['project_id'])
                        except Exception:
                            continue
                        val = rr['IO_num']
                        s = None
                        try:
                            f = float(val)
                            s = str(int(f)) if float(f).is_integer() else str(val)
                        except Exception:
                            s = str(val)
                        vals.setdefault(pid, []).append(s)
                    # de-duplicate and sort numerically when possible
                    for pid, arr in vals.items():
                        # unique preserving order
                        seen = set(); uniq = []
                        for v in arr:
                            if v not in seen:
                                seen.add(v); uniq.append(v)
                        def sort_key(x):
                            try:
                                return (0, int(x))
                            except Exception:
                                return (1, x)
                        uniq_sorted = sorted(uniq, key=sort_key)
                        io_by_pid[pid] = ', '.join(uniq_sorted)
            except Exception:
                io_by_pid = {}

            if 'id' in out.columns:
                try:
                    out['IOs'] = out['id'].apply(lambda pid: io_by_pid.get(int(pid), '') if pid is not None else '')
                except Exception:
                    out['IOs'] = ''
        except Exception:
            out['id'] = None
            out['IOs'] = ''

        # Ensure single IOs column (avoid duplication if already present)
        if 'IOs' not in out.columns:
            out['IOs'] = out.get('IOs', '')
        # Build ordered column set including IOs once
        current_cols = list(out.columns)
        # Remove duplicate occurrences of IOs if any
        seen = set(); unique_order = []
        for c in current_cols:
            if c not in seen:
                seen.add(c); unique_order.append(c)
        ordered = standardize_columns_order(unique_order + (['IOs'] if 'IOs' not in unique_order else []), table_name='projects')
        # Guarantee IOs is at the end if not already in standardized position
        if 'IOs' not in ordered:
            ordered.append('IOs')
        # Filter to existing columns only
        final_cols = [c for c in ordered if c in out.columns]
        out = out[final_cols]
        rows = out.fillna('').to_dict(orient='records')
        return {'columns': final_cols, 'rows': rows}, 200
    except Exception as e:
        return {'columns': [], 'rows': [], 'error': str(e)}, 200


@project_routes.route('/modify_project/delete_project', methods=['POST'])
def delete_project():
    """Delete a project and all associated dependent data.

    Accepts JSON or form data. Preferred identifier is project_id; otherwise
    attempts to resolve by (Project name, Department name, Fiscal Year).

    Cascade deletions:
    - project_forecasts_pc, project_forecasts_nonpc
    - capex_forecasts, capex_budgets, capex_expenses
    - IOs belonging to the project
    - expenses tied to those IOs
    - finally, the project itself
    """
    try:
        payload = request.get_json() if request.is_json else dict(request.form)
        pid = payload.get('project_id') or payload.get('id')
        proj_name = payload.get('project_name') or payload.get('Project') or payload.get('name')
        dept_name = payload.get('department') or payload.get('BU') or payload.get('department_name')
        fiscal_year = payload.get('fiscal_year') or payload.get('Fiscal Year') or payload.get('year')

        db = connect_local()
        cursor, cnxn = db.connect_to_db()

        # Resolve project id
        pid_val = None
        if pid not in (None, ''):
            try:
                pid_val = int(pid)
            except Exception:
                pid_val = None
        if pid_val is None:
            # Try resolve by (name, department, fiscal_year)
            try:
                proj_tbl = select_all_from_table(cursor, cnxn, 'projects')
                dept_tbl = select_all_from_table(cursor, cnxn, 'departments')
                dept_map = {}
                if dept_tbl is not None and not dept_tbl.empty and 'id' in dept_tbl.columns and 'name' in dept_tbl.columns:
                    dept_map = {str(v).strip().lower(): int(k) for k, v in dict(zip(dept_tbl['id'], dept_tbl['name'])).items()}
                dept_id = None
                if dept_name not in (None, ''):
                    dept_id = dept_map.get(str(dept_name).strip().lower())
                fy_val = None
                try:
                    fy_val = int(fiscal_year) if fiscal_year not in (None, '') else None
                except Exception:
                    fy_val = None
                if proj_tbl is not None and not proj_tbl.empty:
                    cand = proj_tbl.copy()
                    if proj_name not in (None, '') and 'name' in cand.columns:
                        cand = cand[cand['name'].astype(str).str.strip().str.lower() == str(proj_name).strip().lower()]
                    if dept_id is not None and 'department_id' in cand.columns:
                        try:
                            cand = cand[cand['department_id'].astype(int) == int(dept_id)]
                        except Exception:
                            cand = cand[cand['department_id'] == dept_id]
                    if fy_val is not None and 'fiscal_year' in cand.columns:
                        try:
                            cand = cand[cand['fiscal_year'].astype(int) == int(fy_val)]
                        except Exception:
                            cand = cand[cand['fiscal_year'] == fy_val]
                    if not cand.empty:
                        try:
                            pid_val = int(cand.iloc[0]['id'])
                        except Exception:
                            pid_val = None
            except Exception:
                pid_val = None

        if pid_val is None:
            return {'status': 'error', 'message': 'Project not found'}, 404

        # Start cascade deletes in order: children first
        try:
            # Delete expenses referencing IOs of this project
            cursor.execute("DELETE FROM expenses WHERE io_id IN (SELECT id FROM IOs WHERE project_id = ?)", (int(pid_val),))
        except Exception:
            pass
        try:
            # Delete IOs for this project
            cursor.execute("DELETE FROM IOs WHERE project_id = ?", (int(pid_val),))
        except Exception:
            pass
        # Delete project forecast tables
        for sql in [
            "DELETE FROM project_forecasts_pc WHERE project_id = ?",
            "DELETE FROM project_forecasts_nonpc WHERE project_id = ?",
        ]:
            try:
                cursor.execute(sql, (int(pid_val),))
            except Exception:
                pass
        # Delete CapEx tables
        for sql in [
            "DELETE FROM capex_forecasts WHERE project_id = ?",
            "DELETE FROM capex_budgets WHERE project_id = ?",
            "DELETE FROM capex_expenses WHERE project_id = ?",
        ]:
            try:
                cursor.execute(sql, (int(pid_val),))
            except Exception:
                pass
        # Finally delete the project itself
        try:
            cursor.execute("DELETE FROM projects WHERE id = ?", (int(pid_val),))
        except Exception:
            pass

        try:
            cnxn.commit()
        except Exception:
            pass
        return {'status': 'ok'}, 200
    except Exception as e:
        return {'status': 'error', 'message': str(e)}, 500
