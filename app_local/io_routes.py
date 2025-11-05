from flask import Blueprint, request, redirect, url_for
import pandas as pd
from backend.connect_local import connect_local, select_all_from_table

io_routes = Blueprint('io_routes', __name__)

@io_routes.route('/modify_io/details', methods=['GET'])
def get_io_details():
    """Return JSON details for a given IO by id, io number, or project name."""
    try:
        q_id = request.args.get('id')
        q_io = request.args.get('io')
        q_project = request.args.get('project_name')

        db = connect_local()
        cursor, cnxn = db.connect_to_db()
        ios_df = select_all_from_table(cursor, cnxn, 'ios')
        if ios_df is None or ios_df.empty:
            return {'error': 'no ios'}, 404

        row = None
        if q_id is not None and 'id' in ios_df.columns:
            try:
                qi = int(q_id)
                row = ios_df[ios_df['id'] == qi].head(1)
            except Exception:
                row = None
        if (row is None or row.empty) and q_io is not None and 'IO_num' in ios_df.columns:
            try:
                qin = int(q_io)
                row = ios_df[ios_df['IO_num'].astype(int) == qin].head(1)
            except Exception:
                row = ios_df[ios_df['IO_num'].astype(str) == str(q_io)].head(1)
        if (row is None or row.empty) and q_project is not None and 'project_id' in ios_df.columns:
            proj_df = select_all_from_table(cursor, cnxn, 'projects')
            pmap = dict(zip(proj_df['name'], proj_df['id'])) if 'name' in proj_df.columns and 'id' in proj_df.columns else {}
            pid = pmap.get(q_project)
            if pid is not None:
                row = ios_df[ios_df['project_id'] == pid].head(1)
        if row is None or row.empty:
            return {'error': 'not found'}, 404

        row = row.iloc[0]
        io_id = int(row['id']) if 'id' in ios_df.columns else None
        io_num = int(row['IO_num']) if 'IO_num' in ios_df.columns and pd.notna(row.get('IO_num')) else None
        project_name = None
        try:
            if 'project_id' in ios_df.columns:
                proj_df = select_all_from_table(cursor, cnxn, 'projects')
                pmap2 = dict(zip(proj_df['id'], proj_df['name'])) if 'id' in proj_df.columns and 'name' in proj_df.columns else {}
                project_name = pmap2.get(int(row['project_id']))
        except Exception:
            pass
        return {'id': io_id, 'io': io_num, 'project_name': project_name}, 200
    except Exception as e:
        return {'error': str(e)}, 500


@io_routes.route('/modify_io/change_io', methods=['POST'])
def change_io():
    """Update an existing IO row by id or project."""
    form = dict(request.form)
    io_id = form.get('io_id') or form.get('existing_io_id')
    existing_project = form.get('existing_project') or form.get('existing_project_name')
    new_io = form.get('IO') or form.get('io')
    new_project = form.get('project_name')

    try:
        db = connect_local()
        cursor, cnxn = db.connect_to_db()
        ios_df = select_all_from_table(cursor, cnxn, 'ios')
        proj_df = select_all_from_table(cursor, cnxn, 'projects')

        # Resolve target IO row id
        target_id = None
        if io_id not in (None, ''):
            try:
                target_id = int(io_id)
            except Exception:
                target_id = None
        if target_id is None and existing_project:
            try:
                pmap = dict(zip(proj_df['name'], proj_df['id'])) if 'name' in proj_df.columns and 'id' in proj_df.columns else {}
                pid = pmap.get(existing_project)
                if pid is not None and 'project_id' in ios_df.columns and 'id' in ios_df.columns:
                    match = ios_df[ios_df['project_id'] == int(pid)].head(1)
                    if not match.empty:
                        target_id = int(match.iloc[0]['id'])
            except Exception:
                target_id = None

        if target_id is None:
            return redirect(url_for('modify_tables.modify_table_router', action='modify_project'))

        # Build new values
        set_parts = []
        params = []
        # New IO number (optional; enforce global uniqueness excluding the current row)
        if new_io not in (None, ''):
            try:
                new_io_val = int(float(new_io))
            except Exception:
                new_io_val = None
            if new_io_val is not None:
                # Check global uniqueness excluding current id
                try:
                    cursor.execute("SELECT 1 FROM ios WHERE IO_num = ? AND id <> ? LIMIT 1", (int(new_io_val), int(target_id)))
                    dup_exists = cursor.fetchone() is not None
                except Exception:
                    dup_exists = False
                if not dup_exists:
                    set_parts.append('IO_num = ?')
                    params.append(int(new_io_val))

        # New project mapping
        if new_project not in (None, ''):
            try:
                pmap2 = dict(zip(proj_df['name'], proj_df['id'])) if 'name' in proj_df.columns and 'id' in proj_df.columns else {}
                new_pid = pmap2.get(new_project)
            except Exception:
                new_pid = None
            if new_pid is not None:
                set_parts.append('project_id = ?')
                params.append(int(new_pid))

        if set_parts:
            q = f"UPDATE ios SET {', '.join(set_parts)} WHERE id = ?"
            params.append(target_id)
            cursor.execute(q, tuple(params))
            cnxn.commit()
    except Exception:
        pass

    return redirect(url_for('modify_tables.modify_table_router', action='modify_project'))


@io_routes.route('/modify_io/list', methods=['GET'])
def list_ios():
    """Return a JSON list of IOs for the generic table."""
    try:
        db = connect_local()
        cursor, cnxn = db.connect_to_db()
        ios_df = select_all_from_table(cursor, cnxn, 'ios')
        proj_df = select_all_from_table(cursor, cnxn, 'projects')
        name_map = {}
        if proj_df is not None and not proj_df.empty and 'id' in proj_df.columns:
            name_col = 'name' if 'name' in proj_df.columns else (proj_df.columns[0] if len(proj_df.columns)>0 else None)
            if name_col:
                try:
                    name_map = dict(zip(proj_df['id'], proj_df[name_col]))
                except Exception:
                    name_map = {}
        out_rows = []
        if ios_df is not None and not ios_df.empty:
            for _, r in ios_df.iterrows():
                # include raw IO id
                io_id = None
                try:
                    if 'id' in ios_df.columns and pd.notna(r.get('id')):
                        io_id = int(r.get('id'))
                except Exception:
                    io_id = r.get('id')
                # Normalize IO number: prefer integer-like string without decimals
                io_raw = r['IO_num'] if 'IO_num' in ios_df.columns else (r.get('io') if isinstance(r, dict) else None)
                io_str = ''
                if pd.notna(io_raw):
                    try:
                        f = float(io_raw)
                        if float(f).is_integer():
                            io_str = str(int(f))
                        else:
                            io_str = str(io_raw)
                    except Exception:
                        io_str = str(io_raw)
                proj_name = None
                try:
                    if 'project_id' in ios_df.columns and pd.notna(r.get('project_id')):
                        proj_name = name_map.get(int(r.get('project_id')))
                except Exception:
                    proj_name = None
                out_rows.append({'id': io_id, 'IO': io_str, 'Project': proj_name})
        columns = ['id', 'IO', 'Project']
        return {'columns': columns, 'rows': out_rows}, 200
    except Exception as e:
        return {'columns': ['id', 'IO', 'Project'], 'rows': [], 'error': str(e)}, 200
