from flask import Flask, flash, render_template, request, redirect, url_for, Blueprint
from werkzeug.utils import secure_filename
from backend.connect_local import connect_local
import pandas as pd
import os
from backend import (
    upload_expenses_local, upload_nonpc_forecasts_local,
    upload_pc_forecasts_local, upload_budgets_local, upload_fundings_local,
    upload_capex_forecasts_local, upload_capex_budget_local, upload_capex_expense_local,
    check_if_all_tables_empty, check_missing_attribute, check_input_integrity
)
from backend.display_names import DISPLAY_NAMES

upload_requests = Blueprint('upload_requests', __name__, template_folder='templates')
conn = connect_local()

@upload_requests.route("/file_upload", methods=['GET', 'POST'])
def workstation_page():
    engine, cursor, cnxn = conn.connect_to_db(engine=True)
    options = [
        'budgets', 'expenses', 'fundings',
        'project_forecasts_nonpc', 'project_forecasts_pc',
        'capex_forecasts', 'capex_budgets', 'capex_expenses'
    ]
    options_map = {
        'project_forecasts_nonpc': upload_nonpc_forecasts_local,
        'project_forecasts_pc': upload_pc_forecasts_local,
        'budgets': upload_budgets_local,
        'fundings': upload_fundings_local,
        'expenses': upload_expenses_local,
        'capex_forecasts': upload_capex_forecasts_local,
        'capex_budgets': upload_capex_budget_local,
        'capex_expenses': upload_capex_expense_local
    }
    selected_option = None
    if request.method == 'POST':
        selected_option = request.form.get('my_dropdown')
        if 'file' not in request.files:
            flash('No file part in request', 'error')
            return redirect(url_for('upload_requests.workstation_page'))
        file = request.files['file']
        if file.filename == '':
            flash('No selected file', 'error')
            return redirect(url_for('upload_requests.workstation_page'))
        if file:
            db_is_empty = check_if_all_tables_empty("local")
            if db_is_empty and selected_option != "project_forecasts_nonpc":
                flash('Database is empty. Upload non-personnel forecasts first.', 'warning')
                return redirect(url_for('upload_requests.workstation_page'))
           
            
            filename = secure_filename(file.filename) # Secure the filename
            df = pd.read_csv(file)
            # status = check_input_integrity(df, selected_option)
            # if status:
            #     return f"Data mismatch "
            status, column = check_missing_attribute(df, selected_option, 'local')
            if status:
                flash(f"Missing value in {column}. Please upload related forecasts first.", 'warning')
                return redirect(url_for('upload_requests.workstation_page'))
            # Remove duplicate rows for expenses uploads before handing to uploader
            if selected_option == 'expenses':
                try:
                    preferred_keys = ['Department', 'fiscal_year', 'from_period', 'Order', 'Cost element']
                    subset_keys = [c for c in preferred_keys if c in df.columns]
                    before = len(df)
                    if subset_keys:
                        df = df.drop_duplicates(subset=subset_keys, keep='first')
                    else:
                        df = df.drop_duplicates(keep='first')
                    removed = before - len(df)
                    if removed > 0:
                        flash(f'Removed {removed} duplicate expense rows before upload.', 'warning')
                except Exception:
                    # Non-fatal: proceed without dedup if anything goes wrong
                    pass

            if options_map[selected_option]:
                try:       
                    options_map[selected_option](df)
                    flash(f'File {filename} uploaded successfully!', 'success')
                except Exception as e:
                    flash(f'Cannot upload {selected_option}. Please check input type.', 'error')
            else:
                flash(f'File type {selected_option} invalid.', 'error')
            return redirect(url_for('upload_requests.workstation_page'))
            
    return render_template("pages/file_upload.html", options=options, selected_option=selected_option, display_names=DISPLAY_NAMES)

@upload_requests.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        flash('No file part', 'error')
        return redirect(url_for('upload_requests.workstation_page'))
    file = request.files['file']
    if file.filename == '':
        flash('No selected file', 'error')
        return redirect(url_for('upload_requests.workstation_page'))
    if file:
        filename = secure_filename(file.filename)
        flash(f'File {filename} uploaded successfully!', 'success')
    return redirect(url_for('upload_requests.workstation_page'))