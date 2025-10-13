from flask import Flask, flash, render_template, request, redirect, url_for, Blueprint
from backend.connect_pyodbc import \
    connect_to_sql, select_all_from_table, initialize_database, close_connection
from backend.select_data import select_projects
from backend.upload_expenses import upload_expenses_df
from werkzeug.utils import secure_filename
from backend.login import valid_login
import pandas as pd
import os

upload_requests = Blueprint('upload_requests', __name__, template_folder='templates')


@upload_requests.route("/file_upload", methods=['GET', 'POST'])
def workstation_page():
    options = [
        'projects', 'departments', 'POs', 'cost_elements', 'budgets', 'expenses', 'fundings',
        'project_categories', 'co_object_names', 'IOs', 'IO_CE_connection', 'human_resource_categories',
        'human_resource_expense', 'project_forecasts_nonpc', 'project_forecasts_pc',
        'capex_forecasts', 'capex_budgets', 'capex_expenses', 'concat'
    ]
    options_map = {
        'expenses': upload_expenses_df
    }
    selected_option = None
    if request.method == 'POST':
        selected_option = request.form.get('my_dropdown')
        print(selected_option)
        if 'file' not in request.files:
            return 'No file selected'
        file = request.files['file']
        if file.filename == '':
            return 'No selected file', 400
        if file:
            filename = secure_filename(file.filename) # Secure the filename
            # file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            df = pd.read_csv(file)
            if options_map[selected_option]:
                options_map[selected_option](df)
                return f'File {filename} uploaded successfully!'
            else:
                return f'File {selected_option} invalid'
            
    return render_template("pages/file_upload.html", options=options, selected_option=selected_option)

@upload_requests.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return 'No file part'
    file = request.files['file']
    if file.filename == '':
        return 'No selected file'
    if file:
        filename = secure_filename(file.filename)
        file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
        
        return f'File {filename} uploaded successfully!'