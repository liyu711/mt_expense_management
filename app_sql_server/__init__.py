from flask import Flask, flash, render_template, request, redirect, url_for
import pandas as pd
import os
from backend.connect_pyodbc import \
    connect_to_sql, select_all_from_table, initialize_database, close_connection
from backend.select_data import select_projects
from backend.display_names import DISPLAY_NAMES
from backend.upload_expenses import upload_expenses_df
from werkzeug.utils import secure_filename
from backend.login import valid_login
from app_sql_server.upload_file import upload_requests
from app_sql_server.manual_upload import manual_upload

app = Flask(__name__)
app.secret_key = os.urandom(24)
# app.config['SECRET_KEY'] = 'a_secret_key_for_session_management' 
app.config['UPLOAD_FOLDER'] = 'uploaded_data/'
app.register_blueprint(upload_requests)
app.register_blueprint(manual_upload)

@app.route('/', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        if username and password:
            return redirect(url_for('homepage'))
        else:
            error = 'Please enter both username and password.'
    return render_template('login.html', error=error)

@app.route('/reset', methods=['GET', 'POST'])
def homepage():
    """Render the home page on GET; perform database reset on POST then render home page.

    Keeps endpoint name `homepage` so url_for('homepage') continues to work.
    """
    if request.method == 'POST':
        try:
            cursor, cnxn = connect_to_sql()
            initialize_database(cursor, cnxn, initial_values=False)
        finally:
            try:
                close_connection(cursor, cnxn)
            except Exception:
                pass
    return render_template("home.html")

@app.route("/input_data")
def input_page():
     return NotImplemented

@app.route('/select', methods=['GET', 'POST'])
def select():
    data = None
    columns = None
    selected_option = None
    
    options = [
        'projects', 'departments', 'POs', 'cost_elements', 'budgets', 'expenses', 'fundings',
        'project_categories', 'co_object_names', 'IOs', 'IO_CE_connection', 'human_resource_categories',
        'human_resource_expense', 'project_forecasts_nonpc', 'project_forecasts_pc',
        'capex_forecasts', 'capex_budgets', 'capex_expenses', 'concat'
    ]

    if request.method == 'POST':
        table_name = request.form.get('table_name')
        selected_option = table_name

        if selected_option:
            cursor, cnxn = connect_to_sql()
            df = select_all_from_table(cursor, cnxn, selected_option)
            # print(df)
            data = df.values.tolist()
            columns = df.columns.tolist()
            
    return render_template('pages/select.html', options=options, selected_option=selected_option, data=data, columns=columns, display_names=DISPLAY_NAMES)

@app.route("/test")
def testpage():
    return render_template("pages/test.html")

if __name__ == "__main__":
        app.run(debug=True) # debug=True enables auto-reloading and debugging