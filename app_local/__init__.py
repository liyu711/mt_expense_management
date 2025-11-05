from flask import Flask, flash, render_template, request, redirect, url_for
import pandas as pd
import os
from backend.select_data import select_projects
from backend.upload_expenses import upload_expenses_df
from werkzeug.utils import secure_filename
from backend.login import valid_login
from app_local.upload_file import upload_requests
from app_local.manual_upload import manual_upload
from app_local.select_data import select_data
from app_local.modify_tables import modify_tables
from app_local.po_routes import po_routes
from app_local.department_routes import department_routes
from app_local.project_category_routes import project_category_routes
from app_local.project_routes import project_routes
from app_local.io_routes import io_routes
from app_local.data_vis import data_vis
from app_local.data_summary import data_summary_bp
from backend.connect_local import \
    connect_local, initialize_database, close_connection, select_all_from_table

conn = connect_local()

app = Flask(__name__)
app.secret_key = os.urandom(24)
app.config['UPLOAD_FOLDER'] = 'uploaded_data/'
app.register_blueprint(upload_requests)
app.register_blueprint(manual_upload)
app.register_blueprint(select_data)
app.register_blueprint(modify_tables)
app.register_blueprint(po_routes)
app.register_blueprint(department_routes)
app.register_blueprint(project_category_routes)
app.register_blueprint(project_routes)
app.register_blueprint(io_routes)
app.register_blueprint(data_vis)
app.register_blueprint(data_summary_bp)


# Jinja filter to render numbers with 1 decimal when possible
def one_decimal(value):
    try:
        # attempt to coerce to float
        n = float(value)
        # format with one decimal and return as string
        return f"{n:.1f}"
    except Exception:
        # not a number, return as-is
        return value

app.jinja_env.filters['one_decimal'] = one_decimal


def as_int(value):
    try:
        # Coerce to float first (handles numeric strings), then to int
        n = float(value)
        return str(int(n))
    except Exception:
        return value

app.jinja_env.filters['as_int'] = as_int


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

    Keeping the endpoint name `homepage` so existing redirects using
    url_for('homepage') continue to work.
    """
    if request.method == 'POST':
        # perform reset
        try:
            cursor, cnxn = conn.connect_to_db()
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


@app.route("/test")
def testpage():
    return render_template("pages/test.html")

if __name__ == "__main__":
    app.run(debug=True) # debug=True enables auto-reloading and debugging