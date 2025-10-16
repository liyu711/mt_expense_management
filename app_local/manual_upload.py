
from flask import Flask, flash, render_template, request, redirect, url_for, Blueprint, jsonify
from werkzeug.utils import secure_filename
from backend.login import valid_login
from backend.connect_local import connect_local, select_columns_from_table, select_all_from_table
from app_local.select_data import transform_table

from backend import \
    upload_nonpc_forecasts_local_m, upload_pc_forecasts_local_m,\
    upload_budgets_local_m, upload_expenses_local, upload_fundings_local, \
    upload_capex_forecast_m, upload_capex_budgets_local_m, upload_capex_expense_local
from backend.upload_forecasts_nonpc import upload_nonpc_forecasts_df, upload_nonpc_forecasts_local_m
import pandas as pd
import os

manual_upload = Blueprint('manual_upload', __name__, template_folder='templates')
input_types = [
        "forecast_nonpc",
        "forecast_pc" ,
        "budgets",
        "fundings",
        "expenses",
        "capex_forecast",
        "capex_budget",
        "capex_expense"
]

display_names = {
    "forecast_nonpc": ['PO', 'IO','Department', "Project Category", "Project Name", "Fiscal Year", "Non-personnel Expense"],
    "forecast_pc" : ['PO', 'IO','Department', "Project Category", "Project Name", "Fiscal Year", 'Human resource category', "Human resource FTE", "Personnel Expense"],
    "budgets": ['PO', 'Department', "Fiscal Year", "Personnel Budget", "Non-personnel Budget"],
    "fundings": ['PO', 'Department', "Fiscal Year", "Funding", "Funding From", "Funding For"],
    "expenses": ['Department', "Fiscal Year", "From Period", "Order(IO)", "CO Object Name", "Cost Element", "Cost Element Name", "Val.in rep.cur.", "Name"],
    "capex_forecast" : ['PO', 'Department', 'CapYear', 'For Project', 'CapEx Description', 'CapEx Forecast', 'Cost Center'],
    "capex_budget" : ['PO',	'Department', 'CapYear', 'For Project',	'Capex Description', 'Approved Budget (k CNY)', 'Comments from finance'],
    "capex_expense":['PO', 'Department', 'CapYear', 'For Project', 'Capex Description', 'Project number', 'Actual (k CNY)', 'Date']
}

upload_columns = {
    "forecast_nonpc": ["PO","IO","Department","Project Category","Project Name","fiscal_year","Non-personnel cost"],
    "forecast_pc" : ["PO","IO","Department","Project Category","Project Name","fiscal_year","Human resource category","Human resource FTE","Personnel cost"],
    "budgets": ["PO", "Department", "fiscal_year", "Human Resources Budget", "Non-Human Resources Budget"],
    "fundings": ["PO", "Department", "fiscal_year", "funding", "funding_from", "funding_for"],
    "expenses": ["Department", "fiscal_year", "from_period", "Order", "CO object name", "Cost element", "Cost element name", "Val.in rep.cur", "Name"],
    "capex_forecast" : ["PO", "Department", "cap_year", "Project name", "capex_description", "Forecast", "cost_center"],
    "capex_budget" : ["PO", "Department", "fiscal_year", "for_project", "capex_description", "budget"],
    "capex_expense": ["PO", "Department", "fiscal_year", "Project Name", "capex_description", "Project number", "Expense"]
}


titles = {
    "forecast_nonpc": "None-Personnel Forecast",
    "forecast_pc": "Personnel Forecast",
    "budgets": "Budgets",
    "fundings": "Fundings",
    "expenses": "Expenses",
    "capex_forecast": "CapEx Forecast",
    "capex_budget": "CapEx Budget",
    "capex_expense": "CapEx Expense"
}

@manual_upload.route("/manual_input")
def render_mannual_input():
    conn = connect_local()
    engine, cursor, cnxn = conn.connect_to_db(engine=True)
    departments = select_all_from_table(cursor, cnxn, 'departments')['name']
    po = select_all_from_table(cursor, cnxn, 'pos')['name']
    io = select_all_from_table(cursor, cnxn, "ios")['IO_num']
    projects = select_all_from_table(cursor, cnxn, 'projects')['name']
    project_categories = select_all_from_table(cursor, cnxn, "project_categories")['category']
    human_resource_categories = select_all_from_table(cursor, cnxn, "human_resource_categories")['name']
    # Fetch forecast table contents to show below the Add button
    try:
        df_nonpc = select_all_from_table(cursor, cnxn, 'project_forecasts_nonpc')
    except Exception:
        df_nonpc = None
    try:
        df_pc = select_all_from_table(cursor, cnxn, 'project_forecasts_pc')
    except Exception:
        df_pc = None

    # map id columns to readable names (same mapping used elsewhere)
    id_name_map = {
        'department_id': ('departments', 'id', 'name', 'Department'),
        'po_id': ('POs', 'id', 'name', 'PO'),
        'PO_id': ('POs', 'id', 'name', 'PO'),
        'project_id': ('projects', 'id', 'name', 'Project'),
        'io_id': ('IOs', 'id', 'IO_num', 'IO'),
        'project_category_id': ('project_categories', 'id', 'category', 'Project Category'),
    }
    def map_and_prepare(df, table_name=None):
        if df is None:
            return [], []
        try:
            for id_col, (ref_table, ref_id, ref_name, new_col_name) in id_name_map.items():
                if id_col in df.columns:
                    ref_df = select_all_from_table(cursor, cnxn, ref_table)
                    ref_dict = dict(zip(ref_df[ref_id], ref_df[ref_name]))
                    df[new_col_name] = df[id_col].map(ref_dict)
            drop_cols = [col for col in ['department_id', 'po_id', 'PO_id', 'project_id', 'io_id', 'project_category_id'] if col in df.columns]
            df2 = df.drop(columns=drop_cols)
            # Apply the same transforms used in the select view so naming and ordering match
            try:
                df2 = transform_table(df2, table_name or '', cursor, cnxn)
            except Exception:
                pass
            return df2.columns.tolist(), df2.values.tolist()
        except Exception:
            return [], []

    pf_nonpc_columns, pf_nonpc_data = map_and_prepare(df_nonpc, 'project_forecasts_nonpc')
    pf_pc_columns, pf_pc_data = map_and_prepare(df_pc, 'project_forecasts_pc')

    return render_template("pages/mannual_input.html", 
                           input_types = input_types, 
                           titles = titles, 
                           display_names=display_names, 
                           upload_columns=upload_columns,
                           departments = departments,
                           pos = po,
                           ios = io,
                           projects = projects,
                           project_categories=project_categories,
                           human_resource_categories= human_resource_categories,
                           pf_nonpc_columns=pf_nonpc_columns,
                           pf_nonpc_data=pf_nonpc_data,
                           pf_pc_columns=pf_pc_columns,
                           pf_pc_data=pf_pc_data
                           )


@manual_upload.route('/api/hr_cost', methods=['GET'])
def api_hr_cost():
    """Return the cost for a given human resource category name and year.
    Query params: category (name), year (int)
    Returns JSON: {"cost": <float>|null}
    """
    category = request.args.get('category')
    year = request.args.get('year')
    if not category or not year:
        return jsonify({'cost': None}), 200
    try:
        db = connect_local()
        cursor, cnxn = db.connect_to_db()
        # find category id
        cursor.execute("SELECT id FROM human_resource_categories WHERE name = ?", (category,))
        row = cursor.fetchone()
        if not row:
            return jsonify({'cost': None}), 200
        category_id = row[0]
        # find cost for that category and year
        try:
            cursor.execute("SELECT cost FROM human_resource_cost WHERE category_id = ? AND year = ?", (category_id, int(year)))
            r2 = cursor.fetchone()
        except Exception:
            r2 = None

        # Fallback: some rows may have category_id stored as the category name (string). Try that too.
        if not r2:
            try:
                cursor.execute("SELECT cost FROM human_resource_cost WHERE category_id = ? AND year = ?", (category, int(year)))
                r2 = cursor.fetchone()
            except Exception:
                r2 = None

        if not r2:
            return jsonify({'cost': None}), 200
        return jsonify({'cost': r2[0]}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        try:
            cursor.close()
            cnxn.close()
        except:
            pass


@manual_upload.route("/upload_forecast_nonpc", methods=['POST'])
def upload_forecast_nonpc_r():
    df = pd.DataFrame(columns = ["PO","IO","Department","Project Category","Project Name","fiscal_year","Non-personnel cost"])
    row = {}
    for fieldname, value in request.form.items():
        # print(fieldname, value)
        row[fieldname] = value
    df.loc[len(df)] = row
    df["IO"] = df["IO"].astype(int)
    try:
        columns_changed = upload_nonpc_forecasts_local_m(df)
        if columns_changed > 0:
            return "Upload successful"
        else:
            return "Value already exist"
    except:
        return "Upload failed. Please check your input values."

    # return "Upload successful"

@manual_upload.route("/upload_forecast_pc", methods=['POST'])
def upload_forecast_pc_r():
    df = pd.DataFrame(columns=upload_columns["forecast_pc"])
    row = {}
    for fieldname, value in request.form.items():
        # print(fieldname, value)
        row[fieldname] = value
    df.loc[len(df)] = row
    df["IO"] = df["IO"].astype(int)
    # project_forecasts_pc table does not store personnel_expense - drop Personnel cost before upload
    if 'Personnel cost' in df.columns:
        df = df.drop(columns=['Personnel cost'])
    try:
        columns_changed = upload_pc_forecasts_local_m(df)
        if columns_changed > 0:
            return "Upload successful"
        else:
            return "Value already exist"
    except:
        return "Upload failed. Please check your input values."
    # return "Upload successful"

@manual_upload.route("/upload_budgets", methods=['POST'])
def upload_budgets_r():
    df = pd.DataFrame(columns=upload_columns["budgets"])
    row = {}
    for fieldname, value in request.form.items():
        # print(fieldname, value)
        row[fieldname] = value
    df.loc[len(df)] = row
    print(df)
    res = upload_budgets_local_m(df)
    return "Upload successful"

@manual_upload.route("/upload_expenses", methods=['POST'])
def upload_expenses_r():
    df = pd.DataFrame(columns=upload_columns["expenses"])
    row = {}
    for fieldname, value in request.form.items():
        # print(fieldname, value)
        row[fieldname] = value
    df.loc[len(df)] = row
    # try:
    res = upload_expenses_local(df)
    return "successful"

@manual_upload.route("/upload_fundings", methods=['POST'])
def upload_fundings_r():
    df = pd.DataFrame(columns=upload_columns["fundings"])
    row = {}
    for fieldname, value in request.form.items():
        # print(fieldname, value)
        row[fieldname] = value
    df.loc[len(df)] = row
    # try:
    res = upload_fundings_local(df)
    return "successful"

@manual_upload.route("/upload_capex_forecast", methods=['POST'])
def upload_capex_forecast_r():
    df = pd.DataFrame(columns=upload_columns["capex_forecast"])
    row = {}
    for fieldname, value in request.form.items():
        # print(fieldname, value)
        row[fieldname] = value
    df.loc[len(df)] = row
    print(df)
    res = upload_capex_forecast_m(df)
    return "Upload successful"

@manual_upload.route("/upload_capex_budget", methods=['POST'])
def upload_capex_budget_r():
    df = pd.DataFrame(columns=upload_columns["capex_budget"])
    row = {}
    for fieldname, value in request.form.items():
        # print(fieldname, value)
        row[fieldname] = value
    df.loc[len(df)] = row
    print(df)
    res = upload_capex_budgets_local_m(df)
    return "Upload successful"

@manual_upload.route("/upload_capex_expense", methods=['POST'])
def upload_capex_expense_r():
    df = pd.DataFrame(columns=upload_columns["capex_expense"])
    row = {}
    for fieldname, value in request.form.items():
        # print(fieldname, value)
        row[fieldname] = value
    df.loc[len(df)] = row
    # try:
    res = upload_capex_expense_local(df, clear=False)
    return "successful"


@manual_upload.route("/upload_forecast", methods=['POST'])
def upload_forecast_merged():
    # Extract form data
    form = request.form
    # Prepare for both forecast types
    # Non-personnel
    nonpc_fields = ["PO","IO","Department","Project_Category","Project_Name","fiscal_year","Non_personnel_cost"]
    nonpc_row = {
        "PO": form.get("PO"),
        "IO": form.get("IO"),
        "Department": form.get("Department"),
        "Project Category": form.get("Project_Category"),
        "Project Name": form.get("Project_Name"),
        "fiscal_year": form.get("fiscal_year"),
        "Non-personnel cost": form.get("Non_personnel_cost")
    }
    # Personnel
    pc_fields = ["PO","IO","Department","Project_Category","Project_Name","fiscal_year","Human_resource_category","Human_resource_FTE","Personnel_cost"]
    pc_row = {
        "PO": form.get("PO"),
        "IO": form.get("IO"),
        "Department": form.get("Department"),
        "Project Category": form.get("Project_Category"),
        "Project Name": form.get("Project_Name"),
        "fiscal_year": form.get("fiscal_year"),
        "Human resource category": form.get("Human_resource_category"),
        "Human resource FTE": form.get("Human_resource_FTE"),
        "Personnel cost": form.get("Personnel_cost")
    }
    # Track results
    results = []
    # Only upload if at least one relevant field is filled
    # Non-personnel
    if form.get("Non_personnel_cost"):
        df_nonpc = pd.DataFrame([nonpc_row])
        try:
            df_nonpc["IO"] = df_nonpc["IO"].astype(int)
        except:
            pass
        try:
            from backend.upload_forecasts_nonpc import upload_nonpc_forecasts_local_m
            changed = upload_nonpc_forecasts_local_m(df_nonpc)
            if changed > 0:
                results.append("Non-personnel forecast uploaded successfully.")
            else:
                results.append("Non-personnel forecast already exists.")
        except Exception as e:
            results.append(f"Non-personnel upload failed: {e}")
    # Personnel
    if form.get("Personnel_cost"):
        df_pc = pd.DataFrame([pc_row])
        try:
            df_pc["IO"] = df_pc["IO"].astype(int)
        except:
            pass
        # project_forecasts_pc table does not store personnel_expense - drop Personnel cost before upload
        if 'Personnel cost' in df_pc.columns:
            df_pc = df_pc.drop(columns=['Personnel cost'])
        try:
            from backend.upload_forecasts_pc import upload_pc_forecasts_local_m
            changed = upload_pc_forecasts_local_m(df_pc)
            if changed > 0:
                results.append("Personnel forecast uploaded successfully.")
            else:
                results.append("Personnel forecast already exists.")
        except Exception as e:
            results.append(f"Personnel upload failed: {e}")
    if not results:
        return "No forecast data provided. Please fill at least one forecast field.", 400
    return "<br>".join(results)