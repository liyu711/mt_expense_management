from flask import Flask, flash, render_template, request, redirect, url_for, Blueprint
from backend.connect_pyodbc import \
    connect_to_sql, select_all_from_table, initialize_database, close_connection
from backend.select_data import select_projects
from backend.upload_expenses import upload_expenses_df
from werkzeug.utils import secure_filename
from backend.login import valid_login
from backend.upload_forecasts_nonpc import upload_nonpc_forecasts_df
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
    "forecast_pc" : ["PO","IO","Department","Project Category","Project Name","fiscal year","Human resource category","Human resource FTE","Personnel cost"],
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
    return render_template("pages/manual_input.html", input_types = input_types, titles = titles, display_names=display_names, upload_columns=upload_columns)

@manual_upload.route("/upload_forecast_pc", methods=['POST'])
def upload_forecast_pc_r():
    df = pd.DataFrame(columns = ["PO","IO","Department","Project Category","Project Name","fiscal_year","Human resource category","Human resource FTE","Personnel cost"])
    row = {}
    for fieldname, value in request.form.items():
        print(fieldname, value)
    return "Upload successful"

@manual_upload.route("/upload_forecast_nonpc", methods=['POST'])
def upload_forecast_nonpc_r():
    df = pd.DataFrame(columns = ["PO","IO","Department","Project Category","Project Name","fiscal_year","Non-personnel cost"])
    row = {}
    for fieldname, value in request.form.items():
        # print(fieldname, value)
        row[fieldname] = value
    df.loc[len(df)] = row
    df["IO"] = df["IO"].astype(int)
    upload_nonpc_forecasts_df(df)
    return "Upload successful"