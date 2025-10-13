from generate_data import generate_tables
from connect_pyodbc import connect_to_sql, close_connection, initialize_database
from upload_forecasts_nonpc import upload_nonpc_forecasts
from upload_forecasts_pc import upload_pc_forecasts
from upload_expenses import upload_expenses
from upload_budgets import upload_budgets, upload_fundings
from upload_capex_forecast import upload_capex_forecasts


funding_path = 'processed_data\\budgets\\fundings_.csv'
filepath_nonpc = 'processed_data\\forecasts\\forecasts_nonpc_.csv'
filepath_pc = 'processed_data\\forecasts\\forecasts_pc_.csv'
file_path_expenses = "processed_data/expenses/expenses_.csv"
budget_path = 'processed_data\\budgets\\budgets_.csv'
capex_forecast_path = "processed_data/forecasts/capex_forecasts_.csv"
generate_tables()
engine, cursor, cnxn = connect_to_sql(engine=True)
initialize_database(cursor, cnxn)

upload_nonpc_forecasts(filepath_nonpc)
upload_pc_forecasts(filepath_pc)
upload_expenses(file_path_expenses)
upload_budgets(budget_path)
upload_fundings(funding_path)
upload_capex_forecasts(capex_forecast_path)
close_connection(cursor, cnxn)