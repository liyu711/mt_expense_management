from backend import check_missing_attribute
import pandas as pd

if __name__ == '__main__':
    df_upload = pd.read_csv("C:\\Users\\li-776\\OneDrive - Mettler Toledo LLC\\文档\\intern\\Expense Management APP\\management_app\\processed_data\\forecasts\\forecasts_pc_.csv")
    res, columns = check_missing_attribute(df_upload, 'project_forecasts_pc', 'local')
    print(res)
    print(columns)
