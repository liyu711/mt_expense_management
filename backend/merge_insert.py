import pandas as pd

def join_tables(left, right, left_col, right_col, drops, rename_dic):
    res = pd.merge(left, right, left_on=left_col, right_on=right_col, how='left')
    print(res.columns)
    res.drop(drops, axis=1, inplace=True)
    res.rename(columns=rename_dic, inplace=True)
    return res

def merge_dataframes(cloud, local, columns, merge_on):
    local = local.drop_duplicates().reset_index(drop=True)
    local.columns = columns
    upload = local[~local[merge_on].isin(cloud[merge_on])]
    return upload


# Not used, need to check if is used
def merge_cost_elements(cloud, local, columns, merge_on):
    local = local.drop_duplicates('Cost element').reset_index(drop=True)
    local.columns = columns
    upload = local[~local[merge_on].isin(cloud[merge_on])]
    return upload

def merge_departments(cloud, local):
    local = local.drop_duplicates().reset_index(drop=True)
    local.columns = ['name', 'po_id']
    # print(type(local))
    upload = local[~local['name'].isin(cloud['name'])]
    return upload

def insert_into_departments(cursor, cnxn, dataframe):
    insert_query = \
    """
    INSERT INTO departments (
    name
    )
    VALUES (
        ?
    );
    """
    for i, row in dataframe.iterrows():
        values = (row['name'])
        cursor.execute(insert_query, values)
        cnxn.commit()
