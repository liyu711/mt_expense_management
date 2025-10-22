import pandas as pd

def join_tables(left, right, left_col, right_col, drops, rename_dic):
    res = pd.merge(left, right, left_on=left_col, right_on=right_col, how='left')
    res.drop(drops, axis=1, inplace=True)
    res.rename(columns=rename_dic, inplace=True)
    return res

def merge_dataframes(cloud, local, columns, merge_on, departments=False):
    """
    Return rows from `local` that are not exact duplicates of any row in `cloud`.

    This merges `local` with `cloud` using the full set of target `columns` and
    keeps only rows that do not have an exact match in `cloud`.
    """
    # Normalize column names on the incoming local DataFrame
    local.columns = columns

    # Ensure cloud has the same columns so we can compare row-wise
    cloud_subset = cloud.copy()
    for c in columns:
        if c not in cloud_subset.columns:
            cloud_subset[c] = pd.NA

    # Merge on all columns and use the indicator to find left-only rows
    merged = pd.merge(local, cloud_subset[columns], on=columns, how='left', indicator=True)
    upload = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge']).reset_index(drop=True)
    return upload

# Not used, need to check if is used
def merge_cost_elements(cloud, local, columns, merge_on):
    # Deduplication intentionally left to caller; preserve incoming rows as-is
    local.columns = columns
    upload = local[~local[merge_on].isin(cloud[merge_on])]
    return upload

def merge_departments(cloud, local):
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
