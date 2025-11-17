tables_to_consider = [
    "project_forecasts_nonpc",
    "project_forecasts_pc",
    "budgets",
    "capex_forecasts",
    "capex_budgets"
]

table_column_dict = {
    "project_forecasts_nonpc": [
        "id", "PO_id", "department_id", "project_category_id", "project_id", "fiscal_year"
    ],
    "project_forecasts_pc": [
        "id", "PO_id", "department_id", "project_category_id", "project_id", "fiscal_year", "human_resource_category_id"
    ],
    "budgets": [
        "id", "po_id", "department_id", "fiscal_year"
    ],
    "fundings": [
        "id", "po_id", "department_id", "fiscal_year", "funding", "funding_from", "funding_for"
    ],
    "expenses": [
        "id", "co_object_id", "department_id", "fiscal_year", "from_period", "io_id",
        "cost_element_id", "co_element_name", "expense_value", "name"
    ],
    "capex_forecasts": [
        "id", "po_id", "department_id", "cap_year", "project_id", "capex_description",
        "cost_center"
    ],
    "capex_budgets": [
        "id", "po_id", "department_id", "cap_year", "project_id", "capex_description"
    ],
    "capex_expenses": [
        "id", "po_id", "department_id", "cap_year", "project_id", "capex_description",
        "project_number", "expense", "expense_date"
    ]
}
