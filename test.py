from backend import \
    get_departments_display, get_forecasts_display, get_pc_display, get_projects_display, \
    get_project_cateogory_display, get_IO_display_table, get_nonpc_display, \
    get_hr_category_display, create_funding_display, get_budget_display_table, get_capex_expenses_display, \
    get_expenses_display
    

if __name__ == '__main__':
    res = get_capex_expenses_display()
    print(res)

