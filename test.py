from backend import \
    get_departments_display, get_forecasts_display, get_pc_display, get_projects_display, \
    get_project_cateogory_display, get_IO_display_table, get_nonpc_display, \
    get_hr_category_display

if __name__ == '__main__':
    res = get_hr_category_display()
    print(res)
    print(res.columns)


