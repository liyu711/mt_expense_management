from backend import \
    get_departments_display, get_forecasts_display, get_pc_display, get_projects_display, \
    get_project_cateogory_display

if __name__ == '__main__':
    res = get_projects_display()
    print(res)
    print(res.columns)
