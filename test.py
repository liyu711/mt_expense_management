from backend import \
    get_departments_display, get_forecasts_display, get_pc_display, get_projects_display, \
    get_project_cateogory_display, get_IO_display_table, get_nonpc_display

if __name__ == '__main__':
    res = get_pc_display()
    res = res[res['Personnel Cost']==14664]
    print(res)
    print(res.columns)
    print(res['Personnel Cost'])


