import pandas as pd
import random
departments = ['IW', 'OME', 'S&I', 'VEH']
POs = ['4500012345', '4500012346', '4500012347', '4500012348', '4500012349']
fiscal_years = [2022, 2023, 2024, 2025]
from_periods = list(range(1,13))
project_numbers = [random.randint(100000, 999999) for _ in range(75)]
cost_elements = pd.read_csv('processed_data\\info\\cost_elements.csv')
io_projects = pd.read_csv('processed_data\\info\\io_projects.csv')
human_resource_categories = pd.read_csv('processed_data\\info\\personnel_categories.csv')['Personnel Categories'].tolist()
project_categories = [
    'Category A',
    'Category B',
    'Category C',
    'Category D',
    'Category E',
    'Category F'
]
cost_centers = [
    'CC1001',
    'CC1002',
    'CC1003',
    'CC1004',
    'CC1005',
    'CC1006',
    'CC1007',
    'CC1008',
    'CC1009',
    'CC1010'
]
personnel_cost_per_hour = [
    120.0,   # MT Headcount
    100.0,   # Rehired Employee
    90.0,    # Outsourcing_MT Internal
    80.0,    # Borrowed Technician
    110.0    # Outsourcing_Neusoft
]
projects = io_projects['Project Name'].unique().tolist()
project_categories_mapping = {project: random.choice(project_categories) for project in projects}
personnel_cost_mapping = {human_resource_categories[i]: personnel_cost_per_hour[i] for i in range(len(human_resource_categories))}
io_projects['IO'] = io_projects['IO'].astype(int)
io = io_projects['IO'].unique().tolist()

def generate_random_expenses(num_records, save):
    expenses = pd.DataFrame(columns=['Department', 'fiscal_year','from_period','Order', 'CO object name', 'Cost element', 'Cost element name','Val.in rep.cur', 'Name'])
    io_choices = io_projects.sample(frac=1, random_state=42)
    io_choices = io_choices[:50]
    io_choices = io_choices['IO'].tolist()
    for i in range(num_records):
        row = {}
        row['Department'] = random.choice(departments)
        row['from_period'] = random.randint(1, 12)
        row['fiscal_year'] = random.choice(fiscal_years)
        row['Order'] = random.choice(io_choices)
        row['CO object name'] = 'Project ' + str(random.randint(1, 100))
        ce_row = cost_elements.sample()
        row['Cost element'] = ce_row['Cost Element'].values[0]
        row['Cost element name'] = ce_row['Cost element name'].values[0]
        row['Val.in rep.cur'] = round(random.uniform(1000, 10000), 2)
        row['Name'] = 'Expense ' + str(random.randint(1, 1000))
        expenses.loc[len(expenses)] = row
    if save:
        expenses.to_csv('processed_data\\expenses\\expenses_' + '.csv', index=False)
    return expenses

def generate_random_budgets(nums_record, save):
    budgets = pd.DataFrame(columns=['PO', 'Department', 'fiscal_year', 'Human Resources Budget', 'Non-Human Resources Budget'])
    fundings = pd.DataFrame(columns=['PO', 'Department', 'fiscal_year', 'funding', 'funding_from', 'funding_for'])
    io_choices = io_projects.sample(frac=1, random_state=42)
    io_choices = io_choices[:nums_record]
    for i in range(nums_record):
        io_row = io_choices.iloc[i]
        row = {}
        row['PO'] = random.choice(POs)
        row['Department'] = random.choice(departments)
        row['fiscal_year'] = random.choice(fiscal_years)
        row['Human Resources Budget'] = round(random.uniform(50000, 200000), 2)
        row['Non-Human Resources Budget'] = round(random.uniform(20000, 100000), 2)
        budgets.loc[len(budgets)] = row

        f_row = {}
        f_row['PO'] = random.choice(POs)
        f_row['Department'] = random.choice(departments)
        f_row['fiscal_year'] = random.choice(fiscal_years)
        f_row['funding'] = round(random.uniform(10000, 50000), 2)
        f_row['funding_from'] = 'Source ' + str(random.randint(1, 10))
        f_row['funding_for'] = io_row['Project Name']
        fundings.loc[len(fundings)] = f_row

    if save:
        budgets.to_csv('processed_data\\budgets\\budgets_' + '.csv', index=False)
        fundings.to_csv('processed_data\\budgets\\fundings_' + '.csv', index=False)
    return budgets, fundings

def generate_random_forecasts(num_records, save):
    forecasts_pc = pd.DataFrame(columns=['PO','IO', 'Department', 'Project Category', 'Project Name', 'fiscal_year','Human resource category', 'Human resource FTE', 'Personnel cost'])
    forecasts_nonpc = pd.DataFrame(columns=['PO','IO', 'Department', 'Project Category', 'Project Name', 'fiscal_year','Non-personnel cost'])
    row_pc = {}
    row_nonpc = {}
    io_choices = io_projects.sample(frac=1, random_state=42)
    io_choices = io_choices[:num_records]
    for i in range(num_records):
        department = random.choice(departments)
        io_row = io_choices.iloc[i]
        fiscal_year = random.choice(fiscal_years)
        po = random.choice(POs)

        for j in range(5):
            row_pc['PO'] = po
            row_pc['IO'] = io_row['IO']
            row_pc['Department'] = department
            row_pc['Project Category'] = project_categories_mapping[io_row['Project Name']]
            row_pc['Project Name'] = io_row['Project Name']
            row_pc['fiscal_year'] = fiscal_year
            row_pc['Human resource category'] = human_resource_categories[j]
            row_pc['Human resource FTE'] = round(random.uniform(1.0, 10.0), 2)
            row_pc['Personnel cost'] = personnel_cost_mapping[human_resource_categories[j]] * row_pc['Human resource FTE']
            forecasts_pc.loc[len(forecasts_pc)] = row_pc
            row_pc = {}
        
        row_nonpc['PO'] = po
        row_nonpc['IO'] = io_row['IO']
        row_nonpc['Department'] = department
        row_nonpc['Project Category'] = project_categories_mapping[io_row['Project Name']]
        row_nonpc['Project Name'] = io_row['Project Name']
        row_nonpc['fiscal_year'] = fiscal_year
        row_nonpc['Non-personnel cost'] = round(random.uniform(1000, 10000), 2)
        forecasts_nonpc.loc[len(forecasts_nonpc)] = row_nonpc
        row_nonpc = {}
    if save:
        forecasts_pc.to_csv('processed_data\\forecasts\\forecasts_pc_' + '.csv', index=False)
        forecasts_nonpc.to_csv('processed_data\\forecasts\\forecasts_nonpc_' + '.csv', index=False)
    return forecasts_pc, forecasts_nonpc

def generate_capex_budgets(num_records, save):
    io_choices = io_projects.sample(frac=1, random_state=42)
    io_choices = io_choices[:num_records]
    capex_budgets = pd.DataFrame(columns = ['PO', 'Department', 'fiscal_year', 'for_project', 'capex_description', 'budget'])
    for i in range(num_records):
        io_row = io_choices.iloc[i]
        row = {}
        row['PO'] = random.choice(POs)
        row['Department'] = random.choice(departments)
        row['fiscal_year'] = random.choice(fiscal_years)
        row['for_project'] = io_row['Project Name']
        row['capex_description'] = 'Capex ' + str(random.randint(1, 100))
        row['budget'] = round(random.uniform(5000, 50000), 2)
        capex_budgets.loc[len(capex_budgets)] = row

    if save:
        capex_budgets.to_csv('processed_data\\budgets\\capex_budgets_' + '.csv', index=False)
    return capex_budgets

def generate_capex_expenses(num_records, save):
    io_choices = io_projects.sample(frac=1, random_state=42)
    io_choices = io_choices[:50]

    project_choices = io_choices['Project Name'].to_list()
    # print(project_choices)
    capex_expenses = pd.DataFrame(columns = ['PO', 'Department', 'fiscal_year', 'Project Name', 'capex_description', 'Project number', 'Expense'])
    for i in range(num_records):
        row = {}
        row['PO'] = random.choice(POs)
        row['Department'] = random.choice(departments)
        row['fiscal_year'] = random.choice(fiscal_years)
        row['Project Name'] = random.choice(project_choices)
        row['capex_description'] = 'Capex ' + str(random.randint(1, 100))
        row['Project number'] = random.randint(100000, 999999)
        row['Expense'] = round(random.uniform(1000, 10000), 2)
        capex_expenses.loc[len(capex_expenses)] = row

    if save:
        capex_expenses.to_csv('processed_data\\expenses\\capex_expenses_' + '.csv', index=False)
    return capex_expenses

def generate_capex_forecasts(num_records, save):
    capex_forecasts = pd.DataFrame(columns = ['PO', 'Department', 'cap_year', 'Project name','capex_description', 'Forecast', 'cost_center'])
    
    io_choices = io_projects.sample(frac=1, random_state=42)
    io_choices = io_choices[:num_records]
    
    for i in range(num_records):
        io_row = io_choices.iloc[i]
        row = {}
        row['PO'] = random.choice(POs)
        row['Department'] = random.choice(departments)
        row['cap_year'] = random.choice(fiscal_years)
        row['Project name'] = io_row['Project Name']
        row['project_number'] = project_numbers[i]
        row['capex_description'] = 'Capex ' + str(random.randint(1, 100))
        row['cost_center'] = random.choice(cost_centers)
        row['Forecast'] = round(random.uniform(1000, 10000), 2)
        capex_forecasts.loc[len(capex_forecasts)] = row
    
    if save:
        capex_forecasts.to_csv('processed_data\\forecasts\\capex_forecasts_' + '.csv', index=False) 
    return capex_forecasts

def generate_tables():
    forecasts_pc, forecasts_nonpc = generate_random_forecasts(50, save=True)
    budgets, fundings = generate_random_budgets(50, save=True)
    expenses = generate_random_expenses(100, save=True)
    capex_expenses = generate_capex_expenses(100, save=True)
    capex_forecasts = generate_capex_forecasts(50, save=True)
    capex_budgets = generate_capex_budgets(50, save=True)


if __name__ == "__main__":
    # generate_tables()
    # budgets, fundings = generate_random_budgets(50, save=True)
    # capex_budgets = generate_capex_budgets(50, save=True)
    capex_expenses = generate_capex_expenses(100, save=True)
