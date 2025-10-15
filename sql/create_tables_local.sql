-- Query for creating all the tables in cost
CREATE TABLE roles(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    role_name TEXT NOT NULL
);

CREATE TABLE users (
    ID INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL,
    name TEXT,
    role_id INTEGER,
    CONSTRAINT fk_users_roles FOREIGN KEY (role_id) REFERENCES roles(id)
);

CREATE TABLE co_object_names(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL
);

-- Cost element
CREATE TABLE cost_elements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    co_id INTEGER NOT NULL,
    name TEXT NOT NULL
);

CREATE TABLE POs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL
);

CREATE TABLE project_categories(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    category TEXT NOT NULL
);

CREATE TABLE departments(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL
);

CREATE TABLE projects(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    category_id INTEGER,
    FOREIGN KEY (category_id) REFERENCES project_categories(id)
);

CREATE TABLE IOs(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    IO_num INTEGER,
    project_id INTEGER,
    FOREIGN KEY (project_id) REFERENCES projects(id)
);

CREATE TABLE IO_CE_connection(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    IO_id INTEGER,
    cost_element_id INTEGER,
    FOREIGN KEY (IO_id) REFERENCES IOs(id),
    FOREIGN KEY (cost_element_id) REFERENCES cost_elements(id)
);

CREATE TABLE human_resource_categories(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL
);

CREATE TABLE human_resource_cost(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    category_id INTEGER,
    cost REAL,
    year INTEGER,
    FOREIGN KEY (category_id) REFERENCES human_resource_categories(id)
);

CREATE TABLE project_forecasts_nonpc(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    PO_id INTEGER,
    department_id INTEGER,
    project_category_id INTEGER,
    project_id INTEGER,
    io_id INTEGER,
    fiscal_year INTEGER,
    non_personnel_expense REAL,
    FOREIGN KEY (department_id) REFERENCES departments(id),
    FOREIGN KEY (project_id) REFERENCES projects(id),
    FOREIGN KEY (PO_id) REFERENCES POs(id),
    FOREIGN KEY (project_category_id) REFERENCES project_categories(id),
    FOREIGN KEY (io_id) REFERENCES IOs(id)
);

CREATE TABLE project_forecasts_pc(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    PO_id INTEGER,
    department_id INTEGER,
    project_category_id INTEGER,
    project_id INTEGER,
    io_id INTEGER,
    fiscal_year INTEGER,
    human_resource_category_id INTEGER,
    huamn_resource_fte REAL,
    personnel_expense REAL,
    FOREIGN KEY (department_id) REFERENCES departments(id),
    FOREIGN KEY (project_id) REFERENCES projects(id),
    FOREIGN KEY (PO_id) REFERENCES POs(id),
    FOREIGN KEY (project_category_id) REFERENCES project_categories(id),
    FOREIGN KEY (io_id) REFERENCES IOs(id),
    FOREIGN KEY (human_resource_category_id) REFERENCES human_resource_categories(id)
);

---- Budget
CREATE TABLE budgets(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    po_id INTEGER,
    department_id INTEGER,
    fiscal_year INTEGER,
    human_resource_expense REAL NOT NULL,
    non_personnel_expense REAL NOT NULL,
    FOREIGN KEY (po_id) REFERENCES POs(id),
    FOREIGN KEY (department_id) REFERENCES departments(id)
);

CREATE TABLE fundings(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    po_id INTEGER,
    department_id INTEGER,
    fiscal_year INTEGER,
    funding REAL NOT NULL,
    funding_from TEXT NOT NULL,
    funding_for TEXT NOT NULL,
    FOREIGN KEY (po_id) REFERENCES POs(id),
    FOREIGN KEY (department_id) REFERENCES departments(id)
);

---- Expenses
CREATE TABLE expenses(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    co_object_id INTEGER,
    department_id INTEGER,
    fiscal_year INTEGER,
    from_period INTEGER,
    io_id INTEGER,
    cost_element_id INTEGER,
    co_element_name TEXT,
    expense_value REAL NOT NULL,
    name TEXT,
    FOREIGN KEY (co_object_id) REFERENCES co_object_names(id),
    FOREIGN KEY (department_id) REFERENCES departments(id),
    FOREIGN KEY (io_id) REFERENCES IOs(id),
    FOREIGN KEY (cost_element_id) REFERENCES cost_elements(id)
);

---- CapEx
---- Predictions
CREATE TABLE capex_forecasts(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    po_id INTEGER,
    department_id INTEGER,
    cap_year INTEGER,
    project_id INTEGER,
    capex_description TEXT NOT NULL,
    capex_forecast REAL NOT NULL,
    cost_center TEXT NOT NULL,
    FOREIGN KEY (po_id) REFERENCES pos(id),
    FOREIGN KEY (project_id) REFERENCES projects(id),
    FOREIGN KEY (department_id) REFERENCES departments(id)
);

---- Budget
CREATE TABLE capex_budgets(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    po_id INTEGER,
    department_id INTEGER,
    cap_year INTEGER,
    project_id INTEGER,
    capex_description TEXT NOT NULL,
    budget REAL NOT NULL,
    FOREIGN KEY (po_id) REFERENCES pos(id),
    FOREIGN KEY (project_id) REFERENCES projects(id),
    FOREIGN KEY (department_id) REFERENCES departments(id)
);

--- Expenses
CREATE TABLE capex_expenses(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    po_id INTEGER,
    department_id INTEGER,
    cap_year INTEGER,
    project_id INTEGER,
    capex_description TEXT NOT NULL,
    project_number INTEGER NOT NULL,
    expense REAL,
    expense_date TEXT,
    FOREIGN KEY (po_id) REFERENCES pos(id),
    FOREIGN KEY (project_id) REFERENCES projects(id),
    FOREIGN KEY (department_id) REFERENCES departments(id)
);