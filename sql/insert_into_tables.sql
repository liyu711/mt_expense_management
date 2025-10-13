insert into cost_elements (co_id, name)
	values (9430210, 'Engineering Labor'),
		   (2, 'Invent''y Consump. CC'),
		   (7250050, 'R&D Suppl. & Mat''ls')
insert into POs(name)
	values('po1'),
			('po2'),
			('po3')

insert into project_categories (category)
	values ('AI'),
			('Manufacture'),
			('Electricity')



insert into departments (name)
	values ('R&D'),
		   ('POIND'),
		   ('MUE')

insert into projects (category_id,name)
	values (1, 'IND400'),
			(2, 'IND590'),
			(3, 'IND500x_P2'),
			(2, 'IND700')

insert into IOs (IO_num, project_id)
	values (7660, 1),
		   (1940, 1),
		   (3570, 2),
		   (3990, 3)


insert into human_resource_categories(name) 
	values ('Personnel cost_Technican'),
		('Test case')

INSERT INTO expenses (
    department_id,
    fiscal_year,
    from_period,
    io_id,
    cost_element_id,
    expense_value
)
VALUES
(
    2,  -- Must exist in the 'departments' table
    '2025',
    '2025',
    1, -- Must exist in the 'IOs' table
    2, -- Must exist in the 'cost_elements' table
    500
),
(
    2,  -- Must exist in the 'departments' table
    '2025',
    '2025',
    1, -- Must exist in the 'IOs' table
    2, -- Must exist in the 'cost_elements' table
    750.50
),
(
    2,  -- Must exist in the 'departments' table
    '2025',
    '2025',
    3, -- Must exist in the 'IOs' table
    2, -- Must exist in the 'cost_elements' table
    12000.75
),
(
    2,  -- Must exist in the 'departments' table
    '2025',
    '2025',
    3, -- Must exist in the 'IOs' table
    2, -- Must exist in the 'cost_elements' table
    1000.75
)

insert into project_forecasts_pc (
	PO_id,
	department_id,
	project_category_id,
	project_id,
	io_id,
	fiscal_year,
	human_resource_category_id,
	huamn_resource_fte,
	personnel_expense
)
values 
	(2, 1, 1, 3, 4, '2025-01-01', 1, 4, 100),
	(2, 1, 1, 3, 4, '2025-01-01', 1, 4, 100 ),
	(2, 2, 1, 1, 1, '2025-01-01', 1, 2.5, 150),
	-- Project 1, IO 2
	(2, 2, 1, 1, 2, '2025-01-01', 2, 1.0, 75),
	-- Project 2, IO 3
	(2, 2, 2, 2, 3, '2025-01-01', 1, 0.5, 40),
	-- Project 3, IO 4
	(2, 2, 1, 3, 4, '2025-01-01', 2, 1.5, 90)


insert into project_forecasts_nonpc (
	PO_id,
	department_id,
	project_category_id,
	project_id,
	io_id,
	fiscal_year,
	non_personnel_expense
)
values 
	(2, 1, 1, 3, 4, '2025-01-01', 200),
	(2, 1, 1, 3, 4, '2025-01-01', 200),
	(2, 2, 1, 1, 1, '2025-01-01', 500),
	-- Project 1, IO 2
	(2, 2, 1, 1, 2, '2025-01-01', 100),
	-- Project 2, IO 3
	(2, 2, 2, 2, 3, '2025-01-01', 50),
	-- Project 3, IO 4
	(2, 2, 1, 3, 4, '2025-01-01', 250)



INSERT INTO capex_forecasts (
    po_id,
    department_id,
    cap_year,
    project_id,
    capex_description,
    capex_forecast,
    cost_center
)
VALUES (
    1, -- Example po_id. Ensure this exists in the `pos` table.
    2, -- Example department_id. Ensure this exists in the `departments` table.
    '2025-09-01', -- Example cap_year.
    2, -- Example project_id. Ensure this exists in the `projects` table.
    'Description of capital expenditure for Q3 2025.', -- Example capex_description.
    50000.00, -- Example capex_forecast.
    'CC12345' -- Example cost_center.
)

INSERT INTO capex_budgets (
    po_id,
    department_id,
    cap_year,
    project_id,
    capex_description,
    budget
)
VALUES
    -- Example 1: Add your values here.
    (1, 2, '2025-01-01', 1, 'Upgrade server hardware for Q1', 50000.00),

    -- Example 2: Another sample record.
    (2, 2, '2025-02-15', 2, 'Marketing campaign for new product launch', 25000.50),

    -- Example 3: Additional sample record.
    (3, 1, '2025-03-10', 3, 'Renovation of office space', 150000.75)


INSERT INTO capex_expenses (
    po_id,
    department_id,
    cap_year,
    project_id,
    capex_description,
    project_number,
    expense,
    expense_date
)
VALUES
(
    1,  -- Example po_id. This must exist in the 'pos' table.
    2,  -- Example department_id. This must exist in the 'departments' table.
    '2025-01-15',
    2,  -- Example project_id. This must exist in the 'projects' table.
    'Server equipment upgrade for development team.',
    98765,
    15000.50,
    '2025-03-22'
)
