-- Query for creating all the tables in cost


-- remove all tables

---- Predictions
/*ϵͳ��ɫ��ְ���б�
- id pk
- ְ��
*/
create table roles(
	id int identity(1,1) primary key,
	role_name varchar(255) not null
);

CREATE TABLE users (
	ID INT IDENTITY(1,1) PRIMARY KEY,
	username varchar(255) not null,
	name varchar(255),
	role_id int,
	constraint fk_users_roles foreign key (role_id) references roles(id)
);

create table co_object_names(
	id int identity(1,1) primary key,
	name varchar(256) not null
);

-- Cost element �б�
create table cost_elements (
	id int identity(1,1) primary key,
	co_id int not null,
	name varchar(256) not null,
);

create table POs (
	id int identity(1,1) primary key,
	name varchar(256) not null
);

create table project_categories(
	id int identity(1,1) primary key,
	category varchar(256) not null
);


create table departments(
	id int identity(1,1) primary key,
	name varchar(1000) not null
);

create table projects(
	id int identity(1,1) primary key,
	name varchar(256),
	category_id int,
	foreign key (category_id) references project_categories(id) 
);

create table IOs(
	id int identity(1,1) primary key,
	IO_num int,
	project_id int,
	foreign key (project_id) references projects(id)
);

create table IO_CE_connection(
	id int identity(1,1) primary key,
	IO_id int,
	cost_element_id int,
	foreign key (IO_id) references IOs(id),
	foreign key (cost_element_id) references cost_elements(id)
);

create table human_resource_categories(
	id int identity(1,1) primary key,
	name varchar(1000) not null,
	po_id int,
	department_id int,
	foreign key (po_id) references POs(id),
	foreign key (department_id) references departments(id)
);

create table human_resource_expense(
	id int identity(1,1) primary key,
	value int not null,
	category_id int,
	constraint k_human_resource_category_human_resource_expense
		foreign key (category_id) references human_resource_categories(id)
);

create table project_forecasts_nonpc(
	id int identity(1,1) primary key,
	PO_id int,
	department_id int,
	project_category_id int,
	project_id int,
	io_id int,
	fiscal_year DATE,
	non_personnel_expense float,
	foreign key (department_id) references departments(id),
	foreign key (project_id) references projects(id),
	foreign key (PO_id) references POs(id),
	foreign key (project_category_id) references project_categories(id),
	foreign key (io_id) references IOs(id)
);



create table project_forecasts_pc(
	id int identity(1,1) primary key,
	PO_id int,
	department_id int,
	project_category_id int,
	project_id int,
	io_id int,
	fiscal_year DATE,
	human_resource_category_id int,
	huamn_resource_fte float,
	personnel_expense float,
	foreign key (department_id) references departments(id),
	foreign key (project_id) references projects(id),
	foreign key (PO_id) references POs(id),
	foreign key (project_category_id) references project_categories(id),
	foreign key (io_id) references IOs(id),
	foreign key (human_resource_category_id) references human_resource_categories(id),
);

---- Budget
create table budgets(
	id int identity(1,1) primary key,
	po_id int,
	department_id int,
	fiscal_year DATE,
	human_resource_expense float not null,
	non_personnel_expense float not null,
	foreign key (po_id) references POs(id),
	foreign key (department_id) references departments(id)
);

create table fundings(
	id int identity(1,1) primary key,
	po_id int,
	department_id int,
	fiscal_year DATE,
	funding float not null,
	funding_from varchar(1000) not null,
	funding_for varchar(1000) not null,
	foreign key (po_id) references POs(id),
	foreign key (department_id) references departments(id)
);


---- Expenses
create table expenses(
	id int identity(1,1) primary key,
	co_object_id int,
	department_id int,
	fiscal_year int,
	from_period int,
	io_id int,
	cost_element_id int,
	co_element_name varchar(256),
	expense_value float not null,
	name varchar(1000),
	foreign key (co_object_id) references co_object_names(id),
	foreign key (department_id) references departments(id),
	foreign key (io_id) references IOs(id),
	foreign key (cost_element_id) references cost_elements(id),
);

---- CapEx
---- Predictions
create table capex_forecasts(
	id int identity(1,1) primary key,
	po_id int,
	department_id int,
	cap_year date,
	project_id int,
	capex_description varchar(1000) not null,
	capex_forecast float not null,
	cost_center varchar(1000) not null,
	foreign key (po_id) references pos(id),
	foreign key (project_id) references projects(id),
	foreign key (department_id) references departments(id),
);

---- Buedget
create table capex_budgets(
	id int identity(1,1) primary key,
	po_id int,
	department_id int,
	cap_year date,
	project_id int,
	capex_description varchar(1000) not null,
	budget float not null,
	foreign key (po_id) references pos(id),
	foreign key (project_id) references projects(id),
	foreign key (department_id) references departments(id),
);

--- Expenses
create table capex_expenses(
	id int identity(1,1) primary key,
	po_id int,
	department_id int,
	cap_year date,
	project_id int,
	capex_description varchar(1000) not null,
	project_number int not null,
	expense float,
	expense_date date,
	foreign key (po_id) references pos(id),
	foreign key (project_id) references projects(id),
	foreign key (department_id) references departments(id),
)

-- test 




-- For display & report

---- CapEx testing data

--- select all capex



