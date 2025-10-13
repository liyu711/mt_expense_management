select 'Forecasts' as title,
		*
from project_forecasts

-- For export
-- create project forecast table

select 
	project_forecasts.fiscal_year as year,
	POs.name as PO,
	departments.name as department,
	project_categories.category as project_category,
	projects.name as project,
	IOs.IO_num as IO,
	personnel_expense,
	non_personnel_expense,
	project_forecasts.huamn_resource_fte as fte
from project_forecasts
left join POs on PO_id = POs.id
left join departments on department_id = departments.id
left join project_categories on project_category_id = project_categories.id
left join projects on project_id = projects.id
left join human_resource_categories on human_resource_category_id = human_resource_categories.id
left join IOs on io_id = IOs.id

-- Summarize Forecast by project 
select 
	'Forecast summary by Project and IO' as title,
	sub.project as projects,
	sub.IO as IO,
	sum(project_forecasts.personnel_expense) as personnel_expense,
	sum(project_forecasts.non_personnel_expense) as non_personnel_expense,
	sum(project_forecasts.personnel_expense) + sum(project_forecasts.non_personnel_expense) as total_expense
from (
	select 
		projects.name as project,
		IOs.IO_num as IO
	from IOs
	join projects on projects.id = IOs.project_id
	group by projects.name, IO_num

) sub
join IOs on sub.IO = IOs.IO_num
join projects on sub.project = projects.name
join project_forecasts on IOs.id = project_forecasts.io_id
group by sub.project, sub.IO

--- Create expense summary table 
select 'Expenses' as title,
	*
from expenses

select
	'Expense Summary' as title,
	total.projects as projects,
	total.IO as IO,
	projects.department_id as dep_id,
	total.total_expense as prediction,
	expenses.expense_value as expense,
	expenses.fiscal_year as year
from (
	select 
	sub.project as projects,
	sub.IO as IO,
	sum(project_forecasts.personnel_expense) as personnel_expense,
	sum(project_forecasts.non_personnel_expense) as non_personnel_expense,
	sum(project_forecasts.personnel_expense) + sum(project_forecasts.non_personnel_expense) as total_expense
from (
	select 
		projects.name as project,
		IOs.IO_num as IO
	from IOs
	join projects on projects.id = IOs.project_id
	group by projects.name, IO_num

) sub
join IOs on sub.IO = IOs.IO_num
join projects on sub.project = projects.name
join project_forecasts on IOs.id = project_forecasts.io_id
group by sub.project, sub.IO
) as total
join projects on projects.name = total.projects
join IOs on IOs.IO_num = total.IO
join departments on projects.department_id = departments.id
join project_forecasts on IOs.id = project_forecasts.io_id
left join expenses on IOs.id = expenses.io_id


select 'CapEx Forecast' as title,
	*
from capex_forecasts

select 'Capex Budget' as title,
	*
from capex_budgets


select 'CapEx Expense' as title,
		*
from capex_expenses


---- Export capex summary for each project
select
	'CapEx summary for each project' as title,
	POs.name as Po,
	departments.name as Department,
	capex_expenses.cap_year as CapYear,
	projects.name as Project,
	capex_expenses.capex_description as CapDescription,
	capex_forecasts.capex_forecast as CapForecast,
	capex_forecasts.cost_center as CostCenter,
	capex_budgets.budget as ApprovedBudget,
	capex_expenses.project_number as ProjectNumber,
	capex_expenses.expense as Actual,
	capex_budgets.budget - capex_expenses.expense as Remained
from capex_expenses
join capex_budgets on capex_expenses.project_id = capex_budgets.project_id
join capex_forecasts on capex_expenses.project_id = capex_forecasts.project_id
join projects on capex_expenses.project_id = projects.id
join departments on capex_expenses.department_id = departments.id
join POs on capex_expenses.po_id = POs.id