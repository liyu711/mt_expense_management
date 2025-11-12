CREATE UNIQUE INDEX IF NOT EXISTS UQ_name_id ON cost_elements (co_id, name);

-- Drop PO foreign keys from project forecast tables if they exist (for migrated databases)
PRAGMA foreign_keys=off;
BEGIN TRANSACTION;
-- project_forecasts_nonpc
CREATE TABLE IF NOT EXISTS _temp_project_forecasts_nonpc AS SELECT * FROM project_forecasts_nonpc;
DROP TABLE IF EXISTS project_forecasts_nonpc;
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
	FOREIGN KEY (project_category_id) REFERENCES project_categories(id),
	FOREIGN KEY (io_id) REFERENCES IOs(id)
);
INSERT OR IGNORE INTO project_forecasts_nonpc SELECT * FROM _temp_project_forecasts_nonpc;
DROP TABLE _temp_project_forecasts_nonpc;
-- project_forecasts_pc
CREATE TABLE IF NOT EXISTS _temp_project_forecasts_pc AS SELECT * FROM project_forecasts_pc;
DROP TABLE IF EXISTS project_forecasts_pc;
CREATE TABLE project_forecasts_pc(
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	PO_id INTEGER,
	department_id INTEGER,
	project_category_id INTEGER,
	project_id INTEGER,
	io_id INTEGER,
	fiscal_year INTEGER,
	human_resource_category_id INTEGER,
	human_resource_fte REAL,
	FOREIGN KEY (department_id) REFERENCES departments(id),
	FOREIGN KEY (project_id) REFERENCES projects(id),
	FOREIGN KEY (project_category_id) REFERENCES project_categories(id),
	FOREIGN KEY (io_id) REFERENCES IOs(id),
	FOREIGN KEY (human_resource_category_id) REFERENCES human_resource_categories(id)
);
INSERT OR IGNORE INTO project_forecasts_pc SELECT * FROM _temp_project_forecasts_pc;
DROP TABLE _temp_project_forecasts_pc;
COMMIT;
PRAGMA foreign_keys=on;