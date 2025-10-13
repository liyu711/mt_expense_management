alter table cost_elements
ADD CONSTRAINT UQ_name_id UNIQUE (co_id, name)