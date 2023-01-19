-- Your SQL goes here
ALTER TABLE user_permissions ADD CONSTRAINT unique_user_project_permission UNIQUE(user_id, project_id);
