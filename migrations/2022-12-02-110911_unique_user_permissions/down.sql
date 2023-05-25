-- This file should undo anything in `up.sql`

-- The UNIQUE constraint cannot be dropped directly inside Cockroach DB.
-- To remove the constraint, drop the index that was created by the constraint, e.g., DROP INDEX ... CASCADE

-- Warning:
--   CASCADE drops all dependent objects without listing them, which can lead to inadvertent and difficult-to-recover losses.
--   To avoid potential harm, it is recommended dropping objects individually in most cases.
DROP INDEX unique_user_project_permission;
