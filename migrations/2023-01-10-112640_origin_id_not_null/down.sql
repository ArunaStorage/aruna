-- This file should undo anything in `up.sql`
ALTER TABLE objects ALTER COLUMN origin_id DROP NOT NULL;