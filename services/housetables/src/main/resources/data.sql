-- Initial value for feature toggle tables
-- When enabling/disabling some feature, please ensure they are checked-in and reviewed through this file

INSERT IGNORE INTO table_toggle_rule (feature, database_pattern, table_pattern, id, creation_time_ms) VALUES ('demo', 'demodb', 'demotable', DEFAULT, DEFAULT);