INSERT IGNORE INTO table_toggle_rule (feature, database_pattern, table_pattern, id, creation_time_ms) VALUES ('demo', 'demodb', 'demotable', DEFAULT, 987);
INSERT IGNORE INTO table_toggle_rule (feature, database_pattern, table_pattern, id, creation_time_ms) VALUES ('dummy1', 'db', 'tbl', DEFAULT, 987);
INSERT IGNORE INTO table_toggle_rule (feature, database_pattern, table_pattern, id, creation_time_ms) VALUES ('dummy1', 'db', 'testtbl1', DEFAULT, 987);
INSERT IGNORE INTO table_toggle_rule (feature, database_pattern, table_pattern, id, creation_time_ms) VALUES ('dummy2', 'db', 'tbl', DEFAULT, 987);
INSERT IGNORE INTO table_toggle_rule (feature, database_pattern, table_pattern, id, creation_time_ms) VALUES ('stop_create', 'db_no_create', '*', DEFAULT, 987);
