-- Per-(table, operation_type) retry attempt counter: the attempt number since the last success
-- (1 for the first attempt after a success, incremented per retry), carried forward across attempts
-- and reset on success. Lets the retry backoff count consecutive non-successes directly on the row,
-- independent of the history windows (which can misalign and miscount a recorded success as a
-- timeout). NOT NULL with a default so existing rows start a fresh streak.
ALTER TABLE table_operations ADD COLUMN attempts_since_success INT NOT NULL DEFAULT 0;
