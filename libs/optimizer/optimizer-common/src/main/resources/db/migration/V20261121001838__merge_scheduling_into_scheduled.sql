-- The scheduler now claims an operation as SCHEDULED, and job_id stays NULL until the Jobs Service
-- accepts the job. SCHEDULING is no longer a status, so a row that a previous scheduler left in it
-- would fail to load.
UPDATE table_operations SET status = 'SCHEDULED' WHERE status = 'SCHEDULING';
