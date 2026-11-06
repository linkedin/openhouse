-- Earliest time this (table, operation_type) may be (re)scheduled. Written when an attempt reaches a
-- terminal outcome: a reported success sets the success cadence, a reported failure or an expiry
-- (no callback by the deadline) sets the failure backoff. The scheduler/analyzer gates on this so a
-- reported failure retries after its backoff instead of waiting out the stale-timeout, and a dead
-- job backs off instead of rescheduling immediately. NULL means available now (never run / running).
ALTER TABLE table_operations ADD COLUMN available_at TIMESTAMP(6) NULL;
