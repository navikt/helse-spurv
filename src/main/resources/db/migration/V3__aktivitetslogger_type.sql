ALTER TABLE aktivitetslogger_aktivitet ADD COLUMN type VARCHAR(32) NOT NULL DEFAULT 'ERROR';
ALTER TABLE aktivitetslogger_aktivitet ALTER COLUMN type DROP DEFAULT;
