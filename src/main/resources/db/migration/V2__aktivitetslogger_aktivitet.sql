CREATE TABLE aktivitetslogger_aktivitet
(
    melding TEXT NOT NULL,
    dato    DATE NOT NULL
);

create index "index_melding" on aktivitetslogger_aktivitet using btree (melding);
