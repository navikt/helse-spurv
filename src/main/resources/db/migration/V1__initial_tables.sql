CREATE TABLE vedtaksperiode_tilstand
(
    vedtaksperiode_id VARCHAR(64) NOT NULL,
    tilstand          VARCHAR(64) NOT NULL,
    dato              DATE        NOT NULL,
    PRIMARY KEY (vedtaksperiode_id, dato)
);

CREATE TABLE aktivitetslogger_aktivitet
(
    type VARCHAR(32) NOT NULL,
    melding TEXT NOT NULL,
    dato    DATE NOT NULL
);

create index "index_type" on aktivitetslogger_aktivitet using btree (type);
create index "index_melding" on aktivitetslogger_aktivitet using btree (melding);
