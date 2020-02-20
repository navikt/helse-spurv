CREATE TABLE vedtaksperiode_tilstand
(
    vedtaksperiode_id VARCHAR(64) NOT NULL,
    tilstand          VARCHAR(64) NOT NULL,
    dato              DATE        NOT NULL,
    PRIMARY KEY (vedtaksperiode_id, dato)
);
