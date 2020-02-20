package no.nav.helse.spurv

import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import java.time.LocalDate
import javax.sql.DataSource

internal class AktivitetsloggerAktivitetDao(private val dataSource: DataSource) {

    fun lagRapport(dato: LocalDate) =
        using(sessionOf(dataSource)) {
            it.run(queryOf("SELECT melding, COUNT(1) FROM aktivitetslogger_aktivitet WHERE dato = ? GROUP BY melding", dato).map {
                it.string(1) to it.long(2)
            }.asList)
        }.associate { it }

    fun leggInnAktivitet(melding: String, dato: LocalDate) =
        using(sessionOf(dataSource)) {
            it.run(
                queryOf(
                    "INSERT INTO  aktivitetslogger_aktivitet (melding, dato) VALUES (?, ?)",
                    melding,
                    dato
                ).asExecute
            )
        }
}
