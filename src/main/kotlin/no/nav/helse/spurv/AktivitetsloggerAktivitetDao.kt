package no.nav.helse.spurv

import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import java.time.LocalDate
import javax.sql.DataSource

internal class AktivitetsloggerAktivitetDao(private val dataSource: DataSource) {

    fun lagRapport(dato: LocalDate) =
        using(sessionOf(dataSource)) {
            it.run(
                queryOf(
                    "SELECT type, melding, COUNT(1) FROM aktivitetslogger_aktivitet WHERE dato = ? GROUP BY type, melding",
                    dato
                ).map {
                    Triple(it.string(1), it.string(2), it.long(3))
                }.asList
            )
        }.groupBy { it.first }
            .mapValues { it.value.associate { it.second to it.third } }

    fun leggInnAktivitet(type: String, melding: String, dato: LocalDate) =
        using(sessionOf(dataSource)) {
            it.run(
                queryOf(
                    "INSERT INTO  aktivitetslogger_aktivitet (type, melding, dato) VALUES (?, ?, ?)",
                    type,
                    melding,
                    dato
                ).asExecute
            )
        }
}
