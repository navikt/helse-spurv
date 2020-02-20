package no.nav.helse.spurv

import kotliquery.Session
import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import java.time.LocalDate
import java.util.*
import javax.sql.DataSource

internal class VedtaksperioderapportDao(private val dataSource: DataSource) {

    fun lagRapport(dato: LocalDate) =
        using(sessionOf(dataSource)) {
            it.run(queryOf("SELECT DISTINCT ON(vedtaksperiode_id) vedtaksperiode_id, tilstand, dato FROM vedtaksperiode_tilstand WHERE dato <= ?", dato).map {
                UUID.fromString(it.string(1)) to (it.string(2) to it.localDate(3))
            }.asList)
        }.associate { it }

    fun leggInnVedtaksperiode(id: UUID, tilstand: String, dato: LocalDate) =
        using(sessionOf(dataSource)) {
            it.transaction { tx ->
                if (!finnes(tx, id, dato)) leggInn(tx, id, tilstand, dato)
                else oppdater(tx, id, tilstand, dato)
            }
        }

    private fun finnes(session: Session, id: UUID, dato: LocalDate): Boolean {
        return 1 == session.run(queryOf("SELECT COUNT(1) FROM vedtaksperiode_tilstand WHERE vedtaksperiode_id=? AND dato=?", id.toString(), dato).map {
            it.int(1)
        }.asSingle)
    }

    private fun leggInn(session: Session, id: UUID, tilstand: String, dato: LocalDate) {
        session.run(
            queryOf(
                "INSERT INTO  vedtaksperiode_tilstand (vedtaksperiode_id, tilstand, dato) VALUES (?, ?, ?)",
                id,
                tilstand,
                dato
            ).asExecute
        )
    }

    private fun oppdater(session: Session, id: UUID, tilstand: String, dato: LocalDate) {
        session.run(queryOf("UPDATE vedtaksperiode_tilstand SET tilstand=? WHERE vedtaksperiode_id=? AND dato=?", tilstand, id.toString(), dato).asExecute)
    }
}
