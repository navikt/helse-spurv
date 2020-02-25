package no.nav.helse.spurv

import com.opentable.db.postgres.embedded.EmbeddedPostgres
import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.TestInstance.Lifecycle
import java.sql.Connection
import java.time.LocalDate
import javax.sql.DataSource

@TestInstance(Lifecycle.PER_CLASS)
internal class AktivitetsloggerAktivitetDaoTest {

    private lateinit var embeddedPostgres: EmbeddedPostgres
    private lateinit var postgresConnection: Connection
    private lateinit var dataSource: DataSource

    private lateinit var aktivitetDao: AktivitetsloggerAktivitetDao

    private val idag = LocalDate.now()
    private val imorgen = idag.plusDays(1)
    private val igår = idag.minusDays(1)
    private val iforgårs = igår.minusDays(1)

    @BeforeAll
    fun setup() {
        embeddedPostgres = EmbeddedPostgres.builder().start()
        postgresConnection = embeddedPostgres.postgresDatabase.connection

        val dataSourceBuilder = DataSourceBuilder(mapOf(
            "DATABASE_JDBC_URL" to embeddedPostgres.getJdbcUrl("postgres", "postgres")
        ))

        dataSource = dataSourceBuilder.getDataSource()
        dataSourceBuilder.migrate()

        aktivitetDao = AktivitetsloggerAktivitetDao(dataSource)
    }

    @AfterAll
    fun teardown() {
        dataSource.connection.close()
        postgresConnection.close()
        embeddedPostgres.close()
    }

    @BeforeEach
    fun clearTable() {
        using(sessionOf(dataSource)) {
            it.run(queryOf("DELETE FROM aktivitetslogger_aktivitet").asExecute)
        }
    }

    @Test
    internal fun `leggInnAktivitet`() {
        assertEquals(0, antall())
        val melding = "Støtter ikke gradert sykmelding"
        aktivitetDao.leggInnAktivitet("WARN", melding, igår)
        assertEquals(1, antall())
        aktivitetDao.leggInnAktivitet("WARN", melding, igår)
        assertEquals(2, antall())
    }

    @Test
    internal fun `lager rapport for i dag`() {
        val melding = "Støtter ikke gradert sykmelding"
        aktivitetDao.leggInnAktivitet("WARN", melding, igår)
        assertTrue(aktivitetDao.lagRapport(idag).isEmpty())

        aktivitetDao.leggInnAktivitet("WARN", melding, idag)
        assertTrue(aktivitetDao.lagRapport(idag).isNotEmpty())
    }

    @Test
    internal fun `teller forekomster av melding`() {
        "Støtter ikke gradert sykmelding".also {
            aktivitetDao.leggInnAktivitet("WARN", it, igår)
            aktivitetDao.leggInnAktivitet("WARN", it, igår)
            aktivitetDao.leggInnAktivitet("WARN", it, idag)
        }
        "Her gikk det gale".also {
            aktivitetDao.leggInnAktivitet("WARN", it, igår)
            aktivitetDao.leggInnAktivitet("WARN", it, igår)
            aktivitetDao.leggInnAktivitet("WARN", it, igår)
        }
        val rapport = aktivitetDao.lagRapport(igår)

        assertEquals(1, rapport.size)
        assertEquals(2, rapport.getValue("WARN").size)
        assertEquals(2, rapport.getValue("WARN")["Støtter ikke gradert sykmelding"])
        assertEquals(3, rapport.getValue("WARN")["Her gikk det gale"])
    }

    private fun antall() =
        using(sessionOf(dataSource)) {
            it.run(queryOf("SELECT COUNT(1) FROM aktivitetslogger_aktivitet").map {
                it.int(1)
            }.asSingle)
        }
}
