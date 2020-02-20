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
import java.util.*
import javax.sql.DataSource

@TestInstance(Lifecycle.PER_CLASS)
internal class VedtaksperioderapportDaoTest {

    private lateinit var embeddedPostgres: EmbeddedPostgres
    private lateinit var postgresConnection: Connection
    private lateinit var dataSource: DataSource

    private lateinit var vedtaksperioderapportDao: VedtaksperioderapportDao

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

        vedtaksperioderapportDao = VedtaksperioderapportDao(dataSource)
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
            it.run(queryOf("DELETE FROM vedtaksperiode_tilstand").asExecute)
        }
    }

    @Test
    internal fun `en vedtaksperiode per dato`() {
        assertEquals(0, antall())
        val id = UUID.randomUUID()
        val tilstand = "SYKMELDING_MOTTATT"
        vedtaksperioderapportDao.leggInnVedtaksperiode(id, tilstand, igår)
        assertEquals(1, antall())
        vedtaksperioderapportDao.leggInnVedtaksperiode(id, tilstand, igår)
        assertEquals(1, antall())
        vedtaksperioderapportDao.leggInnVedtaksperiode(id, tilstand, idag)
        assertEquals(2, antall())
    }

    @Test
    internal fun `i dag`() {
        val id = UUID.randomUUID()
        vedtaksperioderapportDao.leggInnVedtaksperiode(id, "SYKMELDING_MOTTATT", idag)
        val rapport = vedtaksperioderapportDao.lagRapport(idag)
        assertTrue(rapport.isEmpty())
    }

    @Test
    internal fun `i går`() {
        val id = UUID.randomUUID()
        vedtaksperioderapportDao.leggInnVedtaksperiode(id, "SYKMELDING_MOTTATT", igår)
        val rapport = vedtaksperioderapportDao.lagRapport(idag)
        assertEquals("SYKMELDING_MOTTATT", rapport[id]?.first)
        assertEquals(igår, rapport[id]?.second)
    }

    @Test
    internal fun `i forgårs`() {
        val id = UUID.randomUUID()
        vedtaksperioderapportDao.leggInnVedtaksperiode(id, "SYKMELDING_MOTTATT", iforgårs)
        val rapport = vedtaksperioderapportDao.lagRapport(idag)
        assertEquals("SYKMELDING_MOTTATT", rapport[id]?.first)
        assertEquals(iforgårs, rapport[id]?.second)
    }

    @Test
    internal fun `igår og iforgårs`() {
        val id = UUID.randomUUID()
        vedtaksperioderapportDao.leggInnVedtaksperiode(id, "AVVENTER_INNTEKTSMELDING", igår)
        vedtaksperioderapportDao.leggInnVedtaksperiode(id, "SYKMELDING_MOTTATT", iforgårs)
        val rapport = vedtaksperioderapportDao.lagRapport(idag)
        assertEquals("AVVENTER_INNTEKTSMELDING", rapport[id]?.first)
        assertEquals(igår, rapport[id]?.second)
    }

    @Test
    internal fun `samme dato`() {
        val id = UUID.randomUUID()
        vedtaksperioderapportDao.leggInnVedtaksperiode(id, "SYKMELDING_MOTTATT", igår)
        vedtaksperioderapportDao.leggInnVedtaksperiode(id, "AVVENTER_INNTEKTSMELDING", igår)
        val rapport = vedtaksperioderapportDao.lagRapport(idag)
        assertEquals("AVVENTER_INNTEKTSMELDING", rapport[id]?.first)
        assertEquals(igår, rapport[id]?.second)
    }

    private fun antall() =
        using(sessionOf(dataSource)) {
            it.run(queryOf("SELECT COUNT(1) FROM vedtaksperiode_tilstand").map {
                it.int(1)
            }.asSingle)
        }
}
