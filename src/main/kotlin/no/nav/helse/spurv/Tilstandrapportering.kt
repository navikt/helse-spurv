package no.nav.helse.spurv

import no.nav.helse.rapids_rivers.*
import no.nav.helse.spurv.Tilstandrapportering.TilstandType.TIL_INFOTRYGD
import no.nav.helse.spurv.Tilstandrapportering.TilstandType.TIL_UTBETALING
import org.slf4j.LoggerFactory
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.util.*

internal class Tilstandrapportering(
    rapidsConnection: RapidsConnection,
    private val vedtaksperioderapportDao: VedtaksperioderapportDao,
    private val aktivitetDao: AktivitetsloggerAktivitetDao,
    private val slackClient: SlackClient?
) :
    River.PacketListener {

    private companion object {
        private val log = LoggerFactory.getLogger(Tilstandrapportering::class.java)
    }

    init {
        River(rapidsConnection).apply {
            validate { it.requireValue("@event_name", "vedtaksperiode_endret") }
            validate { it.requireKey("aktørId") }
            validate { it.requireKey("fødselsnummer") }
            validate { it.requireKey("organisasjonsnummer") }
            validate { it.requireKey("vedtaksperiodeId") }
            validate { it.requireKey("forrigeTilstand") }
            validate { it.requireKey("gjeldendeTilstand") }
            validate { it.requireKey("endringstidspunkt") }
            validate { it.requireKey("på_grunn_av") }
            validate { it.requireKey("aktivitetslogger.aktiviteter") }
            validate { it.requireKey("timeout") }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        vedtaksperioderapportDao.leggInnVedtaksperiode(
            UUID.fromString(packet["vedtaksperiodeId"].asText()),
            packet["gjeldendeTilstand"].asText(),
            packet["endringstidspunkt"].asLocalDateTime().toLocalDate()
        )

        leggInnErrormeldinger(packet)

        lagRapport()
    }

    private fun leggInnErrormeldinger(packet: JsonMessage) {
        packet["aktivitetslogger.aktiviteter"].filter { "ERROR" == it.path("alvorlighetsgrad").asText() }
            .mapNotNull { it["melding"]?.asText()?.takeIf(String::isNotEmpty) }
            .forEach {
                aktivitetDao.leggInnAktivitet(it, packet["endringstidspunkt"].asLocalDateTime().toLocalDate())
            }
    }

    private var lastReportTime = LocalDateTime.MIN
    private val schedule: (LocalDateTime) -> Boolean = ::hverDagEtterKl9

    private fun lagRapport() {
        val rapportdag = LocalDate.now().minusDays(1)

        if (!schedule(lastReportTime)) return

        val rapport = vedtaksperioderapportDao.lagRapport(rapportdag)

        val (behandletIgår, resten) = rapport.map { (_, periode) -> periode }
            .partition { (_, dato) -> dato == rapportdag }

        val antallVedtaksperioderIgår = behandletIgår.size
        val sb = StringBuilder()
        sb.append("I går behandlet vi ")
            .append(antallVedtaksperioderIgår)
            .appendln(" vedtaksperioder:")

        behandletIgår.groupBy { it.first }
            .mapValues { it.value.size }
            .map { it.key to it.value }
            .sortedByDescending { it.second }
            .forEach { (tilstand, antall) ->
                formater(sb, tilstand, antall)
            }
        sb.appendln()

        aktivitetDao.lagRapport(rapportdag).takeIf(Map<*, *>::isNotEmpty)?.let {
            sb.appendln("Årsaker til manuell behandling:")
            it.map { it.key to it.value }
                .sortedByDescending { it.second }
                .forEach { (melding, antall) ->
                    sb.append(melding)
                        .append(": ").
                        appendln(antall)
                }
            sb.appendln()
        }

        resten.map { TilstandType.valueOf(it.first) }.also {
            val ferdigBehandlet = it.filter { it in listOf(TIL_INFOTRYGD, TIL_UTBETALING) }
            val tilInfotrygd = ferdigBehandlet.filter { it == TIL_INFOTRYGD }.size
            val tilUtbetaling = ferdigBehandlet.filter { it == TIL_UTBETALING }.size
            val tilGodkjenning = it.filter { it == TilstandType.AVVENTER_GODKJENNING }.size
            val avventerBehandling = it.size - ferdigBehandlet.size - tilGodkjenning

            sb.appendln("Frem til i går hadde vi behandlet ")
                .append(resten.size)
                .append(" andre saker, hvorav ")
                .append(tilGodkjenning)
                .append(" er til godkjenning, ")
                .append(ferdigBehandlet.size)
                .append(" er ferdig behandlet (")
                .append(tilUtbetaling)
                .append(" er utbetalt, ")
                .append(tilInfotrygd)
                .append(" er i Infotrygd), og ")
                .append(avventerBehandling)
                .appendln(" avventer videre behandling")
        }

        slackClient?.postMessage(sb.toString(), ":man_in_business_suit_levitating:") ?: log.info("not alerting slack because URL is not set")

        lastReportTime = LocalDateTime.now()
    }

    private fun hvert5Minutt(lastReportTime: LocalDateTime): Boolean {
        return lastReportTime < LocalDateTime.now().minusMinutes(5)
    }

    private fun hverDagEtterKl9(lastReportTime: LocalDateTime): Boolean {
        val igår = LocalDate.now().minusDays(1)
        if (lastReportTime.toLocalDate() > igår) return false
        val kl9 = LocalTime.of(9, 0, 0)
        if (kl9 > LocalTime.now()) return false
        return true
    }

    private fun formater(sb: StringBuilder, tilstand: String, antall: Int) {
        sb.append(antall)
            .append(" gikk til ")
            .appendln(tilstand)
    }

    override fun onError(problems: MessageProblems, context: RapidsConnection.MessageContext) {}

    private enum class TilstandType {
        START,
        MOTTATT_SYKMELDING,
        AVVENTER_SØKNAD,
        AVVENTER_TIDLIGERE_PERIODE_ELLER_INNTEKTSMELDING,
        AVVENTER_TIDLIGERE_PERIODE,
        UNDERSØKER_HISTORIKK,
        AVVENTER_INNTEKTSMELDING,
        AVVENTER_VILKÅRSPRØVING,
        AVVENTER_HISTORIKK,
        AVVENTER_GODKJENNING,
        TIL_UTBETALING,
        TIL_INFOTRYGD
    }
}
