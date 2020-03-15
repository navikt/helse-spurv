package no.nav.helse.spurv

import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import no.nav.helse.spurv.Tilstandrapport.TilstandType.*
import org.slf4j.LoggerFactory
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime

internal class Tilstandrapport(
    rapidsConnection: RapidsConnection,
    private val vedtaksperioderapportDao: VedtaksperioderapportDao,
    private val aktivitetDao: AktivitetsloggerAktivitetDao,
    private val slackClient: SlackClient?
) :
    River.PacketListener {

    private companion object {
        private val log = LoggerFactory.getLogger(Tilstandrapport::class.java)
    }

    private var lastReportTime = LocalDateTime.MIN
    private val schedule: (LocalDateTime) -> Boolean = ::hverDagEtterKl9
    //private val schedule: (LocalDateTime) -> Boolean = ::hvert5Minutt

    init {
        River(rapidsConnection).register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        if (!schedule(lastReportTime)) return

        lagRapport()
    }

    private fun lagRapport() {
        val rapportdag = LocalDate.now().minusDays(1)
        val rapport = vedtaksperioderapportDao.lagRapport(rapportdag)

        val (behandletIgår, resten) = rapport.map { (_, periode) -> periode }
            .partition { (_, dato) -> dato == rapportdag }

        val antallVedtaksperioderIgår = behandletIgår.size
        val sb = StringBuilder()
        sb.append("I går fikk vi ")
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
            appendMeldinger(sb, it["ERROR"], "Forekomster av feil/ting som ikke er støttet:")
            appendMeldinger(sb, it["WARN"], "Forekomster av ting saksbehandler må vurdere:")
        }

        resten.map { TilstandType.valueOf(it.first) }.also {
            val ferdigBehandlet = it.filter { it in listOf(TIL_INFOTRYGD, AVSLUTTET) }
            val tilInfotrygd = ferdigBehandlet.filter { it == TIL_INFOTRYGD }.size
            val tilUtbetaling = ferdigBehandlet.filter { it == AVSLUTTET }.size
            val tilGodkjenning = it.filter { it == AVVENTER_GODKJENNING }.size
            val avventerBehandling = it.size - ferdigBehandlet.size - tilGodkjenning

            sb.appendln("Frem til i går hadde vi ")
                .append(resten.size)
                .append(" andre saker, hvorav ")
                .append(tilGodkjenning)
                .append(" er til godkjenning, ")
                .append(ferdigBehandlet.size)
                .append(" er ferdig håndtert (")
                .append(tilUtbetaling)
                .append(" er utbetalt, ")
                .append(tilInfotrygd)
                .append(" gikk til Infotrygd), og ")
                .append(avventerBehandling)
                .appendln(" perioder er avventende.")
        }

        slackClient?.postMessage(sb.toString(), ":man_in_business_suit_levitating:") ?: log.info("not alerting slack because URL is not set")

        lastReportTime = LocalDateTime.now()
    }

    private fun appendMeldinger(sb: StringBuilder, meldinger: Map<String, Long>?, text: String) {
        meldinger?.map { (melding, antall) -> melding to antall }
            ?.sortedByDescending { it.second }
            ?.also { sb.appendln(text) }
            ?.forEach { (melding, antall) ->
                sb.append(melding)
                    .append(": ").
                        appendln(antall)
            }
        sb.appendln()
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

    private enum class TilstandType {
        AVVENTER_HISTORIKK,
        AVVENTER_GODKJENNING,
        TIL_UTBETALING,
        TIL_INFOTRYGD,
        AVSLUTTET,
        UTBETALING_FEILET,
        START,
        MOTTATT_SYKMELDING_FERDIG_FORLENGELSE,
        MOTTATT_SYKMELDING_UFERDIG_FORLENGELSE,
        MOTTATT_SYKMELDING_FERDIG_GAP,
        MOTTATT_SYKMELDING_UFERDIG_GAP,
        AVVENTER_SØKNAD_FERDIG_GAP,
        AVVENTER_SØKNAD_UFERDIG_GAP,
        AVVENTER_VILKÅRSPRØVING_GAP,
        AVVENTER_GAP,
        AVVENTER_INNTEKTSMELDING_FERDIG_GAP,
        AVVENTER_INNTEKTSMELDING_UFERDIG_GAP,
        AVVENTER_UFERDIG_GAP,
        AVVENTER_INNTEKTSMELDING_UFERDIG_FORLENGELSE,
        AVVENTER_SØKNAD_UFERDIG_FORLENGELSE,
        AVVENTER_UFERDIG_FORLENGELSE
    }
}
