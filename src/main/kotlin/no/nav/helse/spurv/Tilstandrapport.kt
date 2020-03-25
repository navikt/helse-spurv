package no.nav.helse.spurv

import java.time.LocalDate

internal class Tilstandrapport(
    private val vedtaksperioderapportDao: VedtaksperioderapportDao,
    private val aktivitetDao: AktivitetsloggerAktivitetDao,
    private val slackClient: SlackClient
) {

    fun lagRapport(rapportdag: LocalDate) {
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

        resten.map(Pair<String, *>::first).also {
            val ferdigBehandlet = it.filter { it in listOf("TIL_INFOTRYGD", "AVSLUTTET") }
            val tilInfotrygd = ferdigBehandlet.filter { it == "TIL_INFOTRYGD" }.size
            val tilUtbetaling = ferdigBehandlet.filter { it == "AVSLUTTET" }.size
            val tilGodkjenning = it.filter { it == "AVVENTER_GODKJENNING" }.size
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

        slackClient.postMessage(sb.toString(), ":man_in_business_suit_levitating:")
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

    private fun formater(sb: StringBuilder, tilstand: String, antall: Int) {
        sb.append(antall)
            .append(" gikk til ")
            .appendln(tilstand)
    }
}
