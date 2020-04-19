package no.nav.helse.spurv

import java.time.LocalDate

internal class Tilstandrapport(
    private val vedtaksperioderapportDao: VedtaksperioderapportDao,
    private val aktivitetDao: AktivitetsloggerAktivitetDao,
    private val slackClient: SlackClient
) {

    fun lagRapport(rapportdag: LocalDate) {
        val rapport = vedtaksperioderapportDao.lagRapport(rapportdag)

        val (behandletIgår, resten) = rapport.partition { (_, dato) -> dato == rapportdag }

        val antallVedtaksperioderIgår = behandletIgår.sumBy { it.third }
        val sb = StringBuilder()
        sb.append("I går behandlet vi ")
            .append(antallVedtaksperioderIgår)
            .appendln(" vedtaksperioder:")

        behandletIgår
            .map { it.first to it.third }
            .sortedByDescending { it.second }
            .forEach { (tilstand, antall) ->
                formater(sb, tilstand, antall)
            }
        sb.appendln()

        aktivitetDao.lagRapport(rapportdag).takeIf(Map<*, *>::isNotEmpty)?.let {
            appendMeldinger(sb, it["ERROR"], "Forekomster av feil/ting som ikke er støttet:")
            appendMeldinger(sb, it["WARN"], "Forekomster av ting saksbehandler må vurdere:")
        }

        resten
            .groupBy({ it.first }) { it.third }
            .mapValues { it.value.sum() }
            .also {
                val total = it.map { it.value }.sum()
                val tilInfotrygd = it.antall("TIL_INFOTRYGD")
                val avsluttet = it.antall("AVSLUTTET", "AVSLUTTET_UTEN_UTBETALING", "AVSLUTTET_UTEN_UTBETALING_MED_INNTEKTSMELDING")
                val ferdigBehandlet = tilInfotrygd + avsluttet
                val tilGodkjenning = it.antall("AVVENTER_GODKJENNING")
                val avventerBehandling = total - ferdigBehandlet - tilGodkjenning

                sb.appendln("Frem til i går hadde vi ")
                    .append(total)
                    .append(" andre perioder, hvorav ")
                    .append(tilGodkjenning)
                    .append(" er til godkjenning, ")
                    .append(ferdigBehandlet)
                    .append(" er ferdig håndtert (")
                    .append(avsluttet)
                    .append(" er avsluttet, ")
                    .append(tilInfotrygd)
                    .append(" gikk til Infotrygd), og ")
                    .append(avventerBehandling)
                    .appendln(" perioder er avventende.")
            }

        slackClient.postMessage(sb.toString(), ":man_in_business_suit_levitating:")
    }

    private fun Map<String, Int>.antall(vararg tilstand: String) =
        this.filterKeys { it in tilstand }.map { it.value }.sum()

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
