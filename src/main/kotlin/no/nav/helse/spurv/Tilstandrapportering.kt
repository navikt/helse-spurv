package no.nav.helse.spurv

import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import no.nav.helse.rapids_rivers.asLocalDateTime
import java.util.*

internal class Tilstandrapportering(
    rapidsConnection: RapidsConnection,
    private val vedtaksperioderapportDao: VedtaksperioderapportDao,
    private val aktivitetDao: AktivitetsloggerAktivitetDao
) :
    River.PacketListener {

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
            validate { it.requireKey("aktivitetslogg.aktiviteter") }
            validate { it.requireKey("timeout") }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        vedtaksperioderapportDao.leggInnVedtaksperiode(
            UUID.fromString(packet["vedtaksperiodeId"].asText()),
            packet["gjeldendeTilstand"].asText(),
            packet["endringstidspunkt"].asLocalDateTime().toLocalDate()
        )

        leggInnWarnmeldinger(packet)
        leggInnErrormeldinger(packet)
    }

    private fun leggInnWarnmeldinger(packet: JsonMessage) {
        leggInnMeldinger(packet, "WARN")
    }

    private fun leggInnErrormeldinger(packet: JsonMessage) {
        leggInnMeldinger(packet, "ERROR")
    }

    private fun leggInnMeldinger(packet: JsonMessage, type: String) {
        packet["aktivitetslogg.aktiviteter"].filter { type == it.path("alvorlighetsgrad").asText() }
            .mapNotNull { it["melding"]?.asText()?.takeIf(String::isNotEmpty) }
            .forEach {
                aktivitetDao.leggInnAktivitet(type, it, packet["endringstidspunkt"].asLocalDateTime().toLocalDate())
            }
    }
}
