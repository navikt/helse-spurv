package no.nav.helse.spurv

import no.nav.helse.rapids_rivers.*
import java.util.*

internal class Tilstandrapportering(
    rapidsConnection: RapidsConnection,
    private val vedtaksperioderapportDao: VedtaksperioderapportDao,
    slackClient: SlackClient?
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
            validate { it.requireKey("på_grunn_av") }
            validate { it.requireKey("timeout") }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        vedtaksperioderapportDao.leggInnVedtaksperiode(
            UUID.fromString(packet["vedtaksperiodeId"].asText()),
            packet["gjeldendeTilstand"].asText(),
            packet["endringstidspunkt"].asLocalDateTime().toLocalDate()
        )
    }

    override fun onError(problems: MessageProblems, context: RapidsConnection.MessageContext) {}
}
