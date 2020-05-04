package no.nav.helse.spurv

import com.fasterxml.jackson.databind.JsonNode
import no.nav.helse.rapids_rivers.*
import org.slf4j.LoggerFactory
import java.util.*

internal class Tilstandrapportering(
    rapidsConnection: RapidsConnection,
    private val vedtaksperioderapportDao: VedtaksperioderapportDao,
    private val aktivitetDao: AktivitetsloggerAktivitetDao
) :
    River.PacketListener {

    private companion object {
        private val sikkerLogg = LoggerFactory.getLogger("tjenestekall")
    }

    init {
        River(rapidsConnection).apply {
            validate { it.demandValue("@event_name", "vedtaksperiode_endret") }
            validate { it.requireKey("aktørId") }
            validate { it.requireKey("fødselsnummer") }
            validate { it.requireKey("organisasjonsnummer") }
            validate { it.requireKey("vedtaksperiodeId") }
            validate { it.requireKey("forrigeTilstand") }
            validate { it.requireKey("gjeldendeTilstand") }
            validate { it.require("@opprettet", JsonNode::asLocalDateTime) }
            validate { it.requireKey("aktivitetslogg.aktiviteter") }
        }.register(this)
    }

    override fun onError(problems: MessageProblems, context: RapidsConnection.MessageContext) {
        sikkerLogg.error("kan ikke forstå vedtaksperiode_endret:\n${problems.toExtendedReport()}")
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        vedtaksperioderapportDao.leggInnVedtaksperiode(
            UUID.fromString(packet["vedtaksperiodeId"].asText()),
            packet["gjeldendeTilstand"].asText(),
            packet["@opprettet"].asLocalDateTime().toLocalDate()
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
                aktivitetDao.leggInnAktivitet(type, it, packet["@opprettet"].asLocalDateTime().toLocalDate())
            }
    }
}
