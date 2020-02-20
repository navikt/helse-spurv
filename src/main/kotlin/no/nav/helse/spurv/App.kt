package no.nav.helse.spurv

import io.ktor.util.KtorExperimentalAPI
import no.nav.helse.rapids_rivers.RapidApplication

@KtorExperimentalAPI
fun main() {
    val env = System.getenv().toMutableMap()
    env.putIfAbsent("KAFKA_CONSUMER_GROUP_ID", "spurv-v1")
    env.putIfAbsent("KAFKA_RAPID_TOPIC", "helse-rapid-v1")

    val dataSourceBuilder = DataSourceBuilder(env)
    dataSourceBuilder.migrate()

    val slackClient = env["SLACK_WEBHOOK_URL"]?.let {
        SlackClient(
            webhookUrl = it,
            defaultChannel = "#område-helse-rapportering",
            defaultUsername = "spurv"
        )
    }

    RapidApplication.create(env).apply {
        Tilstandrapportering(this, VedtaksperioderapportDao(dataSourceBuilder.getDataSource()), slackClient)
    }.start()
}
