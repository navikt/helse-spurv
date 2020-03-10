package no.nav.helse.spurv

import no.nav.helse.rapids_rivers.RapidApplication
import no.nav.helse.rapids_rivers.RapidsConnection

fun main() {
    val env = System.getenv()

    val dataSourceBuilder = DataSourceBuilder(env)
    val dataSource = dataSourceBuilder.getDataSource()

    val slackClient = env["SLACK_WEBHOOK_URL"]?.let {
        SlackClient(
            webhookUrl = it,
            defaultChannel = "#område-helse-rapportering",
            defaultUsername = "spurv"
        )
    }

    RapidApplication.create(env).apply {
        Tilstandrapportering(this, VedtaksperioderapportDao(dataSource), AktivitetsloggerAktivitetDao(dataSource), slackClient)
    }.apply {
        register(object : RapidsConnection.StatusListener {
            override fun onStartup(rapidsConnection: RapidsConnection) {
                dataSourceBuilder.migrate()
            }
        })
    }.start()
}
