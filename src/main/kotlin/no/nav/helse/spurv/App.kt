package no.nav.helse.spurv

import no.nav.helse.rapids_rivers.RapidApplication
import no.nav.helse.rapids_rivers.RapidsConnection
import org.slf4j.LoggerFactory
import java.time.LocalDate

fun main() {
    val env = System.getenv()
    if ("true" == env["CRON_JOB_MODE"]?.toLowerCase()) return rapportJob(env)
    rapidApp(env)
}

private fun rapidApp(env: Map<String, String>) {
    val dataSourceBuilder = DataSourceBuilder(env)
    val dataSource = dataSourceBuilder.getDataSource()

    RapidApplication.create(env).apply {
        Tilstandrapportering(this, VedtaksperioderapportDao(dataSource), AktivitetsloggerAktivitetDao(dataSource))
    }.apply {
        register(object : RapidsConnection.StatusListener {
            override fun onStartup(rapidsConnection: RapidsConnection) {
                dataSourceBuilder.migrate()
            }
        })
    }.start()
}

private fun rapportJob(env: Map<String, String>) {
    Thread.setDefaultUncaughtExceptionHandler { _, throwable -> LoggerFactory.getLogger("no.nav.helse.Spurv").error(throwable.message, throwable) }
    val dataSourceBuilder = DataSourceBuilder(env)
    val dataSource = dataSourceBuilder.getDataSource(DataSourceBuilder.Role.ReadOnly)

    val slackClient = SlackClient(
        webhookUrl = env.getValue("SLACK_WEBHOOK_URL"),
        defaultChannel = "#område-helse-rapportering",
        defaultUsername = "spurv"
    )

    Tilstandrapport(VedtaksperioderapportDao(dataSource), AktivitetsloggerAktivitetDao(dataSource), slackClient)
        .lagRapport(LocalDate.now().minusDays(1))
}
