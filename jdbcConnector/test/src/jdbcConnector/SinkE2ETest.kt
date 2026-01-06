import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.*
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName
import org.testcontainers.utility.MountableFile
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Path
import java.time.Duration
import java.util.Properties

@EnabledIfEnvironmentVariable(named = "REDSHIFT_JDBC_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "REDSHIFT_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "REDSHIFT_PASSWORD", matches = ".+")
class SinkE2ETest {

    private val http = HttpClient.newHttpClient()
    private val network = Network.newNetwork()
    private val kafka = KafkaContainer(DockerImageName.parse("apache/kafka:3.7.1"))
        .withNetwork(network)
        .withNetworkAliases("kafka")
    private lateinit var connect: GenericContainer<*>
    private val db: DbAdapter = RedshiftAdapter()
    private lateinit var table: String

    @BeforeEach
    fun setup() {
        kafka.start()
        // dependency on build artifacts
        val pluginJarOnHost = Path.of("out/jdbcConnector/assemble.dest/out.jar").toAbsolutePath()
        connect = GenericContainer(DockerImageName.parse("confluentinc/cp-kafka-connect:7.5.6"))
            .withNetwork(network)
            .withNetworkAliases("connect")
            .withExposedPorts(8083)
            .withEnv("CONNECT_BOOTSTRAP_SERVERS", "kafka:9092")
            .withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", "connect")
            .withEnv("CONNECT_REST_PORT", "8083")
            .withEnv("CONNECT_GROUP_ID", "it-connect")
            .withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "connect-configs")
            .withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "connect-offsets")
            .withEnv("CONNECT_STATUS_STORAGE_TOPIC", "connect-status")
            .withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1")
            .withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1")
            .withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1")
            .withEnv("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
            .withEnv("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
            .withEnv("CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE", "false")
            .withEnv("CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE", "false")
            .withEnv("CONNECT_PLUGIN_PATH", "/usr/share/java,/usr/share/confluent-hub-components,/usr/share/java/my-plugins")
            .withCopyFileToContainer(
                MountableFile.forHostPath(pluginJarOnHost),
                "/usr/share/java/my-plugins/my-connector.jar"
            )

        connect.start()
        waitForConnectHealthy()

        table = db.newTableName()
        db.createTable(table)

        val baseCfg = linkedMapOf(
            "connector.class" to "jdbcConnector.SinkConnector",
            "tasks.max" to "1",
            "topics" to "events-topic"
        )
        val cfg = baseCfg + db.connectorConfig(table)

        createConnector("sink-it", cfg)
        waitForConnectorRunning("sink-it")
    }

    @AfterEach
    fun teardown() {
        try { db.dropTable(table) } catch (_: Exception) {}
        try { connect.stop() } catch (_: Exception) {}
        try { kafka.stop() } catch (_: Exception) {}
        try { network.close() } catch (_: Exception) {}
    }

    @Test
    fun `sink writes produced kafka records into db`() {
        produce("events-topic", listOf("k1" to "hello", "k2" to "world", "k3" to db.name))
        db.awaitRowCount(table, expected = 3, timeout = Duration.ofMinutes(2))
    }

    // ---------- Kafka produce ----------
    private fun produce(topic: String, pairs: List<Pair<String, String>>) {
        val props = Properties().apply {
            put("bootstrap.servers", kafka.bootstrapServers)
            put("acks", "all")
            put("key.serializer", StringSerializer::class.java.name)
            put("value.serializer", StringSerializer::class.java.name)
        }
        KafkaProducer<String, String>(props).use { p ->
            for ((k, v) in pairs) p.send(ProducerRecord(topic, k, v)).get()
            p.flush()
        }
    }

    // ---------- Connect REST ----------
    private fun connectBaseUrl(): String =
        "http://${connect.host}:${connect.getMappedPort(8083)}"

    private fun waitForConnectHealthy() {
        val deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos()
        while (System.nanoTime() < deadline) {
            try {
                val req = HttpRequest.newBuilder()
                    .uri(URI.create("${connectBaseUrl()}/connectors"))
                    .timeout(Duration.ofSeconds(2))
                    .GET()
                    .build()
                val resp = http.send(req, HttpResponse.BodyHandlers.ofString())
                if (resp.statusCode() in 200..299) return
            } catch (_: Exception) {}
            Thread.sleep(250)
        }
        fail("Kafka Connect REST never became healthy")
    }

    private fun createConnector(name: String, config: Map<String, String>) {
        val json = buildConnectorJson(name, config)
        val req = HttpRequest.newBuilder()
            .uri(URI.create("${connectBaseUrl()}/connectors"))
            .timeout(Duration.ofSeconds(10))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(json))
            .build()

        val resp = http.send(req, HttpResponse.BodyHandlers.ofString())
        if (resp.statusCode() !in 200..299 && resp.statusCode() != 409) {
            fail("Failed to create connector. status=${resp.statusCode()} body=${resp.body()}")
        }
    }

    private fun waitForConnectorRunning(name: String) {
        val deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos()
        while (System.nanoTime() < deadline) {
            val req = HttpRequest.newBuilder()
                .uri(URI.create("${connectBaseUrl()}/connectors/$name/status"))
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build()
            val resp = http.send(req, HttpResponse.BodyHandlers.ofString())
            if (resp.statusCode() in 200..299) {
                if (resp.body().contains("\"state\":\"RUNNING\"")) return
            }
            Thread.sleep(500)
        }
        fail("Connector $name did not reach RUNNING")
    }

    private fun buildConnectorJson(name: String, config: Map<String, String>): String {
        fun esc(s: String) = s.replace("\\", "\\\\").replace("\"", "\\\"")
        val cfgJson = config.entries.joinToString(",") { "\"${esc(it.key)}\":\"${esc(it.value)}\"" }
        return """{"name":"${esc(name)}","config":{${cfgJson}}}"""
    }
}
