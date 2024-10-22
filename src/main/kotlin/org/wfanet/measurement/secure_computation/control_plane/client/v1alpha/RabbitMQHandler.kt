import com.rabbitmq.client.ConnectionFactory
import java.io.File
import java.io.IOException
import com.rabbitmq.client.Delivery
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection

class RabbitMQHandler {

    private lateinit var connection: Connection
    private lateinit var channel: Channel

    init {
        try {
            
            Runtime.getRuntime().addShutdownHook(Thread {
                unsubscribeFromQueue()
            })

        } catch (e: IOException) {
            println("Error reading secret file: \${e.message}")
            throw e
        } catch (e: Exception) {
            println("An error occurred: \${e.message}")
            throw e
        }
    }

    private fun readSecretFromEnv(variableName: String): String {
        return System.getenv(variableName) ?: throw IllegalArgumentException("Environment variable $variableName not found")
    }

    private fun subscribeToQueue(rabbit_host: String, rabbit_port: Int, rabbit_username: String, rabbit_password: String, rabbit_queue_name: String) {
        val factory = ConnectionFactory()
        factory.host = rabbit_host
        factory.port = rabbit_port
        factory.username = rabbit_username
        factory.password = rabbit_password
        connection = factory.newConnection()
        channel = connection.createChannel()
        channel.basicConsume(rabbit_queue_name, false, { _: String, delivery: Delivery ->
            val body = delivery.body
            val process = ProcessBuilder("/app/tee_app.jar").start()
            process.inputStream.bufferedReader().use { it.readText() }
            process.errorStream.bufferedReader().use { it.readText() }
            val exitCode = process.waitFor()
            if (exitCode == 0) {
                channel.basicAck(delivery.envelope.deliveryTag, false)
            } else {
                channel.basicNack(delivery.envelope.deliveryTag, false, true)
            }
        }, { consumerTag -> })
    }

    protected fun unsubscribeFromQueue() {
        try {
            if (::channel.isInitialized) {
                channel.close()
            }
            if (::connection.isInitialized) {
                connection.close()
            }
        } catch (e: Exception) {
            println("An error occurred during unsubscription: \${e.message}")
            throw e
        }
    }
}

fun main() {
    val rabbitMQHandler = RabbitMQHandler() 
    rabbit_host = readSecretFromEnv("RABBIT_HOST")
    rabbitPort = readSecretFromEnv("RABBIT_PORT")
    rabbitUsername = readSecretFromEnv("RABBIT_USERNAME")
    rabbitPassword = readSecretFromEnv("RABBIT_PASSWORD")
    rabbitQueueName = readSecretFromEnv("RABBIT_QUEUE_NAME")
    subscribeToQueue(rabbitHost, rabbitPort, rabbitUsername, rabbitPassword, rabbitQueueName)
}
