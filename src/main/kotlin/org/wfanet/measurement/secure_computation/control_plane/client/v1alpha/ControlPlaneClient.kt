import com.rabbitmq.client.ConnectionFactory
import java.io.File
import java.io.IOException
import com.rabbitmq.client.Delivery
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection

abstract class ControlPlaneClient {

    init {
        try {
            rabbit_host = readSecretFromFile("/etc/secrets/rabbit_host")
            rabbitPort = readSecretFromFile("/etc/secrets/rabbitPort")
            rabbitUsername = readSecretFromFile("/etc/secrets/rabbitUsername")
            rabbitPassword = readSecretFromFile("/etc/secrets/rabbitPassword")
            rabbitQueueName = readSecretFromFile("/etc/secrets/rabbitQueueName")
            subscribeToQueue(rabbitHost, rabbitPort, rabbitUsername, rabbitPassword, rabbitQueueName)
        } catch (e: IOException) {
            println("Error reading secret file: \${e.message}")
            throw e
        } catch (e: Exception) {
            println("An error occurred: \${e.message}")
            throw e
        }
    }

    private fun readSecretFromFile(filePath: String): String {
        return File(filePath).readText(Charsets.UTF_8).trim()
    }

    abstract fun runWork(work: ByteArray)

    private fun subscribeToQueue(rabbit_host: String, rabbit_port: Int, rabbit_username: String, rabbit_password: String, rabbit_queue_name: String) {
        val factory = ConnectionFactory()
        factory.host = rabbit_host
        factory.port = rabbit_port
        factory.username = rabbit_username
        factory.password = rabbit_password
        val connection = factory.newConnection()
        val channel: Channel = connection.createChannel()
        channel.basicConsume(rabbit_queue_name, false, { _: String, delivery: Delivery ->
            val body = delivery.body
            val process = ProcessBuilder("/path/to/command").start()
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
            val connection: Connection = // Obtain connection instance
            val channel: Channel = connection.createChannel()
            channel.close()
            connection.close()
        } catch (e: Exception) {
            println("An error occurred during unsubscription: \${e.message}")
            throw e
        }
    }
}
