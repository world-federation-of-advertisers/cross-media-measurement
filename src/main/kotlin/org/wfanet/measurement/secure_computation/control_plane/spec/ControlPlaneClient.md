# CLASS
## ControlPlaneClient
* An abstract class designed to handle message processing from a RabbitMQ queue in a Kubernetes environment.
* Utilizes the Kotlin standard library and RabbitMQ Java client library.
* Provides a framework for subclasses to implement specific work tasks upon message reception.

# CONSTRUCTORS
## ControlPlaneClient()
* Initializes the ControlPlaneClient class and sets up the RabbitMQ connection.
### USAGE
* Automatically invoked when a subclass of ControlPlaneClient is instantiated.
### IMPL
* Reads the k8s secret file the following information:
* * `rabbit_host`
* * `rabbit_port`
* * `rabbit_username`
* * `rabbit_password`
* * `rabbit_queue_name`
* Uses above variable to invoke the the private `subscribeToQueue` method.
* Handles exceptions by printing error messages to the console and re-raising them.

# METHODS
## `abstract fun runWork(work: ByteArray)`
### USAGE
* Implemented by subclasses to define specific work tasks upon message reception.
* `work`: The message payload received from the RabbitMQ queue.
### IMPL
* To be implemented by subclasses.

## `private fun subscribeToQueue(rabbit_host: String, rabbit_port: Int, rabbit_username: String, rabbit_password: String, rabbit_queue_name: String)`
### USAGE
* Connects to the RabbitMQ queue using the `rabbit_host`, `rabbit_port`, `rabbit_username`, `rabbit_password`.
* Create a channel using the `rabbit_queue_name`. The channel must not have auto-ack set as it will be
* Listens for incoming messages and invokes `messageReceived` upon message receipt.
### IMPL
* Connects to the RabbitMQ queue using `ConnectionFactory` class provided by Rabbitmq java library using `rabbit_username` and `rabbit_password` valus.
* Uses the `channle.basicConsume` method to consume messages from `rabbit_queue_name`. AutoAck is set to false.
* Hanlde incoming messages by doing the following:
* * Override the `handleDelivery` method to handle incoming messages
* * Launch a new Process and invoke the `runWork` concrete method implemented by subclasses on this new process by passing the `body` params from the `handleDelivery` overriden method as `work`.
* * Wait for the process to complete.
* * If the process exit code indicates a failure, send a NACK to rabbit mq using `basicNack` method.
* * If the process exit code indicates a success, send an ACK to rabbit mq using `basicAck` method.

## `protected fun unsubscribeFromQueue()`
### USAGE
* Disconnects from the RabbitMQ queue.
### IMPL
* Performs disconnection from the RabbitMQ queue using the java default RabbitMQ library.
* Does not perform any additional cleanup or logging actions.
