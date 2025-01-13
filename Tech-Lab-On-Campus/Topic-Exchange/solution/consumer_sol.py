import pika
import os

class mqConsumer:
    def __init__(self, binding_key: str, exchange_name: str, queue_name: str) -> None:
        self.binding_key = binding_key
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.setupRMQConnection()

    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)
        
        # Establish Channel
        self.channel = self.connection.channel()
        
        # Create the topic exchange
        self.channel.exchange_declare(
            exchange=self.exchange_name,
            exchange_type="topic"
        )
        
        # Create queue
        self.channel.queue_declare(queue=self.queue_name)
        
        # Bind queue to exchange with binding key
        self.channel.queue_bind(
            queue=self.queue_name,
            exchange=self.exchange_name,
            routing_key=self.binding_key
        )
        
        # Setup callback
        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=self.on_message_callback,
            auto_ack=False
        )

    def on_message_callback(self, channel, method_frame, header_frame, body):
        message = body.decode()
        print(f" [x] Received {message}")
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)

    def startConsuming(self) -> None:
        print(" [*] Waiting for messages. To exit press CTRL+C")
        self.channel.start_consuming()