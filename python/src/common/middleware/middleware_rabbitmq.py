import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange

# clase hija MessageMiddlewareQueueRabbitMQ, clase padre MessageMiddlewareQueue
class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        # abre una conexión TCP/AMQP con RabbitMQ.
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host)
        )

        # conexion con el receiver a traves de un channel
        self.channel = self.connection.channel()
        self.queue_name = queue_name        # seguro se necesita para mass tarde

        self.channel.queue_declare(
            queue=queue_name,
            # durable=True,               # se persiste en disco, sobrevive reboots de rabbitmq
            # arguments={"x-queue-type": "quorum"}
        )

        # message = ' '.join(sys.argv[1:]) or "Hello World!"

        # # publico un mensaje
        # channel.basic_publish(
        #     exchange="",
        #     routing_key="hello",
        #     body=message,
        #     # we need to mark our messages as persistent - by supplying a delivery_mode property with the value of pika.DeliveryMode.Persistent
        #     properties=pika.BasicProperties(
        #         delivery_mode = pika.DeliveryMode.Persistent
        #     )

        # )

        # print(f" [x] Sent {message}")

    def start_consuming(self, on_message_callback):
        pass
    
    def stop_consuming(self):
        pass
    
    def send(self, message):
        pass

    def close(self):
        # cerramos el canal y luego la conexion
        self.channel.close()

        self.connection.close()
        # no destruyo la cola

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass
