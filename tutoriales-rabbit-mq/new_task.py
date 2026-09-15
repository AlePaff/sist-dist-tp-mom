import sys

import pika

# abre una conexión TCP/AMQP con RabbitMQ.
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost")
)

# conexion con el receiver a traves de un channel
channel = connection.channel()

channel.queue_declare(          # es idempotente, hacerlo varias veces crea el mismo resultado
    queue="task_queue",          # crea la queue "hello"
    durable=True,               # se persiste en disco, sobrevive reboots de rabbitmq
    arguments={"x-queue-type": "quorum"}
)

message = ' '.join(sys.argv[1:]) or "Hello World!"

# publico un mensaje
channel.basic_publish(
    exchange="",
    routing_key="hello",
    body=message,
    # we need to mark our messages as persistent - by supplying a delivery_mode property with the value of pika.DeliveryMode.Persistent
    properties=pika.BasicProperties(
        delivery_mode = pika.DeliveryMode.Persistent
    )

)

print(f" [x] Sent {message}")


connection.close()