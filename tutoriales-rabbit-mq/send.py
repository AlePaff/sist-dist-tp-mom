import pika

# abre una conexión TCP/AMQP con RabbitMQ.
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost")
)

# conexion con el receiver a traves de un channel
channel = connection.channel()

channel.queue_declare(          # es idempotente, hacerlo varias veces crea el mismo resultado
    queue="hello",          # crea la queue "hello"
    durable=True,
    arguments={"x-queue-type": "quorum"}
)

# publico un mensaje
channel.basic_publish(
    exchange="",
    routing_key="hello",
    body="Hello World!"
)

print("[x] Sent 'Hello World!'")

connection.close()