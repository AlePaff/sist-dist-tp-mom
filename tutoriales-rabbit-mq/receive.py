import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost")
)

channel = connection.channel()

# se declara igual que en send.py (ya que no se sabe cual se ejecutará primero)
channel.queue_declare(
    queue="hello",
    durable=True,
    arguments={"x-queue-type": "quorum"}
)


def callback(ch, method, properties, body):
    print(f"[x] Received {body.decode()}")          # body tiene el mensaje

# consumir un mensaje y cuando llegue uno ejecutar la función "callback"
channel.basic_consume(
    queue="hello",
    on_message_callback=callback,
    auto_ack=True
)

print("[*] Waiting for messages. To exit press CTRL+C")

# se queda esperando mensajes nuevos
channel.start_consuming()