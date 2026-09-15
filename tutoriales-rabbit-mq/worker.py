import time

import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost")
)

channel = connection.channel()

# don't dispatch a new message to a worker until it has processed and acknowledged the previous one. Instead, it will dispatch it to the next worker that is not still busy.
channel.basic_qos(prefetch_count=1)

# se declara igual que en send.py (ya que no se sabe cual se ejecutará primero)
channel.queue_declare(
    queue="task_queue",
    durable=True,
    arguments={"x-queue-type": "quorum"}
)

def callback(ch, method, properties, body):
    print(f" [x] Received {body.decode()}")
    time.sleep(body.count(b'.') )
    print(" [x] Done")
    ch.basic_ack(delivery_tag = method.delivery_tag)

    

# consumir un mensaje y cuando llegue uno ejecutar la función "callback"
channel.basic_consume(
    queue="hello",
    on_message_callback=callback,
    # auto_ack=True     # True = desactiva los acks de recepción
)

print("[*] Waiting for messages. To exit press CTRL+C")

# se queda esperando mensajes nuevos
channel.start_consuming()