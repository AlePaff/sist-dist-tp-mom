#!/usr/bin/env python
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='logs', exchange_type='fanout')

# al dejar queue vacio, le esta poniendo un nombre generico y distinto en cada ejecución
# exlusive=True. una vez se cierre este programa, matar la cola
result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue

# aca suscribo la queue nueva al exchange "logs"
channel.queue_bind(exchange='logs', queue=queue_name)

print(' [*] Waiting for logs. To exit press CTRL+C')

def callback(ch, method, properties, body):
    print(f" [x] {body}")

# quedarse escuchando la cola de nombre 'queue_name' y que ejecute callback
channel.basic_consume(
    queue=queue_name, on_message_callback=callback, auto_ack=True)

channel.start_consuming()