
## Tutoriales para aprender a usar RabbitMQ
https://www.rabbitmq.com/tutorials



correr un rabbit
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4-management


-p 5672:5672
      │
      └── puerto AMQP

-p 15672:15672
      │
      └── interfaz web de administración


instalo pika (libreria oficial de python para rabbit mq)
python -c "import pika; print(pika.__version__)"


### Hello world
https://www.rabbitmq.com/tutorials/tutorial-one-python


docker start rabbitmq

python receive.py

python send.py

Ver que no importa si ejecuto send y luego receive, o al reves. La cola de mensajes está guardada.
> una de las razones por las que existe un message broker: desacopla temporalmente a los programas.

y para detenerlo "docker stop rabbitmq"


### Work queues
probar con 2 workers, ver que si envío el mensaje que es muy lento y en el medio murió, entonces rabbitMQ se asegura de entregar el mensaje a otro worker, para que la información no se pierda
ejemplo
consola1: python worker.py
consola2: python worker.py
consola3: python new_task py tarea lenta ......
consola1 (que recibió el msg): apretar CTRL+C
-tp-mom\tutoriales-rabbit-mq> python worker.py
[*] Waiting for messages. To exit press CTRL+C
 [x] Received 3.....
ERROR

consola2: toma el mensaje
-tp-mom\tutoriales-rabbit-mq> python .\worker.py
[*] Waiting for messages. To exit press CTRL+C
 [x] Received 3.....
 [x] Done


### Message durability
> When RabbitMQ quits or crashes it will forget the queues and messages unless you tell it not to. Two things are required to make sure that messages aren't lost: we need to mark both the queue and messages as durable.

Se debe declarar la cola como durable y los mensajes tambien

Luego intentar enviar un mensaje largo
python new_task.py largo..................mucho
python new_task.py chico1
python new_task.py chico2
python new_task.py chico3
python new_task.py chico4
python new_task.py chico5

Se va a quedar procesando el mensaje largo, si al mismo tiempo matamos el server con "docker stop rabbitmq" y matamos el proceso. Al volver a iniciarlo notar que los mensajes chico* se van a seguir mandando. Es decir, persistieron a la muerte del server y los workers

### Fair dispatch
channel.basic_qos(prefetch_count=1)
Configura cómo RabbitMQ le entrega mensajes a un worker. Por eso se configura solo en worker.py y no en new_task.py

> don't dispatch a new message to a worker until it has processed and acknowledged the previous one. Instead, it will dispatch it to the next worker that is not still busy.







