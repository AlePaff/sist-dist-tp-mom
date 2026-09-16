
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




### Exchanges
https://www.rabbitmq.com/tutorials/tutorial-three-python

An exchange is a very simple thing. On one side it receives messages from producers and on the other side it pushes them to queues. The exchange must know exactly what to do with a message it receives. Should it be appended to a particular queue? Should it be appended to many queues? Or should it get discarded. The rules for that are defined by the exchange type.


Para ejecutar un comando "docker exec rabbitmq rabbitmqctl list_exchanges"
La docu decia que si ya lo tenias instalado se hace asi "rabbitmqctl list_exchanges". Pero para correrlo si tengo docker es "docker exec <nombre contenedor> <comandos...>

El exchange default es ''

Se ejecuta con 3 consolas
python .\exchanges\receive_logs.py
python .\exchanges\receive_logs.py
python .\exchanges\emit_log.py 
esta ultima le envia el mismo mensaje a las otras dos queues mediante un exchange



#### Tipos de exchanges:

1. Fanout (El Megáfono / Radiodifusión)

* Cómo funciona: Ignora por completo las palabras clave (routing_key). Simplemente toma el mensaje y lo duplica para enviarlo a absolutamente todas las colas que estén conectadas a él.
* Caso de uso ideal: Transmisión de datos en tiempo real (puntuaciones de fútbol, cotizaciones de bolsa) o sistemas de logs donde quieres que múltiples servicios guarden el mismo registro a la vez.

2. Direct (El Cartero Preciso)

* Cómo funciona: El mensaje se envía únicamente a la cola cuya clave de conexión coincida exactamente con la routing_key del mensaje. Es una relación uno a uno por coincidencia exacta.
* Caso de uso ideal: Clasificación estricta. Por ejemplo, si el mensaje tiene la clave error, solo va a la cola encargada de mandar alertas al equipo de soporte, ignorando las colas de info o warning.

3. Topic (El Filtro Inteligente / Publicación-Suscripción)

* Cómo funciona: Es una versión avanzada del tipo Direct. Permite enrutar mensajes basados en patrones con comodines, separando las palabras por puntos (ej. usa.noticias.deportes). Las colas pueden usar:
* * (asterisco) para reemplazar exactamente una palabra.
   * '#' (hash) para reemplazar cero o más palabras.
* Caso de uso ideal: Sistemas complejos de notificación. Una cola puede suscribirse a *.noticias.# para recibir noticias de cualquier país y de cualquier categoría.

4. Headers (El Inspector de Atributos)

* Cómo funciona: Ignora por completo el routing_key. En su lugar, decide a qué cola enviar el mensaje analizando los atributos del encabezado (headers) de los metadatos del mensaje. Puedes configurar la cola para que exija que coincidan todos los encabezados (x-match: all) o al menos uno (x-match: any).
* Caso de uso ideal: Cuando necesitas enrutar mensajes usando múltiples criterios complejos que no se adaptan bien a una simple cadena de texto como el routing_key (por ejemplo, filtrar por formato de archivo, región del usuario y nivel de prioridad al mismo tiempo).




