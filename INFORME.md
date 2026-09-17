## Resumen
La idea principal del TP es encapsular la complejidad de RabbitMQ detras de dos clases que funcionen como una api que permite: iniciar consumo, detener, enviar mensajes, etc.

Se escogió el lenguaje Python, usando la librería pika.


## Explicación tests
Se muestra 68% porque se recolectaron 13 de los 19 tests en total y representa la primera parte. 13/19 = 0.68 => 68%


## Test propio para verificar comportamiento de excepciones
En una consola se ejecuta `docker compose up rabbitmq`

En otra el siguiente script si se encuentra en src/common, por ejemplo `python -m common.test`

Por ultimo se da un margen de 10 segundos para detenerlo y ver en los logs los errores, o ir modificando este script para ir viendo cada error `docker compose stop rabbitmq`


```python
import time
from common.middleware import middleware_rabbitmq as m

queue = m.MessageMiddlewareQueueRabbitMQ("localhost", "test_queue")
queue.send(b"mensaje 1")   # funciona

print("Apagá RabbitMQ ahora. Tenés 10 segundos...")
time.sleep(10)

try:
    queue.send(b"mensaje 2")   # debería fallar
except m.MessageMiddlewareDisconnectedError as e:
    print(f"OK: DisconnectedError -> {e}")
except m.MessageMiddlewareMessageError as e:
    print(f"Ojo, dio MessageError -> {e}")
finally:
    try:
        queue.close()
    except m.MessageMiddlewareCloseError:
        pass
```