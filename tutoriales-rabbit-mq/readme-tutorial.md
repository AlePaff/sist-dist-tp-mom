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


