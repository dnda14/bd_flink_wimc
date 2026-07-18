# Proyecto Kafka Chat (EC2 y KRaft)

Este proyecto implementa una arquitectura de mensajeria en tiempo real utilizando Apache Kafka desplegado en una instancia de Amazon EC2. El sistema simula salas de chat mediante el uso de productores y consumidores programados en Python, demostrando conceptos clave como particiones, consumer groups y ruteo por llaves.

## Requisitos Previos

- Cuenta de AWS y llave SSH (archivo .pem) para acceder a la instancia EC2.
- Python 3.10+ en el entorno local.
- Libreria confluent-kafka para Python.

Para instalar las dependencias locales, ejecute:
pip install confluent-kafka

## Arquitectura de la Solucion

1. Broker de Kafka (EC2): Servidor Ubuntu que ejecuta Kafka en modo KRaft (sin ZooKeeper).
2. Productor (producer_chat.py / producer_chat2.py): Scripts que generan mensajes de diferentes usuarios asignados a salas especificas. Utilizan el nombre de la sala como "key" para garantizar el orden de entrega.
3. Consumidor (consumer_chat.py / consumer_chat2.py): Scripts que se suscriben al topic y procesan los mensajes en tiempo real utilizando el grupo de consumidores "grupo-notifi".

## Despliegue de la Infraestructura

El proyecto incluye el script levantar_ec2.py que automatiza la creacion y configuracion del servidor Kafka mediante la libreria Boto3. 

El script realiza lo siguiente:
- Crea un Security Group con los puertos 22 (SSH) y 9092 (Kafka) abiertos.
- Lanza una instancia t3.medium.
- Instala Java 17 y descarga Apache Kafka.
- Configura Kafka en modo KRaft, asignando la IP publica al archivo server.properties.
- Crea un servicio Systemd para mantener Kafka en ejecucion continua.

Para desplegar la infraestructura:
python3 levantar_ec2.py

## Uso de la Aplicacion

### 1. Productores
Para enviar mensajes al topic configurado, ejecute uno o varios productores en terminales distintas:
python3 producer_chat.py
python3 producer_chat2.py

### 2. Consumidores
Para recibir los mensajes, inicie uno o multiples consumidores en terminales paralelas:
python3 consumer_chat.py
python3 consumer_chat2.py

Nota: Si inicia multiples consumidores con el mismo "group.id", Kafka balanceara automaticamente la carga repartiendo las particiones disponibles entre ellos.


