# Lab de Ventas en Tiempo Real (Kafka + Flink)

Este laboratorio implementa una arquitectura distribuida de Big Data para el procesamiento en tiempo real de eventos de e-commerce. Utiliza **Apache Kafka** como bus de mensajería y **Apache Flink** como motor de procesamiento en streaming (procesamiento de datos continuos).

Todo el despliegue está automatizado usando scripts en Python y **Boto3** para levantar infraestructura en **Amazon EC2**.

---

##  Arquitectura de la Solución

El flujo de los datos sigue esta estructura:

1. **Simulador / Productor de Ventas (`producer_venta.py`)**:
   Genera eventos JSON aleatorios simulando acciones de usuarios en una tienda online (`VIEW_PRODUCT`, `ADD_CART`, `PURCHASE`, `SEARCH`).
   Los eventos se envían al topic `eventos_topic_1` utilizando el ID del usuario (`user`) como **Partition Key**, lo que garantiza el balanceo de carga y el orden cronológico.

2. **Broker de Kafka (Servidor EC2)**:
   Almacena los eventos JSON. El topic está configurado con **2 particiones** para habilitar la lectura en paralelo.

3. **Apache Flink (Cluster en EC2)**:
   - **1 JobManager**: Coordinador central.
   - **2 TaskManagers**: Nodos trabajadores. Cada uno consume una partición distinta en paralelo (gracias al paralelismo = 2).
   El Job en Java (`DataStreamJob.java`) procesa, filtra, transforma y agrupa las estadísticas de forma continua.

---

##  Requisitos Previos

- **Credenciales AWS**: Configuración local (ej. `aws_exports.sh` y llave `.pem`) para Boto3.
- **Python 3+**: Instalar librerías `boto3` y `kafka-python`.
- **Maven**: Para compilar el proyecto de Java Flink.

---

##  Guía Rápida de Despliegue y Ejecución

### 1. Despliegue de Infraestructura Flink (EC2)
*Asegúrate de que tu servidor Kafka ya esté corriendo.*

Levanta el nodo principal (Coordinador):
```bash
python3 levantar_ec2_flink_jm.py
```
*(Espera un par de minutos a que la instancia se instale y anota la IP Pública para acceder a la UI en el puerto 8081)*.

Levanta los nodos de trabajo (Ejecutar este comando **2 veces** para tener 2 servidores):
```bash
python3 levantar_ec2_flink_tm.py
```

### 2. Configurar Kafka
En tu servidor Kafka, crea el topic con 2 particiones:
```bash
kafka-topics.sh --create --topic eventos_topic_1 --bootstrap-server localhost:9092 --partitions 2 --replication-factor 1
```

### 3. Compilar el Job de Flink
Navega a la carpeta del proyecto Java y compílalo:
```bash
cd flink-ventas
mvn clean package -DskipTests
```
Se generará un archivo JAR: `target/flink-ventas-1.0-SNAPSHOT.jar`.

### 4. Desplegar el Job en Flink
1. Abre el Panel Web de Flink: `http://[IP_JOBMANAGER]:8081`.
2. Ve a **Submit New Job**.
3. Sube el `.jar` generado y presiona **Submit**.

### 5. Iniciar la Simulación de Ventas
Ejecuta el script productor en tu computadora local:
```bash
python3 producer_venta.py
```

En la UI de Flink (TaskManagers > Stdout), empezarás a ver en tiempo real:
- `2.1`: Listado de todos los eventos.
- `2.2`: Filtro exclusivo de Compras y Carritos.
- `2.4`: Conteo y estadísticas globales acumuladas por acción.
- `2.5`: Ranking de productos más interactuados.

---

##  Estructura del Evento JSON

```json
{
  "user": "USR042",
  "event": "PURCHASE",
  "product": "Laptop Lenovo",
  "category": "Electronics",
  "city": "Arequipa",
  "price": 3200.0,
  "timestamp": "2026-07-20T19:15:20"
}
```
*(El job de Flink lee este JSON y lo enriquece automáticamente agregando la `hora`, el `día`, el `mes` y la validación de `esFinDeSemana`).*
