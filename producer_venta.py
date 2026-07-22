import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer

KAFKA_BROKER = "IP_DE_TU_KAFKA:9092"
TOPIC = "eventos_topic_1"

EVENTOS = ["VIEW_PRODUCT", "ADD_CART", "PURCHASE", "SEARCH"]

PRODUCTOS = {
    "Electronics": [
        ("Laptop Lenovo", 3200),
        ("Laptop Dell", 4500),
        ("iPhone 15", 5200),
        ("Samsung Galaxy S24", 3800),
        ("Audífonos Sony", 450),
        ("Monitor LG 27\"", 1200),
    ],
    "Hogar": [
        ("Aspiradora Robot", 1800),
        ("Licuadora Oster", 350),
        ("Cafetera Nespresso", 900),
    ],
    "Ropa": [
        ("Zapatillas Nike", 650),
        ("Polo Adidas", 180),
        ("Casaca North Face", 1200),
    ],
}

CIUDADES = ["Lima", "Arequipa", "Cusco", "Moquegua", "Ayacucho", "Ica", "Tacna", "Puno"]

def generar_evento():
    categoria = random.choice(list(PRODUCTOS.keys()))
    producto, precio_base = random.choice(PRODUCTOS[categoria])
    
    precio = round(precio_base * random.uniform(0.85, 1.15), 2)
    
    ahora = datetime.now() + timedelta(seconds=random.randint(-60, 60))
    
    evento = {
        "user": f"USR{random.randint(1, 500):03d}",
        "event": random.choice(EVENTOS),
        "product": producto,
        "category": categoria,
        "city": random.choice(CIUDADES),
        "price": precio,
        "timestamp": ahora.strftime("%Y-%m-%dT%H:%M:%S")
    }
    return evento

def main():
    
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    
    contador = 0
    try:
        while True:
            evento = generar_evento()
            producer.send(TOPIC, key=evento["user"].encode("utf-8"), value=evento)
            contador += 1
            print(f"[{contador}] Enviado: {evento['event']} | {evento['product']} | {evento['user']} | {evento['city']}")
            
            time.sleep(random.uniform(0.5, 2.0))
            
    except KeyboardInterrupt:
        print(f"\nDetenido. Total de eventos enviados: {contador}")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()
