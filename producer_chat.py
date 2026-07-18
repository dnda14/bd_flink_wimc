import json
import time
import random
from confluent_kafka import Producer

KAFKA_BROKER = '54.90.200.251:9092'
TOPIC_NAME = 'chat_rapido2'

def generar_mensaje():
    salas = ['sala-general', 'sala-juegos', 'sala-trabajo', 'sala-familia']
    usuarios = ['Jose', 'Lenin', 'Loco', 'Maria', 'Edilson']
    textos = ['Hola a todos!', '¿Cómo están?', 'Ya envié el reporte', 'GG', '¿Qué hay de cenar?']
    
    sala = random.choice(salas)
    usuario = random.choice(usuarios)
    texto = random.choice(textos)
    
    return sala, {
        "sala_id": sala,
        "usuario": usuario,
        "mensaje": texto,
        "timestamp": int(time.time())
    }

def delivery_report(err, msg):
    if err is not None:
        print(f"Mensaje fallido: {err}")

def main():
    conf = {'bootstrap.servers': KAFKA_BROKER}
    producer = Producer(conf)
    
    try:
        while True:
            sala, msj = generar_mensaje()
            
            val_bytes = json.dumps(msj).encode('utf-8')
            key_bytes = sala.encode('utf-8')
            
            producer.produce(TOPIC_NAME, key=key_bytes, value=val_bytes, callback=delivery_report)
            producer.poll(0)
            
            print(f"Enviado a {sala}: [{msj['usuario']}] {msj['mensaje']}")
            time.sleep(1.5)
            
    except KeyboardInterrupt:
        print("\nDeteniendo el Productor...")
    finally:
        producer.flush()

if __name__ == "__main__":
    main()
