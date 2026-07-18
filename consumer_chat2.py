import json
from confluent_kafka import Consumer, KafkaError

KAFKA_BROKER = '54.90.200.251:9092'
TOPIC_NAME = 'chat-rapido'
CONSUMER_GROUP = 'grupo-notifi2'

def main():
    print(f"conectándose a {KAFKA_BROKER}...")
    
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': CONSUMER_GROUP,
        'auto.offset.reset': 'earliest'
    }
    
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC_NAME])
    
    print(f"Esperando  en el topic '{TOPIC_NAME}' bajo el grupo '{CONSUMER_GROUP}'...")
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            
            msj = json.loads(msg.value().decode('utf-8'))
            sala = msj['sala_id']
            usuario = msj['usuario']
            texto = msj['mensaje']
            
            print(f"[NOTIFICACIÓN - Partición {msg.partition()}] {sala} -> {usuario}: {texto}")
                
    except KeyboardInterrupt:
        print("\nDeteniendo el Consumidor...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
