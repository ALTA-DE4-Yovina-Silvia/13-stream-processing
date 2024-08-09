import json
from uuid import uuid4
from confluent_kafka import KafkaException
from confluent_kafka.avro import AvroProducer 
from confluent_kafka import avro

def delivery_callback(error, message):
    if error:
        print("Failed to send the message: %s" % error)
    else:
        print(f"Message with the key {message.key()} has been produced to the topic {message.topic()}")

def load_avro_schema_from_file():
    key_schema_string = """
    {"type": "string"}
    """
    
    key_schema = avro.loads(key_schema_string)
    value_schema = avro.load('C:/Users/USER/Alta/belajar-bc/stream-data/pubsub/schemas/click_event.avsc')

    return key_schema, value_schema

def produce():
    config = {
        'bootstrap.servers' : "34.101.224.54:19092",
        'schema.registry.url' : "http://34.101.224.54:18081"
    }

    key_schema, value_schema = load_avro_schema_from_file()

    producer = AvroProducer(
        config,
        default_key_schema = key_schema,
        default_value_schema = value_schema
    )

    countdata = 0

    try:
        while True:
            key = str(uuid4())
            value_str = '{"user_id":1,"event_type":"AVRO","ts":"2021-11-8"}'
            value = json.loads(value_str) 

            producer.produce(
            topic = "Task_Yovina_Silvia_DE_4",
            key = key,
            value = value,
            on_delivery = delivery_callback
            )
            countdata+=1
            if countdata == 2000:
                break;
            else:
                pass
        producer.poll(10000)
        producer.flush()

    except KafkaException as e:
        print("Error occurred during message production:", e)

    print("Done!")

def main():
    produce()

if __name__ == "__main__":
    main()