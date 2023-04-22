from kafka import KafkaConsumer, KafkaProducer
import json

consumer = KafkaConsumer('message-topic',
                         bootstrap_servers=['localhost:9094'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         group_id='my-group')

producer = KafkaProducer(bootstrap_servers='localhost:9094')

for message in consumer:
    data = json.loads(message.value.decode('utf-8'))

    if data['type'] == 'message':
        print('Done!')
    else:
        producer.send('dead-letter-topic', message.value)
        print('dead-letter!')
consumer.close()