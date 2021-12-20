const { Kafka } = require('kafkajs')

const brokers  = [process.env.KAFKA_BROKER || 'localhost:9092']
const clientId = process.env.CLIENT_ID || 'single-node-sample'
const topic    = process.env.TOPIC_NAME || 'Testzao'

const main = async () => {

  console.log('Producing messages');

  const kafka = new Kafka({
    'clientId': clientId,
    'brokers': brokers
  })

  const producer = kafka.producer()
  console.log('Trying to connect...')

  await producer.connect()
  console.log('Okay!')

  const messages = Array.from({length: 100}, (v, i) => {
    return {
      'value': i.toString() + 'abcs',
      'partition': Math.floor(Math.random() * 2) // 0 or 1 random partition
    }
  })
  console.log('Generate messages' , messages)

  const result = await producer.send({
    'topic': topic,
    'messages': messages
  })

  console.log('Success ', result)
  producer.disconnect()

}

main();