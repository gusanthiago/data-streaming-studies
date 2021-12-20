const { Kafka } = require('kafkajs')

const brokers  = [process.env.KAFKA_BROKER || 'localhost:9092']
const clientId = process.env.CLIENT_ID || 'single-node-sample'
const topic    = process.env.TOPIC_NAME || 'Testzao'

const main = async () => {
  const kafka = new Kafka({
    'clientId': clientId,
    'brokers': brokers,
  })

  const consumer = kafka.consumer({ groupId: 'testa' })
  
  console.log(`Consuming messages -> topic ${topic}`, brokers);
  
  console.log('Trying to connect...')
  
  await consumer.connect()
  console.log('Okay!')

  await consumer.subscribe({
    'topic': topic,
    'fromBeginning': false
  })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
        console.log(topic, partition, {
            key: message.key.toString(),
            value: message.value.toString(),
            headers: message.headers,
        })
    },
  })
}

main().catch(async error => {
  console.error(error)
  try {
    await consumer.disconnect()
  } catch (e) {
    console.error('Failed to gracefully disconnect consumer', e)
  }
  process.exit(1)
})