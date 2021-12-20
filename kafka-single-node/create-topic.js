
const { Kafka } = require('kafkajs')

const brokers  = [process.env.KAFKA_BROKER || 'localhost:9092']
const clientId = process.env.CLIENT_ID || 'single-node-sample"'

const main = async () => {
  console.log('Creating topic messages');

  const kafka = new Kafka({
    'clientId': clientId,
    'brokers': brokers
  })

  const admin = kafka.admin();
  console.log('Trying to connect...')
  
  await admin.connect()
  console.log('Okay!')

  await admin.createTopics({
    'topics': [{
      'topic': 'Testzao',
      'numPartitions': 2
    }]
  });
  console.log('Topic is created')

  await admin.disconnect();
}

main();

