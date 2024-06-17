const Kafka = require('node-rdkafka');

const bootstrapServers = "";
const username = "";
const password = "";
const mechanisms = "PLAIN";
const protocol = "SASL_SSL";
const topic = "topic0";

function createConsumer(onData) {
  const consumer = new Kafka.KafkaConsumer({
    'bootstrap.servers': bootstrapServers,
    'sasl.username': username,
    'sasl.password': password,
    'security.protocol': protocol,
    'sasl.mechanisms': mechanisms,
    'group.id': 'group-1'
  }, {
    'auto.offset.reset': 'earliest'
  });

  return new Promise((resolve, reject) => {
    consumer
      .on('ready', () => resolve(consumer))
      .on('data', onData);

    consumer.connect();
  });
}

async function consumerExample() {

  console.log(`Consuming records from ${topic}`);

  let seen = 0;

  const consumer = await createConsumer(({key, value, partition, offset}) => {
    console.log(`Consumed record with key ${key} and value ${value} of partition ${partition} @ offset ${offset}. Updated total count to ${++seen}`);
  });

  consumer.subscribe([topic]);
  consumer.consume();

  process.on('SIGINT', () => {
    console.log('\nDisconnecting consumer ...');
    consumer.disconnect();
  });
}

consumerExample()
  .catch((err) => {
    console.error(`Something went wrong:\n${err}`);
    process.exit(1);
  });
