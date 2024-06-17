const Kafka = require('node-rdkafka');

const ERR_TOPIC_ALREADY_EXISTS = 36;
const bootstrapServers = "";
const username = "";
const password = "";
const mechanisms = "PLAIN";
const protocol = "SASL_SSL";
const topic = "topic0";

function ensureTopicExists() {
  const adminClient = Kafka.AdminClient.create({
    'bootstrap.servers': bootstrapServers,
    'sasl.username': username,
    'sasl.password': password,
    'security.protocol': protocol,
    'sasl.mechanisms': mechanisms
  });

  return new Promise((resolve, reject) => {
    adminClient.createTopic({
      topic: topic,
      num_partitions: 2,
      replication_factor: 3
    }, (err) => {
      if (!err) {
        console.log(`Created topic ${topic}`);
        return resolve();
      }

      if (err.code === ERR_TOPIC_ALREADY_EXISTS) {
        return resolve();
      }

      return reject(err);
    });
  });
}

function createProducer(onDeliveryReport) {
  const producer = new Kafka.Producer({
    'bootstrap.servers': bootstrapServers,
    'sasl.username': username,
    'sasl.password': password,
    'security.protocol': protocol,
    'sasl.mechanisms': mechanisms,
    'dr_msg_cb': true
  });

  return new Promise((resolve, reject) => {
    producer
      .on('ready', () => resolve(producer))
      .on('delivery-report', onDeliveryReport)
      .on('event.error', (err) => {
        console.warn('event.error', err);
        reject(err);
      });
    producer.connect();
  });
}

async function produceExample() {

  await ensureTopicExists();

  const producer = await createProducer((err, report) => {
    if (err) {
      console.warn('Error producing', err)
    } else {
      const {topic, partition, value} = report;
      console.log(`Successfully produced record to topic "${topic}" partition ${partition} ${value}`);
    }
  });

  for (let idx = 0; idx < 10; ++idx) {
    const key = 'key0';
    const value = Buffer.from(JSON.stringify({ count: idx }));

    console.log(`Producing record ${key}\t${value}`);

    producer.produce(topic, 0, value, key);
  }

  for (let idx = 0; idx < 5; ++idx) {
    const key = 'key1';
    const value = Buffer.from(JSON.stringify({ count: idx }));

    console.log(`Producing record ${key}\t${value}`);

    producer.produce(topic, 1, value, key);
  }

  producer.flush(10000, () => {
    producer.disconnect();
  });
}

produceExample()
  .catch((err) => {
    console.error(`Something went wrong:\n${err.stack}`);
    process.exit(1);
  });
