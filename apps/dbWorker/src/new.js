const { Kafka } =  require("kafkajs");

class Queue {
  
  constructor() {
    this.kafka = new Kafka({
      clientId:"chatApp",
      brokers: ["192.168.1.41:9092"],
    });
    this.producer = null;
    this.consumer = null;
  }

  async initTopics(topics) {
    const admin = this.kafka.admin();
    await admin.connect();
    const res = await admin.createTopics({topics});
    await admin.disconnect();
  }
  
  async initProducer(){
    this.producer = this.kafka.producer();
    await this.producer.connect();
  }

  async produce(topic, partition, message) {
    await this.producer.send({
      topic,
      messages: [{ value: message, partition }],
    });
  }

  async disconnectQueue(){
    await this.producer.disconnect();
    await this.consumer.disconnect();
  }

  getKafkaInstance() {
    return this.kafka;
  }
}

async function produceMessages() {
  const queue = new Queue();

  // Initialize the Kafka producer
  await queue.initProducer();

  const topic = "675568f74bfe2ed8da5d2b43"; // Replace with your topic name
  const totalMessages = 1000;

  console.log(`Producing ${totalMessages} messages to topic: ${topic}`);
  for (let i = 0; i < totalMessages; i++) {
    const message = { id: i, text: `Message number ${i}` };

    try {
      await queue.produce(topic, 0, JSON.stringify(message)); // Replace 0 with desired partition
      console.log(`Produced message ${i + 1}/${totalMessages}`);
    } catch (err) {
      console.error(`Failed to produce message ${i + 1}:`, err);
    }
  }

  console.log("Finished producing messages.");
  // Disconnect the producer
  await queue.disconnectQueue();
}

produceMessages().catch((err) => console.error("Error producing messages:", err));
