import { Kafka } from "kafkajs";

class Queue {
  private kafka: Kafka;
  private producer: any = null;
  private consumer: any = null;

  constructor() {
    this.kafka = new Kafka({
      clientId:"chatApp",
      brokers: ["192.168.1.41:9092"],
    });
  }

  async initTopics(topics:{topic:string,numPartitions:number}[]) {
    console.log(topics);
    const admin = this.kafka.admin();
    await admin.connect();
    await admin.createTopics({topics});
    await admin.disconnect();
  }
  
  async initProducer(){
    this.producer = this.kafka.producer();
    await this.producer.connect();
  }
  
  async produce(topic: string, partition:number, message: string) {
    await this.producer.send({
      topic,
      messages: [{ value: message, partition }],
    });
  }

  async disconnectQueue(){
    await this.producer.disconnect();
    await this.consumer.disconnect();
  }

  getKafkaInstance(): Kafka {
    return this.kafka;
  }
}

export default Queue;
