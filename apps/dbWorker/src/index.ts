import { Server } from "socket.io";
import express from "express";
import cors from "cors";
import Queue from "./handlers/queueHandler";
import { PrismaClient } from "@prisma/client";
import { createServer } from "http";
import { Consumer } from "kafkajs";
import crypto, { hash } from "crypto";


const app = express();
const PORT = 3003;
const httpServer = createServer(app);
const prisma = new PrismaClient();
app.use(cors());

const server = new Server(httpServer, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
    credentials: true,
  },
});

const dbQueue = new Queue();
var dbConsumer:Consumer;
var topics:string[];

async function restartServer(str:string){
  if(dbConsumer){
    const resp = await dbConsumer.stop();
    console.log(resp);
    await dbConsumer.disconnect();
  }
  dbConsumer = dbQueue.getKafkaInstance().consumer({ groupId: "dbWorker" });
  await dbConsumer.connect();
  console.log("consumer connected");
  await dbConsumer.subscribe({topics});
  console.log("consumer starting to read messages");
  dbConsumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      if (!message.value) {
        return;
      }
      const data = JSON.parse(message.value?.toString());
      console.log(data);
      console.log(str);
      const dbAck = await prisma.message.create({
        data: {
          senderId: data.senderId,
          recieverId: data.receiverId,
          text: data.text,
          timestamp: data.timestamp,
        },
      });
      console.log(dbAck);
    },
  });
  console.log("consumer run");

}

server.on("connect", async(socket)=>{
  const queue = new Queue();
  socket.on("topic",async(data,ack)=>{
    try{
      const {topic}:{topic:string} = data;
      await dbQueue.initTopics(
        [{ topic, numPartitions: 1 }]
      );
      topics.push(topic);
      await restartServer(Math.random().toString());
      ack({success:true});
    }
    catch(err){
      console.log(err);
      ack({success:false});
    }
  });


  socket.on("getMessages", async(data)=>{
    try {
      console.log("get Messages");
      const id: string = data.id;
      const user = await prisma.user.findUnique({
        where: { id },
        select: { offset: true },
      });
  
      console.log(id);
      if (!user || !user.hasOwnProperty("offset")) {
        console.log("1");
        socket.emit("response",{ success:false, message: "user not found" });
        return;
      }

      console.log(user);
      const messages: any[] = [];

      const admin = queue.getKafkaInstance().admin();
      await admin.connect();
      const topicOffsets = await admin.fetchTopicOffsets(id);
      await admin.disconnect();
      const latestOffset = topicOffsets.find(({ partition }) => partition == 0)?.high;
      if (!latestOffset) {
        console.log("2");
        socket.emit("response",{success:true, messages});
        return;
      }

      console.log(topicOffsets);

      const consumer = queue.getKafkaInstance().consumer({ groupId: id + "-getMessage" });
      await consumer.connect();
      await consumer.subscribe({ topic: id });
      console.log("starting to run");
      consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          if (!message.value) {
            return;
          }
          console.log(message.value.toString());
          messages.push(JSON.parse(message.value.toString()));
          if (message.offset === (parseInt(latestOffset) - 1).toString()) {
            console.log("message is being sent");
            console.log("3");
            socket.emit("response",{success:true, messages });
            await consumer.disconnect();
          }
        },
      });
      console.log("starting seek");
      consumer.seek({topic: id,partition: 0,offset: user ? user.offset.toString() : "0"});
      console.log("seek done");
    } catch (err) {
      console.log(err);
      console.log("5");
      socket.emit("response",{success:false, message:"Internal server error" });
    }
  })
});

httpServer.listen(PORT, async () => {
  console.log(`Server running on http://localhost:${PORT}`);
  const users = await prisma.user.findMany({ select: { id: true } });
  topics = users.map((user) => user.id);
  console.log(topics);
  await dbQueue.initTopics(
    topics.map((topic) => {
      return { topic, numPartitions: 1 };
    })
  );
  await dbQueue.initTopics(
    topics.map((topic) => {
      return { topic:topic+"-online", numPartitions: 1 };
    })
  );
  await restartServer(Math.random().toString());
});