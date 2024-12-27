import { Server } from "socket.io";
import express, { Request, Response } from "express";
import cors from "cors";
import bodyParser from "body-parser";
import Queue from "./handlers/queueHandler";
import { PrismaClient } from "@prisma/client";
import { createServer, get } from "http";


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

const queue = new Queue();

server.on("connect", async(socket)=>{
  const queue = new Queue();
  socket.on("topic",async(data,ack)=>{
    try{
      const {topic}:{topic:string} = data;
      const consumer = queue.getKafkaInstance().consumer({ groupId: "dbWorker" });
      await consumer.subscribe({topic});
      ack({success:true});
    }
    catch(err){
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
        socket.emit("response",{success:true, messages});
        return;
      }

      console.log(topicOffsets);

      const consumer = queue.getKafkaInstance().consumer({ groupId: id + "-getMessage" });
      await consumer.connect();
      await consumer.subscribe({ topic: id });
      
      let continueConsuming = true;
      console.log("starting to run");
      consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          if (!message.value) {
            return;
          }
          console.log(message.value.toString());
          messages.push(JSON.parse(message.value.toString()));
          if (message.offset === (parseInt(latestOffset) - 1).toString()) {
            continueConsuming = false;
            await consumer.disconnect();
            socket.emit("response",{success:true, messages });
          }
        },
      });
      console.log("starting seek");
      consumer.seek({topic: id,partition: 0,offset: user ? user.offset.toString() : "0"});
      console.log("seek done");
      if (continueConsuming) {
        await consumer.disconnect();
        socket.emit("response",{success:true, messages });
      }
    } catch (err) {
      console.log(err);
      socket.emit("response",{success:false, message:"Internal server error" });
    }
  })
});

httpServer.listen(PORT, async () => {
  console.log(`Server running on http://localhost:${PORT}`);
  const queue = new Queue();
  const consumer = queue.getKafkaInstance().consumer({ groupId: "dbWorker" });
  await consumer.connect();
  const users = await prisma.user.findMany({ select: { id: true } });
  const topics = users.map((user) => user.id);
  console.log(topics);
  await queue.initTopics(
    topics.map((topic) => {
      return { topic, numPartitions: 1 };
    })
  );
  await consumer.subscribe({ topics });
  console.log("consumer starting to read messages");
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      if (!message.value) {
        return;
      }
      const data = JSON.parse(message.value?.toString());
      console.log(data);
      // const dbAck = await prisma.message.create({
      //   data: {
      //     senderId: data.senderId,
      //     recieverId: data.receiverId,
      //     text: data.text,
      //     timestamp: data.timestamp,
      //   },
      // });
      // console.log(dbAck);
    },
  });
});