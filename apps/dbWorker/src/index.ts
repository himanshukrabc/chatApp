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
const consumer = queue.getKafkaInstance().consumer({ groupId: "dbWorker" });

server.on("connect", async(socket)=>{
  socket.on("topic",async(data,ack)=>{
    try{
      const {topic}:{topic:string} = data;
      await consumer.subscribe({topic});
      ack({success:true});
    }
    catch(err){
      ack({success:false});
    }
  });
  socket.on("getMessage", async(data,ack)=>{
    try {
      console.log("Message");
      const id: string = data.id;
      const user = await prisma.user.findUnique({
        where: { id },
        select: { offset: true },
      });
      console.log(id);
      if (!user || !user.hasOwnProperty("offset")) {
        ack({ success:false, message: "user not found" });
      }
      console.log(user);
      const messages: any[] = [];
  
      const admin = queue.getKafkaInstance().admin();
      await admin.connect();
      const topicOffsets = await admin.fetchTopicOffsets(id);
      await admin.disconnect();
      const latestOffset = topicOffsets.find(({ partition }) => partition == 0)?.high;
      if (!latestOffset) {
        ack({success:true});
        socket.emit("newMessages",{messages});
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
            res.status(200).json({ messages });
          }
        },
      });
      console.log("starting seek");
      consumer.seek({topic: id,partition: 0,offset: user ? user.offset.toString() : "0"});
      console.log("seek done");
      if (continueConsuming) {
        await consumer.disconnect();
        return res.status(200).json({ success: true, messages });
      }
    } catch (err) {
      console.log(err);
      return res.status(500).json({ message: "Internal server error" });
    }
  })
});

app.post("/getMessage", async (req: Request, res: Response): Promise<any> => {
  try {
    console.log("Message");
    const id: string = req.body.id;
    const user = await prisma.user.findUnique({
      where: { id },
      select: { offset: true },
    });
    console.log(id);
    if (!user || !user.hasOwnProperty("offset")) {
      return res.status(404).send({ message: "user not found" });
    }
    console.log(user);
    const messages: any[] = [];

    const admin = queue.getKafkaInstance().admin();
    await admin.connect();
    const topicOffsets = await admin.fetchTopicOffsets(id);
    await admin.disconnect();
    const latestOffset = topicOffsets.find(({ partition }) => partition == 0)?.high;
    if (!latestOffset) {
      return res.status(200).json({ messages });
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
          res.status(200).json({ messages });
        }
      },
    });
    console.log("starting seek");
    consumer.seek({topic: id,partition: 0,offset: user ? user.offset.toString() : "0"});
    console.log("seek done");
    if (continueConsuming) {
      await consumer.disconnect();
      return res.status(200).json({ success: true, messages });
    }
  } catch (err) {
    console.log(err);
    return res.status(500).json({ message: "Internal server error" });
  }
});

httpServer.listen(PORT, async () => {
  console.log(`Server running on http://localhost:${PORT}`);
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