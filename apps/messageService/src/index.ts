import { Server } from "socket.io";
import express from "express";
import { createServer, get } from "http";
import cors from "cors";
import Redis from "ioredis";
import crypto, { hash } from "crypto";
import { PrismaClient } from "@prisma/client";
import { io, Socket } from "socket.io-client";
import Queue from "./handlers/queueHandler";

const app = express();
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

function getChannelName(id: string): string {
  const hashedChannelName = crypto.createHash("sha256").update(id).digest("hex");
  return hashedChannelName;
}

server.on("connect", async (socket) => {
  console.log("User connected");
  const publisher = new Redis();
  const subscriber = new Redis();
  const presenceSocket = io("http://localhost:3001");
  const onlineQueue:Queue= new Queue();
  const dbQueue:Queue= new Queue();
  var userId: string = "";
  var timerId:any;
  socket.on("init", async (data, ack) => {
    console.log("init");
    try {
      userId = data.senderId;
      const topics:{topic:string,numPartitions:number}[]=[
        {topic:getChannelName(userId+"-online"),numPartitions:1},
        {topic:data.receiverId,numPartitions:1}
      ]
      await onlineQueue.initTopics(topics);
      await dbQueue.initProducer();
      await onlineQueue.initProducer();
      subscriber.subscribe(getChannelName(userId));
      presenceSocket.emit("login", data, (presenceAck: any) => {
        if (!presenceAck.success) {
          throw "Presence service login failed";
        } else {
          timerId = setInterval(() => {
            presenceSocket.emit("heartbeat", data, (presenceAck: any) => {
              if (!presenceAck.success) {
                throw "Presence service heartbeat failed";
              }
            });
          }, 29000);
        }
      });
      ack({ success: true });
    } catch (err) {
      ack({ success: false });
    }
  });

  socket.on("message", async (data, ack) => {
    console.log("message")
    try {
      const receiver = await prisma.user.findUnique({
        where: { id: data.receiverId },
        select: {
          id: true,
        },
      });
      if (receiver) {
        const channelName = getChannelName(data.receiverId);
        const message = JSON.stringify({ senderId: data.senderId, text: data.text });
        presenceSocket.emit( "getStatus", { id: data.receiverId }, async (presenceAck: any) => {
          if (presenceAck.success) {
            if (presenceAck.status) {
              console.log("published to redis");
              publisher.publish(channelName, message);
            } else {
              console.log(userId + " sending to DB queue");
              console.log(data.receiverId);
              await dbQueue.produce(data.receiverId, 0, JSON.stringify(data));
              console.log(userId + " sent to DB queue");
            }
          } else {
            console.log("Unable to get online status");
          }
        });
      }
      ack({ success: true });
    } catch (err) {
      ack({ success: false });
    }
  });

  subscriber.on("message", async (channel, message) => {
    presenceSocket.emit("getStatus", {id:userId}, async (presenceAck: any) => {
      if (presenceAck.status) {
        await onlineQueue.produce(getChannelName(userId+"-online"), 0, message);
      } else {
        console.log("Unable to get online status");
      }
    });
  });

  socket.on("online", async () => {
    try{
      const topic = getChannelName(userId+"-online");
      const consumer = onlineQueue.getKafkaInstance().consumer({ groupId:userId  });
      await consumer.connect();
      await consumer.subscribe({
        topic
      });
      await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          if (!message.value) {
            return;
          }
          const data = JSON.parse(message.value.toString());
          socket.emit("message",data.text);
          await dbQueue.produce(data.receiverId, 0, JSON.stringify(data));        },
      });
    }
    catch(err){
      console.log(err);
    }
  });

  socket.on("disconnect", () => {
    clearInterval(timerId);
    console.log("done " + userId);
    presenceSocket.emit("logout", userId, (presenceAck: any) => {
      if (!presenceAck.success) {
        throw "Presence service login failed";
      }
      else{
        console.log("logged out");
      }
    });
  });
});

httpServer.listen(3000, () => {
  console.log("Server is running on http://localhost:3000");
});
