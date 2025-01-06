import Redis from "ioredis";
import { Server } from "socket.io";
import express from "express";
import { createServer } from "http";

const port = 3001;
const app = express();
const httpServer = createServer(app);
const io = new Server(httpServer, {
  cors: { origin: "*", methods: ["GET", "POST"], credentials: true },
});

const redis = new Redis();

const ONLINE_USERS_KEY = "online_users";
const USER_TTL = 30;

io.on("connection", (socket) => {
  console.log("User connected");

  socket.on("login", async (data, ack) => {
    try {
      console.log(`${data.senderId} logged in.`);
      await redis.set(data.senderId, 1, "EX", USER_TTL);
      ack({ success: true });
    } catch (err) {
      ack({ success: false });
    }
  });

  socket.on("heartbeat", async (data, ack) => {
    try {
      console.log(`Heartbeat from user: ${data.senderId}`);
      await redis.set(data.senderId, 1, "EX", USER_TTL);
      ack({ success: true });
    } catch (err) {
      ack({ success: false });
    }
  });

  socket.on("getStatus", async (data, ack) => {
    try {
      console.log(`Request status for user: ${data.id}`);
      const isOnline = await redis.exists(data.id); 
      console.log(isOnline);   
      ack({ success: true, status: (isOnline==1) });
    } catch (err) {
      ack({ success: false });
    }
  });

  socket.on("logout", async (data: any) => {
    console.log("User logged out:", data);
    try {
      await redis.hdel(ONLINE_USERS_KEY, data);
    }
    catch(err){
        console.log("redis delete failed");
    }
  });
});

httpServer.listen(port, () => {
  console.log(`App listening at http://localhost:${port}`);
});
