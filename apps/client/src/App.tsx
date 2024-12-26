import { io, Socket } from "socket.io-client";
import { useEffect, useRef, useState } from "react";


function App() {
  const [socket] = useState(() => io("http://localhost:3000"));
  const [senderId, setSenderId] = useState("");
  const [receiverId, setReceiverId] = useState("");
  const messageRef = useRef<HTMLInputElement | null>(null);

  const emitAsync = (
    socket: Socket,
    event: string,
    data?: { senderId: string, receiverId?: string, text?: string, timestamp?:number },
  ) => {
    return new Promise((resolve, reject) => {
      socket.emit(event, data, (ack: any) => {
        if (ack.success) {
          resolve(ack);
        } else {
          reject(new Error(ack.error || "Unknown error"));
        }
      });
    });
  };

  useEffect(() => {
    async function initSocket() {
      if (!senderId || !receiverId) {
        console.error("senderId & receiverId is required to initialize the socket");
        return;
      }
      try {
        const ack: any = (await emitAsync(socket, "init", { senderId, receiverId }));
        if (ack.success) {
          console.log(ack);
          console.log("Socket initialized for senderId:", senderId);
          console.log("Emitting to online queue");
          socket.emit("online");
          console.log("emit complete");
        }
      } catch (error) {
        console.error("Socket initialization error:", error);
      }
    }
    initSocket();
  }, [senderId,receiverId]);


  const callFunction = async () => {
    try {
      const message = messageRef.current?.value;

      if (!message || !receiverId || !senderId) {
        console.error("Missing input values");
        return;
      }

      await emitAsync(socket, "message", {
        senderId,
        receiverId,
        text: message,
        timestamp:Date.now()
      });
    } catch (error) {
      console.error("Error:", error);
    }
  };

  useEffect(() => {
    socket.on("message", (data) => {
      console.log(data);
      const msgDiv = document.getElementById("messages");
      if (msgDiv) {
        msgDiv.innerText += data + "\n";
      }
    });

    return () => {
      socket.off("message");
    };
  }, [socket]);

  return (
    <>
      <h1>Hello There!!!</h1>
      Id:{" "}
      <input
        type="text"
        id="id"
        value={senderId}
        onChange={(e) => setSenderId(e.target.value)}
      />
      <br />
      sendTo: 
      <input
        type="text"
        id="id"
        value={receiverId}
        onChange={(e) => setReceiverId(e.target.value)}
      />
      <br />
      message: <input type="text" id="message" ref={messageRef} />
      <br />
      <button onClick={callFunction}>Send Message</button>
      <div id="messages" style={{ whiteSpace: "pre-wrap" }}></div>
    </>
  );
}

export default App;
