import express, { Request, Response } from "express";
import cors from "cors";
import bodyParser from "body-parser";
import crypto, { hash } from "crypto";
import { PrismaClient } from "@prisma/client";

const app = express();
const PORT = 3004;
const prisma = new PrismaClient();

app.use(cors());
app.use(bodyParser.json());

app.post("/topic", async (req: Request, res: Response) => {
});

app.listen(PORT, async () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
