"use strict";

const http = require("http");
const crypto = require("crypto");
const WebSocket = require("ws");

const PORT = process.env.PORT || 3000;
const CODE_LIFETIME = 10 * 60 * 1000;

const server = http.createServer((req, res) => {
  res.setHeader("Content-Type", "application/json");
  res.setHeader("Cache-Control", "no-store");

  if (req.url === "/health" && req.method === "GET") {
    res.writeHead(200);
    return res.end(JSON.stringify({
      status: "ok",
      service: "Live Audio Link signaling"
    }));
  }

  res.writeHead(404);
  res.end(JSON.stringify({ error: "Not found" }));
});

const wss = new WebSocket.Server({
  server,
  maxPayload: 16 * 1024
});

const rooms = new Map();
const attempts = new Map();

function getClientIp(req) {
  // Use the direct socket address rather than trusting
  // arbitrary client-supplied forwarding headers.
  return req.socket.remoteAddress || "unknown";
}

function allowed(ip) {
  const now = Date.now();
  let entry = attempts.get(ip);

  if (!entry || now - entry.start > 60_000) {
    entry = { start: now, count: 0 };
    attempts.set(ip, entry);
  }

  entry.count++;
  return entry.count <= 30;
}

function send(ws, message) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(message));
  }
}

function makeCode() {
  // Eight characters, using an unambiguous alphabet.
  const alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  const bytes = crypto.randomBytes(8);

  return Array.from(bytes, b => alphabet[b % alphabet.length])
    .join("");
}

function removePeer(ws) {
  const code = ws.roomCode;
  if (!code) return;

  const room = rooms.get(code);
  ws.roomCode = null;

  if (!room) return;

  if (room.sender === ws) {
    room.sender = null;
    if (room.receiver) {
      send(room.receiver, { type: "peer-disconnected" });
      room.receiver.close(1000, "Sender disconnected");
    }
    rooms.delete(code);
    return;
  }

  if (room.receiver === ws) {
    room.receiver = null;

    if (room.sender) {
      send(room.sender, { type: "peer-disconnected" });
    }

    if (!room.sender) rooms.delete(code);
  }
}

function cleanupExpiredRooms() {
  const now = Date.now();

  for (const [code, room] of rooms) {
    if (now >= room.expiresAt) {
      for (const peer of [room.sender, room.receiver]) {
        if (peer) {
          send(peer, { type: "room-expired" });
          peer.close(1000, "Pairing code expired");
        }
      }

      rooms.delete(code);
    }
  }

  for (const [ip, entry] of attempts) {
    if (now - entry.start > 120_000) {
      attempts.delete(ip);
    }
  }
}

setInterval(cleanupExpiredRooms, 30_000).unref();

wss.on("connection", (ws, req) => {
  const ip = getClientIp(req);

  if (!allowed(ip)) {
    ws.close(1008, "Too many requests");
    return;
  }

  ws.isAlive = true;

  ws.on("pong", () => {
    ws.isAlive = true;
  });

  ws.on("message", raw => {
    if (!allowed(ip)) {
      ws.close(1008, "Too many requests");
      return;
    }

    let message;

    try {
      message = JSON.parse(raw.toString());
    } catch {
      send(ws, { type: "error", message: "Invalid JSON" });
      return;
    }

    if (!message || typeof message.type !== "string") {
      send(ws, { type: "error", message: "Invalid message" });
      return;
    }

    if (message.type === "create") {
      if (ws.roomCode) {
        send(ws, { type: "error", message: "Already paired" });
        return;
      }

      let code;

      do {
        code = makeCode();
      } while (rooms.has(code));

      const room = {
        sender: ws,
        receiver: null,
        expiresAt: Date.now() + CODE_LIFETIME
      };

      rooms.set(code, room);
      ws.roomCode = code;

      send(ws, {
        type: "created",
        code,
        expiresAt: room.expiresAt
      });

      return;
    }

    if (message.type === "join") {
      if (ws.roomCode) {
        send(ws, { type: "error", message: "Already paired" });
        return;
      }

      if (
        typeof message.code !== "string" ||
        !/^[A-HJ-NP-Z2-9]{8}$/.test(message.code)
      ) {
        send(ws, { type: "error", message: "Invalid pairing code" });
        return;
      }

      const code = message.code;
      const room = rooms.get(code);

      if (!room || Date.now() >= room.expiresAt) {
        send(ws, { type: "error", message: "Code expired or not found" });
        return;
      }

      if (!room.sender || room.receiver) {
        send(ws, { type: "error", message: "Room unavailable" });
        return;
      }

      room.receiver = ws;
      ws.roomCode = code;

      send(ws, { type: "joined" });
      send(room.sender, { type: "peer-joined" });

      return;
    }

    if (message.type === "signal") {
      const room = rooms.get(ws.roomCode);

      if (!room || !room.sender || !room.receiver) {
        send(ws, {
          type: "error",
          message: "Both devices must be connected first"
        });
        return;
      }

      const data = message.data;

      if (
        !data ||
        typeof data !== "object" ||
        !["offer", "answer", "ice-candidate"].includes(data.type)
      ) {
        send(ws, { type: "error", message: "Invalid signaling data" });
        return;
      }

      const peer = room.sender === ws ? room.receiver : room.sender;

      send(peer, { type: "signal", data });
      return;
    }

    if (message.type === "leave") {
      removePeer(ws);
      send(ws, { type: "left" });
      return;
    }

    send(ws, { type: "error", message: "Unknown message type" });
  });

  ws.on("close", () => {
    removePeer(ws);
  });

  ws.on("error", () => {
    removePeer(ws);
  });
});

const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }

    ws.isAlive = false;
    ws.ping();
  }
}, 30_000);

heartbeat.unref();

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Live Audio Link signaling server listening on ${PORT}`);
});

server.on("error", err => {
  console.error("Server error:", err.message);
});
