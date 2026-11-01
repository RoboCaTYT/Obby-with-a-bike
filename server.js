const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const PORT = process.env.PORT || 3000;
const SCORES_FILE = path.join(__dirname, "scores.json");

// Helper to load scores
function loadScores() {
  try {
    if (fs.existsSync(SCORES_FILE)) {
      const data = fs.readFileSync(SCORES_FILE, "utf8");
      return JSON.parse(data);
    }
  } catch (e) {
    console.error("Error loading scores:", e);
  }
  return [];
}

// Helper to save scores
function saveScores(scores) {
  try {
    fs.writeFileSync(SCORES_FILE, JSON.stringify(scores, null, 2), "utf8");
  } catch (e) {
    console.error("Error saving scores:", e);
  }
}

const server = http.createServer((req, res) => {
  const parsedUrl = new URL(req.url, `http://${req.headers.host}`);
  const pathname = parsedUrl.pathname;

  // Serve index.html
  if (
    req.method === "GET" &&
    (pathname === "/" || pathname === "/index.html")
  ) {
    const filePath = path.join(__dirname, "index.html");
    fs.readFile(filePath, "utf8", (err, data) => {
      if (err) {
        res.writeHead(500, { "Content-Type": "text/plain; charset=utf-8" });
        res.end("500 Internal Server Error");
        return;
      }
      res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
      res.end(data);
    });
  }
  // Get Leaderboard API
  else if (req.method === "GET" && pathname === "/api/leaderboard") {
    const scores = loadScores();
    res.writeHead(200, { "Content-Type": "application/json; charset=utf-8" });
    res.end(JSON.stringify(scores));
  }
  // Post Score API
  else if (req.method === "POST" && pathname === "/api/score") {
    let body = "";
    req.on("data", (chunk) => {
      body += chunk.toString();
    });
    req.on("end", () => {
      try {
        const { name, time } = JSON.parse(body);
        if (!name || typeof time !== "number") {
          res.writeHead(400, {
            "Content-Type": "application/json; charset=utf-8",
          });
          res.end(JSON.stringify({ error: "Invalid data" }));
          return;
        }

        const sanitizedName = String(name).trim().substring(0, 16) || "Rider";
        let scores = loadScores();

        // Add or update score logic
        scores.push({
          name: sanitizedName,
          time,
          date: new Date().toISOString(),
        });
        scores.sort((a, b) => a.time - b.time);
        scores = scores.slice(0, 10); // Keep top 10

        saveScores(scores);

        res.writeHead(200, {
          "Content-Type": "application/json; charset=utf-8",
        });
        res.end(JSON.stringify(scores));
      } catch (e) {
        res.writeHead(400, {
          "Content-Type": "application/json; charset=utf-8",
        });
        res.end(JSON.stringify({ error: "Bad Request" }));
      }
    });
  } else if (req.method === "GET" && pathname === "/api/events") {
    openSSE(req, res, parsedUrl);
  } else if (req.method === "POST" && pathname === "/api/s") {
    postState(req, res, parsedUrl);
  } else {
    res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
    res.end("404 Not Found");
  }
});

const GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const MAX_ROOM = 30;
const rooms = new Map();
const conns = new Set();
const byId = new Map();
let nextId = 1;

function frame(op, data) {
  const payload = Buffer.from(data);
  const len = payload.length;
  const head =
    len < 126
      ? Buffer.from([0x80 | op, len])
      : Buffer.from([0x80 | op, 126, len >> 8, len & 255]);
  return Buffer.concat([head, payload]);
}

function newClient(roomRaw) {
  return {
    buf: Buffer.alloc(0),
    frag: [],
    alive: true,
    closing: false,
    dead: false,
    last: 0,
    room: null,
    roomName:
      String(roomRaw || "main")
        .replace(/[^\w-]/g, "")
        .substring(0, 32) || "main",
    id: null,
    name: "",
    state: null,
    socket: null,
    tx: () => {},
    ping: () => {},
    kill: () => {},
  };
}

function send(c, obj) {
  if (!c.dead) c.tx(JSON.stringify(obj));
}

function broadcast(room, obj, except) {
  const text = JSON.stringify(obj);
  room.clients.forEach((c) => {
    if (c !== except && !c.dead) c.tx(text);
  });
}

function closeConn(c, code) {
  if (c.closing || c.dead) return;
  c.closing = true;
  c.kill(code);
}

function leave(c) {
  const room = c.room;
  if (!room) return;
  c.room = null;
  byId.delete(c.id);
  room.clients.delete(c);
  if (room.clients.size === 0) {
    for (const [k, v] of rooms) if (v === room) rooms.delete(k);
  } else {
    broadcast(room, { t: "leave", id: c.id });
  }
}

function onMessage(c, text) {
  let m;
  try {
    m = JSON.parse(text);
  } catch {
    return;
  }
  if (!m || typeof m !== "object") return;
  if (m.t === "join" && !c.room) {
    let room = rooms.get(c.roomName);
    if (!room) {
      room = { clients: new Set(), dirty: false };
      rooms.set(c.roomName, room);
    }
    if (room.clients.size >= MAX_ROOM) {
      closeConn(c, 1013);
      return;
    }
    c.name =
      String(m.name || "")
        .trim()
        .substring(0, 16) || "Rider";
    c.id = String(nextId++);
    send(c, {
      t: "init",
      id: c.id,
      players: [...room.clients].map((o) => ({ id: o.id, name: o.name })),
    });
    c.room = room;
    room.clients.add(c);
    byId.set(c.id, c);
    broadcast(room, { t: "join", id: c.id, name: c.name }, c);
  } else if (m.t === "name" && c.room) {
    c.name =
      String(m.name || "")
        .trim()
        .substring(0, 16) || "Rider";
    broadcast(c.room, { t: "name", id: c.id, name: c.name }, c);
  } else if (m.t === "s" && c.room && Array.isArray(m.v) && m.v.length === 6) {
    const now = Date.now();
    if (now - c.last < 30) return;
    const v = m.v.map(Number);
    if (!v.every(Number.isFinite)) return;
    c.last = now;
    c.state = v.map(
      (n) => Math.round(Math.max(-5000, Math.min(5000, n)) * 100) / 100,
    );
    c.room.dirty = true;
  }
}

function readFrames(c) {
  while (c.buf.length >= 2 && !c.closing) {
    const b0 = c.buf[0];
    const b1 = c.buf[1];
    const fin = b0 & 0x80;
    const op = b0 & 15;
    const masked = b1 & 0x80;
    let len = b1 & 127;
    let off = 2;
    if (len === 126) {
      if (c.buf.length < 4) return;
      len = c.buf.readUInt16BE(2);
      off = 4;
    } else if (len === 127) {
      closeConn(c, 1009);
      return;
    }
    if (!masked || len > 4096) {
      closeConn(c, masked ? 1009 : 1002);
      return;
    }
    if (c.buf.length < off + 4 + len) return;
    const mask = c.buf.subarray(off, off + 4);
    const data = Buffer.alloc(len);
    for (let i = 0; i < len; i++) data[i] = c.buf[off + 4 + i] ^ mask[i & 3];
    c.buf = c.buf.subarray(off + 4 + len);
    if (op === 8) {
      closeConn(c, 1000);
      return;
    }
    if (op === 9) {
      c.socket.write(frame(10, data));
      continue;
    }
    if (op === 10) continue;
    if (op === 1 || op === 2) c.frag = [];
    c.frag.push(data);
    if (c.frag.length > 4) {
      closeConn(c, 1009);
      return;
    }
    if (fin) {
      onMessage(c, Buffer.concat(c.frag).toString("utf8"));
      c.frag = [];
    }
  }
}

function openSSE(req, res, url) {
  res.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache, no-transform",
    Connection: "keep-alive",
    "X-Accel-Buffering": "no",
  });
  res.write(":\n\n");
  const c = newClient(url.searchParams.get("room"));
  c.tx = (t) => res.write("data: " + t + "\n\n");
  c.ping = () => res.write(":\n\n");
  c.kill = () => res.end();
  conns.add(c);
  req.on("close", () => {
    c.dead = true;
    conns.delete(c);
    leave(c);
  });
  onMessage(
    c,
    JSON.stringify({ t: "join", name: url.searchParams.get("name") }),
  );
}

function postState(req, res, url) {
  let body = "";
  req.on("data", (d) => {
    if (body.length < 1024) body += d;
  });
  req.on("end", () => {
    const c = byId.get(String(url.searchParams.get("id")));
    if (c && !c.socket) onMessage(c, body);
    res.writeHead(204);
    res.end();
  });
}

server.on("upgrade", (req, socket) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const key = req.headers["sec-websocket-key"];
  if (url.pathname !== "/ws" || !key) {
    socket.destroy();
    return;
  }
  const accept = crypto
    .createHash("sha1")
    .update(key + GUID)
    .digest("base64");
  socket.write(
    "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " +
      accept +
      "\r\n\r\n",
  );
  socket.setNoDelay(true);
  const c = newClient(url.searchParams.get("room"));
  c.socket = socket;
  c.tx = (t) => socket.write(frame(1, t));
  c.ping = () => socket.write(frame(9, ""));
  c.kill = (code) => socket.end(frame(8, Buffer.from([code >> 8, code & 255])));
  conns.add(c);
  socket.on("data", (d) => {
    c.alive = true;
    c.buf = Buffer.concat([c.buf, d]);
    readFrames(c);
  });
  socket.on("error", () => {});
  socket.on("close", () => {
    c.dead = true;
    conns.delete(c);
    leave(c);
  });
});

setInterval(() => {
  rooms.forEach((room) => {
    if (!room.dirty) return;
    room.dirty = false;
    const p = [];
    room.clients.forEach((c) => c.state && p.push([c.id, ...c.state]));
    broadcast(room, { t: "s", p });
  });
}, 50);

setInterval(() => {
  conns.forEach((c) => {
    if (c.dead) return;
    if (c.socket) {
      if (!c.alive) {
        c.socket.destroy();
        return;
      }
      c.alive = false;
    }
    c.ping();
  });
}, 20000);

server.listen(PORT, () => {
  console.log(`🚀 Server running at http://localhost:${PORT}`);
});
