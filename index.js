const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] }
});

// sessionId -> { hostId, viewerId }
const sessions = new Map();

app.get('/health', (_, res) => res.json({ status: 'ok', sessions: sessions.size }));

io.on('connection', (socket) => {
  console.log(`[+] Connected: ${socket.id}`);

  socket.on('create-session', (sessionId) => {
    if (sessions.has(sessionId)) {
      socket.emit('session-error', 'ID já está em uso');
      return;
    }
    sessions.set(sessionId, { hostId: socket.id, viewerId: null });
    socket.join(sessionId);
    socket.emit('session-created', sessionId);
    console.log(`[S] Session created: ${sessionId} by ${socket.id}`);
  });

  socket.on('join-session', (sessionId) => {
    const session = sessions.get(sessionId);
    if (!session) {
      socket.emit('session-error', 'Sessão não encontrada');
      return;
    }
    if (session.viewerId) {
      socket.emit('session-error', 'Sessão já está em uso');
      return;
    }
    session.viewerId = socket.id;
    socket.join(sessionId);
    socket.emit('session-joined', { hostId: session.hostId });
    io.to(session.hostId).emit('viewer-joined', { viewerId: socket.id });
    console.log(`[S] Viewer ${socket.id} joined session ${sessionId}`);
  });

  // Relay WebRTC signals between peers
  socket.on('signal', ({ targetId, signal }) => {
    io.to(targetId).emit('signal', { fromId: socket.id, signal });
  });

  socket.on('disconnect', () => {
    console.log(`[-] Disconnected: ${socket.id}`);
    for (const [sessionId, session] of sessions.entries()) {
      if (session.hostId === socket.id) {
        sessions.delete(sessionId);
        if (session.viewerId) {
          io.to(session.viewerId).emit('host-disconnected');
        }
        console.log(`[S] Session ${sessionId} closed (host left)`);
      } else if (session.viewerId === socket.id) {
        session.viewerId = null;
        io.to(session.hostId).emit('viewer-disconnected');
        console.log(`[S] Viewer left session ${sessionId}`);
      }
    }
  });
});

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`Signaling server running on http://localhost:${PORT}`);
});
