const express = require('express');
const http    = require('http');
const { Server } = require('socket.io');

const app    = express();
const server = http.createServer(app);
const io     = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
});

// sessionId -> { hostId, viewerId, pendingViewerId, createdAt }
const sessions = new Map();

// ===== RATE LIMITING =====
// ip -> { count, windowStart }
const joinAttempts = new Map();
const RATE_WINDOW  = 60_000; // 1 minuto
const RATE_MAX     = 8;      // máximo 8 tentativas por minuto por IP

function isRateLimited(ip) {
  const now   = Date.now();
  const entry = joinAttempts.get(ip) || { count: 0, windowStart: now };
  if (now - entry.windowStart > RATE_WINDOW) {
    entry.count       = 0;
    entry.windowStart = now;
  }
  entry.count++;
  joinAttempts.set(ip, entry);
  return entry.count > RATE_MAX;
}

// ===== SESSION EXPIRY (60 min sem viewer conectado) =====
const SESSION_TTL = 60 * 60_000;
setInterval(() => {
  const now = Date.now();
  for (const [id, s] of sessions.entries()) {
    if (!s.viewerId && now - s.createdAt > SESSION_TTL) {
      sessions.delete(id);
      io.to(s.hostId).emit('session-expired');
    }
  }
}, 60_000);

app.get('/health', (_, res) => res.json({ status: 'ok', sessions: sessions.size }));

io.on('connection', (socket) => {
  const ip = (socket.handshake.headers['x-forwarded-for'] || socket.handshake.address || '').split(',')[0].trim();

  socket.on('create-session', (sessionId) => {
    // Validação de formato
    if (typeof sessionId !== 'string' || !/^\d{6}$/.test(sessionId)) {
      socket.emit('session-error', 'ID inválido');
      return;
    }
    if (sessions.has(sessionId)) {
      socket.emit('session-error', 'ID já está em uso');
      return;
    }
    sessions.set(sessionId, { hostId: socket.id, viewerId: null, pendingViewerId: null, createdAt: Date.now() });
    socket.join(sessionId);
    socket.emit('session-created', sessionId);
  });

  socket.on('join-session', (sessionId) => {
    // Validação de formato
    if (typeof sessionId !== 'string' || !/^\d{6}$/.test(sessionId)) {
      socket.emit('session-error', 'ID inválido');
      return;
    }
    // Rate limiting
    if (isRateLimited(ip)) {
      socket.emit('session-error', 'Muitas tentativas. Aguarde 1 minuto.');
      return;
    }
    const session = sessions.get(sessionId);
    if (!session) {
      socket.emit('session-error', 'Sessão não encontrada');
      return;
    }
    if (session.viewerId) {
      socket.emit('session-error', 'Sessão já está em uso');
      return;
    }
    if (session.pendingViewerId) {
      socket.emit('session-error', 'Aguardando aprovação de outra conexão');
      return;
    }
    // Pede aprovação ao host — não conecta automaticamente
    session.pendingViewerId = socket.id;
    socket.emit('waiting-approval');
    io.to(session.hostId).emit('join-request', { viewerId: socket.id });
  });

  // Host aprova ou rejeita a conexão
  socket.on('join-response', ({ approved, viewerId }) => {
    if (typeof approved !== 'boolean' || typeof viewerId !== 'string') return;
    for (const [, session] of sessions.entries()) {
      if (session.hostId === socket.id && session.pendingViewerId === viewerId) {
        session.pendingViewerId = null;
        if (approved) {
          session.viewerId = viewerId;
          io.to(viewerId).emit('session-joined', { hostId: socket.id });
          io.to(socket.id).emit('viewer-joined', { viewerId });
        } else {
          io.to(viewerId).emit('join-denied');
        }
        return;
      }
    }
  });

  socket.on('signal', ({ targetId, signal }) => {
    if (typeof targetId !== 'string') return;
    io.to(targetId).emit('signal', { fromId: socket.id, signal });
  });

  socket.on('disconnect', () => {
    for (const [sessionId, session] of sessions.entries()) {
      if (session.hostId === socket.id) {
        sessions.delete(sessionId);
        if (session.viewerId)        io.to(session.viewerId).emit('host-disconnected');
        if (session.pendingViewerId) io.to(session.pendingViewerId).emit('host-disconnected');
      } else if (session.viewerId === socket.id) {
        session.viewerId = null;
        io.to(session.hostId).emit('viewer-disconnected');
      } else if (session.pendingViewerId === socket.id) {
        session.pendingViewerId = null;
      }
    }
  });
});

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => console.log(`Signaling server on port ${PORT}`));
