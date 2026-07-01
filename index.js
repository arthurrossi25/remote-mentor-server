const express = require('express');
const http    = require('http');
const { Server } = require('socket.io');

const app    = express();
const server = http.createServer(app);
// Only the desktop app (Electron, served from file://) should reach the
// signaling server. Blocking browser origins stops any random web page from
// scripting socket.io-client to brute-force / enumerate session codes.
function isAllowedOrigin(origin) {
  // Native app requests have no Origin header (undefined) or a non-web scheme.
  if (!origin || origin === 'null') return true;
  return /^(file|app):\/\//.test(origin);
}

const io = new Server(server, {
  // allowRequest HARD-rejects the handshake (HTTP 403) for disallowed origins.
  // Unlike the `cors` option — which only sets response headers that solely a
  // browser's XHR layer honors — this also blocks WebSocket upgrades and any
  // non-browser client, which is what actually stops abuse of the signaling
  // server from a malicious web page or bot.
  allowRequest: (req, cb) => cb(null, isAllowedOrigin(req.headers.origin)),
  cors: {
    origin: (origin, cb) => cb(null, isAllowedOrigin(origin)),
    methods: ['GET', 'POST'],
  },
});

// sessionId -> { hostId, viewerId, pendingViewerId, createdAt }
const sessions = new Map();

// ===== RATE LIMITING =====
// ip -> { joinCount, createCount, windowStart }
const ipAttempts  = new Map();
const RATE_WINDOW = 60_000;
const JOIN_MAX    = 8;   // [A2] join attempts per minute per IP
const CREATE_MAX  = 5;   // [A2] create-session per minute per IP
const SIGNAL_MAX_BYTES = 65_536; // [B1] 64 KB signal payload limit

function checkRate(ip, field, max) {
  const now   = Date.now();
  const entry = ipAttempts.get(ip) || { joinCount: 0, createCount: 0, windowStart: now };
  if (now - entry.windowStart > RATE_WINDOW) {
    entry.joinCount   = 0;
    entry.createCount = 0;
    entry.windowStart = now;
  }
  entry[field]++;
  ipAttempts.set(ip, entry);
  return entry[field] > max;
}

function isJoinLimited(ip)   { return checkRate(ip, 'joinCount',   JOIN_MAX);   }
function isCreateLimited(ip) { return checkRate(ip, 'createCount', CREATE_MAX); }

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
  // Prune stale rate-limit entries so the Map can't grow unbounded (slow DoS)
  for (const [ip, entry] of ipAttempts.entries()) {
    if (now - entry.windowStart > RATE_WINDOW) ipAttempts.delete(ip);
  }
}, 60_000);

app.get('/health', (_, res) => res.json({ status: 'ok', sessions: sessions.size }));

io.on('connection', (socket) => {
  const ip = (socket.handshake.headers['x-forwarded-for'] || socket.handshake.address || '').split(',')[0].trim();

  socket.on('create-session', (sessionId) => {
    if (typeof sessionId !== 'string' || !/^\d{6}$/.test(sessionId)) {
      socket.emit('session-error', 'ID inválido');
      return;
    }
    // [A2] Rate limit create-session per IP
    if (isCreateLimited(ip)) {
      socket.emit('session-error', 'Muitas sessões criadas. Aguarde 1 minuto.');
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
    if (isJoinLimited(ip)) {
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
    // [B1] Limit signal payload size
    try {
      if (JSON.stringify(signal).length > SIGNAL_MAX_BYTES) return;
    } catch { return; }
    // [A1] Only relay between participants of the same session
    let authorized = false;
    for (const s of sessions.values()) {
      const hostToViewer = s.hostId === socket.id &&
        (s.viewerId === targetId || s.pendingViewerId === targetId);
      const viewerToHost = s.hostId === targetId &&
        (s.viewerId === socket.id || s.pendingViewerId === socket.id);
      if (hostToViewer || viewerToHost) { authorized = true; break; }
    }
    if (!authorized) return;
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
