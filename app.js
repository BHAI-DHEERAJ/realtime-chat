/* Pulse Chat — static realtime chat using MQTT over WebSockets.
   No backend, no signup. Messages are public to anyone in the same room. */

const BROKERS = [
  "wss://broker.hivemq.com:8884/mqtt",
  "wss://broker.emqx.io:8084/mqtt",
];

const AVATAR_COLORS = [
  "#7c5cff", "#19d3c5", "#ff7a90", "#f5b942",
  "#4d8dff", "#a06aff", "#2ee6a8", "#ff8f5e",
];

const EMOJIS = [
  "😀", "😂", "🥹", "😍", "😎", "🤔", "🫡", "🥳",
  "👍", "👎", "👏", "🙌", "❤️", "🔥", "✨", "🎉",
  "🚀", "💀", "👀", "☕", "🍕", "🌍", "💯", "✅",
];

const HISTORY_LIMIT = 30;
const PRESENCE_TTL = 120000;

const $ = (id) => document.getElementById(id);
const els = {
  messages: $("messages"), welcome: $("welcome"), composer: $("composer"),
  input: $("messageInput"), send: $("sendBtn"), counter: $("counter"),
  status: $("status"), statusText: $("statusText"), online: $("onlineCount"),
  roomLabel: $("roomLabel"), footerRoom: $("footerRoom"), welcomeRoom: $("welcomeRoom"),
  typing: $("typing"), toast: $("toast"), modal: $("settingsModal"),
  nameInput: $("nameInput"), roomInput: $("roomInput"),
  emojiPanel: $("emojiPanel"),
};

let client = null;
let brokerIndex = 0;
let connectionFailures = 0;
let reconnectTimer = null;

let clientId = "";
let room = "lobby";
let userName = "Guest";
let userColor = "#7c5cff";
let topics = {};

const seenMessages = new Set();
const presence = new Map();
const typingUsers = new Map();
const historyTopics = new Map();
const pendingMessages = [];

let historySync = false;
let presenceSync = false;
let syncTimer = null;
let typingTimer = null;
let lastTypingPing = 0;
let toastTimer = null;

/* ---------- helpers ---------- */

function randomId(len = 8) {
  return Math.random().toString(36).slice(2, 2 + len);
}

function cleanRoom(value) {
  return (
    String(value || "")
      .toLowerCase()
      .trim()
      .replace(/\s+/g, "-")
      .replace(/[^a-z0-9_-]/g, "")
      .slice(0, 24) || "lobby"
  );
}

function cleanName(value) {
  return String(value || "").trim().slice(0, 20) || `Guest-${randomId(4)}`;
}

function initials(name) {
  return name.replace(/[^\p{L}\p{N} ]/gu, "").split(/\s+/).filter(Boolean)
    .slice(0, 2).map((word) => word[0]).join("").toUpperCase() || "?";
}

function colorFromName(name) {
  let hash = 0;
  for (const char of name) hash = (hash * 31 + char.charCodeAt(0)) >>> 0;
  return AVATAR_COLORS[hash % AVATAR_COLORS.length];
}

function timeString(ts) {
  return new Date(ts).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

function toast(message) {
  els.toast.textContent = message;
  els.toast.classList.add("show");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => els.toast.classList.remove("show"), 2300);
}

function setStatus(state, text) {
  els.status.className = `status ${state}`;
  els.statusText.textContent = text;
}

function makeTopics() {
  const base = `pulse-chat/v1/${room}`;
  return {
    messages: `${base}/messages`,
    history: `${base}/history/+`,
    historyRoot: `${base}/history/`,
    presence: `${base}/presence/${clientId}`,
    presenceAll: `${base}/presence/+`,
    presenceRoot: `${base}/presence/`,
    typing: `${base}/typing/${clientId}`,
    typingAll: `${base}/typing/+`,
  };
}

/* ---------- rendering ---------- */

function hideWelcome() {
  if (els.welcome) els.welcome.hidden = true;
}

function addSystem(text) {
  hideWelcome();
  const el = document.createElement("div");
  el.className = "system";
  el.textContent = text;
  els.messages.appendChild(el);
  scrollToBottom();
}

function renderMessage(msg) {
  if (!msg || !msg.id || seenMessages.has(msg.id)) return;
  seenMessages.add(msg.id);
  hideWelcome();

  const mine = msg.clientId === clientId;
  const row = document.createElement("div");
  row.className = `message${mine ? " mine" : ""}`;

  const avatar = document.createElement("div");
  avatar.className = "avatar";
  avatar.style.background = msg.color || colorFromName(msg.user || "?");
  avatar.textContent = initials(msg.user || "?");

  const bubble = document.createElement("div");
  bubble.className = "bubble";

  const meta = document.createElement("div");
  meta.className = "bubble-meta";
  const name = document.createElement("span");
  name.className = "bubble-name";
  name.textContent = mine ? "You" : msg.user || "Anonymous";
  const time = document.createElement("span");
  time.className = "bubble-time";
  time.textContent = timeString(msg.ts || Date.now());
  meta.append(name, time);

  const text = document.createElement("p");
  text.className = "bubble-text";
  text.textContent = msg.text || "";

  bubble.append(meta, text);
  row.append(avatar, bubble);
  els.messages.appendChild(row);
  scrollToBottom();
}

function scrollToBottom() {
  requestAnimationFrame(() => {
    els.messages.scrollTop = els.messages.scrollHeight;
  });
}

function renderTyping() {
  const names = [...typingUsers.entries()]
    .filter(([, ts]) => Date.now() - ts < 4000)
    .map(([name]) => name)
    .filter((name) => name !== userName);

  if (!names.length) {
    els.typing.textContent = "";
    return;
  }
  const label = names.length === 1 ? `${names[0]} is typing` : `${names.length} people are typing`;
  els.typing.textContent = "";
  const dots = document.createElement("span");
  dots.className = "typing-dots";
  dots.innerHTML = "<i></i><i></i><i></i>";
  els.typing.append(document.createTextNode(`${label} `), dots);
}

function updateOnlineCount() {
  els.online.textContent = String(presence.size);
}

function prunePresence() {
  const now = Date.now();
  for (const [topic, entry] of presence) {
    if (now - entry.ts > PRESENCE_TTL) {
      presence.delete(topic);
      safePublish(topic, "", true);
    }
  }
  updateOnlineCount();
}

/* ---------- MQTT ---------- */

function safePublish(topic, payload, retain = false) {
  if (client && client.connected) client.publish(topic, payload, { qos: 0, retain });
}

function presencePayload(offline = false) {
  return JSON.stringify({ user: userName, color: userColor, ts: Date.now(), offline, room });
}

function connect() {
  clearTimeout(reconnectTimer);
  if (client) {
    try { client.end(true); } catch (err) { /* ignore */ }
  }

  clientId = `web_${randomId(10)}`;
  topics = makeTopics();

  const will = {
    topic: topics.presence,
    payload: presencePayload(true),
    qos: 0,
    retain: true,
  };

  setStatus("", "Connecting…");
  client = mqtt.connect(BROKERS[brokerIndex], {
    clientId,
    clean: true,
    keepalive: 30,
    connectTimeout: 12000,
    reconnectPeriod: 2000,
    will,
  });

  client.on("connect", onConnect);
  client.on("reconnect", () => setStatus("", "Reconnecting…"));
  client.on("message", onMessage);
  client.on("error", (err) => {
    console.warn("[Pulse Chat] broker error:", err.message);
    connectionFailures += 1;
    if (connectionFailures >= 3) switchBroker();
  });
  client.on("close", () => {
    presence.clear();
    typingUsers.clear();
    updateOnlineCount();
    renderTyping();
    setStatus("offline", "Offline");
  });
}

function switchBroker() {
  connectionFailures = 0;
  brokerIndex = (brokerIndex + 1) % BROKERS.length;
  setStatus("", "Switching server…");
  if (client) { try { client.end(true); } catch (err) { /* ignore */ } }
  reconnectTimer = setTimeout(connect, 600);
}

function onConnect() {
  connectionFailures = 0;
  setStatus("online", "Live");

  seenMessages.clear();
  presence.clear();
  typingUsers.clear();
  historyTopics.clear();
  historySync = true;
  presenceSync = true;

  client.subscribe(
    [topics.messages, topics.history, topics.presenceAll, topics.typingAll],
    { qos: 0 },
    (err) => {
      if (err) { console.warn("[Pulse Chat] subscribe failed", err); return; }
      clearTimeout(syncTimer);
      syncTimer = setTimeout(finishSync, 1000);
    }
  );

  safePublish(topics.presence, presencePayload(), true);

  while (pendingMessages.length) {
    const text = pendingMessages.shift();
    publishMessage(text);
  }
}

function finishSync() {
  historySync = false;
  presenceSync = false;

  const entries = [...historyTopics.entries()]
    .filter(([, msg]) => msg && msg.text)
    .sort((a, b) => a[1].ts - b[1].ts);

  entries.slice(-HISTORY_LIMIT).forEach(([, msg]) => renderMessage(msg));

  // Keep only the most recent retained messages on the public broker.
  entries.slice(0, Math.max(0, entries.length - HISTORY_LIMIT)).forEach(([topic]) => {
    safePublish(topic, "", true);
    historyTopics.delete(topic);
  });

  updateOnlineCount();
  renderTyping();
  scrollToBottom();
}

function onMessage(topic, payload, packet) {
  let data;
  try { data = JSON.parse(payload.toString()); } catch (err) { return; }
  if (!data || typeof data !== "object" || data.room !== room) return;

  if (topic === topics.messages) {
    renderMessage(data);
    return;
  }

  if (topic.startsWith(topics.historyRoot)) {
    if (!data.text) { historyTopics.delete(topic); return; }
    if (seenMessages.has(data.id)) return;
    historyTopics.set(topic, data);
    if (!historySync) renderMessage(data);
    return;
  }

  if (topic.startsWith(topics.presenceRoot)) {
    if (topic === topics.presence) return;
    if (data.offline || !data.user) {
      presence.delete(topic);
    } else {
      const isNew = !presence.has(topic);
      presence.set(topic, { ...data, ts: data.ts || Date.now() });
      if (isNew && !presenceSync && data.user !== userName) {
        addSystem(`${data.user} joined the room`);
      }
    }
    updateOnlineCount();
    return;
  }

  if (topic.startsWith(`${topics.typingAll.replace(/\+$/, "")}`)) {
    if (data.user === userName) return;
    if (data.typing) typingUsers.set(data.user, Date.now());
    else typingUsers.delete(data.user);
    renderTyping();
  }
}

/* ---------- composer ---------- */

function publishMessage(text) {
  const clean = String(text || "").trim().slice(0, 500);
  if (!clean) return;

  const msg = {
    id: `${clientId}_${Date.now()}_${randomId(5)}`,
    clientId,
    user: userName,
    color: userColor,
    room,
    text: clean,
    ts: Date.now(),
  };

  renderMessage(msg);

  if (!client || !client.connected) {
    pendingMessages.push(clean);
    setStatus("offline", "Queued — reconnecting…");
    return;
  }

  client.publish(topics.messages, JSON.stringify(msg), { qos: 0 });
  client.publish(`${topics.historyRoot}${msg.ts}-${randomId(6)}`, JSON.stringify(msg), {
    qos: 0,
    retain: true,
  });
  if (historyTopics.size > HISTORY_LIMIT + 15) {
    const oldest = [...historyTopics.entries()].sort((a, b) => a[1].ts - b[1].ts)[0];
    if (oldest) {
      safePublish(oldest[0], "", true);
      historyTopics.delete(oldest[0]);
    }
  }
}

function publishTyping(typing) {
  if (client && client.connected) {
    client.publish(topics.typing, JSON.stringify({ user: userName, typing, room }), { qos: 0 });
  }
}

function stopTyping() {
  clearTimeout(typingTimer);
  publishTyping(false);
  lastTypingPing = 0;
}

/* ---------- settings ---------- */

function applyIdentity(nextName, nextRoom, announce = true) {
  const newName = cleanName(nextName);
  const newRoom = cleanRoom(nextRoom);

  if (newName === userName && newRoom === room) return;

  if (client && client.connected) {
    safePublish(topics.presence, presencePayload(true), true);
    safePublish(topics.typing, JSON.stringify({ user: userName, typing: false, room }), true);
  }

  const roomChanged = newRoom !== room;
  userName = newName;
  userColor = colorFromName(userName);
  room = newRoom;

  localStorage.setItem("pulsechat.name", userName);
  localStorage.setItem("pulsechat.room", room);

  const url = new URL(window.location.href);
  url.searchParams.set("room", room);
  window.history.replaceState({}, "", url);

  els.roomLabel.textContent = room;
  els.welcomeRoom.textContent = `#${room}`;
  els.footerRoom.textContent = room;
  document.title = `#${room} — Pulse Chat`;

  if (roomChanged) {
    els.messages.querySelectorAll(".message, .system").forEach((el) => el.remove());
    if (els.welcome) els.welcome.hidden = false;
    els.welcomeRoom.textContent = `#${room}`;
    seenMessages.clear();
    historyTopics.clear();
  }

  connect();
  if (announce) toast(roomChanged ? `Joined #${room}` : `You are now ${userName}`);
}

function clearChat() {
  for (const [topic] of historyTopics) safePublish(topic, "", true);
  historyTopics.clear();
  seenMessages.clear();
  els.messages.querySelectorAll(".message, .system").forEach((el) => el.remove());
  if (els.welcome) els.welcome.hidden = false;
  toast("Chat cleared locally and in room history");
}

/* ---------- events ---------- */

function bindEvents() {
  els.composer.addEventListener("submit", (event) => {
    event.preventDefault();
    const text = els.input.value;
    if (!text.trim()) return;
    publishMessage(text);
    els.input.value = "";
    els.input.style.height = "auto";
    els.counter.textContent = "";
    stopTyping();
    els.input.focus();
  });

  els.input.addEventListener("input", () => {
    els.input.style.height = "auto";
    els.input.style.height = `${Math.min(els.input.scrollHeight, 120)}px`;
    const length = els.input.value.length;
    els.counter.textContent = length > 400 ? `${length}/500` : "";

    const now = Date.now();
    if (els.input.value.trim() && now - lastTypingPing > 1200) {
      lastTypingPing = now;
      publishTyping(true);
    }
    clearTimeout(typingTimer);
    typingTimer = setTimeout(stopTyping, 2500);
  });

  els.input.addEventListener("blur", stopTyping);

  $("themeBtn").addEventListener("click", () => {
    const next = document.documentElement.dataset.theme === "light" ? "dark" : "light";
    document.documentElement.dataset.theme = next;
    localStorage.setItem("pulsechat.theme", next);
  });

  $("settingsBtn").addEventListener("click", () => {
    els.nameInput.value = userName;
    els.roomInput.value = room;
    els.modal.classList.remove("hidden");
    els.nameInput.focus();
  });

  $("closeSettings").addEventListener("click", () => els.modal.classList.add("hidden"));
  els.modal.addEventListener("click", (event) => {
    if (event.target === els.modal) els.modal.classList.add("hidden");
  });

  $("settingsForm").addEventListener("submit", (event) => {
    event.preventDefault();
    els.modal.classList.add("hidden");
    applyIdentity(els.nameInput.value, els.roomInput.value);
  });

  $("clearBtn").addEventListener("click", clearChat);

  $("inviteBtn").addEventListener("click", async () => {
    const url = new URL(window.location.href);
    url.searchParams.set("room", room);
    try {
      await navigator.clipboard.writeText(url.toString());
      toast("Invite link copied!");
    } catch (err) {
      toast(url.toString());
    }
  });

  $("emojiBtn").addEventListener("click", (event) => {
    event.stopPropagation();
    els.emojiPanel.hidden = !els.emojiPanel.hidden;
  });

  EMOJIS.forEach((emoji) => {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.textContent = emoji;
    btn.addEventListener("click", () => {
      els.input.value += emoji;
      els.input.dispatchEvent(new Event("input"));
      els.input.focus();
    });
    els.emojiPanel.appendChild(btn);
  });

  document.addEventListener("click", (event) => {
    if (!els.emojiPanel.hidden && !els.emojiPanel.contains(event.target) && event.target.id !== "emojiBtn") {
      els.emojiPanel.hidden = true;
    }
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      els.modal.classList.add("hidden");
      els.emojiPanel.hidden = true;
    }
  });

  window.addEventListener("beforeunload", () => {
    if (client && client.connected) {
      safePublish(topics.presence, presencePayload(true), true);
      safePublish(topics.typing, JSON.stringify({ user: userName, typing: false, room }), true);
    }
  });

  setInterval(prunePresence, 20000);
}

/* ---------- boot ---------- */

function init() {
  const params = new URLSearchParams(window.location.search);
  document.documentElement.dataset.theme = localStorage.getItem("pulsechat.theme") || "dark";

  room = cleanRoom(params.get("room") || localStorage.getItem("pulsechat.room") || "lobby");
  userName = cleanName(params.get("user") || localStorage.getItem("pulsechat.name") || `Guest-${randomId(4)}`);
  userColor = colorFromName(userName);

  els.roomLabel.textContent = room;
  els.welcomeRoom.textContent = `#${room}`;
  els.footerRoom.textContent = room;
  document.title = `#${room} — Pulse Chat`;

  const url = new URL(window.location.href);
  url.searchParams.set("room", room);
  window.history.replaceState({}, "", url);

  bindEvents();
  if (typeof mqtt === "undefined") {
    setStatus("offline", "Library blocked");
    toast("Could not load the MQTT library. Check your connection.");
    return;
  }
  connect();
  els.input.focus();
}

init();
