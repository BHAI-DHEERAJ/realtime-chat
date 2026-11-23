# Pulse Chat

A realtime chat room that runs entirely on GitHub Pages. No backend, no signup, no database.

It connects to a free public MQTT broker over WebSockets ([HiveMQ](https://www.hivemq.com/mqtt/public-mqtt-broker/), with [EMQX](https://www.emqx.com/en/public-broker) as fallback) and uses retained messages for a small in-room history.

## Live demo

Once GitHub Pages is enabled: `https://<username>.github.io/realtime-chat/`

Open the same room link in two browsers (or share the **Invite** button) to see messages arrive instantly.

## Features

- ⚡ Realtime messaging with automatic reconnect and broker failover
- 👀 Chat history loads immediately — read first, pick a name only when you're ready to type
- 👤 Name prompt appears when you focus the message box; your name is saved for next time
- 🏠 Everyone meets in one fixed room: `#lobby`
- 👥 Live online count using MQTT presence + last-will messages
- ⌨️ Typing indicators
- 🕘 Last 30 messages restored via retained topics
- 🔗 One-click invite copy
- 🎨 Dark/light theme, emoji picker, mobile-friendly layout
- 🧱 XSS-safe rendering (all user text is inserted as text, never HTML)

## Run locally

```bash
python3 -m http.server 8000
# open http://localhost:8000
```

A local server is recommended (instead of opening `index.html` directly) so clipboard and URL APIs behave normally.

## Deploy your own

1. Fork or clone this repo.
2. Go to **Settings → Pages**.
3. Set **Source** to `Deploy from a branch` → `main` / `/ (root)`.
4. Save and wait a minute for the site to publish.

No build step or secrets required.

## Rooms and privacy

Everyone at the site shares the single public `#lobby` room. Everything goes through a **public** broker — do not share passwords or personal information. Message history is retained on the broker until it is pruned or cleared from the room.

To make it private, swap `BROKERS` in `app.js` for your own authenticated MQTT broker (WSS) and add token-based auth.

## Files

| File | Purpose |
| --- | --- |
| `index.html` | Page structure |
| `styles.css` | Theme and layout |
| `app.js` | MQTT client, presence, history, UI logic |
