const express = require("express");
const http = require("http");
const fs = require("fs");
const path = require("path");
const latestPlayers = new Map();
const { WebSocketServer } = require("ws");

const app = express();

// Allow the deployed WebXR site (or any origin) to read responses from this relay.
// Without this, browser-based fetch/XHR requests from emmlive.onrender.com are
// silently blocked client-side even if the server responds successfully —
// this is why the Godot editor (not subject to CORS) could see recordings
// but the deployed web build could not.
app.use((req, res, next) => {
	res.header("Access-Control-Allow-Origin", "*");
	res.header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
	res.header("Access-Control-Allow-Headers", "Content-Type, X-Telemetry-Secret");
	if (req.method === "OPTIONS") {
		return res.sendStatus(200);
	}
	next();
});

app.use(express.urlencoded({ extended: true, limit: "10kb" }));
app.use(express.json({ limit: "1mb" }));

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const clients = new Set();

const TELEMETRY_SECRET = process.env.TELEMETRY_SECRET;

// Rate limit is per-connection-window, not truly per-player, but it's raised
// well above what a full server of players would need at the tracker's
// current tick rate. The old cap of 50/sec globally was actually LOWER than
// what ~5+ players at 10Hz would produce on their own (50+ req/sec), meaning
// legitimate telemetry was already hitting 429s under normal play — worth
// raising regardless of the Lua-side tick-rate change, since this endpoint
// already requires the shared secret and isn't exposed to public abuse.
const RATE_LIMIT_WINDOW_MS = 1000;
const RATE_LIMIT_MAX = 60; // batched now: one request per tick covers every player
let requestTimestamps = [];

function isRateLimited() {
	const now = Date.now();
	requestTimestamps = requestTimestamps.filter((t) => now - t < RATE_LIMIT_WINDOW_MS);
	requestTimestamps.push(now);
	return requestTimestamps.length > RATE_LIMIT_MAX;
}

wss.on("connection", (ws) => {
	const MAX_CLIENTS = 200;
	if (clients.size >= MAX_CLIENTS) {
		ws.close(1013, "Too many connections");
		return;
	}

	clients.add(ws);
	console.log(`Client connected. Total clients: ${clients.size}`);

	ws.on("close", () => {
		clients.delete(ws);
		console.log(`Client disconnected. Total clients: ${clients.size}`);
	});

	ws.isAlive = true;
	ws.on("pong", () => { ws.isAlive = true; });
});

setInterval(() => {
	for (const ws of clients) {
		if (ws.isAlive === false) {
			ws.terminate();
			clients.delete(ws);
			continue;
		}
		ws.isAlive = false;
		ws.ping();
	}
}, 30000);

function broadcast(data) {
	const payload = JSON.stringify(data);
	for (const ws of clients) {
		if (ws.readyState === ws.OPEN) {
			ws.send(payload);
		}
	}
}

const RECORDINGS_DIR = path.join(__dirname, "recordings");
if (!fs.existsSync(RECORDINGS_DIR)) {
	fs.mkdirSync(RECORDINGS_DIR);
}

let activeRecording = null;

function requireSecret(req, res) {
	if (!TELEMETRY_SECRET || req.get("X-Telemetry-Secret") !== TELEMETRY_SECRET) {
		res.status(401).json({ error: "Unauthorized" });
		return false;
	}
	return true;
}

function mostCommonGamemode(counts) {
	let best = "unknown";
	let bestCount = -1;
	for (const [gamemode, count] of Object.entries(counts)) {
		if (count > bestCount) {
			best = gamemode;
			bestCount = count;
		}
	}
	return best;
}

app.post("/recording/start", (req, res) => {
	if (!requireSecret(req, res)) return;

	if (activeRecording) {
		return res.status(400).json({ error: "A recording is already in progress" });
	}

	activeRecording = {
		id: Date.now().toString(),
		started_at: Date.now(),
		frames: {},
		gamemode_counts: {},
	};

	console.log("Recording started:", activeRecording.id);
	res.status(200).json({ ok: true, id: activeRecording.id });
});

app.post("/recording/stop", (req, res) => {
	if (!requireSecret(req, res)) return;

	if (!activeRecording) {
		return res.status(400).json({ error: "No recording in progress" });
	}

	const finished = activeRecording;
	activeRecording = null;

	const playerNames = Object.keys(finished.frames);
	const gamemode = mostCommonGamemode(finished.gamemode_counts);

	const metadata = {
		id: finished.id,
		gamemode: gamemode,
		player_count: playerNames.length,
		player_names: playerNames,
		duration_seconds: (Date.now() - finished.started_at) / 1000,
		saved_at: Date.now(),
	};

	const output = {
		metadata: metadata,
		frames: finished.frames,
	};

	const filePath = path.join(RECORDINGS_DIR, `${finished.id}.json`);
	fs.writeFileSync(filePath, JSON.stringify(output));

	console.log("Recording saved:", finished.id, metadata);
	res.status(200).json({ ok: true, id: finished.id, metadata: metadata });
});

app.get("/recordings", (req, res) => {
	const files = fs.readdirSync(RECORDINGS_DIR).filter((f) => f.endsWith(".json"));

	const list = files.map((f) => {
		const raw = fs.readFileSync(path.join(RECORDINGS_DIR, f), "utf8");
		const parsed = JSON.parse(raw);
		return parsed.metadata;
	});

	list.sort((a, b) => b.saved_at - a.saved_at);

	res.status(200).json({ recordings: list });
});

app.get("/recordings/:id", (req, res) => {
	const filePath = path.join(RECORDINGS_DIR, `${req.params.id}.json`);

	if (!fs.existsSync(filePath)) {
		return res.status(404).json({ error: "Recording not found" });
	}

	const data = fs.readFileSync(filePath, "utf8");
	res.status(200).send(data);
});

// Shared processing for a single player's telemetry entry: validates,
// records (if active), and broadcasts. Used by both /telemetry (single,
// kept for compatibility) and /telemetry/batch (one POST covering every
// player on a tick, instead of one POST per player per tick).
// Returns true if the entry was valid and processed, false otherwise.
function processTelemetryEntry(parsed) {
	const { name, x, y, z, vel, vel_x, vel_y, vel_z, pitch, yaw, roll, time, gamemode, role, health,
	        role_r, role_g, role_b, is_spectating, wall_hit, ground_hit, impact_strength,
	        took_damage, damage_amount, was_fall_damage } = parsed;

	if (
		typeof name !== "string" || name.length === 0 || name.length > 64 ||
		[x, y, z, yaw, time].some((v) => typeof v !== "number" || !Number.isFinite(v))
	) {
		return false;
	}

	if (activeRecording && !is_spectating) {
		if (!activeRecording.frames[name]) {
			activeRecording.frames[name] = [];
		}
		const relative_time = (Date.now() - activeRecording.started_at) / 1000;
		activeRecording.frames[name].push({
			time: relative_time,
			x, y, z, yaw,
			role_r: typeof role_r === "number" ? role_r : 255,
			role_g: typeof role_g === "number" ? role_g : 255,
			role_b: typeof role_b === "number" ? role_b : 255,
		});

		const gm = gamemode || "unknown";
		activeRecording.gamemode_counts[gm] = (activeRecording.gamemode_counts[gm] || 0) + 1;
	}

	broadcast({
		type: "player_update",
		player: name,
		x, y, z, vel, vel_x, vel_y, vel_z, pitch, yaw, roll, time,
		gamemode: gamemode || "unknown",
		role: role || "unknown",
		health: health,
		role_r: typeof role_r === "number" ? role_r : 255,
		role_g: typeof role_g === "number" ? role_g : 255,
		role_b: typeof role_b === "number" ? role_b : 255,
		is_spectating: !!is_spectating,
		wall_hit: !!wall_hit,
		ground_hit: !!ground_hit,
		impact_strength: impact_strength || 0,
		took_damage: !!took_damage,
		damage_amount: damage_amount || 0,
		was_fall_damage: !!was_fall_damage,
	});

	latestPlayers.set(parsed.name, {
	  name: parsed.name,
	  x,
	  y,
	  z,
	  velocity: {
	    x: typeof vel_x === "number" ? vel_x : 0,
	    y: typeof vel_y === "number" ? vel_y : 0,
	    z: typeof vel_z === "number" ? vel_z : 0,
	  },
	});
	return true;
}
app.post("/recordings/upload", express.json({ limit: "50mb" }), (req, res) => {
  if (!requireSecret(req, res)) return;

  const recording = req.body;

  if (!recording?.metadata?.id || !recording?.frames) {
    return res.status(400).json({ error: "Invalid recording format" });
  }

  const filePath = path.join(RECORDINGS_DIR, `${recording.metadata.id}.json`);
  fs.writeFileSync(filePath, JSON.stringify(recording));

  res.status(200).json({ ok: true, id: recording.metadata.id });
});

app.post("/telemetry", (req, res) => {
	if (isRateLimited()) {
		return res.status(429).json({ error: "Too many requests" });
	}

	if (!TELEMETRY_SECRET || req.get("X-Telemetry-Secret") !== TELEMETRY_SECRET) {
		return res.status(401).json({ error: "Unauthorized" });
	}

	if (!req.body.data) {
		return res.status(400).json({ error: "Missing 'data' field" });
	}

	let parsed;
	try {
		parsed = JSON.parse(req.body.data);
	} catch (e) {
		return res.status(400).json({ error: "Invalid JSON in 'data' field" });
	}

	const ok = processTelemetryEntry(parsed);
	if (!ok) {
		return res.status(400).json({ error: "Invalid telemetry payload" });
	}

	res.status(200).json({ ok: true });
});

// One request per server tick covering EVERY player, instead of one request
// per player per tick. This is the actual fix for tracker-induced lag: the
// per-player http.Post approach meant player count multiplied request count
// directly, forcing a tradeoff between sample rate (smoothness) and request
// volume (lag). Batching removes that tradeoff -- request count now depends
// only on tick rate, not player count, so sample rate can go back up without
// the lag returning.
app.post("/telemetry/batch", (req, res) => {
	if (isRateLimited()) {
		return res.status(429).json({ error: "Too many requests" });
	}

	if (!TELEMETRY_SECRET || req.get("X-Telemetry-Secret") !== TELEMETRY_SECRET) {
		return res.status(401).json({ error: "Unauthorized" });
	}

	if (!req.body.data) {
		return res.status(400).json({ error: "Missing 'data' field" });
	}

	let parsedEntries;
	try {
		parsedEntries = JSON.parse(req.body.data);
	} catch (e) {
		return res.status(400).json({ error: "Invalid JSON in 'data' field" });
	}

	if (!Array.isArray(parsedEntries)) {
		return res.status(400).json({ error: "'data' must be a JSON array of telemetry entries" });
	}

	let processed = 0;
	for (const entry of parsedEntries) {
		if (processTelemetryEntry(entry)) {
			processed += 1;
		}
	}

	res.status(200).json({ ok: true, processed: processed, total: parsedEntries.length });
});

app.get("/viewers", (req, res) => {
	res.status(200).json({ viewers: clients.size });
});

app.get("/", (req, res) => {
	res.send("Telemetry relay is running.");
});
app.get("/data", (req, res) => {
  res.status(200).json({
    players: Array.from(latestPlayers.values()),
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
	console.log(`Relay listening on port ${PORT}`);
});
