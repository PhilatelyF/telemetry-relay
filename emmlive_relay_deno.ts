// EMMLive telemetry relay — Deno Deploy edition.
//
// Full port of server.js's feature set (recordings, batching, rate limiting,
// keepalive, secret auth, CORS, viewer count)
// relay_base_url and tracker.lua's POST target.

const kv = await Deno.openKv();

const TELEMETRY_SECRET = Deno.env.get("TELEMETRY_SECRET") ?? "";

function corsHeaders(extra: Record<string, string> = {}): HeadersInit {
	return {
		"Access-Control-Allow-Origin": "*",
		"Access-Control-Allow-Methods": "GET, POST, OPTIONS",
		"Access-Control-Allow-Headers": "Content-Type, X-Telemetry-Secret",
		"content-type": "application/json",
		...extra,
	};
}

function json(body: unknown, status = 200): Response {
	return new Response(JSON.stringify(body), { status, headers: corsHeaders() });
}

function requireSecret(req: Request): boolean {
	if (!TELEMETRY_SECRET) {
		console.error("TELEMETRY_SECRET is not set -- rejecting all writes.");
		return false;
	}
	return req.headers.get("X-Telemetry-Secret") === TELEMETRY_SECRET;
}

// --- Rate limiting (per-isolate, see caveat above) --------------------------
const RATE_LIMIT_WINDOW_MS = 1000;
const RATE_LIMIT_MAX = 60; // batched: one request per tick covers every player
let requestTimestamps: number[] = [];

function isRateLimited(): boolean {
	const now = Date.now();
	requestTimestamps = requestTimestamps.filter((t) => now - t < RATE_LIMIT_WINDOW_MS);
	requestTimestamps.push(now);
	return requestTimestamps.length > RATE_LIMIT_MAX;
}

// --- WebSocket broadcast (viewer connections) --------------------------------
const MAX_CLIENTS = 200;
const clients = new Set<WebSocket>();

function broadcast(data: unknown): void {
	const payload = JSON.stringify(data);
	for (const ws of clients) {
		if (ws.readyState === WebSocket.OPEN) {
			try {
				ws.send(payload);
			} catch (_err) {
				clients.delete(ws);
			}
		}
	}
}

// Ping/pong keepalive, same 30s cadence as server.js. Browsers/Godot's
// WebSocketPeer answer pings automatically, so this mirrors the original's
// dead-connection reaping without needing client-side changes.
setInterval(() => {
	for (const ws of clients) {
		if (ws.readyState !== WebSocket.OPEN) {
			clients.delete(ws);
			continue;
		}
		try {
			ws.send(JSON.stringify({ type: "ping" }));
		} catch (_err) {
			clients.delete(ws);
		}
	}
}, 30000);

// --- Active recording state (per-isolate, see caveat above) -----------------
type ActiveRecording = {
	id: string;
	started_at: number;
	frames: Record<string, unknown[]>;
	gamemode_counts: Record<string, number>;
};

let activeRecording: ActiveRecording | null = null;

function mostCommonGamemode(counts: Record<string, number>): string {
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

// --- Telemetry entry processing ---------------------------------------------
// Shared by /telemetry (single) and /telemetry/batch (one POST covering
// every player per tick). Validates, records into activeRecording (if one is
// running), and broadcasts to connected WS viewers. Returns true if valid.
function processTelemetryEntry(parsed: Record<string, unknown>): boolean {
	const {
		name, x, y, z, vel, vel_x, vel_y, vel_z, pitch, yaw, roll, time, gamemode, role, health,
		role_r, role_g, role_b, is_spectating, wall_hit, ground_hit, impact_strength,
		took_damage, damage_amount, was_fall_damage,
	} = parsed as Record<string, number | string | boolean | undefined>;

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
			x, y, z, yaw, pitch, roll,
			role_r: typeof role_r === "number" ? role_r : 255,
			role_g: typeof role_g === "number" ? role_g : 255,
			role_b: typeof role_b === "number" ? role_b : 255,
		});

		const gm = (gamemode as string) || "unknown";
		activeRecording.gamemode_counts[gm] = (activeRecording.gamemode_counts[gm] || 0) + 1;
	}

	broadcast({
		type: "player_update",
		player: name,
		x, y, z, vel, vel_x, vel_y, vel_z, pitch, yaw, roll, time,
		gamemode: gamemode || "unknown",
		role: role || "unknown",
		health,
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

	return true;
}

// Mirrors server.js's req.body.data pattern: the actual telemetry JSON
// arrives as a string in a "data" field, wrapped in either a JSON body
// ({"data": "..."}) or an application/x-www-form-urlencoded body
// (data=...). Returns the parsed inner value, or null if it can't be found.
async function extractDataField(req: Request): Promise<unknown> {
	const contentType = req.headers.get("content-type") ?? "";

	if (contentType.includes("application/x-www-form-urlencoded")) {
		const text = await req.text();
		const params = new URLSearchParams(text);
		const raw = params.get("data");
		return raw === null ? null : JSON.parse(raw);
	}

	// Default: JSON body with a "data" field (also covers no/unexpected
	// content-type, same permissiveness express.json() had).
	const body = await req.json();
	if (typeof body?.data !== "string") return null;
	return JSON.parse(body.data);
}

// --- Recordings (Deno KV instead of local disk) ------------------------------
type RecordingMetadata = {
	id: string;
	gamemode: string;
	player_count: number;
	player_names: string[];
	duration_seconds: number;
	saved_at: number;
};

async function saveRecording(finished: ActiveRecording): Promise<RecordingMetadata> {
	const playerNames = Object.keys(finished.frames);
	const gamemode = mostCommonGamemode(finished.gamemode_counts);

	const metadata: RecordingMetadata = {
		id: finished.id,
		gamemode,
		player_count: playerNames.length,
		player_names: playerNames,
		duration_seconds: (Date.now() - finished.started_at) / 1000,
		saved_at: Date.now(),
	};

	await kv.set(["recording_meta", finished.id], metadata);
	await kv.set(["recording_frames", finished.id], finished.frames);

	return metadata;
}

async function listRecordings(): Promise<RecordingMetadata[]> {
	const list: RecordingMetadata[] = [];
	for await (const entry of kv.list<RecordingMetadata>({ prefix: ["recording_meta"] })) {
		list.push(entry.value);
	}
	list.sort((a, b) => b.saved_at - a.saved_at);
	return list;
}

async function getRecording(id: string): Promise<{ metadata: RecordingMetadata; frames: Record<string, unknown[]> } | null> {
	const meta = await kv.get<RecordingMetadata>(["recording_meta", id]);
	const frames = await kv.get<Record<string, unknown[]>>(["recording_frames", id]);
	if (meta.value === null || frames.value === null) return null;
	return { metadata: meta.value, frames: frames.value };
}

// --- Router -------------------------------------------------------------------

Deno.serve(async (req: Request) => {
	const url = new URL(req.url);

	if (req.method === "OPTIONS") {
		return new Response(null, { status: 200, headers: corsHeaders() });
	}

	// WebSocket upgrade -- viewer connections (Godot clients)
	if (req.headers.get("upgrade") === "websocket") {
		if (clients.size >= MAX_CLIENTS) {
			return new Response("Too many connections", { status: 503 });
		}
		const { socket, response } = Deno.upgradeWebSocket(req);
		socket.onopen = () => {
			clients.add(socket);
			console.log(`Client connected. Total clients: ${clients.size}`);
		};
		socket.onclose = () => {
			clients.delete(socket);
			console.log(`Client disconnected. Total clients: ${clients.size}`);
		};
		socket.onerror = (e) => {
			console.error("Socket error:", e);
			clients.delete(socket);
		};
		return response;
	}

	// --- Recording control ---
	if (url.pathname === "/recording/start" && req.method === "POST") {
		if (!requireSecret(req)) return json({ error: "Unauthorized" }, 401);
		if (activeRecording) return json({ error: "A recording is already in progress" }, 400);

		activeRecording = {
			id: Date.now().toString(),
			started_at: Date.now(),
			frames: {},
			gamemode_counts: {},
		};
		console.log("Recording started:", activeRecording.id);
		return json({ ok: true, id: activeRecording.id });
	}

	if (url.pathname === "/recording/stop" && req.method === "POST") {
		if (!requireSecret(req)) return json({ error: "Unauthorized" }, 401);
		if (!activeRecording) return json({ error: "No recording in progress" }, 400);

		const finished = activeRecording;
		activeRecording = null;
		const metadata = await saveRecording(finished);

		console.log("Recording saved:", finished.id, metadata);
		return json({ ok: true, id: finished.id, metadata });
	}

	if (url.pathname === "/recordings" && req.method === "GET") {
		const list = await listRecordings();
		return json({ recordings: list });
	}

	const recordingMatch = url.pathname.match(/^\/recordings\/([^/]+)$/);
	if (recordingMatch && req.method === "GET") {
		const recording = await getRecording(recordingMatch[1]);
		if (recording === null) return json({ error: "Recording not found" }, 404);
		return json({ metadata: recording.metadata, frames: recording.frames });
	}

	// --- Telemetry ingestion ---
	if (url.pathname === "/telemetry" && req.method === "POST") {
		if (isRateLimited()) return json({ error: "Too many requests" }, 429);
		if (!requireSecret(req)) return json({ error: "Unauthorized" }, 401);

		let parsed: unknown;
		try {
			parsed = await extractDataField(req);
		} catch (_e) {
			return json({ error: "Invalid JSON in 'data' field" }, 400);
		}
		if (parsed === null) return json({ error: "Missing 'data' field" }, 400);

		const ok = processTelemetryEntry(parsed as Record<string, unknown>);
		if (!ok) return json({ error: "Invalid telemetry payload" }, 400);
		return json({ ok: true });
	}

	if (url.pathname === "/telemetry/batch" && req.method === "POST") {
		if (isRateLimited()) return json({ error: "Too many requests" }, 429);
		if (!requireSecret(req)) return json({ error: "Unauthorized" }, 401);

		let parsedEntries: unknown;
		try {
			parsedEntries = await extractDataField(req);
		} catch (_e) {
			return json({ error: "Invalid JSON in 'data' field" }, 400);
		}
		if (parsedEntries === null) return json({ error: "Missing 'data' field" }, 400);
		if (!Array.isArray(parsedEntries)) {
			return json({ error: "'data' must be a JSON array of telemetry entries" }, 400);
		}

		let processed = 0;
		for (const entry of parsedEntries) {
			if (processTelemetryEntry(entry as Record<string, unknown>)) processed += 1;
		}
		return json({ ok: true, processed, total: parsedEntries.length });
	}

	if (url.pathname === "/viewers" && req.method === "GET") {
		return json({ viewers: clients.size });
	}

	if (url.pathname === "/") {
		return new Response("Telemetry relay is running.", { headers: corsHeaders() });
	}

	return new Response("Not found", { status: 404, headers: corsHeaders() });
});
