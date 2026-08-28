#!/usr/bin/env node
/**
 * Scans open enterprise Pylon tickets for timezone misalignment and writes
 * config/handoff_suggestions.json. Designed to run 1 hour before each
 * handoff slot (02:00, 09:00, 17:00 America/Los_Angeles).
 *
 * Usage: PYLON_TOKEN=<token> node src/refresh_suggestions.mjs
 */

import { writeFileSync } from "fs";
import { readFileSync } from "fs";
import { dirname, resolve } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const PYLON_API_BASE = "https://api.usepylon.com";

// Timezone windows (UTC decimal hours) with ±1h grace already applied.
// wraps:true means the window crosses midnight (start > end).
const TZ_WINDOWS = [
  { label: "APAC Australia", start: 21,   end: 10,   wraps: true  },
  { label: "APAC India",     start:  1.5, end: 14.5, wraps: false },
  { label: "EMEA UK",        start:  7,   end: 20,   wraps: false },
  { label: "US East",        start: 11,   end: 24,   wraps: false },
  { label: "US West",        start: 14,   end:  3,   wraps: true  },
];

// Which timezone windows belong to each roster region
const REGION_WINDOWS = {
  apac: ["APAC Australia", "APAC India"],
  emea: ["EMEA UK"],
  us:   ["US East", "US West"],
};

const OPEN_STATES = ["new", "waiting_on_customer", "waiting_on_you", "on_hold"];
const MSG_DELAY_MS = Number(process.env.PYLON_MESSAGES_DELAY_MS || 500);
const SEARCH_MIN_INTERVAL_MS = 3000;

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// Shared rate limiter: enforces minimum 3s between POST /issues/search calls
let lastSearchCallMs = 0;
async function searchRateLimit() {
  const wait = SEARCH_MIN_INTERVAL_MS - (Date.now() - lastSearchCallMs);
  if (wait > 0) await sleep(wait);
  lastSearchCallMs = Date.now();
}

function parseRetryAfterMs(header, fallbackMs = 60_000) {
  if (!header) return fallbackMs;
  const numeric = Number(header);
  if (Number.isFinite(numeric)) return numeric * 1000;
  const date = Date.parse(header);
  if (Number.isFinite(date)) return Math.max(0, date - Date.now());
  return fallbackMs;
}

async function pylonPostSearch(token, body) {
  const path = "/issues/search";
  const MAX_429_RETRIES = 3;
  for (let attempt = 0; attempt <= MAX_429_RETRIES; attempt++) {
    await searchRateLimit();
    const res = await fetch(`${PYLON_API_BASE}${path}`, {
      method: "POST",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (res.status === 429) {
      if (attempt === MAX_429_RETRIES) throw new Error(`Pylon POST ${path} → 429 after ${MAX_429_RETRIES} retries`);
      const retryAfterMs = parseRetryAfterMs(res.headers.get("Retry-After"));
      console.warn(`[RATE LIMIT] /issues/search 429, retrying after ${retryAfterMs / 1000}s (attempt ${attempt + 1}/${MAX_429_RETRIES})`);
      await sleep(retryAfterMs);
      continue;
    }
    if (!res.ok) throw new Error(`Pylon POST ${path} → ${res.status}`);
    return res.json();
  }
}

function requireEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var: ${name}`);
  return v;
}

function loadRosters() {
  const path = resolve(__dirname, "..", "config", "rosters.json");
  return JSON.parse(readFileSync(path, "utf8"));
}

async function pylonPost(token, path, body) {
  const res = await fetch(`${PYLON_API_BASE}${path}`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`Pylon POST ${path} → ${res.status}`);
  return res.json();
}

async function pylonGet(token, path) {
  const res = await fetch(`${PYLON_API_BASE}${path}`, {
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
  });
  if (!res.ok) throw new Error(`Pylon GET ${path} → ${res.status}`);
  return res.json();
}

async function fetchAllUsers(token) {
  const idToName = {};
  let cursor = null;
  while (true) {
    const path = cursor ? `/users?cursor=${encodeURIComponent(cursor)}` : "/users";
    const json = await pylonGet(token, path);
    for (const u of json?.data ?? []) {
      const display =
        (typeof u?.name === "string" && u.name.trim()) ||
        (typeof u?.email === "string" && u.email.trim()) ||
        u?.id;
      if (u?.id) idToName[u.id] = display;
    }
    if (!json?.pagination?.has_next_page || !json?.pagination?.cursor) break;
    cursor = json.pagination.cursor;
  }
  return idToName;
}

async function fetchMessages(token, issueId) {
  const MAX_RETRIES = 3;
  let lastErr;
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const json = await pylonGet(token, `/issues/${issueId}/messages`);
      return Array.isArray(json?.data) ? json.data : [];
    } catch (err) {
      lastErr = err;
      if (attempt < MAX_RETRIES) {
        console.warn(`[MESSAGES] ${issueId} attempt ${attempt} failed: ${err?.message}, retrying...`);
        await sleep(1000 * attempt);
      }
    }
  }
  console.error(`[MESSAGES] ${issueId}: all retries failed: ${lastErr?.message}`);
  throw lastErr;
}

function isEnterpriseTier(tier) {
  const t = (tier ?? "").replace(/-/g, "_").toLowerCase();
  return t === "enterprise" || t === "elite";
}

// Mirrors isCustomerAuthor from handoff_snapshot.mjs
function isCustomerMessage(msg) {
  if (msg?.is_private === true) return false;
  if (msg?.author?.contact) return true;
  if (msg?.author?.type === "contact") return true;
  return false;
}

function utcDecimalHour(isoString) {
  const d = new Date(isoString);
  return d.getUTCHours() + d.getUTCMinutes() / 60;
}

function hourInWindow(hour, { start, end, wraps }) {
  if (wraps) return hour >= start || hour <= end;
  return hour >= start && hour <= end;
}

function inferTimezone(customerMessages) {
  if (customerMessages.length === 0) return null;

  const hours = customerMessages
    .map(m => m?.timestamp || m?.created_at)
    .filter(Boolean)
    .map(utcDecimalHour);

  if (hours.length === 0) return null;

  // Retain windows where ALL customer message hours fit
  const surviving = TZ_WINDOWS.filter(w => hours.every(h => hourInWindow(h, w)));

  // 0 or 4+ surviving windows → not enough signal to infer
  if (surviving.length === 0 || surviving.length >= 4) return null;

  const confidence =
    hours.length === 1           ? "Low"    :
    surviving.length === 1       ? "High"   :
                                   "Medium";

  return { windows: surviving.map(w => w.label), confidence };
}

async function main() {
  const pylonToken = requireEnv("PYLON_TOKEN");
  const rosters = loadRosters();

  const idToName = await fetchAllUsers(pylonToken);
  console.log(`[USERS] Loaded ${Object.keys(idToName).length} users`);

  const nameToId = Object.fromEntries(Object.entries(idToName).map(([id, name]) => [name, id]));

  // Build assigneeId → roster region map across all three slots
  const assigneeToRegion = {};
  for (const region of ["apac", "emea", "us"]) {
    for (const name of rosters[region] || []) {
      const id = nameToId[name];
      if (id) assigneeToRegion[id] = region;
      else console.warn(`[ROSTER] Could not resolve "${name}" to a Pylon ID`);
    }
  }
  const rosterIds = new Set(Object.keys(assigneeToRegion));
  console.log(`[ROSTER] ${rosterIds.size} members across apac/emea/us`);

  // Collect all open enterprise tickets assigned to roster members
  const tickets = new Map();
  for (const state of OPEN_STATES) {
    let cursor = null;
    let page = 0;
    while (true) {
      page++;
      const resp = await pylonPostSearch(
        pylonToken,
        { limit: 200, ...(cursor ? { cursor } : {}), filter: { field: "state", operator: "equals", value: state } }
      );
      const data = Array.isArray(resp?.data) ? resp.data : [];

      for (const issue of data) {
        if (!issue?.id) continue;
        const assigneeId = issue?.assignee?.id;
        if (!rosterIds.has(assigneeId)) continue;
        const tierRaw = issue?.custom_fields?.support_tier?.values?.[0] ?? "unknown";
        if (!isEnterpriseTier(tierRaw)) continue;
        tickets.set(issue.id, { issue, assigneeId, region: assigneeToRegion[assigneeId] });
      }

      console.log(`[SCAN] state=${state} page=${page} fetched=${data.length} enterprise=${tickets.size}`);
      if (!resp?.pagination?.has_next_page || !resp?.pagination?.cursor) break;
      cursor = resp.pagination.cursor;
    }
  }

  console.log(`[SCAN] Total open enterprise roster tickets: ${tickets.size}`);

  // Analyze each ticket for timezone misalignment
  const suggestions = [];
  let i = 0;

  for (const { issue, assigneeId, region } of tickets.values()) {
    i++;
    if (i % 20 === 0) console.log(`[ANALYZE] ${i}/${tickets.size}...`);

    const messages = await fetchMessages(pylonToken, issue.id);
    await sleep(MSG_DELAY_MS);

    const customerMsgs = messages.filter(isCustomerMessage);
    const tz = inferTimezone(customerMsgs);
    if (!tz) continue;

    // Misaligned = all inferred customer windows fall outside the assignee's region
    const regionWindows = REGION_WINDOWS[region] || [];
    const misaligned = tz.windows.every(w => !regionWindows.includes(w));
    if (!misaligned) continue;

    suggestions.push({
      issueNumber: issue.number,
      assigneeId,
      state: issue.state ?? null,
      recommendedRegion: tz.windows.join(" or "),
      confidence: tz.confidence,
    });
  }

  console.log(`[RESULT] ${suggestions.length} misaligned tickets found`);

  // Sort: High → Medium → Low, then by issue number
  const CONF_RANK = { High: 0, Medium: 1, Low: 2 };
  suggestions.sort(
    (a, b) =>
      (CONF_RANK[a.confidence] ?? 3) - (CONF_RANK[b.confidence] ?? 3) ||
      a.issueNumber - b.issueNumber
  );

  const outPath = resolve(__dirname, "..", "config", "handoff_suggestions.json");
  writeFileSync(outPath, JSON.stringify(suggestions, null, 2) + "\n");
  console.log(`[DONE] Wrote ${suggestions.length} entries to ${outPath}`);
}

main().catch(async (err) => {
  console.error("[FATAL]", err);
  const slackToken = process.env.SLACK_BOT_TOKEN;
  const channel = process.env.SLACK_CHANNEL || "#support-automation-test";
  if (slackToken) {
    try {
      const slackRes = await fetch("https://slack.com/api/chat.postMessage", {
        method: "POST",
        headers: { Authorization: `Bearer ${slackToken}`, "Content-Type": "application/json" },
        body: JSON.stringify({
          channel,
          text: `:x: *Refresh suggestions crashed*\n\`\`\`${err?.message ?? String(err)}\`\`\``,
          unfurl_links: false,
        }),
      });
      if (!slackRes.ok) throw new Error(`HTTP ${slackRes.status}`);
      const slackJson = await slackRes.json();
      if (!slackJson.ok) throw new Error(slackJson.error ?? "unknown Slack error");
    } catch (slackErr) {
      console.error("[SLACK] Failed to post crash notification:", slackErr?.message);
    }
  }
  process.exit(1);
});
