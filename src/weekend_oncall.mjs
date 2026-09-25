#!/usr/bin/env node
/**
 * Posts weekend on-call engineers to Slack every Friday evening (18:00 PT).
 * Uses the PagerDuty final schedule (override-aware) and matches each engineer
 * to their region via rosters.json.
 *
 * Usage: PAGERDUTY_TOKEN=<token> SLACK_BOT_TOKEN=<token> node src/weekend_oncall.mjs
 *
 * Required env vars:
 *   PAGERDUTY_TOKEN        — PagerDuty API v2 token (read-only)
 *   SLACK_BOT_TOKEN        — Slack bot token (users:read, users:read.email, chat:write)
 *   SLACK_CHANNEL          — Slack channel to post to
 *   PD_SCHEDULE_ID         — PagerDuty schedule ID (e.g. PK0AZIN)
 *
 * Optional:
 *   WEEKEND_ONCALL_ENABLED — set to "false" to disable without removing the timer
 */

import { readFileSync } from "fs";
import { dirname, resolve } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const PD_API_BASE = "https://api.pagerduty.com";

const REGION_DISPLAY = { us: "Americas", apac: "APAC", emea: "EMEA" };
const REGION_ORDER   = ["EMEA", "Americas", "APAC"];

function loadRosters() {
  const path = resolve(__dirname, "..", "config", "rosters.json");
  return JSON.parse(readFileSync(path, "utf8"));
}

// Match a PagerDuty display name to a roster region by comparing leading
// name tokens. Roster entry "Stacie" matches "Stacie Clere-Enoka" because
// the PD name starts with the same tokens; "Rob" does NOT match "Robert Norrie".
function matchRosterRegion(pdName, rosters) {
  const pdTokens = pdName.toLowerCase().split(/\s+/);
  for (const region of ["us", "apac", "emea"]) {
    for (const name of rosters[region] ?? []) {
      const rosterTokens = name.toLowerCase().split(/\s+/);
      if (rosterTokens.every((t, i) => pdTokens[i] === t)) return region;
    }
  }
  return null;
}

async function pdGet(token, path) {
  const res = await fetch(`${PD_API_BASE}${path}`, {
    headers: {
      Authorization: `Token token=${token}`,
      Accept: "application/vnd.pagerduty+json;version=2",
    },
  });
  if (!res.ok) throw new Error(`PagerDuty GET ${path} → ${res.status}`);
  return res.json();
}

// Returns the final (override-aware) schedule entries for the given window.
async function fetchFinalScheduleEntries(token, scheduleId, since, until) {
  const params = new URLSearchParams({ since, until, time_zone: "UTC" });
  const json = await pdGet(token, `/schedules/${scheduleId}?${params}`);
  return json?.schedule?.final_schedule?.rendered_schedule_entries ?? [];
}

async function fetchUserEmail(token, userId) {
  const json = await pdGet(token, `/users/${userId}`);
  return json?.user?.email ?? null;
}

async function lookupSlackUserByEmail(slackToken, email) {
  const res = await fetch(
    `https://slack.com/api/users.lookupByEmail?email=${encodeURIComponent(email)}`,
    { headers: { Authorization: `Bearer ${slackToken}` } }
  );
  const json = await res.json();
  return json.ok ? json.user : null;
}

async function postSlackMessage(slackToken, channel, text) {
  const res = await fetch("https://slack.com/api/chat.postMessage", {
    method: "POST",
    headers: { Authorization: `Bearer ${slackToken}`, "Content-Type": "application/json" },
    body: JSON.stringify({ channel, text, unfurl_links: false }),
  });
  if (!res.ok) throw new Error(`Slack HTTP ${res.status}`);
  const json = await res.json();
  if (!json.ok) throw new Error(`Slack error: ${json.error ?? "unknown"}`);
  return json;
}

function ptMidnightUtc(year, month, day) {
  const noonUtc = new Date(Date.UTC(year, month - 1, day, 12));
  const tzStr = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Los_Angeles",
    timeZoneName: "shortOffset",
    hour: "numeric",
    hour12: false,
  })
    .formatToParts(noonUtc)
    .find((p) => p.type === "timeZoneName")?.value ?? "GMT-7";
  const offsetHours = -Number(tzStr.replace("GMT", "") || "-7");
  return new Date(Date.UTC(year, month - 1, day, offsetHours, 0, 0));
}

function getWeekendWindow() {
  const now = new Date();
  const ptDow = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Los_Angeles",
    weekday: "short",
  }).format(now);
  const dowIndex = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"].indexOf(ptDow);
  const daysToSat = ((6 - dowIndex + 7) % 7) || 7;

  const approxSat = new Date(now.getTime() + daysToSat * 86_400_000);
  const approxMon = new Date(now.getTime() + (daysToSat + 2) * 86_400_000);

  const ptDateFmt = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Los_Angeles",
    year: "numeric", month: "numeric", day: "numeric",
  });
  const ptParts = (d) =>
    Object.fromEntries(ptDateFmt.formatToParts(d).map((p) => [p.type, p.value]));

  const satPt = ptParts(approxSat);
  const sunPt = ptParts(new Date(approxSat.getTime() + 86_400_000));
  const monPt = ptParts(approxMon);

  return {
    since: ptMidnightUtc(+satPt.year, +satPt.month, +satPt.day).toISOString(),
    until: ptMidnightUtc(+monPt.year, +monPt.month, +monPt.day).toISOString(),
    label: `${satPt.month}/${satPt.day}/${satPt.year}-${sunPt.month}/${sunPt.day}/${sunPt.year}`,
  };
}

async function resolveSlackMention(pdToken, slackToken, pdUser) {
  const name  = pdUser.summary ?? pdUser.name ?? "Unknown";
  let   email = pdUser.email ?? null;

  if (!email && pdUser.id) {
    try { email = await fetchUserEmail(pdToken, pdUser.id); }
    catch (err) { console.warn(`[ONCALL] Could not fetch PD user ${pdUser.id}: ${err?.message}`); }
  }

  if (email) {
    try {
      const slackUser = await lookupSlackUserByEmail(slackToken, email);
      if (slackUser?.id) return { mention: `<@${slackUser.id}>`, name };
    } catch (err) {
      console.warn(`[ONCALL] Slack lookup failed for "${name}": ${err?.message}`);
    }
  }

  console.warn(`[ONCALL] Falling back to plain name for "${name}" (${email ?? "no email"})`);
  return { mention: name, name };
}

async function main() {
  const enabled = (process.env.WEEKEND_ONCALL_ENABLED ?? "true").trim().toLowerCase();
  if (enabled === "false") {
    console.log("[ONCALL] WEEKEND_ONCALL_ENABLED=false — skipping.");
    return;
  }

  const pdToken    = process.env.PAGERDUTY_TOKEN;
  const slackToken = process.env.SLACK_BOT_TOKEN;
  const channel    = process.env.SLACK_CHANNEL;
  const scheduleId = process.env.PD_SCHEDULE_ID;

  if (!pdToken)    throw new Error("Missing required env var: PAGERDUTY_TOKEN");
  if (!slackToken) throw new Error("Missing required env var: SLACK_BOT_TOKEN");
  if (!channel)    throw new Error("Missing required env var: SLACK_CHANNEL");
  if (!scheduleId) throw new Error("Missing required env var: PD_SCHEDULE_ID");

  const rosters = loadRosters();
  const { since, until, label } = getWeekendWindow();
  console.log(`[ONCALL] Querying schedule ${scheduleId} for ${label} (${since} – ${until})`);

  const entries = await fetchFinalScheduleEntries(pdToken, scheduleId, since, until);
  console.log(`[ONCALL] Final schedule entries: ${entries.length}`);

  // Deduplicate by user ID — each engineer appears once per region per weekend.
  const seen = new Set();
  const regionMap = {};

  for (const entry of entries) {
    const pdUser = entry.user;
    if (!pdUser?.id || seen.has(pdUser.id)) continue;
    seen.add(pdUser.id);

    const name   = pdUser.summary ?? pdUser.name ?? "";
    const region = matchRosterRegion(name, rosters);
    if (!region) {
      console.warn(`[ONCALL] "${name}" not found in rosters.json — skipping`);
      continue;
    }

    const displayRegion = REGION_DISPLAY[region] ?? region;
    const engineer      = await resolveSlackMention(pdToken, slackToken, pdUser);
    regionMap[displayRegion] = engineer;
    console.log(`[ONCALL] ${displayRegion}: ${engineer.name}`);
  }

  if (Object.keys(regionMap).length === 0) {
    throw new Error("No on-call entries matched any roster region for the weekend window.");
  }

  const orderedRegions = [
    ...REGION_ORDER.filter((r) => regionMap[r]),
    ...Object.keys(regionMap).filter((r) => !REGION_ORDER.includes(r)),
  ];

  const lines = [`*(Date: ${label})*`, "Weekend on-call duty support engineers."];
  for (const region of orderedRegions) {
    lines.push(`${region}: ${regionMap[region].mention}`);
  }

  await postSlackMessage(slackToken, channel, lines.join("\n"));
  console.log(`[ONCALL] Posted to ${channel}`);
}

main().catch(async (err) => {
  console.error("[FATAL]", err);
  const slackToken = process.env.SLACK_BOT_TOKEN;
  const channel    = process.env.SLACK_CHANNEL;
  if (slackToken && channel) {
    try {
      await fetch("https://slack.com/api/chat.postMessage", {
        method: "POST",
        headers: { Authorization: `Bearer ${slackToken}`, "Content-Type": "application/json" },
        body: JSON.stringify({
          channel,
          text: `:x: *Weekend on-call notification failed*\n\`\`\`${err?.message ?? String(err)}\`\`\``,
          unfurl_links: false,
        }),
      });
    } catch { /* best effort */ }
  }
  process.exit(1);
});
