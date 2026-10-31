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

const REGION_DISPLAY = {
  emea:           "EMEA",
  us_east:        "US East",
  us_west:        "US West",
  apac_india:     "APAC India",
  apac_australia: "APAC Australia",
};
const REGION_ORDER = ["APAC Australia", "APAC India", "EMEA", "US East", "US West"];

function loadRosters() {
  const path = resolve(__dirname, "..", "config", "rosters.json");
  return JSON.parse(readFileSync(path, "utf8"));
}

// Match a PagerDuty display name to a roster region by comparing leading
// name tokens. Roster entry "Stacie" matches "Stacie Clere-Enoka" because
// the PD name starts with the same tokens; "Rob" does NOT match "Robert Norrie".
function matchRosterRegion(pdName, rosters) {
  const pdTokens = pdName.toLowerCase().split(/\s+/);
  for (const region of ["emea", "us_west", "us_east", "apac_india", "apac_australia"]) {
    for (const name of rosters[region] ?? []) {
      const rosterTokens = name.toLowerCase().split(/\s+/);
      // Compare up to the shorter name's token count so "Mohan Kumar" matches
      // roster entry "Mohan Kumar C S" when PD omits trailing initials.
      const cmpLen = Math.min(pdTokens.length, rosterTokens.length);
      if (cmpLen > 0 && rosterTokens.slice(0, cmpLen).every((t, i) => pdTokens[i] === t)) {
        return region;
      }
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

// Returns schedule entries split into explicit overrides and the merged final schedule.
// PagerDuty's final_schedule may not include override-only windows (where the base
// schedule has no entry but an override was created). By returning both sets
// separately, callers can process overrides first so they take precedence.
async function fetchScheduleEntries(token, scheduleId, since, until) {
  const params = new URLSearchParams({ since, until, time_zone: "UTC" });
  const json = await pdGet(token, `/schedules/${scheduleId}?${params}`);
  return {
    overrideEntries: json?.schedule?.overrides_subschedule?.rendered_schedule_entries ?? [],
    finalEntries:    json?.schedule?.final_schedule?.rendered_schedule_entries ?? [],
  };
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

// Returns the UTC instant for a given PT date + hour of day (handles PST/PDT automatically).
function ptHourUtc(year, month, day, hour) {
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
  // offsetHours + hour may exceed 23 — Date.UTC handles the overflow correctly.
  return new Date(Date.UTC(year, month - 1, day, offsetHours + hour, 0, 0));
}

// Window: Friday 18:00 PT → Sunday 18:00 PT.
function getWeekendWindow() {
  const now = new Date();
  const ptDow = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Los_Angeles",
    weekday: "short",
  }).format(now);
  const dowIndex = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"].indexOf(ptDow);
  const daysToFri = (5 - dowIndex + 7) % 7; // 0 when today is Friday

  const approxFri = new Date(now.getTime() + daysToFri * 86_400_000);
  const approxSun = new Date(now.getTime() + (daysToFri + 2) * 86_400_000);

  const ptDateFmt = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Los_Angeles",
    year: "numeric", month: "numeric", day: "numeric",
  });
  const ptParts = (d) =>
    Object.fromEntries(ptDateFmt.formatToParts(d).map((p) => [p.type, p.value]));

  const friPt = ptParts(approxFri);
  const sunPt = ptParts(approxSun);

  return {
    since:    ptHourUtc(+friPt.year, +friPt.month, +friPt.day, 18).toISOString(),
    until:    ptHourUtc(+sunPt.year, +sunPt.month, +sunPt.day, 18).toISOString(),
    label:    `${friPt.month}/${friPt.day}/${friPt.year}-${sunPt.month}/${sunPt.day}/${sunPt.year}`,
    friYear:  +friPt.year,
    friMonth: +friPt.month,
    friDay:   +friPt.day,
  };
}

// Format the region's fixed shift window (defined in PT in rosters.json) in the
// region's local timezone. friYear/Month/Day is the Friday the weekend starts on;
// start_day/end_day offsets are 0=Friday, 1=Saturday, 2=Sunday.
function formatCoverageWindow(regionSlug, rosters, friYear, friMonth, friDay) {
  const tz       = rosters.region_timezones?.[regionSlug] ?? "UTC";
  const shiftKey = rosters.region_shift?.[regionSlug];
  const shift    = rosters.shift_windows_pt?.[shiftKey];
  if (!shift) return null;

  const startUtc = ptHourUtc(friYear, friMonth, friDay + shift.start_day, shift.start_hour);
  const endUtc   = ptHourUtc(friYear, friMonth, friDay + shift.end_day,   shift.end_hour);

  const fmtParts = new Intl.DateTimeFormat("en-GB", {
    timeZone: tz,
    weekday: "short",
    hour: "2-digit",
    minute: "2-digit",
    timeZoneName: "short",
    hour12: false,
  });
  const extract = (d) => {
    const parts = Object.fromEntries(fmtParts.formatToParts(d).map(p => [p.type, p.value]));
    return { day: parts.weekday, time: `${parts.hour}:${parts.minute}`, tz: parts.timeZoneName };
  };
  const s = extract(startUtc);
  const e = extract(endUtc);
  return `${s.day} ${s.time}–${e.day} ${e.time} ${s.tz}`;
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
  const { since, until, label, friYear, friMonth, friDay } = getWeekendWindow();
  console.log(`[ONCALL] Querying schedule ${scheduleId} for ${label} (${since} – ${until})`);

  const { overrideEntries, finalEntries } = await fetchScheduleEntries(pdToken, scheduleId, since, until);
  console.log(`[ONCALL] Override entries: ${overrideEntries.length}, Final schedule entries: ${finalEntries.length}`);

  const seen          = new Set();
  const regionMap     = {};
  const lockedRegions = new Set(); // regions claimed by an explicit PD override

  // --- Pass 1: explicit overrides take precedence over the base schedule ---
  for (const entry of overrideEntries) {
    const pdUser = entry.user;
    if (!pdUser?.id || seen.has(pdUser.id)) continue;
    seen.add(pdUser.id);

    const rawName = pdUser.summary ?? pdUser.name ?? "";
    const name    = rosters.pd_aliases?.[rawName] ?? rawName;
    const region  = matchRosterRegion(name, rosters);
    if (!region) {
      console.warn(`[ONCALL] "${rawName}" (override) not found in rosters.json — skipping`);
      continue;
    }

    const displayRegion = REGION_DISPLAY[region] ?? region.replace(/_/g, " ").replace(/\b\w/g, c => c.toUpperCase());
    const engineer      = await resolveSlackMention(pdToken, slackToken, pdUser);
    regionMap[displayRegion] = { ...engineer, slug: region };
    lockedRegions.add(displayRegion);
    console.log(`[ONCALL] ${displayRegion}: ${engineer.name} (override)`);
  }

  // --- Pass 2: base schedule fills regions not covered by an explicit override ---
  for (const entry of finalEntries) {
    const pdUser = entry.user;
    if (!pdUser?.id || seen.has(pdUser.id)) continue;
    seen.add(pdUser.id);

    const rawName = pdUser.summary ?? pdUser.name ?? "";
    const name    = rosters.pd_aliases?.[rawName] ?? rawName;
    const region  = matchRosterRegion(name, rosters);
    if (!region) {
      console.warn(`[ONCALL] "${rawName}" not found in rosters.json — skipping`);
      continue;
    }

    const displayRegion = REGION_DISPLAY[region] ?? region.replace(/_/g, " ").replace(/\b\w/g, c => c.toUpperCase());
    if (lockedRegions.has(displayRegion)) continue; // override already set for this region

    const engineer = await resolveSlackMention(pdToken, slackToken, pdUser);
    regionMap[displayRegion] = { ...engineer, slug: region };
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
    const { mention, slug } = regionMap[region];
    const coverage = formatCoverageWindow(slug, rosters, friYear, friMonth, friDay);
    const window = coverage ? ` (${coverage})` : "";
    lines.push(`${region}: ${mention}${window}`);
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
