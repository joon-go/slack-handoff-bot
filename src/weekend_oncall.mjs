#!/usr/bin/env node
/**
 * Posts weekend on-call engineers to Slack every Friday evening (18:00 PT).
 * Reads the Support Primary On-call Schedule from PagerDuty (one schedule,
 * three layers: US, APAC, EMEA) and @mentions each engineer on Slack.
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

const PD_API_BASE = "https://api.pagerduty.com";

// Maps PagerDuty layer names → display region names in the Slack message.
const LAYER_REGION_MAP = {
  US:   "Americas",
  APAC: "APAC",
  EMEA: "EMEA",
};

// Display order for the message.
const REGION_ORDER = ["EMEA", "Americas", "APAC"];

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

async function fetchScheduleLayers(token, scheduleId, since, until) {
  const params = new URLSearchParams({ since, until, time_zone: "UTC" });
  const json = await pdGet(token, `/schedules/${scheduleId}?${params}`);
  return json?.schedule?.schedule_layers ?? [];
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

// Returns the UTC timestamp corresponding to midnight America/Los_Angeles
// on the given PT calendar date, correct across DST transitions.
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
    year: "numeric",
    month: "numeric",
    day: "numeric",
  });

  function ptParts(d) {
    return Object.fromEntries(ptDateFmt.formatToParts(d).map((p) => [p.type, p.value]));
  }

  const satPt = ptParts(approxSat);
  const sunPt = ptParts(new Date(approxSat.getTime() + 86_400_000));
  const monPt = ptParts(approxMon);

  const since = ptMidnightUtc(+satPt.year, +satPt.month, +satPt.day).toISOString();
  const until = ptMidnightUtc(+monPt.year, +monPt.month, +monPt.day).toISOString();
  const label = `${satPt.month}/${satPt.day}/${satPt.year}-${sunPt.month}/${sunPt.day}/${sunPt.year}`;

  return { since, until, label };
}

async function resolveSlackMention(pdToken, slackToken, pdUser) {
  const name  = pdUser.summary ?? pdUser.name ?? "Unknown";
  let   email = pdUser.email ?? null;

  // Schedule layer entries return user references without email; fetch if needed.
  if (!email && pdUser.id) {
    try {
      email = await fetchUserEmail(pdToken, pdUser.id);
    } catch (err) {
      console.warn(`[ONCALL] Could not fetch PD user ${pdUser.id}: ${err?.message}`);
    }
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

  const { since, until, label } = getWeekendWindow();
  console.log(`[ONCALL] Querying schedule ${scheduleId} for ${label} (${since} – ${until})`);

  const layers = await fetchScheduleLayers(pdToken, scheduleId, since, until);
  console.log(`[ONCALL] Found ${layers.length} layer(s): ${layers.map(l => l.name).join(", ")}`);

  // Build region → engineer map from schedule layers
  const regionMap = {};
  for (const layer of layers) {
    const region = LAYER_REGION_MAP[layer.name] ?? layer.name;
    const entry  = layer.rendered_schedule_entries?.[0];
    if (!entry?.user) {
      console.warn(`[ONCALL] Layer "${layer.name}" has no rendered entry for this window`);
      continue;
    }
    const engineer = await resolveSlackMention(pdToken, slackToken, entry.user);
    regionMap[region] = engineer;
    console.log(`[ONCALL] ${region}: ${engineer.name}`);
  }

  if (Object.keys(regionMap).length === 0) {
    throw new Error("No on-call entries found for the weekend window.");
  }

  // Emit regions in the defined order, then any unmapped layers afterward
  const orderedRegions = [
    ...REGION_ORDER.filter(r => regionMap[r]),
    ...Object.keys(regionMap).filter(r => !REGION_ORDER.includes(r)),
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
