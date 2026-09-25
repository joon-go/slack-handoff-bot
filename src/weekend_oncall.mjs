#!/usr/bin/env node
/**
 * Posts weekend on-call engineers to Slack every Friday evening (18:00 PT).
 * Fetches on-call schedules from PagerDuty and @mentions engineers on Slack.
 *
 * Usage: PAGERDUTY_TOKEN=<token> SLACK_BOT_TOKEN=<token> node src/weekend_oncall.mjs
 *
 * Required env vars:
 *   PAGERDUTY_TOKEN         — PagerDuty API v2 token
 *   SLACK_BOT_TOKEN         — Slack bot token (needs users:read, users:read.email, chat:write)
 *   SLACK_CHANNEL           — Slack channel to post to
 *
 * Per-region schedule IDs (at least one required):
 *   PD_SCHEDULE_ID_EMEA     — PagerDuty schedule ID for EMEA on-call
 *   PD_SCHEDULE_ID_AMERICAS — PagerDuty schedule ID for Americas on-call
 *   PD_SCHEDULE_ID_APAC     — PagerDuty schedule ID for APAC on-call
 *
 * Optional:
 *   WEEKEND_ONCALL_ENABLED  — set to "false" to disable without removing the timer
 */

const PD_API_BASE = "https://api.pagerduty.com";

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

async function fetchOncallUser(token, scheduleId, since, until) {
  const params = new URLSearchParams();
  params.append("schedule_ids[]", scheduleId);
  params.append("include[]", "users"); // expand user objects to include email
  params.set("since", since);
  params.set("until", until);
  params.set("limit", "25");
  const json = await pdGet(token, `/oncalls?${params}`);
  // Take the primary on-call (lowest escalation level)
  const sorted = (json?.oncalls ?? []).sort(
    (a, b) => (a.escalation_level ?? 99) - (b.escalation_level ?? 99)
  );
  return sorted[0]?.user ?? null;
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
  // Sample the PT UTC offset at noon on that day (well away from any DST boundary).
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

  // Day of week in PT
  const ptDow = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Los_Angeles",
    weekday: "short",
  }).format(now);
  const dowIndex = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"].indexOf(ptDow);
  const daysToSat = ((6 - dowIndex + 7) % 7) || 7;

  // Approximate next Saturday and Monday by adding days in ms, then read back
  // the PT calendar date to handle DST correctly.
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

  const satPt  = ptParts(approxSat);
  const sunPt  = ptParts(new Date(approxSat.getTime() + 86_400_000));
  const monPt  = ptParts(approxMon);

  const since = ptMidnightUtc(+satPt.year, +satPt.month, +satPt.day).toISOString();
  const until = ptMidnightUtc(+monPt.year, +monPt.month, +monPt.day).toISOString();
  const label = `${satPt.month}/${satPt.day}/${satPt.year}-${sunPt.month}/${sunPt.day}/${sunPt.year}`;

  return { since, until, label };
}

async function resolveEngineer(pdToken, slackToken, scheduleId, since, until) {
  const pdUser = await fetchOncallUser(pdToken, scheduleId, since, until);
  if (!pdUser) return null;

  const name  = pdUser.name ?? pdUser.summary ?? "Unknown";
  const email = pdUser.email;

  if (email) {
    try {
      const slackUser = await lookupSlackUserByEmail(slackToken, email);
      if (slackUser?.id) {
        return { mention: `<@${slackUser.id}>`, name };
      }
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

  if (!pdToken)    throw new Error("Missing required env var: PAGERDUTY_TOKEN");
  if (!slackToken) throw new Error("Missing required env var: SLACK_BOT_TOKEN");
  if (!channel)    throw new Error("Missing required env var: SLACK_CHANNEL");

  const scheduleIds = {
    EMEA:     process.env.PD_SCHEDULE_ID_EMEA,
    Americas: process.env.PD_SCHEDULE_ID_AMERICAS,
    APAC:     process.env.PD_SCHEDULE_ID_APAC,
  };

  const configuredRegions = Object.entries(scheduleIds).filter(([, id]) => !!id);
  if (configuredRegions.length === 0) {
    throw new Error(
      "No PagerDuty schedule IDs configured. Set PD_SCHEDULE_ID_EMEA, PD_SCHEDULE_ID_AMERICAS, and/or PD_SCHEDULE_ID_APAC."
    );
  }

  const { since, until, label } = getWeekendWindow();
  console.log(`[ONCALL] Querying on-call for ${label} (${since} – ${until})`);

  const lines = [`*(Date: ${label})*`, "Weekend on-call duty support engineers."];

  for (const [region, scheduleId] of configuredRegions) {
    const engineer = await resolveEngineer(pdToken, slackToken, scheduleId, since, until);
    if (engineer) {
      lines.push(`${region}: ${engineer.mention}`);
      console.log(`[ONCALL] ${region}: ${engineer.name}`);
    } else {
      lines.push(`${region}: _No on-call found_`);
      console.warn(`[ONCALL] ${region}: no on-call user found for schedule ${scheduleId}`);
    }
  }

  const text = lines.join("\n");
  await postSlackMessage(slackToken, channel, text);
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
