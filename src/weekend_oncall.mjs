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
  params.set("since", since);
  params.set("until", until);
  params.set("limit", "25");
  const json = await pdGet(token, `/oncalls?${params}`);
  // Take the lowest escalation level (primary on-call)
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

function getWeekendWindow() {
  const now = new Date();

  // Day of week in PT (0=Sun … 6=Sat)
  const ptDow = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Los_Angeles",
    weekday: "short",
  }).format(now);
  const dowIndex = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"].indexOf(ptDow);
  const daysToSat = ((6 - dowIndex + 7) % 7) || 7;

  const satMs = now.getTime() + daysToSat * 86_400_000;
  const sat = new Date(satMs);
  const sun = new Date(satMs + 86_400_000);
  const mon = new Date(satMs + 2 * 86_400_000);

  const ptFmt = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Los_Angeles",
    month: "numeric",
    day: "numeric",
    year: "numeric",
  });

  return {
    since: sat.toISOString(),
    until: mon.toISOString(),
    label: `${ptFmt.format(sat)}-${ptFmt.format(sun)}`,
  };
}

async function resolveEngineer(pdToken, slackToken, scheduleId, since, until) {
  const pdUser = await fetchOncallUser(pdToken, scheduleId, since, until);
  if (!pdUser) return null;

  const name = pdUser.name ?? pdUser.summary ?? "Unknown";
  const email = pdUser.email;

  if (email) {
    const slackUser = await lookupSlackUserByEmail(slackToken, email);
    if (slackUser?.id) {
      return { mention: `<@${slackUser.id}>`, name };
    }
  }

  // Fallback: plain name if Slack lookup fails
  console.warn(`[ONCALL] Could not find Slack user for PD user "${name}" (${email ?? "no email"})`);
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
  const channel    = process.env.SLACK_CHANNEL || "#support-automation-test";

  if (!pdToken)    throw new Error("Missing required env var: PAGERDUTY_TOKEN");
  if (!slackToken) throw new Error("Missing required env var: SLACK_BOT_TOKEN");

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
  const channel    = process.env.SLACK_CHANNEL || "#support-automation-test";
  if (slackToken) {
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
