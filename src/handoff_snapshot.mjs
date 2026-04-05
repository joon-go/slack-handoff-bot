/**
 * handoff_snapshot.mjs (FULL REGEN)
 *
 * Cron (Pacific Time):
 *   CRON_TZ=America/Los_Angeles
 *   0 3  * * * node /app/handoff_snapshot.mjs apac
 *   0 10 * * * node /app/handoff_snapshot.mjs emea
 *   0 18 * * * node /app/handoff_snapshot.mjs us
 *
 * Usage:
 *   node handoff_snapshot.mjs apac|emea|us
 *
 * Env:
 *   PYLON_TOKEN
 *   SLACK_BOT_TOKEN
 *
 * Optional Env:
 *   SLACK_CHANNEL=#csorg-support-handoff  # override Slack channel (default: #support-automation-test)
 *   SCAN_B_LOOKBACK_DAYS=30               # override the SCAN-B lookback window (default 30)
 *   PYLON_MESSAGES_CONCURRENCY=1          # message API fetch concurrency (default 1, keep low)
 *   PYLON_MESSAGES_DELAY_MS=200           # delay between message API calls (default 200ms)
 *
 * Config files:
 *   config/rosters.json  # shift rosters per region (edit without code changes)
 *
 * Notes:
 * - Uses Node's built-in fetch (Node 18+). No node-fetch dependency.
 * - Until you explicitly say ready for prod, posts to: #support-automation-test
 * - Team filter is enforced locally: only issues with team.id === L1+L2 are counted.
 * - Shift window (Pacific Time):
 *     US:   09:00 -> 18:00
 *     EMEA: 01:00 -> 10:00
 *     APAC: 18:00 -> 03:00 (cross-midnight)
 * - "New tickets during <REGION>" counts issues created in that window (open + closed), team L1+L2 only.
 * - Under New tickets line, prints assignment breakdown for the shift roster:
 *     Assigned (new tickets): Name: N | Name: N | ...
 * - FR SLA Pending buckets are state === "new" (team L1+L2 only):
 *     P0/P1 => priority in [urgent, high]
 *     P2/P3 => priority in [medium, low]
 * - Under FR SLA Pending P0/P1 count line, prints issue line items:
 *     <#1234 link> | Assignee: Name | Subject: Title
 * - Handoff issues = OPEN state AND L1+L2 AND hand_off_region.value is set (single-select).
 * - Handoff issues lines:
 *     <#1234 link> | Assignee: Name | Handoff Region: EMEA/APAC/America | Handoff meeting required: Yes/No
 */

import { DateTime } from "luxon";
import { readFileSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

/** ----------------------------
 *  CONFIG
 *  ---------------------------- */

const PYLON_API_BASE = "https://api.usepylon.com";

// Slack channel: override via env for prod; defaults to test channel for safety
const SLACK_CHANNEL = process.env.SLACK_CHANNEL || "#support-automation-test";

// Team "L1+L2" (enforced locally)
const TEAM_ID_L1_L2 = "0363526b-d360-424a-9306-869bf7c2be4f";

// Custom field slugs
const CF_HANDOFF_REGION = "hand_off_region"; // single-select
const CF_MEETING_REQUIRED = "handoff_call_required";

// Open states (Pylon payload uses `state`)
const OPEN_STATES = new Set(["new", "waiting_on_you", "waiting_on_customer", "on_hold"]);

// Priority buckets (Pylon priority values)
const P0_P1_PRIORITIES = new Set(["urgent", "high"]);
const P2_P3_PRIORITIES = new Set(["medium", "low"]);

// FRT SLA matrix: tier slug -> [P0, P1, P2, P3] in seconds.
// Business hours = M-F 09:00-17:00 PT unless SLA_IS_CALENDAR marks the cell true.
// 1 biz day = 8 biz hrs.  "best effort" tiers use calendar hours with relaxed thresholds.
// Pylon slug -> SLA table column:
//   enterprise_elite = Enterprise Elite
//   enterprise       = Enterprise Standard
//   ultimate         = Pro Plus/Ultimate
//   pro              = Pro                        (confirmed from live data)
//   lite_legacy      = Lite (Legacy) & Pro (Legacy)
//   community        = Free / Open Source / Community
//   unknown          = Unknown                    (confirmed from live data)
const SLA_SECONDS = {
  //                   P0             P1              P2               P3
  lite_legacy:        [8  * 3600,     24 * 3600,      3 * 8 * 3600,    7 * 8 * 3600 ], // 9-5 biz hrs
  pro:                [4  * 3600,     12 * 3600,      2 * 8 * 3600,    5 * 8 * 3600 ], // 9-5 biz hrs
  ultimate:           [2  * 3600,     4  * 3600,      24 * 3600,       3 * 24 * 3600], // 24x5 weekday hrs
  enterprise:         [2  * 3600,     4  * 3600,      24 * 3600,       3 * 24 * 3600], // 24x7 calendar hrs
  enterprise_elite:   [1  * 3600,     4  * 3600,      8  * 3600,       24 * 3600    ], // 24x7 calendar hrs
  community:          [24 * 3600,     24 * 3600,      72 * 3600,       72 * 3600    ], // best effort: calendar
  unknown:            [24 * 3600,     24 * 3600,      72 * 3600,       72 * 3600    ], // best effort: calendar
};

// Coverage mode per tier × priority cell.
// "biz"      = M-F 09:00-17:00 PT (8 h/day) — Lite Legacy, Pro
// "weekday"  = M-F 00:00-24:00 PT (24 h/day) — Ultimate (24x5)
// "calendar" = all hours, all days (24x7) — Enterprise tiers, best-effort tiers
const SLA_COVERAGE = {
  lite_legacy:        ["biz",      "biz",      "biz",      "biz"     ],
  pro:                ["biz",      "biz",      "biz",      "biz"     ],
  ultimate:           ["weekday",  "weekday",  "weekday",  "weekday" ],
  enterprise:         ["calendar", "calendar", "calendar", "calendar"],
  enterprise_elite:   ["calendar", "calendar", "calendar", "calendar"],
  community:          ["biz",      "biz",      "biz",      "biz"     ],
  unknown:            ["biz",      "biz",      "biz",      "biz"     ],
};

// Index mapping for SLA arrays
const PRIORITY_IDX = { urgent: 0, high: 1, medium: 2, low: 3 };

// Header labels (handoff-to sequence)
const SLOT_CONFIG = {
  apac: { headerLabel: "APAC to EMEA" },
  emea: { headerLabel: "EMEA to US" },
  us: { headerLabel: "US to APAC" },
};

// Shift rosters for assignment breakdown (names must match Pylon user names).
// Loaded from config/rosters.json if available; falls back to hardcoded defaults.
const DEFAULT_ROSTERS = {
  emea: ["Dylan Bonar", "Tommy Lundy", "Robert Norrie", "Bryan Nalty"],
  apac: ["Saurabh Lambe", "Chinmay Koratkar"],
  us: ["Feran Morgan", "Tassia Shibuya", "Fariha Marzan", "Tim Perry"],
};

function loadRosters() {
  try {
    const __dirname = dirname(fileURLToPath(import.meta.url));
    const configPath = resolve(__dirname, "..", "config", "rosters.json");
    const raw = readFileSync(configPath, "utf8");
    const parsed = JSON.parse(raw);
    console.log(`[CONFIG] Loaded rosters from ${configPath}`);
    return {
      emea: Array.isArray(parsed.emea) ? parsed.emea : DEFAULT_ROSTERS.emea,
      apac: Array.isArray(parsed.apac) ? parsed.apac : DEFAULT_ROSTERS.apac,
      us: Array.isArray(parsed.us) ? parsed.us : DEFAULT_ROSTERS.us,
    };
  } catch (err) {
    console.warn(`[CONFIG] Could not load config/rosters.json, using defaults: ${err?.message || err}`);
    return DEFAULT_ROSTERS;
  }
}

const REGION_ROSTERS = loadRosters();

// Saved views (Slack hyperlinks)
const SLACK_LINKS = {
  handoffIssues: "https://app.usepylon.com/issues/views/e799d418-120d-4849-bf81-37d5afdba15c",
  frSlaPendingP0P1: "https://app.usepylon.com/issues/views/039f2559-c7ca-4550-929e-31dc881351d3",
  frSlaPendingP2P3: "https://app.usepylon.com/issues/views/70565321-570f-4dfe-856a-c767e4042511",
  discordCommunityOpen: "https://app.usepylon.com/issues/views/f22611f4-c563-483a-bc36-b7d7d7014df6",
};

// Pylon issue permalink
function pylonIssueUrl(conversationId) {
  return `https://app.usepylon.com/issues?conversationID=${conversationId}`;
}

/** ----------------------------
 *  HELPERS
 *  ---------------------------- */

function requireEnv(varName) {
  const v = process.env[varName];
  if (!v) throw new Error(`Missing required env var: ${varName}`);
  return v;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function ptNow() {
  return DateTime.now().setZone("America/Los_Angeles").set({ millisecond: 0 });
}

function formatDatePt(dtPt) {
  return dtPt.toFormat("MM/dd/yyyy");
}

function parseUtcIso(iso) {
  return iso ? DateTime.fromISO(iso, { zone: "utc" }) : null;
}

/**
 * Count elapsed business-hours seconds between createdAtIso and nowDt.
 * Business hours: M-F 09:00-17:00 America/Los_Angeles.
 * Iterates day-by-day; handles tickets created outside business hours.
 */
function businessHoursElapsedSeconds(createdAtIso, nowDt) {
  let dt = DateTime.fromISO(createdAtIso, { zone: "America/Los_Angeles" });
  const end = nowDt.setZone("America/Los_Angeles");
  let elapsed = 0;
  while (dt < end) {
    if (dt.weekday <= 5) { // 1=Mon..5=Fri
      const dayStart = dt.set({ hour: 9,  minute: 0, second: 0, millisecond: 0 });
      const dayEnd   = dt.set({ hour: 17, minute: 0, second: 0, millisecond: 0 });
      const windowStart = dt < dayStart ? dayStart : dt;
      const windowEnd   = end < dayEnd  ? end       : dayEnd;
      if (windowStart < windowEnd) {
        elapsed += windowEnd.diff(windowStart, "seconds").seconds;
      }
    }
    dt = dt.plus({ days: 1 }).set({ hour: 9, minute: 0, second: 0, millisecond: 0 });
  }
  return elapsed;
}

/**
 * Count elapsed weekday seconds between createdAtIso and nowDt.
 * Weekday hours: M-F 00:00-24:00 America/Los_Angeles (24 h/day, no weekend).
 * Used for Ultimate (24x5) coverage.
 */
function weekdayHoursElapsedSeconds(createdAtIso, nowDt) {
  let dt = DateTime.fromISO(createdAtIso, { zone: "America/Los_Angeles" });
  const end = nowDt.setZone("America/Los_Angeles");
  let elapsed = 0;
  while (dt < end) {
    if (dt.weekday <= 5) { // 1=Mon..5=Fri
      const eod = dt.set({ hour: 0, minute: 0, second: 0, millisecond: 0 }).plus({ days: 1 });
      const windowEnd = end < eod ? end : eod;
      elapsed += windowEnd.diff(dt, "seconds").seconds;
    }
    dt = dt.set({ hour: 0, minute: 0, second: 0, millisecond: 0 }).plus({ days: 1 });
  }
  return elapsed;
}

/**
 * Dispatch elapsed-time calculation by SLA coverage mode.
 * "biz"      → M-F 09:00-17:00 PT
 * "weekday"  → M-F 00:00-24:00 PT
 * "calendar" → all hours, all days
 */
function elapsedSeconds(createdAtIso, nowDt, coverage) {
  if (coverage === "calendar") {
    return nowDt.diff(DateTime.fromISO(createdAtIso), "seconds").seconds;
  }
  if (coverage === "weekday") {
    return weekdayHoursElapsedSeconds(createdAtIso, nowDt);
  }
  return businessHoursElapsedSeconds(createdAtIso, nowDt);
}

// Presentation only; not Pylon priority
function statusEmoji({ count, alertLevel = "normal" }) {
  if (count === 0) return "✅";
  if (alertLevel === "critical") return "🚨";
  return "⚠️";
}

function getPriority(issue) {
  return issue?.custom_fields?.priority?.value ?? null;
}

// Map Pylon priority -> internal P0-P3 display
function mapPriorityLabel(pylonPriority) {
  switch (pylonPriority) {
    case "urgent":
      return "P0";
    case "high":
      return "P1";
    case "medium":
      return "P2";
    case "low":
      return "P3";
    default:
      return "P?";
  }
}

// For sorting: P0 first -> P3 last -> unknown
function priorityRank(pLabel) {
  switch (pLabel) {
    case "P0":
      return 0;
    case "P1":
      return 1;
    case "P2":
      return 2;
    case "P3":
      return 3;
    default:
      return 99;
  }
}

function isTeamL1L2(issue) {
  return issue?.team?.id === TEAM_ID_L1_L2;
}

function isDiscordIssue(issue) {
  return issue?.source === "discord" || (Array.isArray(issue?.tags) && issue.tags.includes("discord-bot"));
}

function isOpenState(issue) {
  return OPEN_STATES.has(issue?.state);
}

function getHandoffRegionValue(issue) {
  const v = issue?.custom_fields?.[CF_HANDOFF_REGION]?.value;
  return typeof v === "string" && v.trim() !== "" ? v.trim() : null;
}

function isMeetingRequired(issue) {
  const v = issue?.custom_fields?.[CF_MEETING_REQUIRED]?.value;
  return v === true || v === "true";
}

/**
 * Open handoff issue predicate:
 * - open state
 * - team == L1+L2
 * - handoff region value set
 */
function isOpenHandoffIssue(issue) {
  if (!isOpenState(issue)) return false;
  if (!isTeamL1L2(issue)) return false;
  return getHandoffRegionValue(issue) != null;
}

/**
 * Convert Pylon handoff slugs -> label
 *   america_apac -> APAC
 *   apac_emea    -> EMEA
 *   emea_america -> America
 */
function handoffLabelFromSlug(slug) {
  switch (slug) {
    case "america_apac":
      return "APAC";
    case "apac_emea":
      return "EMEA";
    case "emea_america":
      return "America";
    default:
      return slug ?? "Unknown";
  }
}

/**
 * Shift window (Pacific Time):
 *   US:   09:00 -> 18:00
 *   EMEA: 01:00 -> 10:00
 *   APAC: 18:00 -> 03:00 (cross-midnight)
 */
function getCreatedWindowForSlot(slot, nowPt) {
  const day = nowPt.startOf("day");

  const t01 = day.set({ hour: 1 });
  const t03 = day.set({ hour: 3 });
  const t09 = day.set({ hour: 9 });
  const t10 = day.set({ hour: 10 });
  const t18 = day.set({ hour: 18 });

  if (slot === "us") {
    const startPt = t09;
    const endPt = t18;
    return { startPt, endPt, startUtc: startPt.toUTC(), endUtc: endPt.toUTC() };
  }

  if (slot === "emea") {
    const startPt = t01;
    const endPt = t10;
    return { startPt, endPt, startUtc: startPt.toUTC(), endUtc: endPt.toUTC() };
  }

  // apac: 18:00 prev day -> 03:00 today (cron at 3 AM)
  // If running manually during the live APAC shift (hour >= 18), anchor forward instead:
  //   live shift:      today 18:00 -> tomorrow 03:00
  //   completed shift: yesterday 18:00 -> today 03:00
  {
    const inLiveShift = nowPt.hour >= 18;
    const startPt = inLiveShift ? t18 : t18.minus({ days: 1 });
    const endPt = inLiveShift ? t03.plus({ days: 1 }) : t03;
    return { startPt, endPt, startUtc: startPt.toUTC(), endUtc: endPt.toUTC() };
  }
}

function regionLabelFromSlot(slot) {
  if (slot === "us") return "US";
  if (slot === "emea") return "EMEA";
  return "APAC";
}

/** ----------------------------
 *  PYLON API
 *  ---------------------------- */

async function pylonSearch({ token, limit = 200, cursor = null, filter = null }) {
  const path = "/issues/search";
  const body = { limit };
  if (cursor) body.cursor = cursor;
  if (filter) body.filter = filter;

  const maxAttempts = 8;
  let attempt = 0;

  while (true) {
    attempt += 1;

    const res = await fetch(`${PYLON_API_BASE}${path}`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
        Accept: "application/json",
      },
      body: JSON.stringify(body),
    });

    if (res.status === 429) {
      const retryAfterHeader = res.headers.get("retry-after");
      const retryAfterMs = retryAfterHeader
        ? Number(retryAfterHeader) * 1000
        : Math.min(30000, 750 * 2 ** (attempt - 1));

      if (attempt >= maxAttempts) {
        const text = await res.text();
        throw new Error(`Pylon rate limit (429) after ${attempt} attempts: ${text}`);
      }

      await sleep(retryAfterMs);
      continue;
    }

    const text = await res.text();
    let json;
    try {
      json = JSON.parse(text);
    } catch {
      throw new Error(`Pylon returned non-JSON (${res.status}): ${text}`);
    }

    if (!res.ok)
      throw new Error(`Pylon /issues/search failed (${res.status}): ${JSON.stringify(json)}`);
    if (json.errors?.length) throw new Error(`Pylon error: ${JSON.stringify(json)}`);

    return json;
  }
}

/**
 * Determine if a message author is a customer.
 * Pylon is inconsistent — some messages have author.contact populated,
 * others use author.type === "contact".  Check both to be safe.
 */
function isCustomerAuthor(msg) {
  if (msg?.author?.contact) return true;
  if (msg?.author?.type === "contact") return true;
  return false;
}

/**
 * Parse the timestamp from a Pylon message.
 * Some messages use `timestamp`, some use `created_at`.  Try both.
 */
function parseMsgTime(msg) {
  return parseUtcIso(msg?.timestamp || msg?.created_at);
}

/**
 * Fetch messages for a single issue and determine whether the issue
 * should be flagged as "waiting on support".
 *
 * Logic (non-negotiable — derived from production debugging):
 * 1. Fetch all messages for the issue
 * 2. Filter to PUBLIC messages only (is_private !== true)
 * 3. Find the latest public message
 * 4. Only flag the issue if the latest public speaker is a CUSTOMER
 *    (not support/agent) AND the message is older than the cutoff
 *
 * This prevents false positives where support already replied after
 * the customer's message.
 *
 * Customer detection: author.contact populated OR author.type === "contact"
 * Timestamp detection: msg.timestamp || msg.created_at
 *
 * Returns { isCustomerLast: boolean, latestPublicMsgTime: DateTime|null }
 * Returns null on fetch failure (per-issue error handling — does not crash the run).
 */
async function fetchWaitingOnSupportStatus({ pylonToken, issueId }) {
  const maxAttempts = 4;
  let attempt = 0;

  while (true) {
    attempt += 1;

    const res = await fetch(`${PYLON_API_BASE}/issues/${issueId}/messages`, {
      method: "GET",
      headers: {
        Authorization: `Bearer ${pylonToken}`,
        Accept: "application/json",
      },
    });

    if (res.status === 429) {
      if (attempt >= maxAttempts) {
        console.warn(`[MESSAGES] 429 after ${maxAttempts} attempts for issue ${issueId}; skipping`);
        return null;
      }
      const retryAfterMs = Math.min(30000, 750 * 2 ** (attempt - 1));
      await sleep(retryAfterMs);
      continue;
    }

    const text = await res.text();
    let json;
    try {
      json = JSON.parse(text);
    } catch {
      console.warn(`[MESSAGES] Non-JSON response for issue ${issueId}: ${text.slice(0, 200)}`);
      return null;
    }

    if (!res.ok) {
      console.warn(`[MESSAGES] Failed for issue ${issueId} (${res.status})`);
      return null;
    }

    const messages = Array.isArray(json.data) ? json.data : [];

    // Find the latest PUBLIC message (ignore private/internal notes)
    let latestPublicMsg = null;
    let latestPublicMsgTime = null;
    for (const msg of messages) {
      if (msg.is_private) continue; // skip private notes

      const msgTime = parseMsgTime(msg);
      if (msgTime && (!latestPublicMsgTime || msgTime > latestPublicMsgTime)) {
        latestPublicMsgTime = msgTime;
        latestPublicMsg = msg;
      }
    }

    // If no public messages at all, we can't determine — skip
    if (!latestPublicMsg) {
      return { isCustomerLast: false, latestPublicMsgTime: null };
    }

    // Check if the latest public speaker is a customer
    const isCustomerLast = isCustomerAuthor(latestPublicMsg);

    return { isCustomerLast, latestPublicMsgTime };
  }
}

/**
 * Fetch all users:
 * - Build id -> name map (used for display)
 * - Build name -> id map (used for roster matching)
 */
async function fetchAssigneeMaps({ pylonToken }) {
  try {
    const res = await fetch(`${PYLON_API_BASE}/users`, {
      method: "GET",
      headers: { Authorization: `Bearer ${pylonToken}`, Accept: "application/json" },
    });
    const json = await res.json();
    if (!res.ok || json.errors?.length) throw new Error(JSON.stringify(json));

    const assigneeIdToName = {};
    for (const u of json?.data ?? []) {
      const display =
        (typeof u?.name === "string" && u.name.trim()) ||
        (typeof u?.email === "string" && u.email.trim()) ||
        u?.id;
      if (u?.id) assigneeIdToName[u.id] = display;
    }

    // Inverse map (best effort)
    const assigneeNameToId = {};
    for (const [id, name] of Object.entries(assigneeIdToName)) {
      assigneeNameToId[name] = id;
    }

    console.log(
      `[USERS] Loaded ${Object.keys(assigneeIdToName).length} users for assignee name resolution.`
    );

    return { assigneeIdToName, assigneeNameToId };
  } catch (err) {
    console.warn(
      `[WARN] Could not fetch /users from Pylon. Assignees will show as IDs. Reason: ${err?.message || err}`
    );
    return { assigneeIdToName: {}, assigneeNameToId: {} };
  }
}

/** ----------------------------
 *  DISPLAY LINES
 *  ---------------------------- */

function buildHandoffIssueLines(handoffIssuesList, assigneeIdToName) {
  return handoffIssuesList
    .map((it) => {
      const issueLink = `<${pylonIssueUrl(it.id)}|#${it.number}>`;
      const assignee =
        it.assigneeId ? (assigneeIdToName[it.assigneeId] || it.assigneeId) : "Unassigned";
      const region = it.handoffRegionLabel || "Unknown";
      const meeting = it.meetingRequired ? "Yes" : "No";
      return `${it.priorityLabel}->${issueLink} | Assignee: ${assignee} | Handoff Region: ${region} | Handoff meeting required: ${meeting}`;
    })
    .join("\n");
}

function isEnterpriseTier(tierSlug) {
  return tierSlug === "enterprise" || tierSlug === "enterprise_elite";
}

function buildP0P1IssueLines(p0p1List, assigneeIdToName) {
  return p0p1List
    .map((it) => {
      const issueLink = `<${pylonIssueUrl(it.id)}|#${it.number}>`;
      const assignee =
        it.assigneeId ? (assigneeIdToName[it.assigneeId] || it.assigneeId) : "Unassigned";
      const subject = (it.subject ?? "(No subject)").replace(/\s+/g, " ").trim();
      const tierSlug = it.tier ?? "unknown";
      const tier = tierDisplayName(tierSlug);
      const timeLeft = formatTimeRemaining(it.timeRemainingSeconds, it.isCalendar);
      const suffix = isEnterpriseTier(tierSlug) ? " 📌" : "";
      return `${it.priorityLabel} | ${tier} | ${timeLeft} | ${issueLink} | Assignee: ${assignee} | Subject: ${subject}${suffix}`;
    })
    .join("\n");
}

function buildWaitingOnSupportLines(list, assigneeIdToName) {
  const sorted = [...list].sort((a, b) => priorityRank(a.priorityLabel) - priorityRank(b.priorityLabel));
  return sorted
    .map((it) => {
      const issueLink = `<${pylonIssueUrl(it.id)}|#${it.number}>`;
      const assignee =
        it.assigneeId ? (assigneeIdToName[it.assigneeId] || it.assigneeId) : "Unassigned";
      const subject = (it.subject ?? "(No subject)").replace(/\s+/g, " ").trim();
      const tierSlug = it.tier ?? "unknown";
      const tier = tierDisplayName(tierSlug);
      const overdue = formatOverdue(it.overdueSeconds, it.isCalendar);
      const suffix = isEnterpriseTier(tierSlug) ? " 📌" : "";
      return `${it.priorityLabel} | ${tier} | ${overdue} | ${issueLink} | Assignee: ${assignee} | Subject: ${subject}${suffix}`;
    })
    .join("\n");
}

function tierDisplayName(slug) {
  switch (slug) {
    case "enterprise_elite": return "Enterprise Elite";
    case "enterprise":       return "Enterprise";
    case "ultimate":         return "Ultimate";
    case "pro":              return "Pro";
    case "lite_legacy":      return "Lite Legacy";
    case "community":        return "Community";
    case "unknown":          return "Unknown";
    default:                 return slug;
  }
}

/**
 * Format the overdue duration for display.
 * Calendar: "+2d 3h overdue" or "+5h overdue"
 * Business hours: "+1 biz day 3 biz hrs overdue" or "+5 biz hrs overdue"
 *   (1 biz day = 8 biz hrs)
 */
function formatOverdue(overdueSeconds, isCalendar) {
  const totalHours = Math.floor(overdueSeconds / 3600);
  const minutes = Math.ceil((overdueSeconds % 3600) / 60);
  if (isCalendar) {
    const days = Math.floor(totalHours / 24);
    const remHours = totalHours % 24;
    if (days > 0 && remHours > 0) return `+${days}d ${remHours}h overdue`;
    if (days > 0)                  return `+${days}d overdue`;
    if (totalHours > 0)            return `+${totalHours}h overdue`;
    if (minutes === 0)             return `<1m overdue`;
    return `+${minutes}m overdue`;
  } else {
    const bizDays = Math.floor(totalHours / 8);
    const remBizHours = totalHours % 8;
    if (bizDays > 0 && remBizHours > 0)
      return `+${bizDays}d ${remBizHours}h overdue`;
    if (bizDays > 0)
      return `+${bizDays}d overdue`;
    if (totalHours > 0)
      return `+${totalHours}h overdue`;
    if (minutes === 0) return `<1m overdue`;
    return `+${minutes}m overdue`;
  }
}

/**
 * Format time remaining until FRT SLA.
 * Calendar tiers (24x7, 24x5): "1d 2h left", "3h 20m left"
 * Non-calendar tiers (9-5 biz hrs): always show total hours, never days,
 *   because business hours ≠ calendar days ("24h left" not "1d left").
 * Zero/negative → "overdue"
 */
function formatTimeRemaining(seconds, isCalendar) {
  if (seconds === null || seconds === undefined) return "SLA N/A";
  if (seconds <= 0) return "overdue";
  if (seconds < 60) return "<1m left";
  const totalHours = Math.floor(seconds / 3600);
  const remMins = Math.floor((seconds % 3600) / 60);
  if (isCalendar) {
    const days = Math.floor(totalHours / 24);
    const hrs = totalHours % 24;
    if (days > 0 && hrs > 0)  return `${days}d ${hrs}h left`;
    if (days > 0)              return `${days}d left`;
  }
  if (totalHours > 0 && remMins > 0) return `${totalHours}h ${remMins}m left`;
  if (totalHours > 0)                return `${totalHours}h left`;
  return `${remMins}m left`;
}

function buildSlaBreachedLines(list, assigneeIdToName) {
  const sorted = [...list].sort((a, b) => priorityRank(a.priorityLabel) - priorityRank(b.priorityLabel));
  return sorted
    .map((it) => {
      const issueLink = `<${pylonIssueUrl(it.id)}|#${it.number}>`;
      const assignee =
        it.assigneeId ? (assigneeIdToName[it.assigneeId] || it.assigneeId) : "Unassigned";
      const subject = (it.subject ?? "(No subject)").replace(/\s+/g, " ").trim();
      const tierSlug = it.tier ?? "unknown";
      const tier = tierDisplayName(tierSlug);
      const overdue = formatOverdue(it.overdueSeconds, it.isCalendar);
      const suffix = isEnterpriseTier(tierSlug) ? " 📌" : "";
      return `${it.priorityLabel} | ${tier} | ${overdue} | ${issueLink} | Assignee: ${assignee} | Subject: ${subject}${suffix}`;
    })
    .join("\n");
}

/** ----------------------------
 *  ASSIGNMENT BREAKDOWN (new tickets)
 *  ---------------------------- */

function formatAssignedBreakdownForShift(slot, createdIssues, assigneeIdToName) {
  const roster = REGION_ROSTERS[slot] || [];
  const pylonCounts = new Map(roster.map((n) => [n, 0]));
  const discordCounts = new Map(roster.map((n) => [n, 0]));

  // Warn if any roster member's name doesn't match any Pylon user — counts for
  // that person will silently be 0, which would make the handoff report wrong.
  const knownNames = new Set(Object.values(assigneeIdToName));
  const unresolved = roster.filter((n) => !knownNames.has(n));
  if (unresolved.length > 0) {
    console.warn(
      `[WARN][${slot.toUpperCase()}] Roster members not found in Pylon users (counts will be 0): ${unresolved.join(", ")}. ` +
      `Check config/rosters.json for typos or name changes.`
    );
  }

  for (const issue of createdIssues) {
    const assigneeId = issue?.assignee?.id;
    if (!assigneeId) continue;

    const assigneeName = assigneeIdToName[assigneeId];
    if (!assigneeName || !pylonCounts.has(assigneeName)) continue;

    if (isDiscordIssue(issue)) {
      discordCounts.set(assigneeName, (discordCounts.get(assigneeName) || 0) + 1);
    } else {
      pylonCounts.set(assigneeName, (pylonCounts.get(assigneeName) || 0) + 1);
    }
  }

  const pylonLine = roster.map((n) => `${n}: ${pylonCounts.get(n) || 0}`).join(" | ");
  const discordLine = roster.map((n) => `${n}: ${discordCounts.get(n) || 0}`).join(" | ");

  const assignedCount = [...pylonCounts.values(), ...discordCounts.values()].reduce((s, v) => s + v, 0);
  const discordNew = [...discordCounts.values()].reduce((s, v) => s + v, 0);

  return {
    pylon: pylonLine,
    discord: discordLine,
    assignedCount,
    discordNew,
  };
}

/** ----------------------------
 *  SLACK MESSAGE
 *  ---------------------------- */

function buildSlackHandoffMessage({
  slot,
  headerLabel,
  datePt,
  newTicketsDuringShiftCount,
  newTicketsAssignedPylonBreakdown,
  newTicketsAssignedDiscordBreakdown,
  discordNew,
  discordOpen,
  discordClosed,
  frP0P1,
  frP2P3,
  slaBreached,
  p0p1IssueLines,
  slaBreachedLines,
  waitP0P1,
  waitP0P1Lines,
  waitP2P3,
  waitP2P3Lines,
  handoffIssues,
  handoffIssueLines,
}) {
  const eP0P1 = statusEmoji({ count: frP0P1, alertLevel: "critical" });
  const eP2P3 = statusEmoji({ count: frP2P3 });
  const eSlaBreached = statusEmoji({ count: slaBreached, alertLevel: "critical" });
  const eWaitP0P1 = statusEmoji({ count: waitP0P1, alertLevel: "critical" });
  const eWaitP2P3 = statusEmoji({ count: waitP2P3 });
  const eHandoff = statusEmoji({ count: handoffIssues });

  const frP0P1Label = `<${SLACK_LINKS.frSlaPendingP0P1}|*P0/P1 FR Pending*>`;
  const frP2P3Label = `<${SLACK_LINKS.frSlaPendingP2P3}|*P2/P3 FR Pending*>`;
  const handoffLabel = `<${SLACK_LINKS.handoffIssues}|*Handoff Issues*>`;
  const discordCommunityLabel = `<${SLACK_LINKS.discordCommunityOpen}|*Discord Community Issues*>`;

  const region = regionLabelFromSlot(slot);

  let msg =
`*<${headerLabel} team handoff>*
*Date:* ${datePt}
*New tickets during ${region}:* ${newTicketsDuringShiftCount}
*Assigned (Pylon):* ${newTicketsAssignedPylonBreakdown}
*Assigned (Discord):* ${newTicketsAssignedDiscordBreakdown}
:discord: ${discordCommunityLabel}: New ${discordNew} | Open ${discordOpen} | Closed ${discordClosed}
${eP0P1} ${frP0P1Label}: ${frP0P1}`;

  if (frP0P1 > 0 && p0p1IssueLines) {
    msg += `\n${p0p1IssueLines}`;
  }

  msg += `\n${eP2P3} ${frP2P3Label}: ${frP2P3}`;

  msg += `\n${eSlaBreached} *FR SLA Breached:* ${slaBreached}`;

  if (slaBreached > 0 && slaBreachedLines) {
    msg += `\n${slaBreachedLines}`;
  }

  msg += `\n${eWaitP0P1} *P0/P1 Update SLA Breached (>1 day):* ${waitP0P1}`;

  if (waitP0P1 > 0 && waitP0P1Lines) {
    msg += `\n${waitP0P1Lines}`;
  }

  msg += `\n${eWaitP2P3} *P2/P3 Update SLA Breached (>3 days):* ${waitP2P3}`;

  if (waitP2P3 > 0 && waitP2P3Lines) {
    msg += `\n${waitP2P3Lines}`;
  }

  msg += `\n${eHandoff} ${handoffLabel}: ${handoffIssues}`;

  if (handoffIssues > 0 && handoffIssueLines) {
    msg += `\n${handoffIssueLines}`;
  }

  return msg;
}

/** ----------------------------
 *  SCANS
 *  ---------------------------- */

/**
 * Pass A: collect tickets created during the shift window (open + closed)
 * Counts ONLY those currently assigned to L1+L2 team.
 * We can safely stop paging once oldest created_at < startUtc.
 */
async function scanCreatedDuringShift({ slot, pylonToken }) {
  const nowPt = ptNow();
  const { startUtc, endUtc } = getCreatedWindowForSlot(slot, nowPt);

  const createdIds = new Set();
  const createdIssues = [];

  let cursor = null;
  const seenCursors = new Set();
  let page = 0;
  const MAX_PAGES = 500;

  while (true) {
    page += 1;

    const resp = await pylonSearch({ token: pylonToken, limit: 200, cursor });
    const data = Array.isArray(resp.data) ? resp.data : [];

    for (const issue of data) {
      if (!issue?.id) continue;

      // ✅ only L1+L2 (team null excluded)
      if (!isTeamL1L2(issue)) continue;

      const createdAtUtc = parseUtcIso(issue.created_at);
      if (createdAtUtc && createdAtUtc >= startUtc && createdAtUtc < endUtc) {
        if (!createdIds.has(issue.id)) {
          createdIds.add(issue.id);
          createdIssues.push(issue);
        }
      }
    }

    const oldestUtc = data
      .map((i) => parseUtcIso(i.created_at))
      .filter(Boolean)
      .reduce((min, dt) => (min == null || dt < min ? dt : min), null);

    const hasNext = resp?.pagination?.has_next_page === true;
    const nextCursor = resp?.pagination?.cursor ?? null;

    console.log(
      `[SCAN-A] page=${page} fetched=${data.length} createdInShift=${createdIds.size}`
    );

    if (!hasNext || !nextCursor) break;

    // Safe early-stop for created-window counting
    if (oldestUtc && oldestUtc < startUtc) break;

    if (seenCursors.has(nextCursor)) {
      console.warn(`[SCAN-A] cursor repeated; stopping to avoid infinite paging.`);
      break;
    }
    seenCursors.add(nextCursor);

    if (page >= MAX_PAGES) {
      console.warn(`[SCAN-A] hit MAX_PAGES=${MAX_PAGES}; stopping.`);
      break;
    }

    cursor = nextCursor;
    await sleep(200);
  }

  return { count: createdIds.size, issues: createdIssues };
}

/**
 * Pass B: scan recent issues (within lookback window) collecting:
 * - FR SLA Pending P0/P1 (state=new + urgent/high)
 * - FR SLA Pending P2/P3 (state=new + medium/low)
 * - Handoff issues (open + team L1+L2 + hand_off_region set)
 * - Discord Community Open issues (open + team L1+L2 + discord source or tag)
 *
 * Stops pagination at LOOKBACK_DAYS_SCAN_B (default 30).
 * Waiting-on-support is handled separately by scanWaitingOnSupport()
 * using server-side state filter for performance.
 */
async function scanQueueMetrics({ pylonToken, assigneeIdToName }) {
  const nowPt = ptNow();

  const LOOKBACK_DAYS_SCAN_B = Number(process.env.SCAN_B_LOOKBACK_DAYS || 30);
  const lookbackCutoffUtc = nowPt.minus({ days: LOOKBACK_DAYS_SCAN_B }).toUTC();

  const ids = {
    frP0P1: new Set(),
    frP2P3: new Set(),
    slaBreached: new Set(),
    handoff: new Set(),
    discordOpen: new Set(),
    discordClosed: new Set(),
  };

  const handoffDisplay = new Map();
  const p0p1Details = new Map();
  const slaBreachedDetails = new Map();

  let cursor = null;
  const seenCursors = new Set();
  let page = 0;
  const MAX_PAGES = 500;

  while (true) {
    page += 1;

    const resp = await pylonSearch({ token: pylonToken, limit: 200, cursor });
    const data = Array.isArray(resp.data) ? resp.data : [];

    for (const issue of data) {
      if (!issue?.id) continue;
      if (!isTeamL1L2(issue)) continue;

      const prioRaw = getPriority(issue);
      const prioLabel = mapPriorityLabel(prioRaw);
      const createdAtUtc = parseUtcIso(issue.created_at);

      // FR SLA Pending buckets (state=new only)
      if (issue.state === "new") {
        const tierRaw = issue?.custom_fields?.support_tier?.values?.[0] ?? "unknown";
        const tier = tierRaw.replace(/-/g, "_");

        if (prioRaw && P0_P1_PRIORITIES.has(prioRaw)) {
          ids.frP0P1.add(issue.id);
          // Compute time remaining until FRT SLA for display
          const p0p1PrioIdx = PRIORITY_IDX[prioRaw] ?? null;
          const p0p1SlaSeconds = p0p1PrioIdx !== null ? (SLA_SECONDS[tier]?.[p0p1PrioIdx] ?? null) : null;
          const p0p1Coverage = SLA_COVERAGE[tier]?.[p0p1PrioIdx] ?? "biz";
          let p0p1TimeRemaining = null;
          if (p0p1SlaSeconds !== null && issue.created_at) {
            const p0p1Elapsed = elapsedSeconds(issue.created_at, nowPt, p0p1Coverage);
            p0p1TimeRemaining = p0p1SlaSeconds - p0p1Elapsed;
          }
          p0p1Details.set(issue.id, {
            id: issue.id,
            number: issue.number,
            priorityLabel: prioLabel,
            tier,
            timeRemainingSeconds: p0p1TimeRemaining,
            isCalendar: p0p1Coverage !== "biz",
            assigneeId: issue?.assignee?.id ?? null,
            subject: issue?.title ?? "(No subject)",
          });
        }

        if (prioRaw && P2_P3_PRIORITIES.has(prioRaw)) {
          ids.frP2P3.add(issue.id);
        }

        // FRT SLA breach: check tier × priority threshold
        if (prioRaw && issue.created_at) {
          const prioIdx = PRIORITY_IDX[prioRaw] ?? null;
          const slaSeconds = prioIdx !== null ? (SLA_SECONDS[tier]?.[prioIdx] ?? null) : null;
          if (slaSeconds !== null) {
            const coverage = SLA_COVERAGE[tier]?.[prioIdx] ?? "biz";
            const elapsed = elapsedSeconds(issue.created_at, nowPt, coverage);
            if (elapsed > slaSeconds && !slaBreachedDetails.has(issue.id)) {
              ids.slaBreached.add(issue.id);
              slaBreachedDetails.set(issue.id, {
                id: issue.id,
                number: issue.number,
                priorityLabel: prioLabel,
                tier,
                overdueSeconds: elapsed - slaSeconds,
                isCalendar: coverage !== "biz",
                assigneeId: issue?.assignee?.id ?? null,
                subject: issue?.title ?? "(No subject)",
              });
            }
          }
        }
      }

      // Open handoff issues
      if (isOpenHandoffIssue(issue)) {
        ids.handoff.add(issue.id);
        if (!handoffDisplay.has(issue.id)) {
          const slug = getHandoffRegionValue(issue);
          handoffDisplay.set(issue.id, {
            id: issue.id,
            number: issue.number,
            priorityLabel: prioLabel,
            assigneeId: issue?.assignee?.id ?? null,
            handoffRegionLabel: handoffLabelFromSlug(slug),
            meetingRequired: isMeetingRequired(issue),
          });
        }
      }

      // Discord Community Issues (split by state)
      if (isDiscordIssue(issue)) {
        if (isOpenState(issue) && issue.state !== "new") {
          ids.discordOpen.add(issue.id);
        } else if (!isOpenState(issue)) {
          ids.discordClosed.add(issue.id);
        }
      }
    }

    const hasNext = resp?.pagination?.has_next_page === true;
    const nextCursor = resp?.pagination?.cursor ?? null;

    console.log(
      `[SCAN-B] page=${page} fetched=${data.length} handoff=${ids.handoff.size} p0p1=${ids.frP0P1.size} p2p3=${ids.frP2P3.size} slaBreached=${ids.slaBreached.size}`
    );

    if (!hasNext || !nextCursor) break;

    // Early-stop at lookback cutoff
    const oldestCreatedUtc = data
      .map((i) => parseUtcIso(i.created_at))
      .filter(Boolean)
      .reduce((min, dt) => (min == null || dt < min ? dt : min), null);

    if (oldestCreatedUtc && oldestCreatedUtc < lookbackCutoffUtc) {
      console.log(
        `[SCAN-B] Reached lookback cutoff (${lookbackCutoffUtc.toISO()}); stopping pagination.`
      );
      break;
    }

    if (seenCursors.has(nextCursor)) {
      console.warn(`[SCAN-B] cursor repeated; stopping to avoid infinite paging.`);
      break;
    }
    seenCursors.add(nextCursor);

    if (page >= MAX_PAGES) {
      console.warn(`[SCAN-B] hit MAX_PAGES=${MAX_PAGES}; stopping.`);
      break;
    }

    cursor = nextCursor;
    await sleep(200);
  }

  const handoffIssueLines =
    ids.handoff.size > 0
      ? buildHandoffIssueLines(Array.from(handoffDisplay.values()), assigneeIdToName)
      : "";

  const p0p1IssueLines =
    ids.frP0P1.size > 0
      ? buildP0P1IssueLines(Array.from(p0p1Details.values()), assigneeIdToName)
      : "";

  const slaBreachedLines =
    ids.slaBreached.size > 0
      ? buildSlaBreachedLines(Array.from(slaBreachedDetails.values()), assigneeIdToName)
      : "";

  return {
    frP0P1: ids.frP0P1.size,
    frP2P3: ids.frP2P3.size,
    slaBreached: ids.slaBreached.size,
    p0p1IssueLines,
    slaBreachedLines,
    discordOpen: ids.discordOpen.size,
    discordClosed: ids.discordClosed.size,
    handoffIssues: ids.handoff.size,
    handoffIssueLines,
    lookbackDays: LOOKBACK_DAYS_SCAN_B,
  };
}

/**
 * Pass D: Count open Discord Community issues with no date restriction.
 *
 * Pass B's 30-day lookback silently drops old stale Discord issues (e.g. on_hold
 * tickets opened 35+ days ago). This pass uses server-side state filters — one per
 * open-but-not-new state — to scan ALL issues regardless of age, then filters
 * locally for isDiscordIssue + isTeamL1L2.
 *
 * States scanned: waiting_on_you, waiting_on_customer, on_hold.
 */
async function scanDiscordOpenIssues({ pylonToken }) {
  const OPEN_NON_NEW_STATES = ["waiting_on_you", "waiting_on_customer", "on_hold"];
  const discordOpenIds = new Set();

  for (const stateVal of OPEN_NON_NEW_STATES) {
    const filter = { field: "state", operator: "equals", value: stateVal };
    let cursor = null;
    const seenCursors = new Set();
    let page = 0;
    const MAX_PAGES = 100;

    while (true) {
      page += 1;
      const resp = await pylonSearch({ token: pylonToken, limit: 200, cursor, filter });
      const data = Array.isArray(resp.data) ? resp.data : [];

      for (const issue of data) {
        if (!issue?.id) continue;
        if (!isTeamL1L2(issue)) continue;
        if (!isDiscordIssue(issue)) continue;
        discordOpenIds.add(issue.id);
      }

      const hasNext = resp?.pagination?.has_next_page === true;
      const nextCursor = resp?.pagination?.cursor ?? null;

      console.log(
        `[SCAN-D] state=${stateVal} page=${page} fetched=${data.length} discordOpen=${discordOpenIds.size}`
      );

      if (!hasNext || !nextCursor) break;
      if (seenCursors.has(nextCursor)) {
        console.warn(`[SCAN-D] cursor repeated for state=${stateVal}; stopping.`);
        break;
      }
      seenCursors.add(nextCursor);
      if (page >= MAX_PAGES) {
        console.warn(`[SCAN-D] hit MAX_PAGES for state=${stateVal}; stopping.`);
        break;
      }

      cursor = nextCursor;
      await sleep(200);
    }
  }

  return { discordOpen: discordOpenIds.size };
}

/**
 * Pass C: Scan waiting-on-support issues using server-side state filter.
 *
 * Uses Pylon API filter: { field: "state", operator: "equals", value: "waiting_on_you" }
 * This returns ONLY issues in waiting_on_you state, avoiding full pagination
 * through all 13,000+ issues.
 *
 * Team L1+L2 is filtered locally (compound filters not supported by Pylon API).
 *
 * After collecting candidates, resolves each via the per-issue messages API
 * to check who spoke last (latest-public-speaker check).
 *
 * Update-frequency thresholds (best effort until per-tier values are defined):
 *   P0/P1 (urgent/high): customer last spoke > 1 day ago
 *   P2/P3 (medium/low):  customer last spoke > 3 days ago
 */
async function scanWaitingOnSupport({ pylonToken, assigneeIdToName }) {
  const nowPt = ptNow();

  const WAITING_FILTER = { field: "state", operator: "equals", value: "waiting_on_you" };

  const waitP0P1Candidates = new Map();
  const waitP2P3Candidates = new Map();

  let cursor = null;
  const seenCursors = new Set();
  let page = 0;
  const MAX_PAGES = 200;

  while (true) {
    page += 1;

    const resp = await pylonSearch({ token: pylonToken, limit: 200, cursor, filter: WAITING_FILTER });
    const data = Array.isArray(resp.data) ? resp.data : [];

    for (const issue of data) {
      if (!issue?.id) continue;
      if (!isTeamL1L2(issue)) continue;

      const prioRaw = getPriority(issue);
      const prioLabel = mapPriorityLabel(prioRaw);

      const tierRaw = issue?.custom_fields?.support_tier?.values?.[0] ?? "unknown";
      const candidate = {
        id: issue.id,
        number: issue.number,
        priorityLabel: prioLabel,
        prioRaw,
        tier: tierRaw.replace(/-/g, "_"),
        assigneeId: issue?.assignee?.id ?? null,
        subject: issue?.title ?? "(No subject)",
      };

      if (prioRaw && P0_P1_PRIORITIES.has(prioRaw) && !waitP0P1Candidates.has(issue.id)) {
        waitP0P1Candidates.set(issue.id, candidate);
      }
      if (prioRaw && P2_P3_PRIORITIES.has(prioRaw) && !waitP2P3Candidates.has(issue.id)) {
        waitP2P3Candidates.set(issue.id, candidate);
      }
    }

    const hasNext = resp?.pagination?.has_next_page === true;
    const nextCursor = resp?.pagination?.cursor ?? null;

    console.log(
      `[SCAN-C] page=${page} fetched=${data.length} waitP0P1=${waitP0P1Candidates.size} waitP2P3=${waitP2P3Candidates.size}`
    );

    if (!hasNext || !nextCursor) break;

    if (seenCursors.has(nextCursor)) {
      console.warn(`[SCAN-C] cursor repeated; stopping.`);
      break;
    }
    seenCursors.add(nextCursor);

    if (page >= MAX_PAGES) {
      console.warn(`[SCAN-C] hit MAX_PAGES=${MAX_PAGES}; stopping.`);
      break;
    }

    cursor = nextCursor;
    await sleep(200);
  }

  // Resolve waiting-on-support candidates via per-issue messages API.
  const MSG_DELAY_MS = Number(process.env.PYLON_MESSAGES_DELAY_MS || 200);
  const allWaitCandidates = new Map([...waitP0P1Candidates, ...waitP2P3Candidates]);
  const waitStatusCache = new Map();

  if (allWaitCandidates.size > 0) {
    console.log(`[SCAN-C] Resolving ${allWaitCandidates.size} waiting-on-support candidates via messages API...`);
    for (const issueId of allWaitCandidates.keys()) {
      try {
        const status = await fetchWaitingOnSupportStatus({ pylonToken, issueId });
        if (status) waitStatusCache.set(issueId, status);
      } catch (err) {
        // Per-issue error handling: log and continue, don't abort the run
        console.warn(`[MESSAGES] Unexpected error for issue ${issueId}: ${err?.message || err}`);
      }
      await sleep(MSG_DELAY_MS);
    }
    console.log(`[SCAN-C] Resolved ${waitStatusCache.size}/${allWaitCandidates.size} with message status.`);
  }

  // Apply update-frequency thresholds — only flag if the latest public speaker is
  // the CUSTOMER and their message is older than the cutoff.
  const ids = { waitP0P1: new Set(), waitP2P3: new Set() };

  const waitP0P1Details = new Map();
  for (const [issueId, candidate] of waitP0P1Candidates) {
    const status = waitStatusCache.get(issueId);
    if (status?.isCustomerLast && status.latestPublicMsgTime) {
      const prioIdx = PRIORITY_IDX[candidate.prioRaw] ?? 1;
      const coverage = SLA_COVERAGE[candidate.tier]?.[prioIdx] ?? "biz";
      const elapsed = elapsedSeconds(status.latestPublicMsgTime.toISO(), nowPt, coverage);
      const threshold = coverage === "biz" ? 8 * 3600 : 24 * 3600;
      if (elapsed > threshold) {
        ids.waitP0P1.add(issueId);
        waitP0P1Details.set(issueId, {
          ...candidate,
          overdueSeconds: elapsed - threshold,
          isCalendar: coverage !== "biz",
        });
      }
    }
  }

  const waitP2P3Details = new Map();
  for (const [issueId, candidate] of waitP2P3Candidates) {
    const status = waitStatusCache.get(issueId);
    if (status?.isCustomerLast && status.latestPublicMsgTime) {
      const prioIdx = PRIORITY_IDX[candidate.prioRaw] ?? 3;
      const coverage = SLA_COVERAGE[candidate.tier]?.[prioIdx] ?? "biz";
      const elapsed = elapsedSeconds(status.latestPublicMsgTime.toISO(), nowPt, coverage);
      const threshold = coverage === "biz" ? 24 * 3600 : 72 * 3600;
      if (elapsed > threshold) {
        ids.waitP2P3.add(issueId);
        waitP2P3Details.set(issueId, {
          ...candidate,
          overdueSeconds: elapsed - threshold,
          isCalendar: coverage !== "biz",
        });
      }
    }
  }

  console.log(`[SCAN-C] Waiting on Support final: P0/P1=${ids.waitP0P1.size} P2/P3=${ids.waitP2P3.size}`);

  const waitP0P1Lines =
    ids.waitP0P1.size > 0
      ? buildWaitingOnSupportLines(Array.from(waitP0P1Details.values()), assigneeIdToName)
      : "";

  const waitP2P3Lines =
    ids.waitP2P3.size > 0
      ? buildWaitingOnSupportLines(Array.from(waitP2P3Details.values()), assigneeIdToName)
      : "";

  return {
    waitP0P1: ids.waitP0P1.size,
    waitP0P1Lines,
    waitP2P3: ids.waitP2P3.size,
    waitP2P3Lines,
  };
}

/** ----------------------------
 *  SLACK
 *  ---------------------------- */

async function postToSlack({ slackToken, text }) {
  const res = await fetch("https://slack.com/api/chat.postMessage", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${slackToken}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      channel: SLACK_CHANNEL,
      text,
      unfurl_links: false,
      unfurl_media: false,
    }),
  });

  const data = await res.json();
  if (!data.ok) throw new Error(`Slack error: ${data.error}`);
}

/** ----------------------------
 *  MAIN
 *  ---------------------------- */

async function main() {
  const slot = process.argv[2];
  if (!slot || !SLOT_CONFIG[slot]) throw new Error("Usage: node handoff_snapshot.mjs <apac|emea|us>");

  const pylonToken = requireEnv("PYLON_TOKEN");
  const slackToken = requireEnv("SLACK_BOT_TOKEN");

  const { headerLabel } = SLOT_CONFIG[slot];
  const datePt = formatDatePt(ptNow());

  const { assigneeIdToName } = await fetchAssigneeMaps({ pylonToken });
  if (Object.keys(assigneeIdToName).length === 0) {
    throw new Error(
      "[FATAL] fetchAssigneeMaps returned an empty map. " +
      "Aborting to avoid posting misleading zeros for new tickets during shift."
    );
  }

  // Pass A: created during shift (can early-stop safely)
  const created = await scanCreatedDuringShift({ slot, pylonToken });
  // Assigned breakdown for the roster (split by source)
  const assignedBreakdown = formatAssignedBreakdownForShift(
    slot,
    created.issues,
    assigneeIdToName
  );
  const newTicketsDuringShiftCount = assignedBreakdown.assignedCount;

  // Pass B: queue metrics + open handoff (lookback-bounded scan)
  const metrics = await scanQueueMetrics({ pylonToken, assigneeIdToName });

  // Pass C: waiting-on-support (server-side state filter — no full pagination needed)
  const waiting = await scanWaitingOnSupport({ pylonToken, assigneeIdToName });

  // Pass D: Discord open count (no date restriction — catches stale old issues)
  const discordCounts = await scanDiscordOpenIssues({ pylonToken });

  const slackText = buildSlackHandoffMessage({
    slot,
    headerLabel,
    datePt,
    newTicketsDuringShiftCount,
    newTicketsAssignedPylonBreakdown: assignedBreakdown.pylon,
    newTicketsAssignedDiscordBreakdown: assignedBreakdown.discord,
    discordNew: assignedBreakdown.discordNew,
    discordOpen: discordCounts.discordOpen,
    discordClosed: metrics.discordClosed,
    frP0P1: metrics.frP0P1,
    frP2P3: metrics.frP2P3,
    slaBreached: metrics.slaBreached,
    p0p1IssueLines: metrics.p0p1IssueLines,
    slaBreachedLines: metrics.slaBreachedLines,
    waitP0P1: waiting.waitP0P1,
    waitP0P1Lines: waiting.waitP0P1Lines,
    waitP2P3: waiting.waitP2P3,
    waitP2P3Lines: waiting.waitP2P3Lines,
    handoffIssues: metrics.handoffIssues,
    handoffIssueLines: metrics.handoffIssueLines,
  });

  await postToSlack({ slackToken, text: slackText });

  console.log("Posted handoff snapshot:", {
    slot,
    datePt,
    headerLabel,
    newTicketsDuringShiftCount,
    assignedPylon: assignedBreakdown.pylon,
    assignedDiscord: assignedBreakdown.discord,
    discordNew: assignedBreakdown.discordNew,
    discordOpen: discordCounts.discordOpen,
    discordClosed: metrics.discordClosed,
    frP0P1: metrics.frP0P1,
    frP2P3: metrics.frP2P3,
    slaBreached: metrics.slaBreached,
    waitP0P1: waiting.waitP0P1,
    waitP2P3: waiting.waitP2P3,
    handoffIssues: metrics.handoffIssues,
    handoffIssueLines: metrics.handoffIssueLines || "(empty)",
    slackMessageLength: slackText.length,
    scanBLookbackDays: metrics.lookbackDays,
    enforcedTeamId: TEAM_ID_L1_L2,
    openStates: Array.from(OPEN_STATES),
    slackChannel: SLACK_CHANNEL,
  });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});