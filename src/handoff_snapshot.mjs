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
 *   SCAN_B_LOOKBACK_DAYS=30   # override the SCAN-B lookback window (default 30)
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
 *     Aged > 1 Week (ALL priorities) => created_at < now-7d
 * - Under FR SLA Pending P0/P1 count line, prints issue line items:
 *     <#1234 link> | Assignee: Name | Subject: Title
 * - Under FR SLA Pending Aged > 1 Week, prints issue line items:
 *     P0 | <#1234 link> | Assignee: Name | Subject: Title
 * - Handoff issues = OPEN state AND L1+L2 AND hand_off_region.value is set (single-select).
 * - Handoff issues lines:
 *     <#1234 link> | Assignee: Name | Handoff Region: EMEA/APAC/America | Handoff meeting required: Yes/No
 */

import { DateTime } from "luxon";

/** ----------------------------
 *  CONFIG
 *  ---------------------------- */

const PYLON_API_BASE = "https://api.usepylon.com";

// ✅ Ready for Prod
const SLACK_CHANNEL = "#csorg-support-handoff";

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

// Header labels (handoff-to sequence)
const SLOT_CONFIG = {
  apac: { headerLabel: "APAC to EMEA" },
  emea: { headerLabel: "EMEA to US" },
  us: { headerLabel: "US to APAC" },
};

// Shift rosters for assignment breakdown (names must match Pylon user names)
const REGION_ROSTERS = {
  emea: ["Dylan Bonar", "Tommy Lundy", "Robert Norrie"],
  apac: ["Saurabh Lambe", "Chinmay Koratkar"],
  us: ["Feran Morgan", "Tassia Shibuya", "Fariha Marzan"],
};

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

  const t01 = day.plus({ hours: 1 });
  const t03 = day.plus({ hours: 3 });
  const t09 = day.plus({ hours: 9 });
  const t10 = day.plus({ hours: 10 });
  const t18 = day.plus({ hours: 18 });

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

  // apac: 18:00 prev day -> 03:00 today
  {
    const endPt = t03;
    const startPt = endPt.minus({ hours: 9 });
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

async function pylonSearch({ token, limit = 200, cursor = null }) {
  const path = "/issues/search";
  const body = { limit };
  if (cursor) body.cursor = cursor;

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

function buildAgedIssueLines(agedList, assigneeIdToName) {
  const sorted = [...agedList].sort((a, b) => {
    const pr = priorityRank(a.priorityLabel) - priorityRank(b.priorityLabel);
    if (pr !== 0) return pr;
    if (!a.createdAtUtc && !b.createdAtUtc) return 0;
    if (!a.createdAtUtc) return 1;
    if (!b.createdAtUtc) return -1;
    return a.createdAtUtc.toMillis() - b.createdAtUtc.toMillis();
  });

  return sorted
    .map((it) => {
      const issueLink = `<${pylonIssueUrl(it.id)}|#${it.number}>`;
      const assignee =
        it.assigneeId ? (assigneeIdToName[it.assigneeId] || it.assigneeId) : "Unassigned";
      const subject = (it.subject ?? "(No subject)").replace(/\s+/g, " ").trim();
      // NOTE: you requested spaces around pipe separators (P0 | #1234 | ...)
      return `${it.priorityLabel} | ${issueLink} | Assignee: ${assignee} | Subject: ${subject}`;
    })
    .join("\n");
}

function buildP0P1IssueLines(p0p1List, assigneeIdToName) {
  return p0p1List
    .map((it) => {
      const issueLink = `<${pylonIssueUrl(it.id)}|#${it.number}>`;
      const assignee =
        it.assigneeId ? (assigneeIdToName[it.assigneeId] || it.assigneeId) : "Unassigned";
      const subject = (it.subject ?? "(No subject)").replace(/\s+/g, " ").trim();
      return `${it.priorityLabel} | ${issueLink} | Assignee: ${assignee} | Subject: ${subject}`;
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

  for (const issue of createdIssues) {
    const assigneeId = issue?.assignee?.id;
    if (!assigneeId) continue;

    const assigneeName = assigneeIdToName[assigneeId];
    if (!assigneeName || !pylonCounts.has(assigneeName)) continue;

    const source = issue?.source;
    if (source === "discord") {
      discordCounts.set(assigneeName, (discordCounts.get(assigneeName) || 0) + 1);
    } else {
      // Anything not from Discord is counted as Pylon
      pylonCounts.set(assigneeName, (pylonCounts.get(assigneeName) || 0) + 1);
    }
  }

  const pylonLine = roster.map((n) => `${n}: ${pylonCounts.get(n) || 0}`).join(" | ");
  const discordLine = roster.map((n) => `${n}: ${discordCounts.get(n) || 0}`).join(" | ");

  return {
    pylon: pylonLine,
    discord: discordLine,
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
  discordCommunityOpen,
  frP0P1,
  frP2P3,
  frAgedAll,
  p0p1IssueLines,
  agedIssueLines,
  handoffIssues,
  handoffIssueLines,
}) {
  const eP0P1 = statusEmoji({ count: frP0P1, alertLevel: "critical" });
  const eP2P3 = statusEmoji({ count: frP2P3 });
  const eAged = statusEmoji({ count: frAgedAll });
  const eHandoff = statusEmoji({ count: handoffIssues });

  const frP0P1Label = `<${SLACK_LINKS.frSlaPendingP0P1}|FR SLA Pending P0/P1>`;
  const frP2P3Label = `<${SLACK_LINKS.frSlaPendingP2P3}|FR SLA Pending P2/P3>`;
  const handoffLabel = `<${SLACK_LINKS.handoffIssues}|Handoff Issues>`;
  const discordCommunityLabel = `<${SLACK_LINKS.discordCommunityOpen}|Discord Community Issues>`;

  const region = regionLabelFromSlot(slot);

  let msg =
`*<${headerLabel} team handoff>*
Date: ${datePt}
(New tickets during ${region}: ${newTicketsDuringShiftCount})
Assigned (Pylon): ${newTicketsAssignedPylonBreakdown}
Assigned (Discord): ${newTicketsAssignedDiscordBreakdown}
🎫 ${discordCommunityLabel}: ${discordCommunityOpen}
${eP0P1} ${frP0P1Label}: ${frP0P1}`;

  if (frP0P1 > 0 && p0p1IssueLines) {
    msg += `\n${p0p1IssueLines}`;
  }

  msg += `\n${eP2P3} ${frP2P3Label}: ${frP2P3}
${eAged} FR SLA Pending Aged > 1 Week: ${frAgedAll}`;

  if (frAgedAll > 0 && agedIssueLines) {
    msg += `\n${agedIssueLines}`;
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
 * Pass B: scan issues (no API filters) and locally count:
 * - FR SLA Pending P0/P1 (state=new + urgent/high)
 * - FR SLA Pending P2/P3 (state=new + medium/low)
 * - FR SLA Pending Aged > 1 Week (ALL priorities) (state=new + created_at < now-7d)
 * - Handoff issues (open + team L1+L2 + hand_off_region set)
 *
 * Performance optimization:
 * - Stops paging once oldest issues are older than LOOKBACK_DAYS_SCAN_B (default 30 days).
 *   Increase if you expect very old open / handoff tickets.
 */
async function scanQueueMetrics({ pylonToken, assigneeIdToName }) {
  const nowPt = ptNow();
  const agedCutoffUtc = nowPt.minus({ days: 7 }).toUTC();

  // SCAN-B lookback window (performance optimization)
  // We stop paging once issues are older than this many days.
  // Increase if you expect very old open / handoff tickets.
  const LOOKBACK_DAYS_SCAN_B = Number(process.env.SCAN_B_LOOKBACK_DAYS || 30);
  const lookbackCutoffUtc = nowPt.minus({ days: LOOKBACK_DAYS_SCAN_B }).toUTC();

  const ids = {
    frP0P1: new Set(),
    frP2P3: new Set(),
    frAgedAll: new Set(),
    handoff: new Set(),
    discordCommunityOpen: new Set(),
  };

  const handoffDisplay = new Map();
  const agedDetails = new Map();
  const p0p1Details = new Map();

  let cursor = null;
  const seenCursors = new Set();
  let page = 0;
  const MAX_PAGES = 1000;

  while (true) {
    page += 1;

    const resp = await pylonSearch({ token: pylonToken, limit: 200, cursor });
    const data = Array.isArray(resp.data) ? resp.data : [];

    for (const issue of data) {
      if (!issue?.id) continue;

      // Enforce team locally (ignore team null or other teams)
      if (!isTeamL1L2(issue)) continue;

      const prioRaw = getPriority(issue);
      const prioLabel = mapPriorityLabel(prioRaw);
      const createdAtUtc = parseUtcIso(issue.created_at);

      // FR SLA Pending buckets (state=new only)
      if (issue.state === "new") {
        // P0/P1
        if (prioRaw && P0_P1_PRIORITIES.has(prioRaw)) {
          ids.frP0P1.add(issue.id);

          p0p1Details.set(issue.id, {
            id: issue.id,
            number: issue.number,
            priorityLabel: prioLabel, // ✅ Added correct priority labeling
            assigneeId: issue?.assignee?.id ?? null,
            subject: issue?.title ?? "(No subject)",
          });
        }

        // P2/P3
        if (prioRaw && P2_P3_PRIORITIES.has(prioRaw)) {
          ids.frP2P3.add(issue.id);
        }

        // Aged > 1 week (ALL priorities)
        if (createdAtUtc && createdAtUtc < agedCutoffUtc) {
          ids.frAgedAll.add(issue.id);

          if (!agedDetails.has(issue.id)) {
            agedDetails.set(issue.id, {
              id: issue.id,
              number: issue.number,
              priorityLabel: prioLabel,
              assigneeId: issue?.assignee?.id ?? null,
              subject: issue?.title ?? "(No subject)",
              createdAtUtc,
            });
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
            priorityLabel: prioLabel, // ✅ Added correct priority labeling
            assigneeId: issue?.assignee?.id ?? null,
            handoffRegionLabel: handoffLabelFromSlug(slug),
            meetingRequired: isMeetingRequired(issue),
          });
        }
      }

      // Discord Community Open Issues (open state + team L1+L2 + source=discord)
      if (isOpenState(issue) && issue?.source === "discord") {
        ids.discordCommunityOpen.add(issue.id);
      }
    }

    const hasNext = resp?.pagination?.has_next_page === true;
    const nextCursor = resp?.pagination?.cursor ?? null;

    console.log(
      `[SCAN-B] page=${page} fetched=${data.length} handoff=${ids.handoff.size} p0p1=${ids.frP0P1.size} p2p3=${ids.frP2P3.size} agedAll=${ids.frAgedAll.size}`
    );

    if (!hasNext || !nextCursor) break;

    // Early stop if we've paged past the lookback window
    const oldestUtc = data
      .map((i) => parseUtcIso(i.created_at))
      .filter(Boolean)
      .reduce((min, dt) => (min == null || dt < min ? dt : min), null);

    if (oldestUtc && oldestUtc < lookbackCutoffUtc) {
      console.log(
        `[SCAN-B] early-stop: oldest=${oldestUtc.toISO()} < cutoff=${lookbackCutoffUtc.toISO()}`
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

  const agedIssueLines =
    ids.frAgedAll.size > 0
      ? buildAgedIssueLines(Array.from(agedDetails.values()), assigneeIdToName)
      : "";

  const p0p1IssueLines =
    ids.frP0P1.size > 0
      ? buildP0P1IssueLines(Array.from(p0p1Details.values()), assigneeIdToName)
      : "";

  return {
    frP0P1: ids.frP0P1.size,
    frP2P3: ids.frP2P3.size,
    frAgedAll: ids.frAgedAll.size,
    p0p1IssueLines,
    agedIssueLines,
    discordCommunityOpen: ids.discordCommunityOpen.size,
    handoffIssues: ids.handoff.size,
    handoffIssueLines,
    lookbackDays: LOOKBACK_DAYS_SCAN_B,
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

  // Pass A: created during shift (can early-stop safely)
  const created = await scanCreatedDuringShift({ slot, pylonToken });
  const newTicketsDuringShiftCount = created.count;

  // Assigned breakdown for the roster (split by source)
  const assignedBreakdown = formatAssignedBreakdownForShift(
    slot,
    created.issues,
    assigneeIdToName
  );

  // Pass B: queue metrics + open handoff (lookback window; local truth)
  const metrics = await scanQueueMetrics({ pylonToken, assigneeIdToName });

  const slackText = buildSlackHandoffMessage({
    slot,
    headerLabel,
    datePt,
    newTicketsDuringShiftCount,
    newTicketsAssignedPylonBreakdown: assignedBreakdown.pylon,
    newTicketsAssignedDiscordBreakdown: assignedBreakdown.discord,
    discordCommunityOpen: metrics.discordCommunityOpen,
    frP0P1: metrics.frP0P1,
    frP2P3: metrics.frP2P3,
    frAgedAll: metrics.frAgedAll,
    p0p1IssueLines: metrics.p0p1IssueLines,
    agedIssueLines: metrics.agedIssueLines,
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
    discordCommunityOpen: metrics.discordCommunityOpen,
    frP0P1: metrics.frP0P1,
    frP2P3: metrics.frP2P3,
    frAgedAll: metrics.frAgedAll,
    handoffIssues: metrics.handoffIssues,
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
