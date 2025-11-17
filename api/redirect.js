import { google } from "googleapis";

let CACHE = { rows: null, fetchedAt: 0 };
const TTL_MS = parseInt(process.env.CACHE_TTL_MS || "600000", 10);

async function fetchRowsFromSheets() {
  const now = Date.now();
  if (CACHE.rows && now - CACHE.fetchedAt < TTL_MS) return CACHE.rows;

  const email = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
  const key = (process.env.GOOGLE_PRIVATE_KEY || "").replace(/\\n/g, "\n");
  const sheetId = process.env.SHEET_ID;
  const range = process.env.SHEET_RANGE || "Redirects!A:J";

  if (!email || !key || !sheetId) {
    console.error("Missing Google Sheets credentials");
    CACHE = { rows: [], fetchedAt: now };
    return [];
  }

  const auth = new google.auth.JWT({
    email,
    key,
    scopes: ["https://www.googleapis.com/auth/spreadsheets.readonly"]
  });

  const sheets = google.sheets({ version: "v4", auth });
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: sheetId,
    range
  });

  const [headers = [], ...data] = res.data.values || [];
  const rows = data.map(r =>
    Object.fromEntries(headers.map((h, i) => [h.trim(), (r[i] ?? "").toString().trim()]))
  );

  CACHE = { rows, fetchedAt: now };
  return rows;
}

function pickWeightedGroup(groups, weights) {
  const normalized = weights.map(w => Math.max(parseFloat(w) || 0, 0));
  const total = normalized.reduce((a, b) => a + b, 0) || 1;
  const r = Math.random() * total;
  let acc = 0;
  for (let i = 0; i < normalized.length; i++) {
    acc += normalized[i];
    if (r <= acc) return groups[i];
  }
  return groups[0];
}

function pickRandomKeywords(groupString, count = 3) {
  const keywords = (groupString || "")
    .split("|")
    .map(k => k.trim())
    .filter(Boolean);

  if (keywords.length === 0) return [];

  const shuffled = keywords.sort(() => 0.5 - Math.random());
  return shuffled.slice(0, Math.min(count, shuffled.length));
}

export default async function handler(req, res) {
  try {
    const { rid, ...passthrough } = req.query || {};

    // ---------------------------------------------------------------------
    // MODE B — NO RID → SIMPLE DIRECT REDIRECT (your Adepty use case)
    // ---------------------------------------------------------------------
    if (!rid) {
      const domain = (passthrough.domain || "").replace(/^https?:\/\//, "");
      const slug = (passthrough.slug || "").replace(/^\//, "");

      if (!domain || !slug) {
        return res.status(400).send("Missing domain/slug for non-rid redirect");
      }

      // Build params exactly as sent
      const params = new URLSearchParams(passthrough);

      const finalUrl = `https://${domain}/${slug}/?${params.toString()}`;
      res.setHeader("Cache-Control", "no-store");
      return res.redirect(302, finalUrl);
    }

    // ---------------------------------------------------------------------
    // MODE A — RID PRESENT → FULL ROTATION MODE
    // ---------------------------------------------------------------------
    const rows = await fetchRowsFromSheets();
    const row = rows.find(
      r =>
        String(r.redirect_id) === String(rid) &&
        String(r.active || "").toUpperCase() === "TRUE"
    );

    if (!row) {
      const fallback = rows.find(r => r.fallback_url);
      if (fallback?.fallback_url) return res.redirect(302, fallback.fallback_url);
      return res.status(404).send("Unknown or inactive rid");
    }

    const domain = (row.domain || "").replace(/^https?:\/\//, "");
    const slug = (row.slug || "").replace(/^\//, "");

    if (!domain || !slug) return res.status(500).send("Bad mapping (domain/slug)");

    const groups = Object.keys(row)
      .filter(k => k.startsWith("group"))
      .map(k => row[k])
      .filter(Boolean);

    const weights = (row.weights || "")
      .split(",")
      .map(w => w.trim())
      .filter(Boolean);

    const chosenGroup = pickWeightedGroup(groups, weights);
    const pickedKeywords = pickRandomKeywords(chosenGroup, 3);
    const chosenIndex = groups.indexOf(chosenGroup) + 1;

    // Modify s1pcid (add suffix)
    let s1pcidFinal = passthrough.s1pcid;
    if (s1pcidFinal) {
      s1pcidFinal = s1pcidFinal.replace(/_\d+$/, "");
      s1pcidFinal = `${s1pcidFinal}_${chosenIndex}`;
    }

    const params = new URLSearchParams({
      ...passthrough,
      s1pcid: s1pcidFinal,
      segment: row.segment || "",
      fbid: process.env.STATIC_FBID || "820262166096188",
      fbclick: process.env.STATIC_FBCLICK || "Purchase",
      ...(pickedKeywords[0] ? { forceKeyA: pickedKeywords[0] } : {}),
      ...(pickedKeywords[1] ? { forceKeyB: pickedKeywords[1] } : {}),
      ...(pickedKeywords[2] ? { forceKeyC: pickedKeywords[2] } : {})
    });

    const finalUrl = `https://${domain}/${slug}/?${params.toString()}`;
    res.setHeader("Cache-Control", "no-store");
    res.writeHead(302, { Location: finalUrl });
    res.end();

  } catch (err) {
    console.error("Redirect error:", err);
    res.status(500).send("Redirect error");
  }
}
