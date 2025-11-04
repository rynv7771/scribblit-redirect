import { google } from "googleapis";

let CACHE = { rows: null, fetchedAt: 0 };
const TTL_MS = parseInt(process.env.CACHE_TTL_MS || "600000", 10);

const FALLBACK = [
  // { redirect_id: "101", domain: "adepty.co", article_slug: "home-garden/your-article", forceKeys: "example keyword 1|example keyword 2", active: "TRUE" }
];

async function fetchRowsFromSheets() {
  const now = Date.now();
  if (CACHE.rows && now - CACHE.fetchedAt < TTL_MS) return CACHE.rows;

  const email = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
  const key = (process.env.GOOGLE_PRIVATE_KEY || "").replace(/\\n/g, "\n");
  const sheetId = process.env.SHEET_ID;
  const range = process.env.SHEET_RANGE || "Mappings!A:F";

  if (!email || !key || !sheetId) {
    CACHE = { rows: FALLBACK, fetchedAt: now };
    return FALLBACK;
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
    Object.fromEntries(headers.map((h, i) => [h, (r[i] ?? "").toString()]))
  );

  CACHE = { rows, fetchedAt: now };
  return rows;
}

function pick(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

export default async function handler(req, res) {
  try {
    const { rid, ...passthrough } = req.query || {};
    if (!rid) return res.status(400).send("Missing rid");

    const rows = await fetchRowsFromSheets();
    const row = rows.find(
      r =>
        String(r.redirect_id) === String(rid) &&
        (String(r.active || "TRUE").toUpperCase() === "TRUE")
    );

    if (!row) return res.status(404).send("Unknown or inactive rid");

    const domain = (row.domain || "").replace(/^https?:\/\//, "");
    const slug = (row.article_slug || "").replace(/^\//, "");
    if (!domain || !slug) return res.status(500).send("Bad mapping (domain/slug)");

    const keys = (row.forceKeys || "")
      .split("|")
      .map(s => s.trim())
      .filter(Boolean);
    const keyword = keys.length ? pick(keys) : "";

    const params = new URLSearchParams({
      ...passthrough,
      s1particle: slug,
      ...(keyword ? { forceKeyA: keyword } : {})
    });

    const finalUrl = `https://${domain}/${slug}/?${params.toString()}`;
    res.setHeader("Cache-Control", "no-store");
    res.status(302).setHeader("Location", finalUrl).end();
  } catch (err) {
    console.error(err);
    res.status(500).send("Redirect error");
  }
}
