// Passthrough redirect handler. Preserves ALL query parameters including fbclid.
//
// Optionally splits traffic by time of day: a pool listed in POOL_ROUTES goes to
// its lander inside the configured window and to a System1 RSOC article outside
// it. This exists so a campaign can run 24/7 instead of being dayparted dark —
// the advertiser's hours still gate the lander, but the off-hours click gets
// monetized on search arbitrage rather than thrown away.
//
// STRICTLY OPT-IN. A pool with no POOL_ROUTES entry takes exactly the path it
// took before this was added. gutter, roofing, pest and the existing windows
// pools are unaffected.

const domains = {
  "af": "ask-finn.com"
};

// Time-split routing, keyed by the `pool` param on the ad link.
//   window.days   — 0=Sunday .. 6=Saturday, matching Facebook's adset_schedule
//   window.*Min   — minutes past midnight in `tz`, matching adset_schedule
//                   (480 = 08:00, 1200 = 20:00)
// Inside the window we fall through to the normal domain/slug behaviour.
// Outside it we build the System1 URL below.
const POOL_ROUTES = {
  "windows-snapin": {
    window: {
      days: [1, 2, 3, 4, 5, 6],       // Mon-Sat, Sunday is RSOC all day
      startMin: 480,                   // 08:00
      endMin: 1200,                    // 20:00
      tz: "America/New_York"           // the DA windows ad account timezone
    },
    rsoc: {
      domain: "healthyfamilytip.com",
      article: "home-garden/new-windows-free-consultation-with-no-obligation-en-us",
      segment: "rsoc.gs.healthyfamilytip.001",
      // System1 fires this pixel on their side; the lander fires the same one
      // via the `windows-snapin` offer key, so the campaign sees both legs.
      pixelId: "820262166096188",
      // s1paid is the ad account digits — the account the traffic is bought from.
      s1paid: "27836702282661534",
      headline:
        "New Windows That Snap Right In. The new window goes straight into the " +
        "opening you already have. No demolition. Free in-home consultation.",
      forceKeys: {
        forceKeyA: "New Home Window Free Trial Consultation",
        forceKeyB: "Free In Home Window Consultation",
        forceKeyC: "New Window Replacement Consultation"
      }
    }
  }
};

const WEEKDAY = { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 };

// Wall-clock day and minute in an IANA timezone, without pulling in a date lib.
function nowInTz(tz) {
  const parts = Object.fromEntries(
    new Intl.DateTimeFormat("en-US", {
      timeZone: tz,
      hour12: false,
      weekday: "short",
      hour: "2-digit",
      minute: "2-digit"
    })
      .formatToParts(new Date())
      .map((p) => [p.type, p.value])
  );
  let hour = parseInt(parts.hour, 10);
  if (hour === 24) hour = 0; // hour12:false emits 24 for midnight in some ICU builds
  return {
    day: WEEKDAY[parts.weekday],
    minute: hour * 60 + parseInt(parts.minute, 10)
  };
}

function isInsideWindow(w) {
  const { day, minute } = nowInTz(w.tz);
  if (!w.days.includes(day)) return false;
  return minute >= w.startMin && minute < w.endMin;
}

// System1 RSOC destination. The revenue join is s1pcid == the FB campaign id,
// so utm_campaign has to survive onto s1pcid or the revenue never lands.
function buildRsocUrl(cfg, params) {
  const q = new URLSearchParams();
  q.set("segment", cfg.segment);
  q.set("utm_source", "facebook");
  q.set("fbid", cfg.pixelId);
  q.set("fbclick", "Purchase");
  q.set("s1paid", cfg.s1paid);

  if (params.utm_campaign) q.set("s1pcid", params.utm_campaign);
  if (params.utm_content) q.set("s1pagid", params.utm_content);
  if (params.utm_term) q.set("s1padid", params.utm_term);
  if (params.pl) {
    q.set("s1pplacement", `Facebook_${params.pl}`);
    q.set("ref", `facebook-${params.pl}`);
  } else {
    q.set("ref", "facebook");
  }

  q.set("s1particle", cfg.article.split("/").pop());
  // sub_id is reporting only; prefer the campaign name when the link carries it.
  const subId = params.cn || params.utm_campaign;
  if (subId) q.set("sub_id", subId);
  if (cfg.headline) q.set("headline", cfg.headline);

  for (const [k, v] of Object.entries(cfg.forceKeys || {})) q.set(k, v);

  // Carry the click id through so the S1 side can attribute it.
  if (params.fbclid) q.set("fbclid", params.fbclid);

  return `https://${cfg.domain}/${cfg.article}/?${q.toString()}`;
}

export default async function handler(req, res) {
  try {
    const params = req.query || {};

    // ---- Time-split routing (opt-in per pool) --------------------------------
    // force=in|out overrides the clock so both legs can be tested at any hour.
    const route = POOL_ROUTES[params.pool];
    if (route && route.window && route.rsoc) {
      const forced = params.force === "in" ? true : params.force === "out" ? false : null;
      const inside = forced !== null ? forced : isInsideWindow(route.window);
      if (!inside) {
        const rsocUrl = buildRsocUrl(route.rsoc, params);
        console.log("Redirect(rsoc):", {
          from: req.url,
          to: rsocUrl,
          pool: params.pool,
          forced: params.force || null,
          hasFbclid: params.fbclid ? "YES" : "NO"
        });
        res.setHeader("Cache-Control", "no-store");
        return res.redirect(302, rsocUrl);
      }
    }

    // ---- Normal passthrough (unchanged) -------------------------------------
    let domain;
    if (params.d) {
      domain = domains[params.d];
      if (!domain) {
        console.log("Unknown domain code:", params.d);
        return res.status(400).send(`Unknown domain code: ${params.d}`);
      }
    } else if (params.domain) {
      domain = params.domain.replace(/^https?:\/\//, "").trim();
    }

    const slug = (params.slug || "").replace(/^\//, "").trim();

    if (!domain || !slug) {
      return res.status(400).send("Missing domain or slug parameters");
    }

    // Determine mode: "aff" = passthrough only, "s1" (default) = append fbid/fbclick
    const mode = params.mode || "s1";

    // Build new URLSearchParams with ALL params except control params
    const finalParams = new URLSearchParams();
    const skipKeys = new Set(["domain", "d", "slug", "mode", "force", "cn", "pl"]);

    for (const [key, value] of Object.entries(params)) {
      if (!skipKeys.has(key)) {
        finalParams.append(key, value);
      }
    }

    // In s1 mode, add System1 pixel parameters
    if (mode === "s1") {
      finalParams.append("fbid", "820262166096188");
      finalParams.append("fbclick", "Purchase");
    }

    // Build final URL
    const finalUrl = `https://${domain}/${slug}/?${finalParams.toString()}`;

    // Log for debugging
    console.log("Redirect:", {
      from: req.url,
      to: finalUrl,
      mode,
      resolvedDomain: domain,
      domainSource: params.d ? `code:${params.d}` : "legacy",
      hasFbclid: params.fbclid ? "YES" : "NO"
    });

    // Perform redirect
    res.setHeader("Cache-Control", "no-store");
    return res.redirect(302, finalUrl);

  } catch (err) {
    console.error("Redirect error:", err);
    return res.status(500).send("Redirect error");
  }
}
