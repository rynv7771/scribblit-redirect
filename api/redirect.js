// Simple passthrough redirect handler
// Preserves ALL query parameters including fbclid

const domains = {
  "af": "ask-finn.com"
};

export default async function handler(req, res) {
  try {
    // Parse query parameters
    const params = req.query || {};

    // Resolve domain: short code "d" (preferred) or legacy "domain" param
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
    const skipKeys = new Set(["domain", "d", "slug", "mode"]);

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
