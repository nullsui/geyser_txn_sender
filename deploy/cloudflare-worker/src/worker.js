// Geyser geo-router — routes to nearest proxy based on Cloudflare's geo data
// Configure your backend hostnames — use DNS names (not raw IPs) so
// Cloudflare Workers can reach them. Create A records pointing to your
// VPS IPs with proxy OFF (gray cloud).
const BACKENDS = {
  US_WEST: "http://be-sjc.yourdomain.com:8080",
  ASIA: "http://be-nrt.yourdomain.com:8080",
  EUROPE: "http://be-lhr.yourdomain.com:8080",
};

// Continent codes from Cloudflare request.cf
// NA = North America, SA = South America, EU = Europe, AF = Africa,
// AS = Asia, OC = Oceania, AN = Antarctica
const CONTINENT_MAP = {
  NA: "US_WEST",
  SA: "US_WEST",
  EU: "EUROPE",
  AF: "EUROPE",
  AS: "ASIA",
  OC: "ASIA",
  AN: "US_WEST",
};

function pickBackend(request) {
  const cf = request.cf || {};
  const continent = cf.continent;
  const region = CONTINENT_MAP[continent] || "US_WEST";
  return { url: BACKENDS[region], region };
}

export default {
  async fetch(request) {
    // Health/status endpoint for the router itself
    const url = new URL(request.url);
    if (url.pathname === "/router/status") {
      return Response.json({
        status: "ok",
        backends: BACKENDS,
        your_continent: request.cf?.continent,
        your_country: request.cf?.country,
        your_city: request.cf?.city,
        routed_to: pickBackend(request).region,
      });
    }

    const { url: backendBase, region } = pickBackend(request);
    const backendUrl = backendBase + url.pathname + url.search;
    const hasBody = request.method !== "GET" && request.method !== "HEAD";

    // Build clean headers — don't forward CF-specific headers that can cause 403s
    const forwardHeaders = new Headers();
    forwardHeaders.set("Content-Type", request.headers.get("Content-Type") || "application/json");
    forwardHeaders.set("Accept", request.headers.get("Accept") || "*/*");
    if (request.headers.has("Authorization")) {
      forwardHeaders.set("Authorization", request.headers.get("Authorization"));
    }

    try {
      const response = await fetch(backendUrl, {
        method: request.method,
        headers: forwardHeaders,
        body: hasBody ? request.body : null,
      });

      const newHeaders = new Headers(response.headers);
      newHeaders.set("X-Geyser-Region", region);
      newHeaders.set("X-Geyser-Backend", backendBase);
      newHeaders.set("Access-Control-Allow-Origin", "*");

      return new Response(response.body, {
        status: response.status,
        statusText: response.statusText,
        headers: newHeaders,
      });
    } catch (err) {
      // If primary backend fails, try fallbacks
      const fallbacks = Object.entries(BACKENDS).filter(([r]) => r !== region);
      for (const [fbRegion, fbUrl] of fallbacks) {
        try {
          const response = await fetch(fbUrl + url.pathname + url.search, {
            method: request.method,
            headers: forwardHeaders,
            body: hasBody ? request.body : null,
          });
          const newHeaders = new Headers(response.headers);
          newHeaders.set("X-Geyser-Region", fbRegion);
          newHeaders.set("X-Geyser-Backend", fbUrl);
          newHeaders.set("X-Geyser-Fallback", "true");
          newHeaders.set("Access-Control-Allow-Origin", "*");
          return new Response(response.body, {
            status: response.status,
            statusText: response.statusText,
            headers: newHeaders,
          });
        } catch {
          continue;
        }
      }
      return Response.json({ error: "All backends unreachable" }, { status: 502 });
    }
  },
};
