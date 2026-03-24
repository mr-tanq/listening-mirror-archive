export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: corsHeaders(),
      });
    }

    if (url.pathname === "/" || url.pathname === "/health") {
      return json(
        {
          ok: true,
          service: "listening-mirror-archive",
          message: "Archive worker is alive",
          hasArchiveDb: !!env.ARCHIVE_DB,
          timestamp: new Date().toISOString(),
        },
        200
      );
    }

    return json(
      {
        ok: false,
        error: "Not found",
        path: url.pathname,
      },
      404
    );
  },
};

function corsHeaders(extra = {}) {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
    ...extra,
  };
}

function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: corsHeaders({
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store",
    }),
  });
}
