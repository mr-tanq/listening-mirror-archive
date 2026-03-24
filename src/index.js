export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === "/" || url.pathname === "/health") {
      return json({
        ok: true,
        worker: "listening-mirror-archive",
        time: new Date().toISOString()
      });
    }

    if (url.pathname === "/db-check") {
      try {
        const result = await env.ARCHIVE_DB
          .prepare("SELECT COUNT(*) AS total FROM concerts")
          .first();

        return json({
          ok: true,
          db: "connected",
          total_concerts: result?.total ?? 0
        });
      } catch (err) {
        return json({
          ok: false,
          db: "error",
          error: String(err)
        }, 500);
      }
    }

    return json({
      ok: false,
      error: "Not found",
      path: url.pathname
    }, 404);
  }
};

function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8"
    }
  });
}
