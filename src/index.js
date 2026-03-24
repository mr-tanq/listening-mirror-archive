export default {
  async fetch(request, env, ctx) {

    return new Response(
      JSON.stringify({
        ok: true,
        worker: "listening-mirror-archive",
        time: new Date().toISOString()
      }),
      {
        headers: {
          "content-type": "application/json"
        }
      }
    )

  }
}
