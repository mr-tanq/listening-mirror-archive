export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: corsHeaders(),
      });
    }

    if (url.pathname === "/" || url.pathname === "/health") {
  return json({
    ok: true,
    worker: "listening-mirror-archive",
    build_marker: "GITHUB_VARIANT_TEST_123",
    time: new Date().toISOString(),
  });
    }

    if (url.pathname === "/db-check") {
      try {
        const concertsInfo = await env.ARCHIVE_DB
          .prepare("PRAGMA table_info(concerts)")
          .all();

        const concertsCount = await env.ARCHIVE_DB
          .prepare("SELECT COUNT(*) AS total FROM concerts")
          .first();

        let setlistsInfo = { results: [] };
        let setlistsCount = { total: 0 };

        try {
          setlistsInfo = await env.ARCHIVE_DB
            .prepare("PRAGMA table_info(concert_setlists)")
            .all();

          setlistsCount = await env.ARCHIVE_DB
            .prepare("SELECT COUNT(*) AS total FROM concert_setlists")
            .first();
        } catch {}

        return json({
          ok: true,
          binding: "ARCHIVE_DB",
          schema_mode: detectSchemaMode(concertsInfo.results || []),
          concerts_columns: concertsInfo.results || [],
          concerts_total: concertsCount?.total ?? 0,
          setlists_columns: setlistsInfo.results || [],
          setlists_total: setlistsCount?.total ?? 0,
          has_setlistfm_api_key: !!String(env.SETLISTFM_API_KEY || "").trim(),
          has_lastfm_api_key: !!String(env.LASTFM_API_KEY || "").trim(),
        });
      } catch (err) {
        return json({ ok: false, error: String(err) }, 500);
      }
    }

    if (url.pathname === "/concerts") {
      try {
        const limit = Math.min(Number(url.searchParams.get("limit") || 50), 500);
        const schema = await getConcertSchema(env);

        const rows = await env.ARCHIVE_DB
          .prepare(buildConcertsQuery(schema, true))
          .bind(limit)
          .all();

        const items = (rows.results || []).map((row) => mapConcertRowToApi(row, schema));

        return json({
          ok: true,
          schema_mode: schema.mode,
          total: items.length,
          items,
        });
      } catch (err) {
        return json({ ok: false, error: String(err) }, 500);
      }
    }

    if (url.pathname === "/stats") {
      try {
        const schema = await getConcertSchema(env);

        const allRows = await env.ARCHIVE_DB
          .prepare(buildConcertsQuery(schema, false))
          .all();

        const concerts = (allRows.results || []).map((row) => mapConcertRowToApi(row, schema));

        return json({
          ok: true,
          schema_mode: schema.mode,
          overview: buildOverview(concerts),
          highlights: buildHighlights(concerts),
          top_venues: buildTopVenues(concerts, 10),
          top_cities: buildTopCities(concerts, 10),
          most_seen_artists: buildMostSeenArtists(concerts, 10),
        });
      } catch (err) {
        return json({ ok: false, error: String(err) }, 500);
      }
    }

    if (url.pathname === "/concert-note" && request.method === "POST") {
      try {
        const body = await request.json().catch(() => null);
        const eventKey = asText(body?.event_key);
        const notes = body?.notes == null ? "" : String(body.notes).trim();

        if (!eventKey) {
          return json({ ok: false, error: "Missing event_key" }, 400);
        }

        const schema = await getConcertSchema(env);

        const existing = await env.ARCHIVE_DB
          .prepare(buildFindConcertForNoteQuery(schema))
          .bind(eventKey, eventKey)
          .first();

        if (!existing?.id) {
          return json({
            ok: false,
            error: "Concert not found",
            event_key: eventKey,
            schema_mode: schema.mode,
          }, 404);
        }

        if (schema.mode === "new") {
          await env.ARCHIVE_DB
            .prepare(buildUpdateConcertNoteQuery(schema))
            .bind(notes, Date.now(), existing.id)
            .run();
        } else {
          await env.ARCHIVE_DB
            .prepare(buildUpdateConcertNoteQuery(schema))
            .bind(notes, existing.id)
            .run();
        }

        const updated = await env.ARCHIVE_DB
          .prepare(buildSingleConcertQuery(schema))
          .bind(existing.id)
          .first();

        return json({
          ok: true,
          schema_mode: schema.mode,
          event_key: eventKey,
          item: mapConcertRowToApi(updated, schema),
        });
      } catch (err) {
        return json({ ok: false, error: String(err) }, 500);
      }
    }

    if (url.pathname === "/concert-setlist" && request.method === "GET") {
      try {
        const eventKey = asText(url.searchParams.get("event_key"));
        const debug = asText(url.searchParams.get("debug")) === "1";

        if (!eventKey) {
          return json({ ok: false, error: "Missing event_key" }, 400);
        }

        const saved = await getStoredSetlist(env, eventKey);

        if (saved && hasEstimatedDurationFields(saved)) {
          return json({
            ok: true,
            event_key: eventKey,
            item: mapSetlistRow(saved),
            debug: debug ? { source: "saved_current" } : undefined,
          });
        }

        const result = await refreshConcertSetlist(env, eventKey, { debug, force: true });

        if (!result.ok) {
          return json({
            ok: true,
            event_key: eventKey,
            item: saved ? mapSetlistRow(saved) : null,
            debug: debug
              ? {
                  source: saved ? "saved_stale_refresh_failed" : "refresh_failed_no_saved",
                  refresh_error: result.error || null,
                  refresh_debug: result.debug || null,
                }
              : undefined,
          });
        }

        return json({
          ok: true,
          event_key: eventKey,
          item: result.item,
          debug: debug
            ? {
                source: saved ? "saved_stale_refreshed" : "freshly_built",
                refresh_debug: result.debug || null,
              }
            : undefined,
        });
      } catch (err) {
        return json({ ok: false, error: String(err) }, 500);
      }
    }

    if (url.pathname === "/concert-setlist-fetch" && request.method === "POST") {
      try {
        const body = await request.json().catch(() => null);
        const eventKey = asText(body?.event_key);
        const debug = body?.debug === true;

        if (!eventKey) {
          return json({ ok: false, error: "Missing event_key" }, 400);
        }

        const result = await refreshConcertSetlist(env, eventKey, { debug, force: false });

        if (!result.ok) {
          return json({
            ok: false,
            error: result.error || "No matching setlist found",
            event_key: eventKey,
            debug: debug ? result.debug || null : undefined,
          }, result.status || 404);
        }

        return json({
          ok: true,
          event_key: eventKey,
          item: result.item,
          debug: debug ? result.debug || null : undefined,
        });
      } catch (err) {
        return json({ ok: false, error: String(err) }, 500);
      }
    }

    if (url.pathname === "/concert-setlist-refresh" && request.method === "POST") {
      try {
        const body = await request.json().catch(() => null);
        const eventKey = asText(body?.event_key);
        const debug = body?.debug === true;

        if (!eventKey) {
          return json({ ok: false, error: "Missing event_key" }, 400);
        }

        const result = await refreshConcertSetlist(env, eventKey, { debug, force: true });

        if (!result.ok) {
          return json({
            ok: false,
            error: result.error || "Refresh failed",
            event_key: eventKey,
            debug: debug ? result.debug || null : undefined,
          }, result.status || 404);
        }

        return json({
          ok: true,
          event_key: eventKey,
          item: result.item,
          debug: debug ? result.debug || null : undefined,
        });
      } catch (err) {
        return json({ ok: false, error: String(err) }, 500);
      }
    }

    if (url.pathname === "/concert-setlist-refresh-get" && request.method === "GET") {
      try {
        const eventKey = asText(url.searchParams.get("event_key"));
        const debug = asText(url.searchParams.get("debug")) === "1";

        if (!eventKey) {
          return json({ ok: false, error: "Missing event_key" }, 400);
        }

        const result = await refreshConcertSetlist(env, eventKey, { debug, force: true });

        if (!result.ok) {
          return json({
            ok: false,
            error: result.error || "Refresh failed",
            event_key: eventKey,
            debug: debug ? result.debug || null : undefined,
          }, result.status || 404);
        }

        return json({
          ok: true,
          event_key: eventKey,
          item: result.item,
          debug: debug ? result.debug || null : undefined,
        });
      } catch (err) {
        return json({ ok: false, error: String(err) }, 500);
      }
    }

    return json({
      ok: false,
      error: "Not found",
      path: url.pathname,
    }, 404);
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
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
    }),
  });
}

function asText(value) {
  return String(value ?? "").trim();
}

function splitArtists(value) {
  return String(value || "")
    .split(/[,/]| \u2022 | & /)
    .map((x) => x.trim())
    .filter(Boolean);
}

function normalizeArtistName(value) {
  return asText(value).toLowerCase();
}

function titleCaseWords(value) {
  const v = asText(value);
  if (!v) return "";
  return v
    .split(" ")
    .map((p) => (p ? p[0].toUpperCase() + p.slice(1) : p))
    .join(" ");
}

function titleCaseCity(value) {
  return titleCaseWords(value);
}

function detectSchemaMode(columns) {
  const names = new Set((columns || []).map((c) => String(c?.name || "")));

  if (names.has("artists_main") && names.has("date_local") && names.has("venue_name")) {
    return "new";
  }

  if (names.has("main_artist") && names.has("date") && names.has("venue")) {
    return "legacy";
  }

  return "unknown";
}

async function getConcertSchema(env) {
  const tableInfo = await env.ARCHIVE_DB
    .prepare("PRAGMA table_info(concerts)")
    .all();

  const columns = tableInfo.results || [];
  const mode = detectSchemaMode(columns);

  if (mode === "unknown") {
    throw new Error("Unsupported concerts schema");
  }

  return { mode, columns };
}
function buildConcertsQuery(schema, withLimit) {
  if (schema.mode === "new") {
    return `
      SELECT
        id,
        title,
        artists_main,
        artists_all,
        date_local,
        time_local,
        venue_name,
        city,
        country,
        url,
        image_url,
        genre_hint,
        notes,
        source,
        source_id,
        updated_at
      FROM concerts
      ORDER BY date_local DESC, id DESC
      ${withLimit ? "LIMIT ?" : ""}
    `;
  }

  return `
    SELECT
      id,
      date,
      end_date,
      title,
      main_artist,
      supports,
      venue,
      venue_family,
      city,
      region,
      country,
      festival,
      notes,
      rating,
      event_key,
      updated_at
    FROM concerts
    ORDER BY date DESC, id DESC
    ${withLimit ? "LIMIT ?" : ""}
  `;
}

function buildSingleConcertQuery(schema) {
  if (schema.mode === "new") {
    return `
      SELECT
        id,
        title,
        artists_main,
        artists_all,
        date_local,
        time_local,
        venue_name,
        city,
        country,
        url,
        image_url,
        genre_hint,
        notes,
        source,
        source_id,
        updated_at
      FROM concerts
      WHERE id = ?
      LIMIT 1
    `;
  }

  return `
    SELECT
      id,
      date,
      end_date,
      title,
      main_artist,
      supports,
      venue,
      venue_family,
      city,
      region,
      country,
      festival,
      notes,
      rating,
      event_key,
      updated_at
    FROM concerts
    WHERE id = ?
    LIMIT 1
  `;
}

function buildFindConcertForNoteQuery(schema) {
  if (schema.mode === "new") {
    return `
      SELECT id
      FROM concerts
      WHERE source_id = ? OR id = ?
      LIMIT 1
    `;
  }

  return `
    SELECT id
    FROM concerts
    WHERE event_key = ? OR id = ?
    LIMIT 1
  `;
}

function buildFindConcertByEventKeyQuery(schema) {
  if (schema.mode === "new") {
    return `
      SELECT
        id,
        title,
        artists_main,
        artists_all,
        date_local,
        time_local,
        venue_name,
        city,
        country,
        url,
        image_url,
        genre_hint,
        notes,
        source,
        source_id,
        updated_at
      FROM concerts
      WHERE source_id = ? OR id = ?
      LIMIT 1
    `;
  }

  return `
    SELECT
      id,
      date,
      end_date,
      title,
      main_artist,
      supports,
      venue,
      venue_family,
      city,
      region,
      country,
      festival,
      notes,
      rating,
      event_key,
      updated_at
    FROM concerts
    WHERE event_key = ? OR id = ?
    LIMIT 1
  `;
}

function buildUpdateConcertNoteQuery(schema) {
  if (schema.mode === "new") {
    return `
      UPDATE concerts
      SET notes = ?, updated_at = ?
      WHERE id = ?
    `;
  }

  return `
    UPDATE concerts
    SET notes = ?
    WHERE id = ?
  `;
}

function mapConcertRowToApi(row, schema) {
  if (!row) return null;

  if (schema.mode === "new") {
    const title = asText(row.title);
    const mainArtist = asText(row.artists_main) || title;
    const allArtists = splitArtists(row.artists_all || row.artists_main || title);
    const date = asText(row.date_local);
    const venue = asText(row.venue_name);
    const city = titleCaseCity(row.city);
    const country = asText(row.country);

    const isFestival =
      /festival/i.test(title) ||
      allArtists.length > 4;

    const supports = allArtists
      .filter((name) => normalizeArtistName(name) !== normalizeArtistName(mainArtist))
      .join(", ");

    const venueFamily = normalizeVenueFamily(venue);

    return {
      id: row.id,
      date,
      end_date: date,
      title,
      main_artist: mainArtist,
      supports,
      venue,
      venue_family: venueFamily,
      city,
      region: null,
      country,
      festival: isFestival ? 1 : 0,
      notes: asText(row.notes),
      rating: null,
      event_key: asText(row.source_id || row.id),
      updated_at: row.updated_at,
      url: asText(row.url),
      image_url: asText(row.image_url),
      genre_hint: asText(row.genre_hint),
    };
  }

  return {
    id: row.id,
    date: asText(row.date),
    end_date: asText(row.end_date || row.date),
    title: asText(row.title || row.main_artist),
    main_artist: asText(row.main_artist || row.title),
    supports: asText(row.supports),
    venue: asText(row.venue),
    venue_family: asText(row.venue_family || normalizeVenueFamily(row.venue)),
    city: titleCaseCity(row.city),
    region: asText(row.region) || null,
    country: asText(row.country),
    festival: Number(row.festival || 0),
    notes: asText(row.notes),
    rating: row.rating ?? null,
    event_key: asText(row.event_key || row.id),
    updated_at: row.updated_at,
    url: "",
    image_url: "",
    genre_hint: "",
  };
}

function normalizeVenueFamily(venue) {
  const v = asText(venue);
  if (!v) return "";

  const lower = v.toLowerCase();

  if (lower.includes("de helling")) return "TivoliVredenburg";
  if (lower.includes("tivolivredenburg")) return "TivoliVredenburg";
  if (lower.includes("ronda")) return "TivoliVredenburg";
  if (lower.includes("cloud nine")) return "TivoliVredenburg";
  if (lower.includes("hertz")) return "TivoliVredenburg";
  if (lower.includes("pandora")) return "TivoliVredenburg";

  return v;
}

async function getStoredSetlist(env, eventKey) {
  return await env.ARCHIVE_DB
    .prepare(`
      SELECT
        id,
        event_key,
        source,
        source_url,
        setlist_json,
        parsed_at,
        updated_at
      FROM concert_setlists
      WHERE event_key = ?
      ORDER BY id DESC
      LIMIT 1
    `)
    .bind(eventKey)
    .first();
}

function mapSetlistRow(row) {
  if (!row) return null;

  let parsed = null;
  try {
    parsed = JSON.parse(String(row.setlist_json || "{}"));
  } catch {
    parsed = null;
  }

  return {
    id: row.id,
    event_key: asText(row.event_key),
    source: asText(row.source),
    source_url: asText(row.source_url),
    setlist: parsed,
    parsed_at: row.parsed_at ?? null,
    updated_at: row.updated_at ?? null,
  };
}

function hasEstimatedDurationFields(savedRow) {
  if (!savedRow) return false;

  try {
    const parsed = JSON.parse(String(savedRow.setlist_json || "{}"));
    return (
      Object.prototype.hasOwnProperty.call(parsed, "estimated_duration_sec") &&
      Object.prototype.hasOwnProperty.call(parsed, "matched_tracks") &&
      Object.prototype.hasOwnProperty.call(parsed, "total_tracks")
    );
  } catch {
    return false;
  }
}

async function upsertSetlist(env, input) {
  const eventKey = asText(input?.event_key);
  const source = asText(input?.source) || "setlistfm";
  const sourceUrl = asText(input?.source_url);
  const setlistJson = JSON.stringify(input?.setlist || {});
  const now = Date.now();

  const existing = await getStoredSetlist(env, eventKey);

  if (existing?.id) {
    await env.ARCHIVE_DB
      .prepare(`
        UPDATE concert_setlists
        SET source = ?, source_url = ?, setlist_json = ?, parsed_at = ?, updated_at = ?
        WHERE id = ?
      `)
      .bind(source, sourceUrl, setlistJson, now, now, existing.id)
      .run();

    const updated = await getStoredSetlist(env, eventKey);
    return mapSetlistRow(updated);
  }

  await env.ARCHIVE_DB
    .prepare(`
      INSERT INTO concert_setlists (
        event_key,
        source,
        source_url,
        setlist_json,
        parsed_at,
        updated_at
      ) VALUES (?, ?, ?, ?, ?, ?)
    `)
    .bind(eventKey, source, sourceUrl, setlistJson, now, now)
    .run();

  const inserted = await getStoredSetlist(env, eventKey);
  return mapSetlistRow(inserted);
}
async function refreshConcertSetlist(env, eventKey, { debug = false, force = false } = {}) {
  const setlistApiKey = String(env.SETLISTFM_API_KEY || "").trim();
  if (!setlistApiKey) {
    return { ok: false, error: "Missing SETLISTFM_API_KEY", status: 500 };
  }

  const schema = await getConcertSchema(env);

  const concertRow = await env.ARCHIVE_DB
    .prepare(buildFindConcertByEventKeyQuery(schema))
    .bind(eventKey, eventKey)
    .first();

  if (!concertRow) {
    return { ok: false, error: "Concert not found", status: 404 };
  }

  const existing = await getStoredSetlist(env, eventKey);

  if (existing && !force && hasEstimatedDurationFields(existing)) {
    return {
      ok: true,
      item: mapSetlistRow(existing),
      debug: debug ? { reused_saved_setlist: true } : undefined,
    };
  }

  const concert = mapConcertRowToApi(concertRow, schema);
  const fetched = await findSetlistForConcert(concert, setlistApiKey, { debug });

  if (!fetched?.ok || !fetched?.setlist) {
    return {
      ok: false,
      error: "No matching setlist found",
      status: 404,
      debug: fetched?.debug,
    };
  }

  const enrichedSetlist = await enrichSetlistWithEstimatedDuration(
    fetched.setlist,
    concert.main_artist,
    String(env.LASTFM_API_KEY || "").trim(),
    debug
  );

  const saved = await upsertSetlist(env, {
    event_key: eventKey,
    source: fetched.source || "setlistfm",
    source_url: fetched.source_url || "",
    setlist: enrichedSetlist.setlist,
  });

  return {
    ok: true,
    item: saved,
    debug: debug
      ? {
          setlist_fetch: fetched?.debug || null,
          duration_debug: enrichedSetlist?.debug || null,
        }
      : undefined,
  };
}

async function findSetlistForConcert(concert, apiKey, { debug = false } = {}) {
  const artist = asText(concert?.main_artist);
  const dateIso = asText(concert?.date);
  const city = asText(concert?.city);
  const venue = asText(concert?.venue);

  if (!artist || !dateIso) {
    return {
      ok: false,
      debug: debug ? { reason: "missing_artist_or_date" } : undefined,
    };
  }

  const dateForApi = isoDateToSetlistFmDate(dateIso);
  const year = dateIso.slice(0, 4);

  const attempts = [
    { label: "artist+date+city", params: { artistName: artist, date: dateForApi, cityName: city } },
    { label: "artist+date", params: { artistName: artist, date: dateForApi } },
    { label: "artist+year+city", params: { artistName: artist, year, cityName: city } },
    { label: "artist+year+venue", params: { artistName: artist, year, venueName: venue } },
  ];

  let best = null;
  let bestScore = -1;
  const debugAttempts = [];

  for (const attempt of attempts) {
    const params = Object.fromEntries(
      Object.entries(attempt.params).filter(([, v]) => asText(v))
    );

    if (!Object.keys(params).length) continue;

    const results = await setlistFmSearchSetlists(apiKey, params).catch(() => []);
    const scored = [];

    for (const item of results.slice(0, 20)) {
      const score = scoreSetlistCandidate(concert, item);
      scored.push({
        id: asText(item?.id),
        eventDate: asText(item?.eventDate),
        artist: asText(item?.artist?.name),
        venue: asText(item?.venue?.name),
        city: asText(item?.venue?.city?.name),
        score,
        url: asText(item?.url),
      });

      if (score > bestScore) {
        bestScore = score;
        best = item;
      }
    }

    if (debug) {
      debugAttempts.push({
        label: attempt.label,
        params,
        count: results.length,
        top: scored.slice(0, 5),
      });
    }

    if (bestScore >= 92) break;
  }

  if (!best || bestScore < 75) {
    return {
      ok: false,
      debug: debug
        ? {
            reason: "no_candidate_above_threshold",
            bestScore,
            attempts: debugAttempts,
          }
        : undefined,
    };
  }

  const normalized = normalizeSetlistFmItem(best);
  if (!normalized?.sets?.length) {
    return {
      ok: false,
      debug: debug
        ? {
            reason: "best_candidate_has_no_parsed_sets",
            bestScore,
            bestId: asText(best?.id),
            bestUrl: asText(best?.url),
            attempts: debugAttempts,
          }
        : undefined,
    };
  }

  return {
    ok: true,
    source: "setlistfm",
    source_url: asText(best?.url),
    setlist: {
      artist,
      date: dateIso,
      venue,
      city,
      sets: normalized.sets,
    },
    debug: debug
      ? {
          bestScore,
          bestId: asText(best?.id),
          bestUrl: asText(best?.url),
          attempts: debugAttempts,
        }
      : undefined,
  };
}

async function setlistFmSearchSetlists(apiKey, params) {
  const u = new URL("https://api.setlist.fm/rest/1.0/search/setlists");
  for (const [k, v] of Object.entries(params || {})) {
    if (asText(v)) u.searchParams.set(k, asText(v));
  }
  u.searchParams.set("p", "1");

  const r = await fetch(u.toString(), {
    method: "GET",
    headers: {
      "x-api-key": apiKey,
      "Accept": "application/json",
      "User-Agent": "ListeningMirror/1.0",
    },
  });

  if (!r.ok) throw new Error(`setlist.fm HTTP ${r.status}`);

  const j = await r.json().catch(() => ({}));
  const arr = j?.setlist;

  if (Array.isArray(arr)) return arr;
  if (arr && typeof arr === "object") return [arr];
  return [];
}

function scoreSetlistCandidate(concert, item) {
  const concertArtist = normalizeLoose(concert?.main_artist);
  const concertDateIso = asText(concert?.date);
  const concertDateFm = isoDateToSetlistFmDate(concertDateIso);
  const concertCity = normalizeLoose(concert?.city);
  const concertVenue = normalizeLoose(stripVenueFamilyBits(concert?.venue));

  const itemArtist = normalizeLoose(item?.artist?.name);
  const itemDateFm = asText(item?.eventDate);
  const itemCity = normalizeLoose(item?.venue?.city?.name);
  const itemVenue = normalizeLoose(stripVenueFamilyBits(item?.venue?.name));

  let score = 0;

  if (concertArtist && itemArtist) {
    if (concertArtist === itemArtist) score += 45;
    else if (concertArtist.includes(itemArtist) || itemArtist.includes(concertArtist)) score += 30;
  }

  if (concertDateFm && itemDateFm) {
    if (concertDateFm === itemDateFm) score += 35;
  }

  if (concertCity && itemCity) {
    if (concertCity === itemCity) score += 12;
    else if (concertCity.includes(itemCity) || itemCity.includes(concertCity)) score += 6;
  }

  if (concertVenue && itemVenue) {
    if (concertVenue === itemVenue) score += 18;
    else if (concertVenue.includes(itemVenue) || itemVenue.includes(concertVenue)) score += 10;
  }

  return score;
}

function normalizeSetlistFmItem(item) {
  const rawSets = item?.sets?.set;
  const setArr = Array.isArray(rawSets) ? rawSets : (rawSets ? [rawSets] : []);

  const sets = setArr
    .map((setObj, idx) => {
      const rawSongs = setObj?.song;
      const songArr = Array.isArray(rawSongs) ? rawSongs : (rawSongs ? [rawSongs] : []);

      const songs = songArr
        .map((song) => typeof song === "string" ? asText(song) : asText(song?.name))
        .filter(Boolean);

      const setName =
        asText(setObj?.name) ||
        asText(setObj?.encore) ||
        (idx === 0 ? "Set" : `Set ${idx + 1}`);

      return songs.length ? { name: setName, songs } : null;
    })
    .filter(Boolean);

  return { sets };
}
async function enrichSetlistWithEstimatedDuration(setlist, artistName, lastfmApiKey, debug = false) {
  const sets = Array.isArray(setlist?.sets) ? setlist.sets : [];
  const allSongs = sets.flatMap((s) => Array.isArray(s?.songs) ? s.songs : []).filter(Boolean);

  if (!allSongs.length) {
    return {
      setlist: {
        ...setlist,
        estimated_duration_sec: null,
        matched_tracks: 0,
        total_tracks: 0,
      },
      debug: debug ? { reason: "no_songs" } : undefined,
    };
  }

  if (!asText(lastfmApiKey)) {
    return {
      setlist: {
        ...setlist,
        estimated_duration_sec: null,
        matched_tracks: 0,
        total_tracks: allSongs.length,
      },
      debug: debug ? { reason: "missing_lastfm_api_key" } : undefined,
    };
  }

  let totalMs = 0;
  let matched = 0;
  const debugSongs = [];

  for (let i = 0; i < allSongs.length; i += 1) {
    const song = allSongs[i];
    const nextSong = i < allSongs.length - 1 ? allSongs[i + 1] : "";
    const result = await lookupBestLastfmDuration(lastfmApiKey, artistName, song, nextSong).catch(() => null);

    if (result && Number.isFinite(result.duration_ms) && result.duration_ms > 0) {
      totalMs += result.duration_ms;
      matched += 1;
    }

    if (debug) {
      debugSongs.push({
        song,
        matched: !!(result && Number.isFinite(result.duration_ms) && result.duration_ms > 0),
        duration_ms: result?.duration_ms ?? null,
        variant_used: result?.variant_used || null,
        returned_track: result?.track_name || null,
        returned_artist: result?.artist_name || null,
      });
    }
  }

  return {
    setlist: {
      ...setlist,
      estimated_duration_sec: matched ? Math.round(totalMs / 1000) : null,
      matched_tracks: matched,
      total_tracks: allSongs.length,
    },
    debug: debug
      ? {
          matched_tracks: matched,
          total_tracks: allSongs.length,
          estimated_duration_sec: matched ? Math.round(totalMs / 1000) : null,
          songs: debugSongs,
        }
      : undefined,
  };
}

async function lookupBestLastfmDuration(apiKey, artistName, songTitle, nextSong = "") {
  const variants = buildTrackLookupVariants(songTitle, nextSong);

  for (const variant of variants) {
    const result = await lookupLastfmTrackDurationMs(apiKey, artistName, variant).catch(() => null);
    if (result && Number.isFinite(result.duration_ms) && result.duration_ms > 0) {
      return {
        ...result,
        variant_used: variant,
      };
    }
  }

  return null;
}

function buildTrackLookupVariants(songTitle, nextSong = "") {
  const original = asText(songTitle);
  const next = asText(nextSong);
  const variants = [];
  const seen = new Set();

  function add(value) {
    const v = asText(value);
    if (!v) return;
    const key = v.toLowerCase();
    if (seen.has(key)) return;
    seen.add(key);
    variants.push(v);
  }

  const cleaned = normalizeFancyPunctuation(original);
  const noBoundaryEllipsis = cleaned
    .replace(/^\.\.\.\s*/, "")
    .replace(/\s*\.\.\.$/, "")
    .replace(/^…\s*/, "")
    .replace(/\s*…$/, "")
    .trim();

  add(original);
  add(cleaned);
  add(noBoundaryEllipsis);
  add(cleaned.replace(/[.:;!?'"`()[\]{}]/g, " "));
  add(cleaned.replace(/[–—-]/g, " "));
  add(cleaned.replace(/\s+/g, " ").trim());

  if (/[\u2026]|\.{3}/.test(original) || /^[.…]/.test(original) || /[.…]$/.test(original)) {
    const stripped = original
      .replace(/^\s*[.…]+\s*/, "")
      .replace(/\s*[.…]+\s*$/, "")
      .trim();
    add(stripped);
    add(normalizeFancyPunctuation(stripped));
  }

  if (original.endsWith("…") || original.endsWith("...")) {
    if (next) {
      const merged = `${original.replace(/[.…]+\s*$/, "").trim()} ${next.replace(/^\s*[.…]+\s*/, "").trim()}`.trim();
      add(merged);
      add(normalizeFancyPunctuation(merged));
    }
  }

  if (original.startsWith("…") || original.startsWith("...")) {
    add(original.replace(/^\s*[.…]+\s*/, "").trim());
  }

  return variants.filter(Boolean);
}

function normalizeFancyPunctuation(value) {
  return String(value || "")
    .replace(/\u2026/g, "...")
    .replace(/[“”]/g, '"')
    .replace(/[‘’]/g, "'")
    .replace(/[–—]/g, "-")
    .replace(/\s+/g, " ")
    .trim();
}

async function lookupLastfmTrackDurationMs(apiKey, artistName, songTitle) {
  const artist = asText(artistName);
  const track = asText(songTitle);
  if (!artist || !track || !apiKey) return null;

  const u = new URL("https://ws.audioscrobbler.com/2.0/");
  u.searchParams.set("method", "track.getInfo");
  u.searchParams.set("api_key", apiKey);
  u.searchParams.set("artist", artist);
  u.searchParams.set("track", track);
  u.searchParams.set("autocorrect", "1");
  u.searchParams.set("format", "json");

  const r = await fetch(u.toString(), {
    method: "GET",
    headers: {
      "Accept": "application/json",
      "User-Agent": "ListeningMirror/1.0",
    },
  });

  if (!r.ok) return null;

  const j = await r.json().catch(() => ({}));
  const dur = Number(j?.track?.duration);

  if (!Number.isFinite(dur) || dur <= 0) {
    return {
      duration_ms: null,
      track_name: asText(j?.track?.name),
      artist_name: asText(j?.track?.artist?.name || j?.track?.artist),
    };
  }

  return {
    duration_ms: dur,
    track_name: asText(j?.track?.name),
    artist_name: asText(j?.track?.artist?.name || j?.track?.artist),
  };
}

function normalizeLoose(value) {
  return String(value || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[()]/g, " ")
    .replace(/\b(the|a|an)\b/g, " ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function stripVenueFamilyBits(value) {
  return String(value || "")
    .replace(/^tivolivredenburg\s*\((.*?)\)$/i, "$1")
    .replace(/\s+/g, " ")
    .trim();
}

function isoDateToSetlistFmDate(iso) {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(iso || ""));
  if (!m) return "";
  return `${m[3]}-${m[2]}-${m[1]}`;
}

function buildOverview(concerts) {
  const venueSet = new Set();
  const locationSet = new Set();
  const countrySet = new Set();
  const artistSet = new Set();

  for (const c of concerts) {
    if (c.venue_family) venueSet.add(c.venue_family);
    if (c.city || c.country) locationSet.add(`${c.city}|${c.country}`);
    if (c.country) countrySet.add(c.country);

    const seen = new Set([c.main_artist, ...splitArtists(c.supports)]);
    for (const a of seen) {
      if (a) artistSet.add(a);
    }
  }

  return {
    total_concerts: concerts.length,
    total_festivals: concerts.filter((c) => Number(c.festival) === 1).length,
    venues_visited: venueSet.size,
    locations_visited: locationSet.size,
    countries_visited: countrySet.size,
    unique_artists: artistSet.size,
  };
}

function buildHighlights(concerts) {
  const mostSeenArtists = buildMostSeenArtists(concerts, 1);
  const topVenues = buildTopVenues(concerts, 1);
  const topCities = buildTopCities(concerts, 1);
  const mostActiveYear = buildMostActiveYear(concerts);

  const sortedAsc = [...concerts].sort((a, b) => String(a.date).localeCompare(String(b.date)));
  const sortedDesc = [...concerts].sort((a, b) => String(b.date).localeCompare(String(a.date)));

  return {
    most_seen_artist: mostSeenArtists[0] || null,
    top_venue: topVenues[0]
      ? { name: topVenues[0].venue_family, total: topVenues[0].visits }
      : null,
    top_city: topCities[0] || null,
    most_active_year: mostActiveYear,
    first_concert: sortedAsc[0] || null,
    latest_concert: sortedDesc[0] || null,
  };
}

function buildMostSeenArtists(concerts, limit) {
  const counts = new Map();

  for (const c of concerts) {
    const artists = new Set([c.main_artist, ...splitArtists(c.supports)]);
    for (const artist of artists) {
      if (!artist) continue;
      counts.set(artist, (counts.get(artist) || 0) + 1);
    }
  }

  return [...counts.entries()]
    .map(([name, total]) => ({ name, total }))
    .sort((a, b) => b.total - a.total || a.name.localeCompare(b.name))
    .slice(0, limit);
}

function buildTopVenues(concerts, limit) {
  const counts = new Map();

  for (const c of concerts) {
    const key = c.venue_family || c.venue;
    if (!key) continue;
    counts.set(key, (counts.get(key) || 0) + 1);
  }

  return [...counts.entries()]
    .map(([venue_family, visits]) => ({ venue_family, visits }))
    .sort((a, b) => b.visits - a.visits || a.venue_family.localeCompare(b.venue_family))
    .slice(0, limit);
}

function buildTopCities(concerts, limit) {
  const counts = new Map();

  for (const c of concerts) {
    const key = `${c.city}|${c.country}`;
    if (!c.city) continue;
    counts.set(key, (counts.get(key) || 0) + 1);
  }

  return [...counts.entries()]
    .map(([key, total]) => {
      const [city, country] = key.split("|");
      return { city, country, total };
    })
    .sort((a, b) => b.total - a.total || a.city.localeCompare(b.city))
    .slice(0, limit);
}

function buildMostActiveYear(concerts) {
  const counts = new Map();

  for (const c of concerts) {
    const year = String(c.date || "").slice(0, 4);
    if (!/^\d{4}$/.test(year)) continue;
    counts.set(year, (counts.get(year) || 0) + 1);
  }

  const top = [...counts.entries()]
    .map(([year, total]) => ({ year, total }))
    .sort((a, b) => b.total - a.total || b.year.localeCompare(a.year))[0];

  return top || null;
}
