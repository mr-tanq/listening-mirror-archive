import { seedArchiveToDb, getSeedPreview } from "./archive-seed.js";

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
        build_marker: "ARCHIVE_CONCERTS_V5_FESTIVAL_MATCHING",
        time: new Date().toISOString(),
      });
    }

    if (url.pathname === "/seed-preview") {
      return json({
        ok: true,
        source: "seed.js",
        preview: getSeedPreview(10),
      });
    }

    if (url.pathname === "/seed-import") {
      try {
        const providedKey = String(url.searchParams.get("key") || "").trim();
        const expectedKey = String(env.SEED_IMPORT_KEY || "").trim();

        if (!expectedKey) {
          return json({ ok: false, error: "Missing SEED_IMPORT_KEY" }, 500);
        }

        if (!providedKey || providedKey !== expectedKey) {
          return json({ ok: false, error: "Unauthorized" }, 401);
        }

        const result = await seedArchiveToDb(env);
        return json(result, result.ok ? 200 : 207);
      } catch (err) {
        return json({ ok: false, error: String(err) }, 500);
      }
    }

    if (url.pathname === "/db-check") {
      try {
        const archiveInfo = await env.ARCHIVE_DB
          .prepare("PRAGMA table_info(archive_concerts)")
          .all();

        const archiveCount = await env.ARCHIVE_DB
          .prepare("SELECT COUNT(*) AS total FROM archive_concerts")
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
          archive_columns: archiveInfo.results || [],
          archive_total: archiveCount?.total ?? 0,
          setlists_columns: setlistsInfo.results || [],
          setlists_total: setlistsCount?.total ?? 0,
          has_setlistfm_api_key: !!String(env.SETLISTFM_API_KEY || "").trim(),
          has_lastfm_api_key: !!String(env.LASTFM_API_KEY || "").trim(),
          has_seed_import_key: !!String(env.SEED_IMPORT_KEY || "").trim(),
        });
      } catch (err) {
        return json({ ok: false, error: String(err) }, 500);
      }
    }

    if (url.pathname === "/concerts") {
      try {
        const limit = Math.min(Number(url.searchParams.get("limit") || 50), 1000);

        const rows = await env.ARCHIVE_DB
          .prepare(`
            SELECT
              id,
              event_key,
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
              url,
              image_url,
              genre_hint,
              created_at,
              updated_at
            FROM archive_concerts
            ORDER BY date DESC, id DESC
            LIMIT ?
          `)
          .bind(limit)
          .all();

        const items = (rows.results || []).map(mapArchiveConcertRow);

        return json({
          ok: true,
          total: items.length,
          items,
        });
      } catch (err) {
        return json({ ok: false, error: String(err) }, 500);
      }
    }

    if (url.pathname === "/stats") {
      try {
        const rows = await env.ARCHIVE_DB
          .prepare(`
            SELECT
              id,
              event_key,
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
              url,
              image_url,
              genre_hint,
              created_at,
              updated_at
            FROM archive_concerts
            ORDER BY date DESC, id DESC
          `)
          .all();

        const concerts = (rows.results || []).map(mapArchiveConcertRow);

        return json({
          ok: true,
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

        const existing = await env.ARCHIVE_DB
          .prepare(`
            SELECT id
            FROM archive_concerts
            WHERE event_key = ?
            LIMIT 1
          `)
          .bind(eventKey)
          .first();

        if (!existing?.id) {
          return json({
            ok: false,
            error: "Concert not found",
            event_key: eventKey,
          }, 404);
        }

        await env.ARCHIVE_DB
          .prepare(`
            UPDATE archive_concerts
            SET notes = ?, updated_at = ?
            WHERE id = ?
          `)
          .bind(notes, Date.now(), existing.id)
          .run();

        const updated = await env.ARCHIVE_DB
          .prepare(`
            SELECT
              id,
              event_key,
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
              url,
              image_url,
              genre_hint,
              created_at,
              updated_at
            FROM archive_concerts
            WHERE id = ?
            LIMIT 1
          `)
          .bind(existing.id)
          .first();

        return json({
          ok: true,
          event_key: eventKey,
          item: mapArchiveConcertRow(updated),
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

    if (url.pathname === "/setlists-backfill" && request.method === "GET") {
      try {
        const providedKey = String(url.searchParams.get("key") || "").trim();
        const expectedKey = String(env.SEED_IMPORT_KEY || "").trim();
        const limit = Math.max(1, Math.min(Number(url.searchParams.get("limit") || 10), 50));
        const offset = Math.max(0, Number(url.searchParams.get("offset") || 0));

        if (!expectedKey) {
          return json({ ok: false, error: "Missing SEED_IMPORT_KEY" }, 500);
        }

        if (!providedKey || providedKey !== expectedKey) {
          return json({ ok: false, error: "Unauthorized" }, 401);
        }

        const rows = await env.ARCHIVE_DB
          .prepare(`
            SELECT
              event_key,
              date,
              main_artist,
              supports,
              venue,
              city,
              festival
            FROM archive_concerts
            ORDER BY date DESC, id DESC
            LIMIT ? OFFSET ?
          `)
          .bind(limit, offset)
          .all();

        const items = rows.results || [];
        let attempted = 0;
        let saved = 0;
        let failed = 0;
        const results = [];

        for (const row of items) {
          const eventKey = asText(row?.event_key);
          if (!eventKey) continue;

          attempted += 1;

          try {
            const result = await refreshConcertSetlist(env, eventKey, { debug: false, force: false });

            if (result?.ok) {
              saved += 1;
              results.push({
                event_key: eventKey,
                status: "saved",
                artist: asText(row?.main_artist),
                supports: asText(row?.supports),
                date: asText(row?.date),
                city: asText(row?.city),
                venue: asText(row?.venue),
                festival: Number(row?.festival || 0),
              });
            } else {
              failed += 1;
              results.push({
                event_key: eventKey,
                status: "failed",
                error: asText(result?.error || "Unknown error"),
                artist: asText(row?.main_artist),
                supports: asText(row?.supports),
                date: asText(row?.date),
                city: asText(row?.city),
                venue: asText(row?.venue),
                festival: Number(row?.festival || 0),
              });
            }
          } catch (err) {
            failed += 1;
            results.push({
              event_key: eventKey,
              status: "failed",
              error: String(err),
              artist: asText(row?.main_artist),
              supports: asText(row?.supports),
              date: asText(row?.date),
              city: asText(row?.city),
              venue: asText(row?.venue),
              festival: Number(row?.festival || 0),
            });
          }
        }

        const setlistsCount = await env.ARCHIVE_DB
          .prepare(`SELECT COUNT(*) AS total FROM concert_setlists`)
          .first();

        return json({
          ok: true,
          attempted,
          saved,
          failed,
          limit,
          offset,
          next_offset: offset + items.length,
          has_more: items.length === limit,
          setlists_total: Number(setlistsCount?.total ?? 0),
          results,
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
  if (Array.isArray(value)) {
    return value.map((x) => asText(x)).filter(Boolean);
  }

  const raw = String(value || "").trim();
  if (!raw) return [];

  if ((raw.startsWith("[") && raw.endsWith("]")) || (raw.startsWith('"[') && raw.endsWith(']"'))) {
    try {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) {
        return parsed.map((x) => asText(x)).filter(Boolean);
      }
    } catch {}
  }

  return raw
    .split(/[,/]| \u2022 | & /)
    .map((x) => x.trim())
    .filter(Boolean);
}

function titleCaseWords(value) {
  const v = asText(value);
  if (!v) return "";
  return v
    .split(" ")
    .map((p) => (p ? p[0].toUpperCase() + p.slice(1) : p))
    .join(" ");
}

function mapArchiveConcertRow(row) {
  if (!row) return null;

  const mainArtist = asText(row.main_artist) || asText(row.title);
  const supportsArr = splitArtists(row.supports).filter(
    (x) => x.toLowerCase() !== mainArtist.toLowerCase()
  );

  return {
    id: row.id,
    date: asText(row.date),
    end_date: asText(row.end_date || row.date),
    title: asText(row.title || row.main_artist),
    main_artist: mainArtist,
    supports: supportsArr.join(", "),
    venue: asText(row.venue),
    venue_family: asText(row.venue_family || row.venue),
    city: titleCaseWords(row.city),
    region: asText(row.region) || null,
    country: asText(row.country),
    festival: Number(row.festival || 0),
    notes: asText(row.notes),
    rating: row.rating ?? null,
    event_key: asText(row.event_key || row.id),
    updated_at: row.updated_at,
    url: asText(row.url),
    image_url: asText(row.image_url),
    genre_hint: asText(row.genre_hint),
  };
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

    if (parsed?.kind === "multi_artist" && Array.isArray(parsed?.artist_setlists)) {
      return parsed.artist_setlists.every((x) =>
        Object.prototype.hasOwnProperty.call(x || {}, "estimated_duration_sec") &&
        Object.prototype.hasOwnProperty.call(x || {}, "matched_tracks") &&
        Object.prototype.hasOwnProperty.call(x || {}, "total_tracks")
      );
    }

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

  const concertRow = await env.ARCHIVE_DB
    .prepare(`
      SELECT
        id,
        event_key,
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
        url,
        image_url,
        genre_hint,
        created_at,
        updated_at
      FROM archive_concerts
      WHERE event_key = ?
      LIMIT 1
    `)
    .bind(eventKey)
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

  const concert = mapArchiveConcertRow(concertRow);

  const artistsToResolve = buildArtistsForConcert(concert);
  const artistResults = [];
  const artistSetlists = [];

  for (const artistEntry of artistsToResolve) {
    const fetched = await findSetlistForArtistAtConcert(concert, artistEntry, setlistApiKey, { debug });

    if (!fetched?.ok || !fetched?.setlist) {
      if (debug) {
        artistResults.push({
          artist: artistEntry.artist,
          role: artistEntry.role,
          ok: false,
          debug: fetched?.debug || null,
        });
      }
      continue;
    }

    const enriched = await enrichSetlistWithEstimatedDuration(
      fetched.setlist,
      artistEntry.artist,
      String(env.LASTFM_API_KEY || "").trim(),
      debug
    );

    artistSetlists.push({
      artist: artistEntry.artist,
      role: artistEntry.role,
      source: fetched.source || "setlistfm",
      source_url: fetched.source_url || "",
      sets: enriched.setlist.sets || [],
      estimated_duration_sec: enriched.setlist.estimated_duration_sec ?? null,
      matched_tracks: enriched.setlist.matched_tracks ?? 0,
      total_tracks: enriched.setlist.total_tracks ?? 0,
    });

    if (debug) {
      artistResults.push({
        artist: artistEntry.artist,
        role: artistEntry.role,
        ok: true,
        fetch_debug: fetched?.debug || null,
        duration_debug: enriched?.debug || null,
      });
    }
  }

  if (!artistSetlists.length) {
    return {
      ok: false,
      error: "No matching setlist found",
      status: 404,
      debug: debug ? { artists: artistResults } : undefined,
    };
  }

  const primary = artistSetlists[0] || null;
  const multiSetlist = {
    kind: "multi_artist",
    event_title: concert.title || concert.main_artist,
    date: concert.date,
    venue: concert.venue,
    city: concert.city,
    festival: Number(concert.festival || 0),
    artist_setlists: artistSetlists,
    estimated_duration_sec: artistSetlists.reduce((sum, x) => sum + Number(x.estimated_duration_sec || 0), 0) || null,
    matched_tracks: artistSetlists.reduce((sum, x) => sum + Number(x.matched_tracks || 0), 0),
    total_tracks: artistSetlists.reduce((sum, x) => sum + Number(x.total_tracks || 0), 0),
    source_url: primary?.source_url || "",
  };

  const saved = await upsertSetlist(env, {
    event_key: eventKey,
    source: "setlistfm",
    source_url: primary?.source_url || "",
    setlist: multiSetlist,
  });

  return {
    ok: true,
    item: saved,
    debug: debug ? { artists: artistResults } : undefined,
  };
}

function buildArtistsForConcert(concert) {
  const out = [];
  const seen = new Set();

  function add(artist, role) {
    const a = asText(artist);
    if (!a) return;
    const key = a.toLowerCase();
    if (seen.has(key)) return;
    seen.add(key);
    out.push({ artist: a, role });
  }

  add(concert.main_artist, "main");

  const supports = splitArtists(concert.supports);
  for (const s of supports) {
    add(s, Number(concert.festival || 0) === 1 ? "festival" : "support");
  }

  return out;
}

async function findSetlistForArtistAtConcert(concert, artistEntry, apiKey, { debug = false } = {}) {
  const artist = asText(artistEntry?.artist);
  const role = asText(artistEntry?.role) || "support";
  const dateIso = asText(concert?.date);
  const city = asText(concert?.city);
  const venue = asText(concert?.venue);
  const isFestival = Number(concert?.festival || 0) === 1;

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

    for (const item of results.slice(0, 25)) {
const evalResult = scoreSetlistCandidate(concert, item, {
  role,
  isFestival,
  targetArtist: artist
});    
      const score = evalResult.score;

      scored.push({
        id: asText(item?.id),
        eventDate: asText(item?.eventDate),
        artist: asText(item?.artist?.name),
        venue: asText(item?.venue?.name),
        city: asText(item?.venue?.city?.name),
        score,
        rejected: !!evalResult.rejected,
        rejection_reason: evalResult.rejection_reason || null,
        url: asText(item?.url),
      });

      if (evalResult.rejected) continue;

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

    if (!isFestival && bestScore >= 92) break;
    if (isFestival && bestScore >= 88) break;
  }

  const minScore = isFestival ? 82 : 75;

  if (!best || bestScore < minScore) {
    return {
      ok: false,
      debug: debug
        ? {
            reason: "no_candidate_above_threshold",
            bestScore,
            minScore,
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

function scoreSetlistCandidate(concert, item, { role = "support", isFestival = false, targetArtist = "" } = {}) {  const concertArtist = normalizeLoose(itemSafeArtist(concert?.main_artist));
  const expectedDate = asText(concert?.date);
  const concertDateFm = isoDateToSetlistFmDate(expectedDate);
  const concertCity = normalizeLoose(concert?.city);
  const concertVenue = normalizeLoose(concert?.venue);
  const concertCountry = normalizeLoose(concert?.country);

  const itemArtist = normalizeLoose(item?.artist?.name);
  const itemDateFm = asText(item?.eventDate);
  const itemCity = normalizeLoose(item?.venue?.city?.name);
  const itemVenue = normalizeLoose(item?.venue?.name);
  const itemCountry = normalizeLoose(item?.venue?.city?.country?.name || item?.venue?.city?.country?.code);

  let score = 0;

  if (concertArtist && itemArtist) {
    if (concertArtist === itemArtist) score += 45;
    else if (concertArtist.includes(itemArtist) || itemArtist.includes(concertArtist)) score += 28;
  }

  const dayDiff = dateDiffFromSetlistFm(itemDateFm, concertDateFm);

  if (dayDiff === 0) score += 35;
  else if (isFestival && dayDiff === 1) score += 18;
  else if (isFestival && dayDiff === 2) score += 8;
  else if (!isFestival && dayDiff === 1) score += 4;

  if (concertCity && itemCity) {
    if (concertCity === itemCity) score += 22;
    else if (concertCity.includes(itemCity) || itemCity.includes(concertCity)) score += 10;
  }

  if (concertVenue && itemVenue) {
    if (concertVenue === itemVenue) score += 18;
    else if (concertVenue.includes(itemVenue) || itemVenue.includes(concertVenue)) score += 10;
    else if (isFestival && isStageLikeVenue(itemVenue)) score += 6;
  }

  if (concertCountry && itemCountry) {
    if (concertCountry === itemCountry) score += 8;
  }

  if (role === "main") score += 2;
  if (role === "festival") score -= 2;

  if (isFestival) {
    const wrongCountry = concertCountry && itemCountry && concertCountry !== itemCountry;
    const wrongCitySameDay = dayDiff === 0 && concertCity && itemCity && concertCity !== itemCity;
    const noLocalSignal =
      !(concertCity && itemCity && concertCity === itemCity) &&
      !(concertVenue && itemVenue && (concertVenue === itemVenue || concertVenue.includes(itemVenue) || itemVenue.includes(concertVenue))) &&
      !isStageLikeVenue(itemVenue);

    if (wrongCountry && dayDiff <= 1 && noLocalSignal) {
      return {
        score: -1,
        rejected: true,
        rejection_reason: "festival_wrong_country_no_local_signal",
      };
    }

    if (wrongCitySameDay && noLocalSignal) {
      return {
        score: -1,
        rejected: true,
        rejection_reason: "festival_same_day_wrong_city_no_local_signal",
      };
    }

    if (dayDiff > 2) {
      return {
        score: -1,
        rejected: true,
        rejection_reason: "festival_too_far_in_date",
      };
    }
  }

  return { score, rejected: false, rejection_reason: null };
}

function itemSafeArtist(value) {
  return String(value || "");
}

function isStageLikeVenue(value) {
  const v = normalizeLoose(value);
  if (!v) return false;
  return (
    v.includes("stage") ||
    v.includes("main stage") ||
    v.includes("tent") ||
    v.includes("arena") ||
    v.includes("festival") ||
    v.includes("open air")
  );
}

function parseSetlistFmDate(value) {
  const m = /^(\d{2})-(\d{2})-(\d{4})$/.exec(String(value || "").trim());
  if (!m) return null;
  return Date.UTC(Number(m[3]), Number(m[2]) - 1, Number(m[1]));
}

function dateDiffFromSetlistFm(a, b) {
  const da = parseSetlistFmDate(a);
  const db = parseSetlistFmDate(b);
  if (da == null || db == null) return 999;
  return Math.abs(Math.round((da - db) / 86400000));
}

function normalizeSetlistFmItem(item) {
  const rawSets = item?.sets?.set;
  const setArr = Array.isArray(rawSets) ? rawSets : (rawSets ? [rawSets] : []);

  const sets = setArr
    .map((setObj, idx) => {
      const rawSongs = setObj?.song;
      const songArr = Array.isArray(rawSongs) ? rawSongs : (rawSongs ? [rawSongs] : []);
      const songs = songArr
        .map((song) => (typeof song === "string" ? asText(song) : asText(song?.name)))
        .filter(Boolean);

      const setName = asText(setObj?.name) || (idx === 0 ? "Set" : `Set ${idx + 1}`);
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
      setlist: { ...setlist, estimated_duration_sec: null, matched_tracks: 0, total_tracks: 0 },
      debug: debug ? { reason: "no_songs" } : undefined,
    };
  }

  if (!asText(lastfmApiKey)) {
    return {
      setlist: { ...setlist, estimated_duration_sec: null, matched_tracks: 0, total_tracks: allSongs.length },
      debug: debug ? { reason: "missing_lastfm_api_key" } : undefined,
    };
  }

  let totalMs = 0;
  let matched = 0;
  const debugSongs = [];

  for (let i = 0; i < allSongs.length; i += 1) {
    const song = allSongs[i];
    const nextSong = i < allSongs.length - 1 ? allSongs[i + 1] : "";
    const result = await lookupBestLastfmDuration(lastfmApiKey, artistName, song, nextSong, debug).catch(() => null);

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
        method: result?.method || null,
        returned_track: result?.track_name || null,
        returned_artist: result?.artist_name || null,
        score: result?.score ?? null,
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

async function lookupBestLastfmDuration(apiKey, artistName, songTitle, nextSong = "", debug = false) {
  const variants = buildTrackLookupVariants(songTitle, nextSong);

  for (const variant of variants) {
    const exact = await lookupLastfmTrackDurationMs(apiKey, artistName, variant).catch(() => null);
    if (exact && Number.isFinite(exact.duration_ms) && exact.duration_ms > 0) {
      return {
        ...exact,
        variant_used: variant,
        method: "track.getInfo",
        score: 100,
      };
    }

    const searched = await searchLastfmTrackDurationMs(apiKey, artistName, variant, debug).catch(() => null);
    if (searched && Number.isFinite(searched.duration_ms) && searched.duration_ms > 0) {
      return {
        ...searched,
        variant_used: variant,
        method: "track.search",
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

  function add(v) {
    const x = asText(v);
    if (!x) return;
    const k = x.toLowerCase();
    if (seen.has(k)) return;
    seen.add(k);
    variants.push(x);
  }

  const cleaned = normalizeFancyPunctuation(original);
  const stripped = cleaned
    .replace(/^\.\.\.\s*|^\u2026\s*/g, "")
    .replace(/\s*\.\.\.$|\s*\u2026$/g, "")
    .trim();

  const noPunct = cleaned.replace(/[.:;!?'"`()[\]{}]/g, " ").replace(/\s+/g, " ").trim();
  const noDash = cleaned.replace(/[–—-]/g, " ").replace(/\s+/g, " ").trim();
  const noSlash = cleaned.replace(/[\\/]/g, " ").replace(/\s+/g, " ").trim();
  const noFeat = cleaned.replace(/\b(feat|ft)\.?\b.*$/i, "").trim();
  const noLive = cleaned.replace(/\b(live|edit|version|remaster(ed)?)\b/gi, " ").replace(/\s+/g, " ").trim();

  add(original);
  add(cleaned);
  add(stripped);
  add(noPunct);
  add(noDash);
  add(noSlash);
  add(noFeat);
  add(noLive);

  if ((original.endsWith("…") || original.endsWith("...")) && next) {
    const merged = `${original.replace(/[.…]+\s*$/, "").trim()} ${next.replace(/^\s*[.…]+\s*/, "").trim()}`.trim();
    add(merged);
    add(normalizeFancyPunctuation(merged));
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

  const r = await fetch(u.toString(), { method: "GET" });
  if (!r.ok) return null;

  const j = await r.json().catch(() => ({}));
  const dur = Number(j?.track?.duration);

  return {
    duration_ms: Number.isFinite(dur) && dur > 0 ? dur : null,
    track_name: asText(j?.track?.name),
    artist_name: asText(j?.track?.artist?.name || j?.track?.artist),
  };
}

async function searchLastfmTrackDurationMs(apiKey, artistName, songTitle, debug = false) {
  const artist = asText(artistName);
  const track = asText(songTitle);
  if (!artist || !track || !apiKey) return null;

  const u = new URL("https://ws.audioscrobbler.com/2.0/");
  u.searchParams.set("method", "track.search");
  u.searchParams.set("api_key", apiKey);
  u.searchParams.set("track", track);
  u.searchParams.set("limit", "10");
  u.searchParams.set("format", "json");

  const r = await fetch(u.toString(), { method: "GET" });
  if (!r.ok) return null;

  const j = await r.json().catch(() => ({}));
  const rawMatches = j?.results?.trackmatches?.track;
  const matches = Array.isArray(rawMatches) ? rawMatches : (rawMatches ? [rawMatches] : []);
  if (!matches.length) return null;

  let best = null;
  let bestScore = -1;

  for (const item of matches) {
    const candidateTrack = asText(item?.name);
    const candidateArtist = asText(item?.artist);
    const score = scoreLastfmTrackCandidate(artist, track, candidateArtist, candidateTrack);

    if (score > bestScore) {
      bestScore = score;
      best = item;
    }
  }

  if (!best || bestScore < 78) {
    return null;
  }

  const dur = Number(best?.duration);
  return {
    duration_ms: Number.isFinite(dur) && dur > 0 ? dur : null,
    track_name: asText(best?.name),
    artist_name: asText(best?.artist),
    score: bestScore,
    search_debug: debug
      ? matches.slice(0, 5).map((x) => ({
          track: asText(x?.name),
          artist: asText(x?.artist),
          duration: Number(x?.duration || 0) || null,
          score: scoreLastfmTrackCandidate(artist, track, asText(x?.artist), asText(x?.name)),
        }))
      : undefined,
  };
}

function scoreLastfmTrackCandidate(targetArtist, targetTrack, candidateArtist, candidateTrack) {
  const a1 = normalizeLoose(targetArtist);
  const a2 = normalizeLoose(candidateArtist);
  const t1 = normalizeTrackTitleForCompare(targetTrack);
  const t2 = normalizeTrackTitleForCompare(candidateTrack);

  let score = 0;

  if (a1 && a2) {
    if (a1 === a2) score += 40;
    else if (a1.includes(a2) || a2.includes(a1)) score += 28;
  }

  if (t1 && t2) {
    if (t1 === t2) score += 60;
    else if (t1.includes(t2) || t2.includes(t1)) score += 42;
    else {
      const overlap = tokenOverlapScore(t1, t2);
      score += Math.round(overlap * 35);
    }
  }

  return score;
}

function tokenOverlapScore(a, b) {
  const aSet = new Set(String(a).split(" ").filter(Boolean));
  const bSet = new Set(String(b).split(" ").filter(Boolean));
  if (!aSet.size || !bSet.size) return 0;

  let same = 0;
  for (const token of aSet) {
    if (bSet.has(token)) same += 1;
  }

  return same / Math.max(aSet.size, bSet.size);
}

function normalizeTrackTitleForCompare(value) {
  return String(value || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\b(feat|ft)\.?\b.*$/i, " ")
    .replace(/\b(live|remaster(ed)?|edit|version|instrumental|mono|stereo)\b/gi, " ")
    .replace(/[“”‘’]/g, "")
    .replace(/[(){}[\]]/g, " ")
    .replace(/[\\/|]/g, " ")
    .replace(/[–—-]/g, " ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeLoose(value) {
  return String(value || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[()]/g, " ")
    .replace(/[^a-z0-9]+/g, " ")
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
    for (const a of new Set([c.main_artist, ...splitArtists(c.supports)])) {
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
    top_venue: topVenues[0] ? { name: topVenues[0].venue_family, total: topVenues[0].visits } : null,
    top_city: topCities[0] || null,
    most_active_year: mostActiveYear,
    first_concert: sortedAsc[0] || null,
    latest_concert: sortedDesc[0] || null,
  };
}

function buildMostSeenArtists(concerts, limit) {
  const counts = new Map();
  for (const c of concerts) {
    for (const artist of new Set([c.main_artist, ...splitArtists(c.supports)])) {
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
    if (!c.city) continue;
    const key = `${c.city}|${c.country}`;
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
  return [...counts.entries()]
    .map(([year, total]) => ({ year, total }))
    .sort((a, b) => b.total - a.total || b.year.localeCompare(a.year))[0] || null;
                                                }
