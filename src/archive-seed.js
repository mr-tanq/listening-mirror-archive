import seed from "../seed.js";

function asText(value) {
  return String(value ?? "").trim();
}

function normalizeWhitespace(value) {
  return asText(value).replace(/\s+/g, " ").trim();
}

function uniqueStrings(values) {
  const out = [];
  const seen = new Set();

  for (const value of values || []) {
    const v = normalizeWhitespace(value);
    if (!v) continue;
    const key = v.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(v);
  }

  return out;
}

function sortArtists(artists) {
  return [...(Array.isArray(artists) ? artists : [])].sort((a, b) => {
    const ao = Number(a?.sort_order ?? 9999);
    const bo = Number(b?.sort_order ?? 9999);
    if (ao !== bo) return ao - bo;
    return asText(a?.name).localeCompare(asText(b?.name));
  });
}

function pickMainArtist(entry) {
  const artists = sortArtists(entry?.artists || []);
  const headliner =
    artists.find((a) => asText(a?.role).toLowerCase() === "headliner") ||
    artists.find((a) => asText(a?.role).toLowerCase() === "festival") ||
    artists[0] ||
    null;

  return normalizeWhitespace(headliner?.name || entry?.title || "");
}

function buildArtistsAll(entry) {
  const artists = sortArtists(entry?.artists || []).map((a) => a?.name);
  return uniqueStrings(artists).join(", ");
}

function isFestivalEntry(entry) {
  const kind = asText(entry?.kind).toLowerCase();
  return kind === "festival" || !!asText(entry?.festival_name);
}

function mapSeedEntryToConcertRow(entry, nowTs) {
  const eventKey = asText(entry?.event_key);
  const title = normalizeWhitespace(entry?.title);
  const mainArtist = pickMainArtist(entry);
  const artistsAll = buildArtistsAll(entry);
  const dateLocal = asText(entry?.start_date);
  const timeLocal = "";
  const venueName = normalizeWhitespace(entry?.venue?.raw_name || entry?.venue?.family_name);
  const city = normalizeWhitespace(entry?.city);
  const country = normalizeWhitespace(entry?.country);
  const imageUrl = "";
  const url = "";
  const genreHint = isFestivalEntry(entry)
    ? normalizeWhitespace(entry?.festival_name || entry?.title)
    : "";

  if (!eventKey) {
    throw new Error(`Seed entry missing event_key: ${JSON.stringify(entry)}`);
  }
  if (!title) {
    throw new Error(`Seed entry missing title for ${eventKey}`);
  }
  if (!dateLocal) {
    throw new Error(`Seed entry missing start_date for ${eventKey}`);
  }

  return {
    id: eventKey,
    source: "seed",
    source_id: eventKey,
    title,
    artists_main: mainArtist || title,
    artists_all: artistsAll || mainArtist || title,
    raw_title: title,
    date_local: dateLocal,
    time_local: timeLocal,
    venue_name: venueName,
    city,
    country,
    url,
    image_url: imageUrl,
    genre_hint: genreHint,
    fetched_at: nowTs,
    created_at: nowTs,
    updated_at: nowTs,
  };
}

async function ensureArchiveTables(env) {
  await env.ARCHIVE_DB.prepare(`
    CREATE TABLE IF NOT EXISTS concerts (
      id TEXT PRIMARY KEY,
      source TEXT NOT NULL,
      source_id TEXT NOT NULL,
      title TEXT NOT NULL,
      artists_main TEXT,
      artists_all TEXT,
      raw_title TEXT,
      date_local TEXT,
      time_local TEXT,
      venue_name TEXT,
      city TEXT,
      country TEXT,
      url TEXT NOT NULL DEFAULT '',
      image_url TEXT,
      genre_hint TEXT,
      fetched_at INTEGER,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    )
  `).run();

  await env.ARCHIVE_DB.prepare(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_concerts_source_source_id
    ON concerts(source, source_id)
  `).run();

  await env.ARCHIVE_DB.prepare(`
    CREATE INDEX IF NOT EXISTS idx_concerts_date_local
    ON concerts(date_local)
  `).run();

  await env.ARCHIVE_DB.prepare(`
    CREATE TABLE IF NOT EXISTS concert_setlists (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      event_key TEXT NOT NULL UNIQUE,
      source TEXT,
      source_url TEXT,
      setlist_json TEXT NOT NULL,
      parsed_at INTEGER,
      updated_at INTEGER
    )
  `).run();
}

async function upsertConcert(env, row) {
  const existing = await env.ARCHIVE_DB.prepare(`
    SELECT id, created_at
    FROM concerts
    WHERE id = ?
    LIMIT 1
  `).bind(row.id).first();

  const createdAt = existing?.created_at ?? row.created_at;

  await env.ARCHIVE_DB.prepare(`
    INSERT INTO concerts (
      id,
      source,
      source_id,
      title,
      artists_main,
      artists_all,
      raw_title,
      date_local,
      time_local,
      venue_name,
      city,
      country,
      url,
      image_url,
      genre_hint,
      fetched_at,
      created_at,
      updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
      source = excluded.source,
      source_id = excluded.source_id,
      title = excluded.title,
      artists_main = excluded.artists_main,
      artists_all = excluded.artists_all,
      raw_title = excluded.raw_title,
      date_local = excluded.date_local,
      time_local = excluded.time_local,
      venue_name = excluded.venue_name,
      city = excluded.city,
      country = excluded.country,
      url = excluded.url,
      image_url = excluded.image_url,
      genre_hint = excluded.genre_hint,
      fetched_at = excluded.fetched_at,
      updated_at = excluded.updated_at
  `).bind(
    row.id,
    row.source,
    row.source_id,
    row.title,
    row.artists_main,
    row.artists_all,
    row.raw_title,
    row.date_local,
    row.time_local,
    row.venue_name,
    row.city,
    row.country,
    row.url,
    row.image_url,
    row.genre_hint,
    row.fetched_at,
    createdAt,
    row.updated_at
  ).run();

  return {
    event_key: row.id,
    existed: !!existing,
  };
}

export async function seedArchiveToDb(env) {
  if (!env?.ARCHIVE_DB) {
    throw new Error("Missing ARCHIVE_DB binding");
  }

  await ensureArchiveTables(env);

  const items = Array.isArray(seed) ? seed : [];
  const nowTs = Date.now();

  let inserted = 0;
  let updated = 0;
  const errors = [];

  for (const entry of items) {
    try {
      const row = mapSeedEntryToConcertRow(entry, nowTs);
      const result = await upsertConcert(env, row);

      if (result.existed) updated += 1;
      else inserted += 1;
    } catch (err) {
      errors.push({
        event_key: asText(entry?.event_key),
        error: String(err),
      });
    }
  }

  const count = await env.ARCHIVE_DB.prepare(`
    SELECT COUNT(*) AS total
    FROM concerts
  `).first();

  return {
    ok: errors.length === 0,
    source: "seed.js",
    seed_items: items.length,
    inserted,
    updated,
    errors,
    concerts_total: Number(count?.total ?? 0),
  };
}

export function getSeedPreview(limit = 5) {
  const items = Array.isArray(seed) ? seed : [];
  return items.slice(0, Math.max(0, Number(limit) || 5)).map((entry) => ({
    event_key: asText(entry?.event_key),
    title: normalizeWhitespace(entry?.title),
    date: asText(entry?.start_date),
    venue: normalizeWhitespace(entry?.venue?.raw_name || entry?.venue?.family_name),
    city: normalizeWhitespace(entry?.city),
    country: normalizeWhitespace(entry?.country),
    artists_main: pickMainArtist(entry),
    artists_all: buildArtistsAll(entry),
  }));
}

export default seed;
