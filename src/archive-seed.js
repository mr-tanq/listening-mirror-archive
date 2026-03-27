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

function buildSupports(entry) {
  const artists = sortArtists(entry?.artists || []);
  const main = pickMainArtist(entry).toLowerCase();

  const supports = artists
    .map((a) => normalizeWhitespace(a?.name))
    .filter(Boolean)
    .filter((name) => name.toLowerCase() !== main);

  return uniqueStrings(supports).join(", ");
}

function isFestivalEntry(entry) {
  const kind = asText(entry?.kind).toLowerCase();
  return kind === "festival" || !!asText(entry?.festival_name);
}

function mapSeedEntryToArchiveConcertRow(entry, nowTs) {
  const eventKey = asText(entry?.event_key);
  const title = normalizeWhitespace(entry?.title);
  const mainArtist = pickMainArtist(entry);
  const supports = buildSupports(entry);
  const date = asText(entry?.start_date);
  const endDate = asText(entry?.end_date || entry?.start_date);
  const venue = normalizeWhitespace(entry?.venue?.raw_name || entry?.venue?.family_name);
  const venueFamily = normalizeWhitespace(entry?.venue?.family_name || entry?.venue?.raw_name);
  const city = normalizeWhitespace(entry?.city);
  const region = normalizeWhitespace(entry?.region);
  const country = normalizeWhitespace(entry?.country);
  const festival = isFestivalEntry(entry) ? 1 : 0;
  const url = "";
  const imageUrl = "";
  const genreHint = isFestivalEntry(entry)
    ? normalizeWhitespace(entry?.festival_name || entry?.title)
    : "";
  const notes = "";
  const rating = null;

  if (!eventKey) {
    throw new Error(`Seed entry missing event_key: ${JSON.stringify(entry)}`);
  }
  if (!title) {
    throw new Error(`Seed entry missing title for ${eventKey}`);
  }
  if (!date) {
    throw new Error(`Seed entry missing start_date for ${eventKey}`);
  }

  return {
    event_key: eventKey,
    date,
    end_date: endDate,
    title,
    main_artist: mainArtist || title,
    supports,
    venue,
    venue_family: venueFamily || venue,
    city,
    region: region || null,
    country,
    festival,
    notes,
    rating,
    url,
    image_url: imageUrl,
    genre_hint: genreHint,
    created_at: nowTs,
    updated_at: nowTs,
  };
}

async function ensureArchiveTables(env) {
  await env.ARCHIVE_DB.prepare(`
    CREATE TABLE IF NOT EXISTS archive_concerts (
      id INTEGER PRIMARY KEY,
      event_key TEXT NOT NULL,
      date TEXT NOT NULL,
      end_date TEXT,
      title TEXT NOT NULL,
      main_artist TEXT,
      supports TEXT,
      venue TEXT,
      venue_family TEXT,
      city TEXT,
      region TEXT,
      country TEXT,
      festival INTEGER NOT NULL DEFAULT 0,
      notes TEXT,
      rating INTEGER,
      url TEXT,
      image_url TEXT,
      genre_hint TEXT,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    )
  `).run();

  await env.ARCHIVE_DB.prepare(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_archive_concerts_event_key
    ON archive_concerts(event_key)
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

async function upsertArchiveConcert(env, row) {
  const existing = await env.ARCHIVE_DB.prepare(`
    SELECT id, created_at
    FROM archive_concerts
    WHERE event_key = ?
    LIMIT 1
  `).bind(row.event_key).first();

  const createdAt = existing?.created_at ?? row.created_at;

  await env.ARCHIVE_DB.prepare(`
    INSERT INTO archive_concerts (
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
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(event_key) DO UPDATE SET
      date = excluded.date,
      end_date = excluded.end_date,
      title = excluded.title,
      main_artist = excluded.main_artist,
      supports = excluded.supports,
      venue = excluded.venue,
      venue_family = excluded.venue_family,
      city = excluded.city,
      region = excluded.region,
      country = excluded.country,
      festival = excluded.festival,
      notes = excluded.notes,
      rating = excluded.rating,
      url = excluded.url,
      image_url = excluded.image_url,
      genre_hint = excluded.genre_hint,
      updated_at = excluded.updated_at
  `).bind(
    row.event_key,
    row.date,
    row.end_date,
    row.title,
    row.main_artist,
    row.supports,
    row.venue,
    row.venue_family,
    row.city,
    row.region,
    row.country,
    row.festival,
    row.notes,
    row.rating,
    row.url,
    row.image_url,
    row.genre_hint,
    createdAt,
    row.updated_at
  ).run();

  return {
    event_key: row.event_key,
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
      const row = mapSeedEntryToArchiveConcertRow(entry, nowTs);
      const result = await upsertArchiveConcert(env, row);

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
    FROM archive_concerts
  `).first();

  return {
    ok: errors.length === 0,
    source: "seed.js",
    seed_items: items.length,
    inserted,
    updated,
    errors,
    archive_total: Number(count?.total ?? 0),
  };
}

export function getSeedPreview(limit = 5) {
  const items = Array.isArray(seed) ? seed : [];
  return items.slice(0, Math.max(0, Number(limit) || 5)).map((entry) => ({
    event_key: asText(entry?.event_key),
    title: normalizeWhitespace(entry?.title),
    date: asText(entry?.start_date),
    end_date: asText(entry?.end_date || entry?.start_date),
    venue: normalizeWhitespace(entry?.venue?.raw_name || entry?.venue?.family_name),
    venue_family: normalizeWhitespace(entry?.venue?.family_name || entry?.venue?.raw_name),
    city: normalizeWhitespace(entry?.city),
    country: normalizeWhitespace(entry?.country),
    main_artist: pickMainArtist(entry),
    supports: buildSupports(entry),
    festival: isFestivalEntry(entry) ? 1 : 0,
  }));
}

export default seed;
