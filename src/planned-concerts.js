export async function ensurePlannedConcertsSchema(db) {
  await db.prepare(`
    CREATE TABLE IF NOT EXISTS planned_concerts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      event_key TEXT NOT NULL,
      source TEXT,
      source_id TEXT,
      title TEXT NOT NULL,
      main_artist TEXT,
      date_local TEXT NOT NULL,
      time_local TEXT,
      venue_name TEXT,
      city TEXT,
      country TEXT,
      url TEXT,
      image_url TEXT,
      status TEXT NOT NULL DEFAULT 'planned',
      notes TEXT,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      archived_at INTEGER,
      dismissed_at INTEGER
    )
  `).run();

  await db.prepare(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_planned_concerts_event_key
    ON planned_concerts(event_key)
  `).run();

  await db.prepare(`
    CREATE INDEX IF NOT EXISTS idx_planned_concerts_status
    ON planned_concerts(status)
  `).run();

  await db.prepare(`
    CREATE INDEX IF NOT EXISTS idx_planned_concerts_date_local
    ON planned_concerts(date_local)
  `).run();

  await db.prepare(`
    CREATE INDEX IF NOT EXISTS idx_planned_concerts_status_date
    ON planned_concerts(status, date_local)
  `).run();
}

export async function addPlannedConcert(db, payload) {
  await ensurePlannedConcertsSchema(db);

  const normalized = normalizePlannedPayload(payload);
  const nowTs = Date.now();

  const existing = await db.prepare(`
    SELECT *
    FROM planned_concerts
    WHERE event_key = ?
    LIMIT 1
  `).bind(normalized.event_key).first();

  if (!existing) {
    await db.prepare(`
      INSERT INTO planned_concerts (
        event_key,
        source,
        source_id,
        title,
        main_artist,
        date_local,
        time_local,
        venue_name,
        city,
        country,
        url,
        image_url,
        status,
        notes,
        created_at,
        updated_at,
        archived_at,
        dismissed_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).bind(
      normalized.event_key,
      normalized.source,
      normalized.source_id,
      normalized.title,
      normalized.main_artist,
      normalized.date_local,
      normalized.time_local,
      normalized.venue_name,
      normalized.city,
      normalized.country,
      normalized.url,
      normalized.image_url,
      "planned",
      null,
      nowTs,
      nowTs,
      null,
      null
    ).run();
  } else {
    await db.prepare(`
      UPDATE planned_concerts
      SET
        source = ?,
        source_id = ?,
        title = ?,
        main_artist = ?,
        date_local = ?,
        time_local = ?,
        venue_name = ?,
        city = ?,
        country = ?,
        url = ?,
        image_url = ?,
        status = 'planned',
        dismissed_at = NULL,
        updated_at = ?
      WHERE event_key = ?
    `).bind(
      normalized.source,
      normalized.source_id,
      normalized.title,
      normalized.main_artist,
      normalized.date_local,
      normalized.time_local,
      normalized.venue_name,
      normalized.city,
      normalized.country,
      normalized.url,
      normalized.image_url,
      nowTs,
      normalized.event_key
    ).run();
  }

  await syncPastPlannedConcertsToPending(db);
  return await getPlannedConcertByEventKey(db, normalized.event_key);
}

export async function removePlannedConcert(db, eventKey) {
  await ensurePlannedConcertsSchema(db);

  const key = cleanText(eventKey);
  if (!key) {
    throw new Error("Missing event_key");
  }

  await db.prepare(`
    DELETE FROM planned_concerts
    WHERE event_key = ?
  `).bind(key).run();

  return {
    ok: true,
    event_key: key,
    removed: true
  };
}
export async function markPlannedConcertMissed(db, eventKey) {
  await ensurePlannedConcertsSchema(db);

  const key = cleanText(eventKey);
  if (!key) {
    throw new Error("Missing event_key");
  }

  const nowTs = Date.now();

  await db.prepare(`
    UPDATE planned_concerts
    SET
      status = 'missed',
      updated_at = ?,
      dismissed_at = ?
    WHERE event_key = ?
  `).bind(nowTs, nowTs, key).run();

  return await getPlannedConcertByEventKey(db, key);
}

export async function markPlannedConcertAttended(db, eventKey) {
  await ensurePlannedConcertsSchema(db);

  const key = cleanText(eventKey);
  if (!key) {
    throw new Error("Missing event_key");
  }

  const nowTs = Date.now();

  await db.prepare(`
    UPDATE planned_concerts
    SET
      status = 'archived',
      updated_at = ?,
      archived_at = ?
    WHERE event_key = ?
  `).bind(nowTs, nowTs, key).run();

  return await getPlannedConcertByEventKey(db, key);
}

export async function listPlannedConcerts(db, options = {}) {
  await ensurePlannedConcertsSchema(db);
  await syncPastPlannedConcertsToPending(db);

  const {
    limit = 500,
    includeArchived = false,
    includeMissed = false,
    includeDismissed = false
  } = options;

  const rows = await db.prepare(`
    SELECT
      id,
      event_key,
      source,
      source_id,
      title,
      main_artist,
      date_local,
      time_local,
      venue_name,
      city,
      country,
      url,
      image_url,
      status,
      notes,
      created_at,
      updated_at,
      archived_at,
      dismissed_at
    FROM planned_concerts
    ORDER BY date_local ASC, time_local ASC, title ASC
    LIMIT ?
  `).bind(limit).all();

  const items = (rows?.results || []).map(hydratePlannedConcertRow);

  const filtered = items.filter((item) => {
    if (item.status === "archived" && !includeArchived) return false;
    if (item.status === "missed" && !includeMissed) return false;
    if (item.status === "dismissed" && !includeDismissed) return false;
    return true;
  });

  return {
    ok: true,
    items: filtered,
    counts: {
      total: filtered.length,
      planned: filtered.filter((x) => x.status === "planned").length,
      pending: filtered.filter((x) => x.status === "pending").length,
      archived: filtered.filter((x) => x.status === "archived").length,
      missed: filtered.filter((x) => x.status === "missed").length,
      dismissed: filtered.filter((x) => x.status === "dismissed").length
    }
  };
}

export async function getPlannedConcertByEventKey(db, eventKey) {
  await ensurePlannedConcertsSchema(db);

  const key = cleanText(eventKey);
  if (!key) return null;

  const row = await db.prepare(`
    SELECT
      id,
      event_key,
      source,
      source_id,
      title,
      main_artist,
      date_local,
      time_local,
      venue_name,
      city,
      country,
      url,
      image_url,
      status,
      notes,
      created_at,
      updated_at,
      archived_at,
      dismissed_at
    FROM planned_concerts
    WHERE event_key = ?
    LIMIT 1
  `).bind(key).first();

  return row ? hydratePlannedConcertRow(row) : null;
}
export async function syncPastPlannedConcertsToPending(db) {
  await ensurePlannedConcertsSchema(db);

  const today = amsterdamToday();
  const nowTs = Date.now();

  await db.prepare(`
    UPDATE planned_concerts
    SET
      status = 'pending',
      updated_at = ?
    WHERE status = 'planned'
      AND date_local IS NOT NULL
      AND date_local != ''
      AND date_local < ?
  `).bind(nowTs, today).run();
}

function normalizePlannedPayload(payload) {
  const source = cleanParam(payload?.source);
  const sourceId = cleanText(payload?.source_id);
  const title = cleanText(payload?.title || payload?.artists_main || payload?.main_artist);
  const mainArtist = cleanText(payload?.main_artist || payload?.artists_main || payload?.title);
  const dateLocal = cleanText(payload?.date_local);
  const timeLocal = cleanNullableText(payload?.time_local);
  const venueName = cleanNullableText(payload?.venue_name);
  const city = cleanNullableText(payload?.city);
  const country = cleanNullableText(payload?.country) || "NL";
  const url = cleanNullableText(payload?.url);
  const imageUrl = cleanNullableText(payload?.image_url || payload?.imageUrl);

  if (!title || !dateLocal) {
    throw new Error("Missing required concert fields");
  }

  const eventKey =
    cleanText(payload?.event_key) ||
    buildPlannedConcertEventKey({
      source,
      sourceId,
      title,
      dateLocal,
      venueName,
      city
    });

  return {
    event_key: eventKey,
    source,
    source_id: sourceId,
    title,
    main_artist: mainArtist || title,
    date_local: dateLocal,
    time_local: timeLocal,
    venue_name: venueName,
    city,
    country,
    url,
    image_url: imageUrl
  };
}

function hydratePlannedConcertRow(row) {
  return {
    id: Number(row?.id || 0),
    event_key: cleanText(row?.event_key),
    source: cleanText(row?.source),
    source_id: cleanText(row?.source_id),
    title: cleanText(row?.title),
    main_artist: cleanText(row?.main_artist),
    date_local: cleanText(row?.date_local),
    time_local: cleanNullableText(row?.time_local),
    venue_name: cleanNullableText(row?.venue_name),
    city: cleanNullableText(row?.city),
    country: cleanNullableText(row?.country),
    url: cleanNullableText(row?.url),
    image_url: cleanNullableText(row?.image_url),
    status: cleanText(row?.status) || "planned",
    notes: cleanNullableText(row?.notes),
    created_at: toSafeInteger(row?.created_at),
    updated_at: toSafeInteger(row?.updated_at),
    archived_at: toSafeInteger(row?.archived_at),
    dismissed_at: toSafeInteger(row?.dismissed_at)
  };
}

function buildPlannedConcertEventKey({ source, sourceId, title, dateLocal, venueName, city }) {
  if (source && sourceId) {
    return `planned::${source}::${sourceId}`;
  }

  return [
    "planned",
    slugify(source || "unknown"),
    slugify(title),
    slugify(venueName || ""),
    slugify(city || ""),
    dateLocal
  ].filter(Boolean).join("::");
}
function amsterdamToday() {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Europe/Amsterdam",
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  }).formatToParts(new Date());

  const y = parts.find((p) => p.type === "year")?.value || "";
  const m = parts.find((p) => p.type === "month")?.value || "";
  const d = parts.find((p) => p.type === "day")?.value || "";

  return `${y}-${m}-${d}`;
}

function cleanParam(value) {
  return String(value || "").trim().toLowerCase();
}

function cleanText(value) {
  return String(value || "")
    .replace(/\u00a0/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function cleanNullableText(value) {
  const v = cleanText(value);
  return v || null;
}

function toSafeInteger(value) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : 0;
}

function slugify(value) {
  return String(value || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[’']/g, "")
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .replace(/-+/g, "-");
}