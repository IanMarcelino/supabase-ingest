// supabase/functions/ingest_events/index.ts
// Deno runtime (Supabase Edge). Busca jogos do dia na API-Football e salva via upsert em `ligas`, `times` e `jogos`.
import { createClient } from "npm:@supabase/supabase-js@2";
import { DateTime } from "npm:luxon@3";

/* ===================== Tipos API ===================== */
type ApiFootballResponse<T> = {
  get: string;
  parameters: Record<string, string>;
  errors: Record<string, unknown>;
  results: number;
  paging: { current: number; total: number };
  response: T[];
};

type FixtureApi = {
  fixture: {
    id: number;
    referee: string | null;
    timezone: string;
    date: string; // ISO
    timestamp: number; // unix seconds
    periods: { first: number | null; second: number | null };
    venue: { id: number | null; name: string | null; city: string | null };
    status: { long: string; short: string; elapsed: number | null };
  };
  league: {
    id: number;
    name: string;
    country: string;
    logo: string;
    flag: string | null;
    season: number;
    round: string | null;
  };
  teams: {
    home: { id: number; name: string; logo: string; winner: boolean | null };
    away: { id: number; name: string; logo: string; winner: boolean | null };
  };
  goals: { home: number | null; away: number | null };
  score: unknown;
};

/* ===================== Tipos DB ===================== */
type UUID = string;
type MatchStatus = "scheduled" | "in_progress" | "finished" | "canceled" | "postponed";

type JogosInsert = {
  id_api: string;
  liga_id: UUID | null;
  time_casa_id: UUID | null;
  time_fora_id: UUID | null;
  estadio: string | null;
  kickoff_utc: string;        // ISO com offset - será normalizado pelo Postgres (timestamptz)
  status: MatchStatus;
  round: string | null;
  season: string;
  payload: Record<string, unknown>;
  updated_at: string;
  provider: UUID
  
};

/* ===================== Env & Client ===================== */
const SB_URL = Deno.env.get("SUPABASE_URL")!;
const SB_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const API_FOOTBALL_KEY = Deno.env.get("API_FOOTBALL_KEY")!;
const DEFAULT_LEAGUE = Number(Deno.env.get("DEFAULT_LEAGUE") ?? 71); // Série A
const DEFAULT_TZ = Deno.env.get("DEFAULT_TZ") ?? "America/Sao_Paulo";

const supabase = createClient(SB_URL, SB_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

/* ===================== Utils ===================== */
function todayInTZ(tz: string) {
  return DateTime.now().setZone(tz).toFormat("yyyy-LL-dd"); // YYYY-MM-DD
}

function toISOWithZoneFromUnix(tsSec: number, tz: string) {
  return DateTime.fromSeconds(tsSec).setZone(tz).toISO({ suppressMilliseconds: true })!;
}

function slugify(s: string | null | undefined) {
  const base = (s ?? "").normalize("NFD").replace(/[\u0300-\u036f]/g, "");
  return base.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/(^-|-$)+/g, "");
}

function mapStatus(apiShort: string): MatchStatus {
  switch (apiShort) {
    case "NS": return "scheduled";
    case "1H":
    case "2H":
    case "HT":
    case "ET":
    case "BT":
    case "P":  return "in_progress";
    case "FT":
    case "AET":
    case "PEN": return "finished";
    case "CANC":
    case "ABD": return "canceled";
    case "PST": return "postponed";
    default:    return "scheduled";
  }
}

/* ===================== Fetch fixtures ===================== */
async function fetchFixturesSmart(url: string, headers: HeadersInit) {
  const fixtures: FixtureApi[] = [];

  const r0 = await fetch(url, { headers });
  const t0 = await r0.text();
  if (!r0.ok) throw new Error(`API-Football error ${r0.status}: ${t0}`);
  const j0 = JSON.parse(t0) as ApiFootballResponse<FixtureApi>;

  const hasErrors = j0?.errors && Object.keys(j0.errors).length > 0;
  fixtures.push(...(j0.response ?? []));

  const total = j0?.paging?.total ?? 1;
  if (total > 1) {
    for (let page = 2; page <= total; page++) {
      const u = new URL(url);
      u.searchParams.set("page", String(page));
      const rp = await fetch(u.toString(), { headers });
      const tp = await rp.text();
      if (!rp.ok) throw new Error(`API-Football error ${rp.status}: ${tp}`);
      const jp = JSON.parse(tp) as ApiFootballResponse<FixtureApi>;
      fixtures.push(...(jp.response ?? []));
    }
  }

  return { fixtures, firstJson: j0, hasErrors };
}

/* ===================== Upserts auxiliares ===================== */
async function ensureLeagueId(lg: FixtureApi["league"]): Promise<UUID> {
  const id_api = String(lg.id);
  const row = {
    id_api,
    name: lg.name ?? null,
    slug: slugify(lg.name ?? null),
    country: lg.country ?? null,
    updated_at: DateTime.now().toISO(),
    provider: "api-football",
  };

  const { data, error } = await supabase
    .from("ligas")
    .upsert(row, { onConflict: "id_api" })
    .select("id")
    .single();

  if (error) throw error;
  return data.id as UUID;
}

async function ensureTeamId(t: FixtureApi["teams"]["home"]): Promise<UUID> {
  const id_api = String(t.id);
  const row = {
    id_api,
    name: t.name ?? null,
    slug: slugify(t.name ?? null),
    logo_url: t.logo ?? null,
    updated_at: DateTime.now().toISO(),
    provider: "api-football",
  };

  const { data, error } = await supabase
    .from("times")
    .upsert(row, { onConflict: "id_api" })
    .select("id")
    .single();

  if (error) throw error;
  return data.id as UUID;
}

/* ===================== Upsert jogos ===================== */
async function upsertJogos(rows: JogosInsert[]) {
  if (rows.length === 0) return { created: 0, updated: 0 };

  const ids = rows.map((r) => r.id_api);
  const { data: existing, error: selErr } = await supabase
    .from("jogos")
    .select("id_api")
    .in("id_api", ids);

  if (selErr) throw selErr;

  const existingSet = new Set((existing ?? []).map((e) => e.id_api as string));
  const toInsert = rows.filter((r) => !existingSet.has(r.id_api));
  const toUpdate = rows.filter((r) => existingSet.has(r.id_api));

  const { error: upErr } = await supabase
    .from("jogos")
    .upsert(rows, { onConflict: "id_api" });
  if (upErr) {
    if (toInsert.length) {
      const { error } = await supabase.from("jogos").insert(toInsert);
      if (error) throw error;
    }
    for (const r of toUpdate) {
      const { error } = await supabase.from("jogos").update(r).eq("id_api", r.id_api);
      if (error) throw error;
    }
  }

  return { created: toInsert.length, updated: toUpdate.length };
}

/* ===================== Handler ===================== */
Deno.serve(async (req) => {
  try {
    if (!API_FOOTBALL_KEY) {
      return new Response(
        JSON.stringify({ ok: false, error: "Missing API_FOOTBALL_KEY" }),
        { status: 500, headers: { "content-type": "application/json" } },
      );
    }

    const url = new URL(req.url);
    const leagueParam = url.searchParams.get("league");
    const seasonParam = url.searchParams.get("season");
    const dateParam = url.searchParams.get("date") ?? new Date().toISOString().slice(0, 10);
    const tz = url.searchParams.get("timezone") || DEFAULT_TZ;
    const debugFlag = url.searchParams.has("debug");

    const league =
      leagueParam && /^\d+$/.test(leagueParam)
        ? Number(leagueParam)
        : DEFAULT_LEAGUE;
  
    const season =
      seasonParam && /^\d{4}$/.test(seasonParam)
        ? Number(seasonParam)
        : (dateParam
            ? DateTime.fromISO(`${dateParam}T00:00:00`, { zone: tz }).year
            : DateTime.now().setZone(tz).year);

    const date = dateParam || todayInTZ(tz);

    const base = "https://v3.football.api-sports.io/fixtures";
    const apiUrl = `${base}?league=${league}&season=${season}&date=${date}&timezone=${encodeURIComponent(tz)}`;
    const headers = { "x-apisports-key": API_FOOTBALL_KEY };

    const { fixtures, firstJson, hasErrors } = await fetchFixturesSmart(apiUrl, headers);

    if (hasErrors) {
      return new Response(
        JSON.stringify({
          ok: false,
          reason: "upstream_error",
          status: 200,
          api_errors: firstJson?.errors ?? null,
          last_url: apiUrl,
          ...(debugFlag ? {
            api_key_len: API_FOOTBALL_KEY?.length ?? 0,
            api_key_tail: API_FOOTBALL_KEY ? API_FOOTBALL_KEY.slice(-6) : null,
          } : {}),
        }),
        { status: 502, headers: { "content-type": "application/json" } },
      );
    }

    const rows: JogosInsert[] = [];
    for (const fx of fixtures) {
      const ligaId = await ensureLeagueId(fx.league);
      const homeId = await ensureTeamId(fx.teams.home);
      const awayId = await ensureTeamId(fx.teams.away);

      rows.push({
        id_api: String(fx.fixture.id),
        liga_id: ligaId,
        time_casa_id: homeId,
        time_fora_id: awayId,

        estadio: fx.fixture.venue.name ?? null,
        kickoff_utc: toISOWithZoneFromUnix(fx.fixture.timestamp, tz),
        status: mapStatus(fx.fixture.status.short ?? ""),
        round: fx.league.round ?? null,
        season: String(fx.league.season),
        payload: fx as unknown as Record<string, unknown>,
        updated_at: DateTime.now().toISO(),
        provider: "api-football",
        // kickoff_date_brt removido → calculado pelo Postgres
      });
    }

    const summary = await upsertJogos(rows);

    return new Response(
      JSON.stringify({
        ok: true,
        ...summary,
        debug: {
          query: { league, season, date, timezone: tz },
          count_api: fixtures.length,
          last_url: apiUrl,
          ...(debugFlag ? {
            api_key_len: API_FOOTBALL_KEY?.length ?? 0,
            api_key_tail: API_FOOTBALL_KEY ? API_FOOTBALL_KEY.slice(-6) : null,
          } : {}),
        },
      }),
      { headers: { "content-type": "application/json" } },
    );
  } catch (e) {
    return new Response(
      JSON.stringify({ ok: false, error: String(e?.message ?? e) }),
      { status: 500, headers: { "content-type": "application/json" } },
    );
  }
});
