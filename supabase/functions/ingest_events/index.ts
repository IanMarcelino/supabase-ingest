// supabase/functions/ingest_events/index.ts
import { createClient } from "npm:@supabase/supabase-js@2";

type LeagueCfg = {
  slug: string;
  name: string;
  provider_league_id: number;
  default_season: number;
  season_start?: string;
  season_end?: string;
};

const LEAGUES: Record<string, LeagueCfg> = {
  "br-serie-a": {
    slug: "br-serie-a",
    name: "Brasileirão Série A",
    provider_league_id: 71,
    default_season: 2025,
    season_start: "2025-03-29",
    season_end:   "2025-12-21",
  },
  "br-serie-b": {
    slug: "br-serie-b",
    name: "Brasileirão Série B",
    provider_league_id: 72,
    default_season: 2025,
    season_start: "2025-04-04",
    season_end:   "2025-11-22",
  },
  "libertadores": {
    slug: "libertadores",
    name: "CONMEBOL Libertadores",
    provider_league_id: 13,
    default_season: 2025,
    season_start: "2025-02-05",
    season_end:   "2025-09-24",
  },
  "ucl": {
    slug: "ucl",
    name: "UEFA Champions League",
    provider_league_id: 2,
    default_season: 2025,
    season_start: "2025-07-08",
    season_end:   "2025-08-27",
  },
  "sudamericana": {
    slug: "sudamericana",
    name: "CONMEBOL Sudamericana",
    provider_league_id: 11,
    default_season: 2025,
    season_start: "2025-03-05",
    season_end:   "2025-09-24",
  },
};

function todayISO(date?: string) {
  return (date ? new Date(date) : new Date()).toISOString().slice(0, 10);
}

function toBRTDate(isoUtc: string): string {
  const dt = new Date(isoUtc);
  const fmt = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Sao_Paulo",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  return fmt.format(dt);
}

function mapStatus(short: string | null | undefined): string {
  const s = (short || "").toUpperCase();
  if (s === "PST") return "postponed";
  if (s === "CANC" || s === "ABD") return "canceled";
  if (s === "FT" || s === "AET" || s === "PEN") return "finished";
  if (["1H", "HT", "2H", "ET", "P"].includes(s)) return "live";
  return "scheduled";
}

function slugifyName(name?: string | null) {
  const base = (name ?? "")
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return base || null;
}

Deno.serve(async (req) => {
  // Auth por header (opcional em DEV)
  const SKIP = Deno.env.get("SKIP_CRON_SECRET") === "1";
  if (!SKIP) {
    const expected = Deno.env.get("CRON_SECRET");
    const got = req.headers.get("x-cron-secret");
    if (!expected || got !== expected) {
      return new Response("unauthorized", { status: 401 });
    }
  }

  const SUPABASE_URL = Deno.env.get("SB_URL");
  const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SB_SERVICE_ROLE_KEY");
  const APIFOOTBALL_API_KEY = Deno.env.get("APIFOOTBALL_API_KEY"); // <<< NOVA ENV
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    return new Response("missing Supabase env", { status: 500 });
  }
  if (!APIFOOTBALL_API_KEY) {
    return new Response("missing APIFOOTBALL_API_KEY", { status: 500 });
  }

  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
    auth: { persistSession: false },
  });

  try {
    const url = new URL(req.url);
    const leagueSlug = url.searchParams.get("league") ?? "br-serie-a";
    const baseDate = url.searchParams.get("date") ?? todayISO();
    const daysAhead = Number(url.searchParams.get("days_ahead") ?? "0");
    const seasonOverride = url.searchParams.get("season");

    const cfg = LEAGUES[leagueSlug];
    if (!cfg) {
      return new Response(JSON.stringify({ error: `unknown league '${leagueSlug}'` }), { status: 400 });
    }
    const season = Number(seasonOverride ?? cfg.default_season);

    // janela [from..to]
    const from = new Date(baseDate);
    const to = new Date(baseDate);
    to.setUTCDate(to.getUTCDate() + Math.max(0, daysAhead));

    let fromStr = from.toISOString().slice(0, 10);
    let toStr = to.toISOString().slice(0, 10);

    // Sanidade opcional: limitar à janela real da competição, se informada
    if (cfg.season_start && fromStr < cfg.season_start) fromStr = cfg.season_start;
    if (cfg.season_end && toStr > cfg.season_end) toStr = cfg.season_end;

    // garante liga (onConflict por slug)
    const { data: liga, error: ligaErr } = await sb
      .from("ligas")
      .upsert(
        {
          slug: cfg.slug,
          name: cfg.name,
          id_api: `apisports:${cfg.provider_league_id}`,
          updated_at: new Date().toISOString(),
        },
        { onConflict: "slug" }
      )
      .select("id, slug")
      .single();
    if (ligaErr) throw ligaErr;
    if (!liga?.id) throw new Error("Falha ao garantir 'ligas'");

    let created = 0, updated = 0, skipped = 0;

    let page = 1;
    while (true) {
      const apiUrl = new URL("https://v3.football.api-sports.io/fixtures");
      apiUrl.searchParams.set("league", String(cfg.provider_league_id));
      apiUrl.searchParams.set("season", String(season));
      apiUrl.searchParams.set("from", fromStr);
      apiUrl.searchParams.set("to", toStr);
      apiUrl.searchParams.set("page", String(page));

      const res = await fetch(apiUrl.toString(), {
        headers: { "x-apisports-key": APIFOOTBALL_API_KEY },
      });
      if (!res.ok) {
        throw new Error(`API-FOOTBALL ${res.status} for ${cfg.name} ${fromStr}..${toStr}`);
      }
      const json = await res.json() as any;

      const items = json?.response ?? [];
      if (!items.length && page === 1) break;

      for (const it of items) {
        const fixture = it.fixture ?? {};
        const leagueInfo = it.league ?? {};
        const teams = it.teams ?? {};
        const home = teams.home ?? {};
        const away = teams.away ?? {};

        // teams upsert
        const teamsToUpsert: Array<{ id_api: number | null; name: string | null }> = [
          { id_api: home.id ?? null, name: home.name ?? null },
          { id_api: away.id ?? null, name: away.name ?? null },
        ];
        const teamIds: Record<string, string> = {};

        for (const t of teamsToUpsert) {
          if (!t.id_api) continue;
          const slug = slugifyName(t.name);
          const { data: teamRow, error: teamErr } = await sb
            .from("times")
            .upsert(
              { id_api: String(t.id_api), name: t.name, slug, updated_at: new Date().toISOString() },
              { onConflict: "id_api" }
            )
            .select("id, id_api")
            .single();
          if (teamErr) throw teamErr;
          if (teamRow?.id) teamIds[String(t.id_api)] = teamRow.id;
        }

        // horário e status
        const kickoffUtc: string | null = fixture.date ? new Date(fixture.date).toISOString() : null;
        const statusShort: string | null = fixture.status?.short ?? null;
        const status = mapStatus(statusShort);

        // jogo upsert
        const payload = {
          id_api: fixture.id ? String(fixture.id) : null,
          liga_id: liga.id as string,
          time_casa_id: home.id ? (teamIds[String(home.id)] ?? null) : null,
          time_fora_id: away.id ? (teamIds[String(away.id)] ?? null) : null,
          estadio: fixture.venue?.name ?? null,
          kickoff_utc: kickoffUtc,
          status,
          round: leagueInfo.round ?? null,
          season: leagueInfo.season ?? season,
          payload: it ?? null,
          updated_at: new Date().toISOString(),
          kickoff_date_brt: kickoffUtc ? toBRTDate(kickoffUtc) : null,
        };

        if (!payload.id_api) { skipped++; continue; }

        const { data: up, error: upErr } = await sb
          .from("jogos")
          .upsert(payload, { onConflict: "id_api" })
          .select("id, created_at, updated_at")
          .single();
        if (upErr) throw upErr;

        if (up?.created_at && (up?.created_at === up?.updated_at || !up?.updated_at)) created++;
        else updated++;
      }

      const curr = Number(json?.paging?.current ?? page);
      const total = Number(json?.paging?.total ?? page);
      if (!Number.isFinite(curr) || !Number.isFinite(total) || curr >= total) break;
      page = curr + 1;
    }

    return new Response(JSON.stringify({ ok: true, created, updated, skipped }), {
      headers: { "content-type": "application/json" },
    });
  } catch (e) {
  console.error("ingest_events error:", e);
  const msg =
    e instanceof Error
      ? `${e.message}${e.stack ? " | " + e.stack : ""}`
      : JSON.stringify(e);  
  return new Response(JSON.stringify({ error: msg }), { status: 500 });
}
  
});
