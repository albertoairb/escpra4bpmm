"use strict";

const path = require("path");
const express = require("express");
const helmet = require("helmet");
const compression = require("compression");
const mysql = require("mysql2/promise");
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");

// ===============================
// CONFIG / ENV
// ===============================
const PORT = Number(process.env.PORT || 8080);
process.env.TZ = (process.env.TZ || "America/Sao_Paulo").trim();

// Semana mínima (segunda-feira) para iniciar o sistema automaticamente, sem precisar forçar via variável.
// Ex.: quando a semana anterior já passou, iniciamos diretamente na próxima.
const CUTOVER_WEEK_START = "2026-03-09";
// Se quiser forçar manualmente a semana exibida (ex.: liberar semana futura), defina WEEK_START_OVERRIDE=YYYY-MM-DD (segunda-feira)
const WEEK_START_OVERRIDE = (process.env.WEEK_START_OVERRIDE || "").trim();

const JWT_SECRET = (process.env.JWT_SECRET || "troque-este-segredo").trim();
const DEFAULT_PASSWORD = (process.env.DEFAULT_PASSWORD || "aux123").trim();

const CLOSE_FRIDAY_HOUR = Number(process.env.CLOSE_FRIDAY_HOUR || 15);

const SYSTEM_NAME = (process.env.SYSTEM_NAME || "Escala Semanal de PRAÇAs do EM  4 BPM/M").trim();
const AUTHOR = (process.env.AUTHOR || "Desenvolvido por Alberto Franzini Neto").trim();
const COPYRIGHT_YEAR = (process.env.COPYRIGHT_YEAR || "2026").toString().trim();

function defaultSignatures() {
  return {
    left_name: "AUXILIAR P1",
    left_role: "",
    center_name: "ALBERTO FRANZINI NETO",
    center_role: "CAP PM CH P1/P5",
    right_name: "EDUARDO MOSNA XAVIER",
    right_role: "MAJ PM SUBCMT",
  };
}


// DB: Railway (URL) > Docker/local (DB_HOST...)
const DB_URL = (process.env.DATABASE_URL || process.env.DB_URL || process.env.MYSQL_URL || process.env.MYSQL_PUBLIC_URL || "").trim();

// Defaults para Docker/local (quando DB_URL não existir)
const DB_HOST = (process.env.DB_HOST || "db").trim();
const DB_PORT = Number(process.env.DB_PORT || 3306);
const DB_USER = (process.env.DB_USER || "app").trim();
const DB_PASSWORD = (process.env.DB_PASSWORD || "app").trim();
const DB_NAME = (process.env.DB_NAME || process.env.DB_DATABASE || "escala").trim();

// ===============================
// OFICIAIS (lista fixa)
// - canonical_name: chave única do oficial (sem posto)
// - rank: graduao a exibir
// - name: nome de guerra a exibir
// ===============================
const OFFICERS = [
  {
    "canonical_name": "Fernandes",
    "rank": "1 SGT PM",
    "name": "FERNANDES",
    "group_label": "P/1"
  },
  {
    "canonical_name": "Felipe",
    "rank": "CB PM",
    "name": "FELIPE",
    "group_label": "P/1"
  },
  {
    "canonical_name": "Danielle",
    "rank": "2 SGT PM",
    "name": "DANIELLE",
    "group_label": "P/5"
  },
  {
    "canonical_name": "Diane",
    "rank": "SD PM",
    "name": "DIANE",
    "group_label": "P/5"
  },
  {
    "canonical_name": "Rabelo",
    "rank": "CB PM",
    "name": "RABELO",
    "group_label": "UIS-ODONTO"
  },
  {
    "canonical_name": "Naiara Garcia",
    "rank": "SD PM",
    "name": "NAIARA GARCIA",
    "group_label": "UIS-ODONTO"
  },
  {
    "canonical_name": "Freitas",
    "rank": "SD PM",
    "name": "FREITAS",
    "group_label": "ESTAFETA"
  },
  {
    "canonical_name": "Ferreira",
    "rank": "2 SGT PM",
    "name": "FERREIRA",
    "group_label": "P/3"
  },
  {
    "canonical_name": "Henrique Dias",
    "rank": "CB PM",
    "name": "HENRIQUE DIAS",
    "group_label": "P/3"
  },
  {
    "canonical_name": "R. Fernandes",
    "rank": "SD PM",
    "name": "R. FERNANDES",
    "group_label": "AUX ADM TELEMÁTICA"
  },
  {
    "canonical_name": "Donizetti",
    "rank": "1 SGT PM",
    "name": "DONIZETTI",
    "group_label": "P/4"
  },
  {
    "canonical_name": "E. Batista",
    "rank": "CB PM",
    "name": "E. BATISTA",
    "group_label": "P/4"
  },
  {
    "canonical_name": "Moises",
    "rank": "SD PM",
    "name": "MOISES",
    "group_label": "P/4"
  },
  {
    "canonical_name": "Figueiredo",
    "rank": "CB PM",
    "name": "FIGUEIREDO",
    "group_label": "P/4"
  },
  {
    "canonical_name": "Karina",
    "rank": "SD PM",
    "name": "KARINA",
    "group_label": "P/4"
  },
  {
    "canonical_name": "Queila",
    "rank": "SUBTEN PM",
    "name": "QUEILA",
    "group_label": "PJMD"
  },
  {
    "canonical_name": "Scatolin",
    "rank": "SUBTEN PM",
    "name": "SCATOLIN",
    "group_label": "PJMD"
  },
  {
    "canonical_name": "Valmir Santos",
    "rank": "1 SGT PM",
    "name": "VALMIR SANTOS",
    "group_label": "PJMD"
  },
  {
    "canonical_name": "Aquino",
    "rank": "CB PM",
    "name": "AQUINO",
    "group_label": "PJMD"
  },
  {
    "canonical_name": "Barbara",
    "rank": "SD PM",
    "name": "BARBARA",
    "group_label": "PJMD"
  },
  {
    "canonical_name": "Diana",
    "rank": "CB PM",
    "name": "DIANA",
    "group_label": "APROVISIONAMENTO"
  },
  {
    "canonical_name": "Kelly",
    "rank": "CB PM",
    "name": "KELLY",
    "group_label": "APROVISIONAMENTO"
  },
  {
    "canonical_name": "Canha",
    "rank": "CB PM",
    "name": "CANHA",
    "group_label": "MOTORISTAS CMT BTL 12 x 36"
  },
  {
    "canonical_name": "Tino",
    "rank": "CB PM",
    "name": "TINO",
    "group_label": "MOTORISTAS CMT BTL 12 x 36"
  },
  {
    "canonical_name": "Artur",
    "rank": "CB PM",
    "name": "ARTUR",
    "group_label": "MOTOMEC/MANUT 12 x 36"
  },
  {
    "canonical_name": "Vitor Vieira",
    "rank": "CB PM",
    "name": "VITOR VIEIRA",
    "group_label": "MOTOMEC/MANUT 12 x 36"
  },
  {
    "canonical_name": "Assali",
    "rank": "CB PM",
    "name": "ASSALI",
    "group_label": "MOTOMEC/MANUT 12 x 36"
  },
  {
    "canonical_name": "Romani",
    "rank": "SD PM",
    "name": "ROMANIN",
    "group_label": "MOTOMEC/MANUT 12 x 36"
  },
  {
    "canonical_name": "Vieira",
    "rank": "CB PM",
    "name": "VIEIRA",
    "group_label": "MOTOMEC/MANUT 12 x 36"
  },
  {
    "canonical_name": "Onofre",
    "rank": "CB PM",
    "name": "ONOFRE",
    "group_label": "MOTORISTAS CFP 12 x 36"
  },
  {
    "canonical_name": "Pivetta",
    "rank": "CB PM",
    "name": "PIVETTA",
    "group_label": "MOTORISTAS CFP 12 x 36"
  },
  {
    "canonical_name": "Aguiar",
    "rank": "CB PM",
    "name": "AGUIAR",
    "group_label": "MOTORISTAS CFP 12 x 36"
  },
  {
    "canonical_name": "De Almeida",
    "rank": "SD PM",
    "name": "DE ALMEIDA",
    "group_label": "MOTORISTAS CFP 12 x 36"
  },
  {
    "canonical_name": "Vicente",
    "rank": "CB PM",
    "name": "VICENTE",
    "group_label": "MOTORISTAS CFP 12 x 36"
  },
  {
    "canonical_name": "Milton",
    "rank": "CB PM",
    "name": "MILTON",
    "group_label": "MOTORISTAS CFP 12 x 36"
  },
  {
    "canonical_name": "Diego",
    "rank": "SD PM",
    "name": "DIEGO",
    "group_label": "MOTORISTAS CFP 12 x 36"
  },
  {
    "canonical_name": "Cassimiro",
    "rank": "CB PM",
    "name": "CASSIMIRO",
    "group_label": "RESERVA DE ARMAS BTL"
  },
  {
    "canonical_name": "Coelho Silva",
    "rank": "SD PM",
    "name": "COELHO SILVA",
    "group_label": "RESERVA DE ARMAS BTL"
  },
  {
    "canonical_name": "Silvio",
    "rank": "CB PM",
    "name": "SILVIO",
    "group_label": "RESERVA DE ARMAS BTL"
  },
  {
    "canonical_name": "Julio Cesar",
    "rank": "CB PM",
    "name": "JLIO CESAR",
    "group_label": "RESERVA DE ARMAS BTL"
  },
  {
    "canonical_name": "Valdileno",
    "rank": "CB PM",
    "name": "VALDILENO",
    "group_label": "ENCARREGADO DA GUARDA (SERVIO DE DIA) 12 x 36"
  },
  {
    "canonical_name": "Meireles",
    "rank": "SD PM",
    "name": "MEIRELES",
    "group_label": "ENCARREGADO DA GUARDA (SERVIO DE DIA) 12 x 36"
  },
  {
    "canonical_name": "Santiago",
    "rank": "SD PM",
    "name": "SANTIAGO",
    "group_label": "ENCARREGADO DA GUARDA (SERVIO DE DIA) 12 x 36"
  },
  {
    "canonical_name": "Ricardo Horacio",
    "rank": "CB PM",
    "name": "RICARDO HORCIO",
    "group_label": "ENCARREGADO DA GUARDA (SERVIO DE DIA) 12 x 36"
  }
];
            

const EXTRA_USERS = [
  {
    canonical_name: "Alberto Franzini Neto",
    is_admin: true,
    aliases: [
      "Alberto Franzini Neto",
      "Cap PM Alberto Franzini Neto",
      "CAP PM ALBERTO",
      "Capito PM Alberto Franzini Neto",
      "Alberto",
    ],
  },
  {
    canonical_name: "Eduardo Mosna Xavier",
    is_admin: true,
    aliases: [
      "Eduardo Mosna Xavier",
      "Maj PM Eduardo Mosna Xavier",
      "MAJ PM MOSNA",
      "Major PM Eduardo Mosna Xavier",
      "Maj Mosna",
      "Major Mosna",
      "Mosna",
    ],
  },
  {
    canonical_name: "Helder Antnio de Paula",
    is_admin: true,
    aliases: [
      "Helder Antnio de Paula",
      "Ten Cel PM Helder Antnio de Paula",
      "TEN CEL PM HELDER",
      "Tenente-Coronel PM Helder Antnio de Paula",
      "Helder",
    ],
  },
];

const USER_DIRECTORY = (() => {
  const map = new Map();

  for (const off of OFFICERS) {
    const aliases = [
      `${off.rank} ${off.name}`,
      `${off.rank} ${off.canonical_name}`,
      off.name,
      off.canonical_name,
    ];
    map.set(off.canonical_name, {
      canonical_name: off.canonical_name,
      is_admin: false,
      aliases,
    });
  }

  for (const extra of EXTRA_USERS) {
    map.set(extra.canonical_name, {
      canonical_name: extra.canonical_name,
      is_admin: !!extra.is_admin,
      aliases: Array.isArray(extra.aliases) ? extra.aliases : [extra.canonical_name],
    });
  }

  return Array.from(map.values()).map((u) => ({
    ...u,
    is_admin: !!u.is_admin,
  }));
})();

const USER_DIRECTORY_EXACT_ALIASES = (() => {
  const map = new Map();
  for (const user of USER_DIRECTORY) {
    const aliases = Array.isArray(user.aliases) ? user.aliases : [user.canonical_name];
    for (const alias of aliases) {
      const key = normKey(alias);
      if (!key) continue;
      if (!map.has(key)) map.set(key, user.canonical_name);
    }

    const canonicalKey = normKey(user.canonical_name);
    if (canonicalKey && !map.has(canonicalKey)) {
      map.set(canonicalKey, user.canonical_name);
    }
  }
  return map;
})();

const AUTH_AFTER_LOCK_NAMES = [
  "FERNANDES",
  "FELIPE",
  "DANIELLE",
  "ALBERTO FRANZINI NETO",
  "MOSNA",
];

const AUTH_AFTER_LOCK_CANONICAL = (() => {
  const set = new Set();
  for (const rawName of AUTH_AFTER_LOCK_NAMES) {
    const resolved = resolveOfficerFromInput(rawName);
    if (resolved && resolved.canonical_name) {
      set.add(resolved.canonical_name);
      continue;
    }

    const exact = USER_DIRECTORY_EXACT_ALIASES.get(normKey(rawName));
    if (exact) set.add(exact);
  }
  return set;
})();

function fixDentRanks(list) {
  return (Array.isArray(list) ? list : []).map(o => ({ ...o }));
}


const P1_GROUP_LABEL = "P/1";
const P1_CONTROLLER_CANONICALS = new Set(["Fernandes", "Felipe", "Freitas"]);
const P1_CONTROLLED_GROUPS = new Set([
  "UIS-ODONTO",
  "ESTAFETA",
  "AUX ADM TELEMÁTICA",
  "MOTORISTAS CFP 12 x 36",
  "ENCARREGADO DA GUARDA (SERVIO DE DIA) 12 x 36",
]);
const DONIZETTI_EXTRA_GROUPS = new Set([
  "P/4",
  "APROVISIONAMENTO",
  "MOTOMEC/MANUT 12 x 36",
  "RESERVA DE ARMAS BTL",
]);

function getOfficerByCanonical(canonicalName) {
  return OFFICERS.find(o => o.canonical_name === canonicalName) || null;
}

function getOfficerRoleKind(officer) {
  const rank = String(officer && officer.rank ? officer.rank : "").toUpperCase();
  if (rank.startsWith("CB PM") || rank.startsWith("SD PM")) return "cb_sd";
  return "sgt_subten";
}

function isP1Controller(user) {
  if (!user) return false;
  if (user.is_admin) return true;
  return P1_CONTROLLER_CANONICALS.has(String(user.canonical_name || ""));
}

function isDonizettiUser(user) {
  return !!(user && String(user.canonical_name || "") === "Donizetti");
}

function canViewTargetOfficer(user, targetOfficer) {
  if (!user || !targetOfficer) return false;
  if (user.is_admin || isP1Controller(user)) return true;
  if (String(user.canonical_name || "") === String(targetOfficer.canonical_name || "")) return true;
  if (isDonizettiUser(user) && DONIZETTI_EXTRA_GROUPS.has(String(targetOfficer.group_label || ""))) return true;

  const actorOfficer = getOfficerByCanonical(user.canonical_name);
  if (!actorOfficer) return false;

  if (getOfficerRoleKind(actorOfficer) === "cb_sd") return false;
  return String(actorOfficer.group_label || "") === String(targetOfficer.group_label || "");
}

function canEditTargetOfficer(user, targetOfficer) {
  if (!canViewTargetOfficer(user, targetOfficer)) return false;
  if (!user || !targetOfficer) return false;
  if (user.is_admin || isP1Controller(user)) return true;
  if (P1_CONTROLLED_GROUPS.has(String(targetOfficer.group_label || ""))) return false;

  const actorOfficer = getOfficerByCanonical(user.canonical_name);
  if (!actorOfficer) return false;

  if (getOfficerRoleKind(actorOfficer) === "cb_sd") {
    return String(user.canonical_name || "") === String(targetOfficer.canonical_name || "");
  }

  if (isDonizettiUser(user) && DONIZETTI_EXTRA_GROUPS.has(String(targetOfficer.group_label || ""))) {
    return true;
  }

  return String(actorOfficer.group_label || "") === String(targetOfficer.group_label || "");
}

function filterStateForUser(user, assignments, notes, notes_meta) {
  const visibleOfficers = OFFICERS.filter(off => canViewTargetOfficer(user, off));
  const visibleSet = new Set(visibleOfficers.map(off => off.canonical_name));
  const filteredAssignments = {};
  const filteredNotes = {};
  const filteredNotesMeta = {};

  for (const [key, value] of Object.entries(assignments || {})) {
    const [canonical] = String(key).split("|");
    if (visibleSet.has(canonical)) filteredAssignments[key] = value;
  }

  for (const [key, value] of Object.entries(notes || {})) {
    const [canonical] = String(key).split("|");
    if (visibleSet.has(canonical)) filteredNotes[key] = value;
  }

  for (const [key, value] of Object.entries(notes_meta || {})) {
    const [canonical] = String(key).split("|");
    if (visibleSet.has(canonical)) filteredNotesMeta[key] = value;
  }

  return {
    visibleOfficers,
    filteredAssignments,
    filteredNotes,
    filteredNotesMeta,
  };
}


// Após fechamento (sexta 15h+), somente estes podem alterar (qualquer oficial)
const ADMIN_NAMES = new Set([
  "Fernandes",
  "Felipe",
  "Danielle",
  "Alberto Franzini Neto",
  "Eduardo Mosna Xavier",
  "Helder Antnio de Paula",
]);

// Códigos válidos (tudo em MAISCULO, conforme regra)
// - cdigos terminados em * permitem descrio
// - FOJ: sem descrio
const CODES = ["EXP", "MA", "VE", "FOJ", "FO", "FO*", "SV", "SV*", "LP", "FRIAS", "FERIADO", "CONVALESCENA", "CURSO", "OUTROS", "PF", "EXP_A.F", "VE A.F", "MA A.F", "CAS", "EAP", "PPJM", "SV_DIA", "SV_NOITE", "DS"];

// ===============================
// APP
// ===============================
const app = express();
app.set("trust proxy", true);

app.use(helmet({ contentSecurityPolicy: false }));
app.use(compression());
app.use(express.json({ limit: "3mb" }));

app.use(express.static(path.join(__dirname, "public"), {
  setHeaders(res, filePath) {
    if (/\.(html|js|css)$/i.test(filePath)) {
      res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
      res.setHeader("Pragma", "no-cache");
      res.setHeader("Expires", "0");
    }
  }
}));

// ===============================
// DB POOL
// ===============================
const pool = DB_URL
  ? mysql.createPool(DB_URL)
  : mysql.createPool({
      host: DB_HOST,
      port: DB_PORT,
      user: DB_USER,
      password: DB_PASSWORD,
      database: DB_NAME,
      waitForConnections: true,
      connectionLimit: 10,
      queueLimit: 0,
      timezone: "Z",
      connectTimeout: 5000,
    });

// ===============================
// UTIL
// ===============================
function safeJsonParse(s) {
  try { return JSON.parse(s); } catch { return null; }
}


function fixText(s) {
  const str = String(s ?? "");
  if (!str) return "";
  // Corrige "mojibake" comum (UTF-8 interpretado como Latin-1 e regravado).
  if (/[]/.test(str)) {
    try { return Buffer.from(str, "latin1").toString("utf8"); } catch (_e) {}
  }
  return str;
}

// Remove acentos (usar APENAS para nomes de oficiais, conforme regra).
function stripAccents(s) {
  return fixText(s).normalize("NFD").replace(/[\u0300-\u036f]/g, "");
}

function officerNameNoAccents(s) {
  return stripAccents(s).trim().replace(/\s+/g, " ");
}

function normKey(s) {
  return stripAccents(s).toLowerCase().trim().replace(/\s+/g, " ");
}

function fmtYYYYMMDD(d) {
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function fmtDDMMYYYY(iso) {
  const [y, m, d] = String(iso || "").split("-");
  if (!y || !m || !d) return String(iso || "");
  return `${d}/${m}/${y}`;
}

function appendObservationHistory(existingObs, newObs, when = new Date()) {
  const existing = String(existingObs || "").replace(/\r\n/g, "\n").trim();
  const incoming = String(newObs || "").replace(/\r\n/g, "\n").trim();

  if (!incoming) return existing;
  if (existing && incoming === existing) return existing;
  if (existing && incoming.startsWith(existing)) return incoming;

  const stamped = incoming
    .split(/\n+/)
    .map((part) => String(part || "").trim())
    .filter(Boolean)
    .map((part) => `[${fmtDDMMYYYYHHmm(when)}] ${part}`)
    .join("\n");

  if (!stamped) return existing;
  return existing ? `${existing}\n${stamped}` : stamped;
}

// Formata data/hora em pt-BR (São Paulo) no padrão: dd/mm/aaaa s HHhMM
function fmtDDMMYYYYHHmm(value) {
  if (!value) return "";
  const dt = (value instanceof Date) ? value : new Date(value);
  if (Number.isNaN(dt.getTime())) return "";

  // Usa timeZone explicitamente para não depender do TZ do processo.
  const parts = new Intl.DateTimeFormat("pt-BR", {
    timeZone: "America/Sao_Paulo",
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).formatToParts(dt);

  const get = (type) => (parts.find(p => p.type === type) || {}).value || "";
  const dd = get("day");
  const mm = get("month");
  const yyyy = get("year");
  const hh = get("hour");
  const mi = get("minute");
  if (!dd || !mm || !yyyy) return "";

  const dateStr = `${dd}/${mm}/${yyyy}`;
  if (!hh || !mi) return dateStr;
  return `${dateStr} s ${hh}h${mi}`;
}

// Semana FUTURA: o sistema sempre exibe a prxima segunda-feira at o prximo domingo.
// Exemplo: durante 09/03 a 15/03, mostra 16/03 a 22/03.
// Na virada de domingo 00h para segunda, passa a mostrar a semana seguinte.
// Regra adicional: nunca retornar semana anterior a CUTOVER_WEEK_START (segunda-feira).
// Se WEEK_START_OVERRIDE estiver definido, ele prevalece integralmente.

function getWeekRangeISO() {
  if (WEEK_START_OVERRIDE && /^\d{4}-\d{2}-\d{2}$/.test(WEEK_START_OVERRIDE)) {
    const [oy, om, od] = WEEK_START_OVERRIDE.split("-").map(Number);
    const monday = new Date(oy, om - 1, od);
    monday.setHours(0, 0, 0, 0);
    const sunday = new Date(monday);
    sunday.setHours(0, 0, 0, 0);
    sunday.setDate(monday.getDate() + 6);
    return { start: fmtYYYYMMDD(monday), end: fmtYYYYMMDD(sunday) };
  }

  const now = new Date(); // respeita TZ no processo
  now.setHours(0, 0, 0, 0);

  const day = now.getDay(); // 0=dom, 1=seg, ..., 6=sb
  const daysUntilNextMonday = day === 0 ? 1 : 8 - day;

  const nextMonday = new Date(now);
  nextMonday.setDate(now.getDate() + daysUntilNextMonday);
  nextMonday.setHours(0, 0, 0, 0);

  const [cy, cm, cd] = CUTOVER_WEEK_START.split("-").map(Number);
  const cutover = new Date(cy, cm - 1, cd);
  cutover.setHours(0, 0, 0, 0);

  const monday = new Date(Math.max(nextMonday.getTime(), cutover.getTime()));
  monday.setHours(0, 0, 0, 0);

  const sunday = new Date(monday);
  sunday.setHours(0, 0, 0, 0);
  sunday.setDate(monday.getDate() + 6);

  return { start: fmtYYYYMMDD(monday), end: fmtYYYYMMDD(sunday) };
}

function buildDatesForWeek(startYYYYMMDD) {
  const dates = [];
  const [y, m, d] = startYYYYMMDD.split("-").map(Number);
  const base = new Date(y, m - 1, d);
  base.setHours(0, 0, 0, 0);

  for (let i = 0; i < 7; i++) {
    const cur = new Date(base);
    cur.setDate(base.getDate() + i);
    dates.push(fmtYYYYMMDD(cur));
  }
  return dates;
}

// Fechamento: sexta-feira s 15h (São Paulo) até domingo
function isClosedNow() {
  const now = new Date();
  const day = now.getDay(); // 5=sexta
  const hour = now.getHours();

  if (day < 5) return false;
  if (day === 5) return hour >= CLOSE_FRIDAY_HOUR;
  return true; // sábado/domingo
}

function isAdminName(canonicalName) {
  return ADMIN_NAMES.has(String(canonicalName || "").trim());
}


function canEditAfterLockName(name) {
  const exact = USER_DIRECTORY_EXACT_ALIASES.get(normKey(name));
  if (exact && AUTH_AFTER_LOCK_CANONICAL.has(exact)) return true;

  const resolved = resolveOfficerFromInput(name);
  if (resolved && AUTH_AFTER_LOCK_CANONICAL.has(resolved.canonical_name)) return true;

  return false;
}


// ===============================
// FERIADOS (Brasil - nacionais + móveis)
// ===============================
function easterDate(year) {
  // Computus (Meeus/Jones/Butcher)
  const a = year % 19;
  const b = Math.floor(year / 100);
  const c = year % 100;
  const d = Math.floor(b / 4);
  const e = b % 4;
  const f = Math.floor((b + 8) / 25);
  const g = Math.floor((b - f + 1) / 3);
  const h = (19 * a + b - d - g + 15) % 30;
  const i = Math.floor(c / 4);
  const k = c % 4;
  const l = (32 + 2 * e + 2 * i - h - k) % 7;
  const m = Math.floor((a + 11 * h + 22 * l) / 451);
  const month = Math.floor((h + l - 7 * m + 114) / 31); // 3=março,4=abril
  const day = ((h + l - 7 * m + 114) % 31) + 1;
  return new Date(year, month - 1, day);
}

function addDays(d, n) {
  const x = new Date(d);
  x.setDate(x.getDate() + n);
  return x;
}

function isoFromDate(d) {
  return fmtYYYYMMDD(d);
}

function getHolidaysForWeek(weekDates) {
  if (!Array.isArray(weekDates) || !weekDates.length) return [];
  const year = Number(weekDates[0].slice(0,4));
  const set = new Map();

  // Fixos
  const fixed = [
    ["01-01", "Confraternização Universal"],
    ["21-04", "Tiradentes"],
    ["01-05", "Dia do Trabalhador"],
    ["07-09", "Independência do Brasil"],
    ["12-10", "Nossa Senhora Aparecida"],
    ["02-11", "Finados"],
    ["15-11", "Proclamação da República"],
    ["25-12", "Natal"],
  ];
  for (const [md, name] of fixed) {
    set.set(`${year}-${md}`, name);
  }

  // Móveis (referência nacional)
  const easter = easterDate(year);
  const carnaval = addDays(easter, -47); // terça de carnaval (aprox)
  const sextaSanta = addDays(easter, -2);
  const corpusChristi = addDays(easter, 60);

  set.set(isoFromDate(carnaval), "Carnaval");
  set.set(isoFromDate(sextaSanta), "Paixão de Cristo");
  set.set(isoFromDate(corpusChristi), "Corpus Christi");

  const out = [];
  for (const iso of weekDates) {
    if (set.has(iso)) out.push({ date: iso, name: set.get(iso) });
  }
  return out;
}

// ===============================
// SCHEMA / STATE
// ===============================
async function ensureSchema() {
  const conn = await pool.getConnection();
  try {
    await conn.query(`CREATE TABLE IF NOT EXISTS state_store (
      id INT PRIMARY KEY,
      payload LONGTEXT NOT NULL,
      updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`);

    await conn.query(`CREATE TABLE IF NOT EXISTS users (
      id INT AUTO_INCREMENT PRIMARY KEY,
      canonical_name VARCHAR(255) NOT NULL UNIQUE,
      password_hash VARCHAR(255) NOT NULL,
      must_change TINYINT(1) NOT NULL DEFAULT 1,
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`);

    await conn.query(`CREATE TABLE IF NOT EXISTS action_logs (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      actor_name VARCHAR(255) NOT NULL,
      target_name VARCHAR(255) NOT NULL,
      action VARCHAR(64) NOT NULL,
      details TEXT NULL,
      INDEX idx_at (at),
      INDEX idx_actor (actor_name),
      INDEX idx_target (target_name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`);

    // lançamentos por dia (persistência da semana)
    await conn.query(`CREATE TABLE IF NOT EXISTS escala_lancamentos (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      data DATE NOT NULL,
      oficial VARCHAR(255) NOT NULL,
      codigo VARCHAR(32) NOT NULL,
      observacao TEXT NULL,
      created_by VARCHAR(255) NULL,
      updated_by VARCHAR(255) NULL,
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY uniq_data_oficial (data, oficial),
      INDEX idx_data (data),
      INDEX idx_oficial (oficial)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`);

    // migração defensiva: colunas faltantes em 'escala_lancamentos' (ambientes antigos)
    // (usa information_schema para evitar erro de coluna duplicada)
    try {
      const [cols] = await conn.query(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'escala_lancamentos'"
      );
      const names = new Set((cols || []).map(c => String(c.column_name || c.COLUMN_NAME || "").toLowerCase()));
      if (!names.has("observacao")) await conn.query("ALTER TABLE escala_lancamentos ADD COLUMN observacao TEXT NULL");
      if (!names.has("created_by")) await conn.query("ALTER TABLE escala_lancamentos ADD COLUMN created_by VARCHAR(255) NULL");
      if (!names.has("updated_by")) await conn.query("ALTER TABLE escala_lancamentos ADD COLUMN updated_by VARCHAR(255) NULL");
    } catch (e) {
      // tolera corrida/duplicidade em inicialização concorrente
      const code = String((e && e.code) || "");
      const msg = String((e && e.message) || "");
      if (!code.includes("ER_DUP_FIELDNAME") && !msg.toLowerCase().includes("duplicate column")) throw e;
    }
// logs detalhados de alterações (histórico)
await conn.query(`CREATE TABLE IF NOT EXISTS escala_change_log (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  actor_name VARCHAR(255) NOT NULL,
  target_name VARCHAR(255) NOT NULL,
  data DATE NOT NULL,
  field_name VARCHAR(32) NOT NULL,   -- 'codigo' | 'observacao'
  before_value TEXT NULL,
  after_value TEXT NULL,
  INDEX idx_at (at),
  INDEX idx_target (target_name),
  INDEX idx_data (data)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`);


    const [rows] = await conn.query("SELECT id FROM state_store WHERE id=1 LIMIT 1");
    if (!rows.length) {
      const initial = buildFreshState();
      await conn.query("INSERT INTO state_store (id, payload) VALUES (1, ?)", [JSON.stringify(initial)]);
    }
  } finally {
    conn.release();
  }
}

function buildFreshState() {
  const w = getWeekRangeISO();
  const dates = buildDatesForWeek(w.start);

  return {
    meta: {
      system_name: fixText(SYSTEM_NAME),
      footer_mark: ` ${COPYRIGHT_YEAR} - ${fixText(AUTHOR)}`,
      signatures: defaultSignatures(),
    },
    period: { start: w.start, end: w.end },
    dates,
    codes: CODES.slice(),
    officers: OFFICERS.slice(),
    assignments: {},
    notes: {},
    updated_at: new Date().toISOString(),
  };
}

async function safeQuery(sql, params = []) {
  const ACQUIRE_MS = Number(process.env.DB_ACQUIRE_TIMEOUT_MS || 8000);
  const QUERY_MS = Number(process.env.DB_QUERY_TIMEOUT_MS || 8000);

  const withTimeout = (p, ms, label) => Promise.race([
    p,
    new Promise((_, rej) => setTimeout(() => rej(new Error(label)), ms)),
  ]);

  const conn = await withTimeout(pool.getConnection(), ACQUIRE_MS, "db_acquire_timeout");
  try {
    // mysql2 aceita timeout por query quando enviado como objeto { sql, timeout }
    const queryObj = (typeof sql === "string") ? { sql, timeout: QUERY_MS } : { ...sql, timeout: QUERY_MS };
    const [rows] = await withTimeout(conn.query(queryObj, params), QUERY_MS + 500, "db_query_timeout");
    return rows;
  } finally {
    try { conn.release(); } catch (_e) {}
  }
}
function isoFromDbDate(v) {
  if (!v) return "";
  if (v instanceof Date) {
    const y = v.getUTCFullYear();
    const m = String(v.getUTCMonth() + 1).padStart(2, "0");
    const d = String(v.getUTCDate()).padStart(2, "0");
    return `${y}-${m}-${d}`;
  }

  // strings: aceita 'YYYY-MM-DD', 'YYYY/MM/DD', e valores com hora/offset
  let s = String(v).trim();
  if (!s) return "";

  // pega só a parte de data se vier com hora
  if (s.length >= 10) s = s.slice(0, 10);

  // normaliza separador
  if (s.includes("/")) s = s.replaceAll("/", "-");

  // se vier no formato DD-MM-YYYY por algum motivo, converte
  const m = /^(\d{2})-(\d{2})-(\d{4})$/.exec(s);
  if (m) {
    const dd = m[1];
    const mm = m[2];
    const yy = m[3];
    return `${yy}-${mm}-${dd}`;
  }

  return s;
}

function resolveCanonicalFromDbOfficer(oficialStr) {
  const nk = normKey(oficialStr);
  if (!nk) return null;

  const exact = USER_DIRECTORY_EXACT_ALIASES.get(nk);
  if (exact) return exact;

  let bestCanonical = null;
  let bestScore = -1;

  for (const user of USER_DIRECTORY) {
    const aliases = Array.isArray(user.aliases) ? user.aliases : [user.canonical_name];
    for (const alias of aliases) {
      const candidate = normKey(alias);
      if (!candidate) continue;

      if (nk === candidate) return user.canonical_name;

      let score = 0;
      if (nk.includes(candidate) || candidate.includes(nk)) {
        const maxLen = Math.max(nk.length, candidate.length) || 1;
        score = Math.min(nk.length, candidate.length) / maxLen;
      }

      if (score > bestScore) {
        bestScore = score;
        bestCanonical = user.canonical_name;
      }
    }
  }

  return bestScore >= 0.70 ? bestCanonical : null;
}

async function fetchLancamentosForPeriod(periodStartISO, periodEndISO) {
  // periodStartISO / periodEndISO são YYYY-MM-DD
  // Compatível com coluna 'data' como DATE ou como string (ex.: 'YYYY/MM/DD')
  const sql = `
    SELECT data, oficial, codigo, observacao, created_at, updated_at, created_by, updated_by
      FROM escala_lancamentos
     WHERE (
       CASE
         WHEN CAST(data AS CHAR) LIKE '%/%'
           THEN STR_TO_DATE(CAST(data AS CHAR), '%Y/%m/%d')
         ELSE STR_TO_DATE(SUBSTRING(CAST(data AS CHAR), 1, 10), '%Y-%m-%d')
       END
     ) BETWEEN ? AND ?
  `;
  return safeQuery(sql, [periodStartISO, periodEndISO]);
}

function fetchChangeLogsForPeriod(periodStartISO, periodEndISO, limit = 500) {
  const sql = `
    SELECT at, actor_name, target_name, data, field_name, before_value, after_value
      FROM escala_change_log
     WHERE data BETWEEN ? AND ?
     ORDER BY at ASC
     LIMIT ?
  `;
  return safeQuery(sql, [periodStartISO, periodEndISO, limit]);
}

async function fetchLastActionForPeriod(periodStartISO, periodEndISO) {
  // action_logs.at é TIMESTAMP; filtra pela janela da semana (São Paulo)
  const start = `${periodStartISO} 00:00:00`;
  const end = `${periodEndISO} 23:59:59`;
  const sql = `
    SELECT at, actor_name, action
      FROM action_logs
     WHERE at BETWEEN ? AND ?
       AND action IN ('update_day','update_signatures','reset_week')
     ORDER BY at DESC
     LIMIT 1
  `;
  const rows = await safeQuery(sql, [start, end]);
  return (rows && rows.length) ? rows[0] : null;
}


function buildAssignmentsAndNotesFromLancamentos(rows, validDates) {
  const assignments = {};
  const notes = {};
  const notes_meta = {};

  const valid = new Set(validDates || []);
  const validCodes = new Set(CODES);

  for (const r of rows || []) {
    const iso = isoFromDbDate(r.data);
    if (!valid.has(iso)) continue;

    const canonical = resolveCanonicalFromDbOfficer(r.oficial);
    if (!canonical) continue;

    // normaliza código vindo do DB (legado)
    let code = String(r.codigo || "").trim();
    // remove espaços estranhos
    code = code.replace(/\s+/g, "");
    // mantm FO simples e FOJ como cdigos distintos
    if (/^FO\.?$/i.test(code)) code = "FO";
    if (/^FOJ$/i.test(code)) code = "FOJ";
    // mantm exatamente FO*/SV* (asterisco) e SV simples
    if (/^FO\*$/i.test(code)) code = "FO*";
    if (/^SV$/i.test(code)) code = "SV";
    if (/^SV\*$/i.test(code)) code = "SV*";
    // mantm PF
    if (/^PF$/i.test(code)) code = "PF";
    // mantm FRIAS (aceita FRIAS)
    if (/^FRIAS$/i.test(code)) code = "FRIAS";
    // mantm FERIADO
    if (/^FERIADO$/i.test(code)) code = "FERIADO";
    // mantm CONVALESCENA (aceita sem cedilha)
    if (/^CONVALESCENA$/i.test(code)) code = "CONVALESCENA";
    // mantm cdigos A.F com e sem underscore/espaos
    if (/^EXP[ _]?A\.?F\.?$/i.test(code)) code = "EXP_A.F";
    if (/^VE[ _]?A\.?F\.?$/i.test(code)) code = "VE A.F";
    if (/^MA[ _]?A\.?F\.?$/i.test(code)) code = "MA A.F";
    if (/^CAS$/i.test(code)) code = "CAS";
    if (/^EAP$/i.test(code)) code = "EAP";

    if (!validCodes.has(code)) {
      // ignora códigos desconhecidos/antigos
      continue;
    }

    const key = `${canonical}|${iso}`;
    assignments[key] = code;

    // observao s faz sentido em OUTROS e cdigos terminados em *
    const obs = (r.observacao == null) ? "" : String(r.observacao).trim();
    if (obs && (code === "OUTROS" || /\*$/.test(code))) {
      notes[key] = obs;

      // metadados para exibir no sistema/PDF
      const updatedAt = r.updated_at ? new Date(r.updated_at).toISOString() : null;
      notes_meta[key] = {
        updated_at: updatedAt,
        updated_by: r.updated_by ? String(r.updated_by) : null,
        created_by: r.created_by ? String(r.created_by) : null,
      };
    }
  }

  return { assignments, notes, notes_meta };
}

async function getStateAutoReset() {
  const rows = await safeQuery("SELECT payload FROM state_store WHERE id=1 LIMIT 1");
  let st = rows.length ? safeJsonParse(rows[0].payload) : null;

  const currentWeek = getWeekRangeISO();
  const needReset = !st || !st.period || st.period.start !== currentWeek.start || st.period.end !== currentWeek.end;

  if (needReset) {
    // se existia uma semana anterior registrada, significa virada de semana  limpar lançamentos (domingo fecha e apaga tudo)
    // não remove usuários nem logs, apenas a tabela de registros da escala.
    try {
      if (st && st.period && (st.period.start || st.period.end)) {
        await safeQuery("DELETE FROM escala_lancamentos");
      }
    } catch (_e) {
      // ignora se a tabela não existir em algum ambiente
    }

    st = buildFreshState();
    await safeQuery(
      "INSERT INTO state_store (id, payload) VALUES (1, ?) ON DUPLICATE KEY UPDATE payload=VALUES(payload), updated_at=CURRENT_TIMESTAMP",
      [JSON.stringify(st)]
    );
    return { st, didReset: true };
  }

  // garante campos
  st.meta = st.meta || {};
  st.meta.system_name = SYSTEM_NAME;
  st.meta.footer_mark = ` ${COPYRIGHT_YEAR} - ${AUTHOR}`;
  st.meta.signatures = st.meta.signatures && typeof st.meta.signatures === "object" ? st.meta.signatures : defaultSignatures();
  st.codes = CODES.slice();
  st.officers = OFFICERS.slice();
  st.period = { start: currentWeek.start, end: currentWeek.end };
  st.dates = buildDatesForWeek(currentWeek.start);
  st.assignments = st.assignments && typeof st.assignments === "object" ? st.assignments : {};
  st.notes = st.notes && typeof st.notes === "object" ? st.notes : {};
  return { st, didReset: false };

}

// ===============================
// AUTH
// ===============================
function signToken(me) {
  return jwt.sign(
    { canonical_name: me.canonical_name, is_admin: !!me.is_admin, must_change: !!me.must_change, can_edit_after_lock: !!me.can_edit_after_lock },
    JWT_SECRET,
    { expiresIn: "14d" }
  );
}

// token curto e específico para abrir PDF via URL (window.open não envia headers)
function signPdfToken(me) {
  return jwt.sign(
    { canonical_name: me.canonical_name, is_admin: !!me.is_admin, can_edit_after_lock: !!me.can_edit_after_lock, scope: "pdf" },
    JWT_SECRET,
    { expiresIn: "2m" }
  );
}

function pdfAuth(req, res, next) {
  // 1) Bearer token normal
  const auth = (req.headers["authorization"] || "").toString();
  const m = auth.match(/^Bearer\s+(.+)$/i);
  if (m) {
    try {
      const payload = jwt.verify(m[1], JWT_SECRET);
      req.user = {
        canonical_name: String(payload.canonical_name || "").trim(),
        is_admin: !!payload.is_admin,
        must_change: !!payload.must_change,
        can_edit_after_lock: !!payload.can_edit_after_lock || canEditAfterLockName(payload.canonical_name),
      };
      return next();
    } catch (_e) {
      // continua para tentar token via query
    }
  }

  // 2) token via query (curto, só para PDF)
  const q = (req.query && req.query.token ? String(req.query.token) : "").trim();
  if (!q) return res.status(401).json({ error: "não autenticado" });

  try {
    const payload = jwt.verify(q, JWT_SECRET);
    if (payload.scope !== "pdf") return res.status(401).json({ error: "token inválido" });
    req.user = {
      canonical_name: String(payload.canonical_name || "").trim(),
      is_admin: !!payload.is_admin,
      must_change: false,
      can_edit_after_lock: !!payload.can_edit_after_lock || canEditAfterLockName(payload.canonical_name),
    };
    return next();
  } catch (e) {
    return res.status(401).json({ error: "token inválido" });
  }
}

function authRequired(allowMustChange = false) {
  return (req, res, next) => {
    const auth = (req.headers["authorization"] || "").toString();
    const m = auth.match(/^Bearer\s+(.+)$/i);
    if (!m) return res.status(401).json({ error: "não autenticado" });

    try {
      const payload = jwt.verify(m[1], JWT_SECRET);
      req.user = {
        canonical_name: String(payload.canonical_name || "").trim(),
        is_admin: !!payload.is_admin,
        must_change: !!payload.must_change,
        can_edit_after_lock: !!payload.can_edit_after_lock || canEditAfterLockName(payload.canonical_name),
      };
      if (!allowMustChange && req.user.must_change) {
        return res.status(403).json({ error: "troca de senha obrigatória" });
      }
      return next();
    } catch (e) {
      return res.status(401).json({ error: "token inválido" });
    }
  };
}

async function findOrCreateUser(canonical_name) {
  const rows = await safeQuery("SELECT id, canonical_name, password_hash, must_change FROM users WHERE canonical_name=? LIMIT 1", [canonical_name]);
  if (rows.length) return rows[0];

  // cria com senha padrão e must_change=1
  const hash = await bcrypt.hash(DEFAULT_PASSWORD, 10);
  await safeQuery("INSERT INTO users (canonical_name, password_hash, must_change) VALUES (?, ?, 1)", [canonical_name, hash]);
  const created = await safeQuery("SELECT id, canonical_name, password_hash, must_change FROM users WHERE canonical_name=? LIMIT 1", [canonical_name]);
  return created[0];
}

function resolveOfficerFromInput(nameInput) {
  const nk = normKey(nameInput);
  if (!nk) return null;

  let best = null;
  let bestScore = 0;

  for (const user of USER_DIRECTORY) {
    const aliases = Array.isArray(user.aliases) ? user.aliases : [user.canonical_name];
    for (const alias of aliases) {
      const candidate = normKey(alias);
      if (!candidate) continue;

      const a = new Set(nk.split(" ").filter(Boolean));
      const b = new Set(candidate.split(" ").filter(Boolean));
      const inter = [...a].filter(t => b.has(t)).length;
      const union = new Set([...a, ...b]).size || 1;
      let score = inter / union;

      const aParts = nk.split(" ").filter(Boolean);
      const bParts = candidate.split(" ").filter(Boolean);
      if (aParts.length && bParts.length) {
        if (aParts[0] === bParts[0]) score += 0.10;
        if (aParts[aParts.length - 1] === bParts[bParts.length - 1]) score += 0.15;
      }

      if (score > bestScore) {
        bestScore = score;
        best = {
          canonical_name: user.canonical_name,
          is_admin: !!user.is_admin,
        };
      }
    }
  }

  if (!best || bestScore < 0.55) return null;
  return best;
}

// ===============================
// LOG
// ===============================
async function logAction(actor, target, action, details = "") {
  await safeQuery(
    "INSERT INTO action_logs (actor_name, target_name, action, details) VALUES (?, ?, ?, ?)",
    [actor, target, action, details || ""]
  );
}

// ===============================
// PDF
// ===============================
function requirePdfKitOr501(res) {
  try {
    return require("pdfkit");
  } catch {
    res.status(501).json({ error: "geração de PDF indisponível" });
    return null;
  }
}

// ===============================
// ROTAS
// ===============================
app.get("/api/health", async (_req, res) => {
  try {
    const conn = await pool.getConnection();
    await conn.ping();
    conn.release();
    return res.json({ ok: true, tz: process.env.TZ, db_mode: DB_URL ? "url" : "host", week: getWeekRangeISO() });
  } catch (err) {
    return res.status(500).json({ ok: false, error: err && err.message ? err.message : "falha no health" });
  }
});

// STATUS PBLICO (sem token)  para teste externo e monitoramento no Railway
app.get("/api/status", async (_req, res) => {
  try {
    // não falha se o DB estiver indisponível: retorna o básico
    try {
      const conn = await pool.getConnection();
      await conn.ping();
      conn.release();
    } catch (_e) {
      // ignora
    }

    const week = getWeekRangeISO();
    return res.json({
      ok: true,
      tz: process.env.TZ,
      week,
      locked: isClosedNow(),
      close_friday_hour: CLOSE_FRIDAY_HOUR,
      system_name: fixText(SYSTEM_NAME),
    });
  } catch (err) {
    return res.status(500).json({ ok: false, error: err && err.message ? err.message : "falha no status" });
  }
});

// WEEK PBLICO (sem token)  ajuda o frontend e facilita debug
app.get("/api/week", (_req, res) => {
  const week = getWeekRangeISO();
  return res.json({ ok: true, week, dates: buildDatesForWeek(week.start) });
});

// login: nome + senha
app.post("/api/login", async (req, res) => {
  try {
    const name = (req.body && req.body.name ? req.body.name : "").toString().trim();
    const password = (req.body && req.body.password ? req.body.password : "").toString();

    const off = resolveOfficerFromInput(name);
    if (!off) return res.status(403).json({ error: "nome no reconhecido. use graduao + nome de guerra." });

    const userRow = await findOrCreateUser(off.canonical_name);

    const ok = await bcrypt.compare(password, userRow.password_hash);
    if (!ok) return res.status(403).json({ error: "senha inválida" });

    const me = {
      canonical_name: off.canonical_name,
      is_admin: !!off.is_admin || isAdminName(off.canonical_name),
      must_change: !!userRow.must_change,
      can_edit_after_lock: canEditAfterLockName(off.canonical_name),
    };

    const token = signToken(me);

    // log
    await logAction(me.canonical_name, me.canonical_name, "login", "");

    return res.json({ ok: true, token, me, must_change: me.must_change });
  } catch (err) {
    return res.status(500).json({ error: "erro no login", details: err.message });
  }
});

// troca obrigatória de senha
app.post("/api/change_password", authRequired(true), async (req, res) => {
  try {
    const newPass = (req.body && req.body.new_password ? req.body.new_password : "").toString();
    if (!newPass || newPass.length < 6) return res.status(400).json({ error: "senha muito curta (mínimo 6)" });

    const hash = await bcrypt.hash(newPass, 10);
    await safeQuery("UPDATE users SET password_hash=?, must_change=0 WHERE canonical_name=?", [hash, req.user.canonical_name]);

    await logAction(req.user.canonical_name, req.user.canonical_name, "change_password", "");

    return res.json({ ok: true });
  } catch (err) {
    return res.status(500).json({ error: "erro ao trocar senha", details: err.message });
  }
});

// estado: todos autenticados podem ver (mesmo com must_change)
app.get("/api/state", authRequired(true), async (req, res) => {
  try {
    const { st } = await getStateAutoReset();
    const holidays = getHolidaysForWeek(st.dates);

    // se houver lançamentos no MySQL (escala_lancamentos), eles prevalecem
    let assignments = st.assignments || {};
    const baseNotes = (st.notes && typeof st.notes === "object") ? st.notes : {};
    const baseMeta = (st.notes_meta && typeof st.notes_meta === "object") ? st.notes_meta : {};
    let notes = baseNotes;
    let notes_meta = baseMeta;
    try {
      const rows = await fetchLancamentosForPeriod(st.period.start, st.period.end);
      const built = buildAssignmentsAndNotesFromLancamentos(rows, st.dates);
      if (Object.keys(built.assignments).length) {
        assignments = built.assignments;
        notes = built.notes;
        notes_meta = built.notes_meta || {};
      }
    } catch (_e) {
      // se a tabela ainda não existir em algum ambiente, mantém state_store
    }

    // merge de descrições: mantém state_store.notes quando o MySQL vier sem observação
    try {
      const baseNotes = (st.notes && typeof st.notes === "object") ? st.notes : {};
      const baseMeta = (st.notes_meta && typeof st.notes_meta === "object") ? st.notes_meta : {};
      // se não veio nada do DB, usa o state_store
      if (!notes || Object.keys(notes).length === 0) {
        notes = { ...baseNotes };
      } else {
        for (const k of Object.keys(baseNotes)) {
          const v = String(baseNotes[k] || "").trim();
          if (!v) continue;
          const cur = (notes[k] == null) ? "" : String(notes[k]).trim();
          if (!cur) notes[k] = v;
        }
      }
      if (!notes_meta || Object.keys(notes_meta).length === 0) {
        notes_meta = { ...baseMeta };
      } else {
        for (const k of Object.keys(baseMeta)) {
          if (!notes_meta[k]) notes_meta[k] = baseMeta[k];
        }
      }
    } catch (_e) {}

    const periodLabel = `perodo: ${fmtDDMMYYYY(st.period.start)} a ${fmtDDMMYYYY(st.period.end)}`;
    const scoped = filterStateForUser(req.user, assignments, notes, notes_meta);

    return res.json({
      ok: true,
      me: {
        canonical_name: req.user.canonical_name,
        is_admin: req.user.is_admin,
        can_edit_after_lock: !!req.user.can_edit_after_lock || canEditAfterLockName(req.user.canonical_name),
        can_view_all_sections: isP1Controller(req.user) || !!req.user.is_admin,
      },
      meta: {
        system_name: fixText(SYSTEM_NAME),
        footer_mark: ` ${COPYRIGHT_YEAR} - ${fixText(AUTHOR)}`,
        period_label: periodLabel,
        signatures: (st.meta && st.meta.signatures) ? st.meta.signatures : defaultSignatures(),
      },
      locked: isClosedNow(),
      holidays,
      officers: fixDentRanks(scoped.visibleOfficers).map(o => ({
        ...o,
        rank: fixText(o.rank),
        name: officerNameNoAccents(o.name),
        group_label: fixText(o.group_label),
        can_edit: canEditTargetOfficer(req.user, o),
      })),
      dates: st.dates,
      codes: CODES,
      assignments: scoped.filteredAssignments,
      notes: scoped.filteredNotes,
      notes_meta: scoped.filteredNotesMeta,
    });
  } catch (err) {
    return res.status(500).json({ error: "erro ao carregar", details: err.message });
  }
});

// assinaturas do PDF (somente admin)
app.put("/api/signatures", authRequired(true), async (req, res) => {
  try {
    if (!req.user.is_admin) return res.status(403).json({ error: "não autorizado" });

    const { st } = await getStateAutoReset();
    const cur = (st.meta && st.meta.signatures) ? st.meta.signatures : defaultSignatures();

    const left_name = String(req.body && req.body.left_name ? req.body.left_name : cur.left_name).trim();
    const left_role = String(req.body && req.body.left_role ? req.body.left_role : cur.left_role).trim();
    const center_name = String(req.body && req.body.center_name ? req.body.center_name : cur.center_name).trim();
    const center_role = String(req.body && req.body.center_role ? req.body.center_role : cur.center_role).trim();
    const right_name = String(req.body && req.body.right_name ? req.body.right_name : cur.right_name).trim();
    const right_role = String(req.body && req.body.right_role ? req.body.right_role : cur.right_role).trim();

    if (!left_name || !center_name || !right_name) return res.status(400).json({ error: "nome das assinaturas  obrigatrio" });
    if (left_name.length > 120 || center_name.length > 120 || right_name.length > 120) return res.status(400).json({ error: "nome muito longo" });
    if (left_role.length > 120 || center_role.length > 120 || right_role.length > 120) return res.status(400).json({ error: "cargo/funo muito longa" });

    st.meta = st.meta || {};
    st.meta.signatures = {
      left_name: left_name.toUpperCase(),
      left_role: left_role.toUpperCase(),
      center_name: center_name.toUpperCase(),
      center_role: center_role.toUpperCase(),
      right_name: right_name.toUpperCase(),
      right_role: right_role.toUpperCase(),
    };

    // metadados do último registro (para PDF)
    st.last_edit_actor = req.user.canonical_name;
    st.last_edit_at = new Date().toISOString();

    await safeQuery(
      "INSERT INTO state_store (id, payload) VALUES (1, ?) ON DUPLICATE KEY UPDATE payload=VALUES(payload), updated_at=CURRENT_TIMESTAMP",
      [JSON.stringify(st)]
    );

    await logAction(req.user.canonical_name, req.user.canonical_name, "update_signatures", "assinaturas do PDF atualizadas");

    return res.json({ ok: true, signatures: st.meta.signatures });
  } catch (err) {
    return res.status(500).json({ error: "erro ao salvar assinaturas", details: err.message });
  }
});


// histórico de alterações (somente admin)
app.get("/api/change_logs", authRequired(true), async (req, res) => {
  try {
    if (!req.user.is_admin) return res.status(403).json({ error: "não autorizado" });

    const limit = Math.max(10, Math.min(500, Number(req.query && req.query.limit ? req.query.limit : 200)));
    const sql = `
      SELECT id, at, actor_name, target_name, data, field_name, before_value, after_value
        FROM escala_change_log
       ORDER BY at DESC
       LIMIT ?
    `;
    const rows = await safeQuery(sql, [limit]);
    return res.json({ ok: true, rows: rows || [] });
  } catch (err) {
    return res.status(500).json({ error: "erro ao carregar histórico", details: err.message });
  }
});


// salvar alterações (somente após troca de senha)
app.put("/api/assignments", authRequired(false), async (req, res) => {
  try {
    const { st } = await getStateAutoReset();

    const updates = Array.isArray(req.body && req.body.updates) ? req.body.updates : [];
    if (!updates.length) return res.status(400).json({ error: "nenhuma alteração enviada" });

    const locked = isClosedNow();
    const actor = req.user.canonical_name;

    if (locked && !req.user.can_edit_after_lock && !canEditAfterLockName(req.user.canonical_name)) {
      return res.status(423).json({ error: "edição fechada (sexta 15h até domingo)" });
    }

    const validDates = new Set(st.dates || []);
    const validCodes = new Set(CODES);
    const officersByCanonical = new Set(OFFICERS.map(o => o.canonical_name));

    let applied = 0;

    for (const u of updates) {
      const date = String(u.date || "").trim();
      if (!validDates.has(date)) continue;

      let target = String(u.canonical_name || "").trim();
      if (!officersByCanonical.has(target)) continue;

      const targetOfficer = getOfficerByCanonical(target);
      if (!targetOfficer) continue;
      if (!canEditTargetOfficer(req.user, targetOfficer)) continue;

      let code = String(u.code || "").trim();
      if (!code) code = ""; // limpar
      if (code && !validCodes.has(code)) continue;

      const key = `${target}|${date}`;

      const beforeCode = (st.assignments && st.assignments[key]) ? String(st.assignments[key]) : "";
      const beforeObs = (st.notes && st.notes[key]) ? String(st.notes[key]) : "";

      const needObs = (code === "OUTROS" || /\*$/.test(code));
      const incomingObs = needObs ? String(u.observacao == null ? "" : u.observacao).trim() : "";
      const mergedObs = needObs ? appendObservationHistory(beforeObs, incomingObs, new Date()) : "";

      // atualiza state_store (permite limpar)
      st.assignments = st.assignments || {};
      st.notes = st.notes || {};
      st.notes_meta = st.notes_meta || {};

      if (!code) {
        delete st.assignments[key];
        delete st.notes[key];
        delete st.notes_meta[key];
      } else {
        st.assignments[key] = code;
        if (needObs && mergedObs) {
          st.notes[key] = mergedObs;
          st.notes_meta[key] = {
            updated_at: new Date().toISOString(),
            updated_by: actor,
            created_by: (st.notes_meta[key] && st.notes_meta[key].created_by) ? st.notes_meta[key].created_by : actor,
          };
        } else if (!needObs) {
          delete st.notes[key];
          delete st.notes_meta[key];
        }
      }

      // persistência no MySQL
      try {
        if (!code) {
          await safeQuery("DELETE FROM escala_lancamentos WHERE data=? AND oficial=?", [date, target]);
        } else {
          const obsToSave = (needObs && mergedObs) ? mergedObs : null;
          await safeQuery(
            "INSERT INTO escala_lancamentos (data, oficial, codigo, observacao, created_by, updated_by) VALUES (?, ?, ?, ?, ?, ?) " +
              "ON DUPLICATE KEY UPDATE codigo=VALUES(codigo), observacao=VALUES(observacao), updated_by=VALUES(updated_by), updated_at=CURRENT_TIMESTAMP",
            [date, target, code, obsToSave, actor, actor]
          );
        }
      } catch (_e) {
        // ignora se a tabela não existir em algum ambiente
      }

      // log
      const changedCode = (beforeCode || "") !== (code || "");
      const changedObs = needObs && !!incomingObs && (beforeObs || "") !== (mergedObs || "");
      if (changedCode || changedObs) {
        const logBefore = beforeCode || "-";
        const logAfter = code || "-";
        const logExtra = needObs ? ` | obs: ${(beforeObs || "-")} -> ${(mergedObs || "-")}` : "";
        await logAction(actor, target, "update_day", `${date}: ${logBefore} -> ${logAfter}${logExtra}`);
      
// histórico detalhado
try {
  if (changedCode) {
    await safeQuery(
      "INSERT INTO escala_change_log (actor_name, target_name, data, field_name, before_value, after_value) VALUES (?, ?, ?, 'codigo', ?, ?)",
      [actor, target, date, beforeCode || null, code || null]
    );
  }
  if (changedObs) {
    await safeQuery(
      "INSERT INTO escala_change_log (actor_name, target_name, data, field_name, before_value, after_value) VALUES (?, ?, ?, 'observacao', ?, ?)",
      [actor, target, date, beforeObs || null, mergedObs || null]
    );
  }
} catch (_e) {
  // ignora
}
}

      applied++;
    }

    // metadados do último registro (para PDF)
    st.last_edit_actor = actor;
    st.last_edit_at = new Date().toISOString();
    st.updated_at = st.last_edit_at;
    await safeQuery(
      "INSERT INTO state_store (id, payload) VALUES (1, ?) ON DUPLICATE KEY UPDATE payload=VALUES(payload), updated_at=CURRENT_TIMESTAMP",
      [JSON.stringify(st)]
    );

    return res.json({ ok: true, applied });
  } catch (err) {
    return res.status(500).json({ error: "erro ao salvar", details: err.message });
  }
});


// PDF: todos autenticados podem ler

// gera link autenticado para abrir PDF em nova aba (sem depender de headers)
app.post("/api/pdf_link", authRequired(true), async (req, res) => {
  try {
    const me = {
      canonical_name: req.user.canonical_name,
      is_admin: !!req.user.is_admin,
    };
    const t = signPdfToken(me);
    return res.json({ ok: true, url: `/api/pdf?token=${encodeURIComponent(t)}` });
  } catch (err) {
    return res.status(500).json({ error: "erro ao gerar link do PDF", details: err.message });
  }
});

app.get("/api/pdf", pdfAuth, async (req, res) => {
  const PDFDocument = requirePdfKitOr501(res);
  if (!PDFDocument) return;

  try {
    const { st } = await getStateAutoReset();

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `inline; filename="escala_semanal.pdf"`);

    const doc = new PDFDocument({ margin: 28, size: "A4", layout: "landscape" });
    doc.pipe(res);

    // cabeçalho
    doc.fontSize(16).text(fixText(SYSTEM_NAME), { align: "center" });
    doc.moveDown(0.2);
    doc.fontSize(10).text(`Perodo: ${fmtDDMMYYYY(st.period.start)} a ${fmtDDMMYYYY(st.period.end)}`, { align: "center" });
    doc.moveDown(0.6);

    const dates = st.dates || [];

    // prefere dados do MySQL (escala_lancamentos); fallback para state_store
    let assignments = st.assignments || {};
    // descries (OUTROS/cdigos com asterisco) salvas no state_store (fallback)
    const baseNotes = (st.notes && typeof st.notes === "object") ? st.notes : {};
    const baseMeta = (st.notes_meta && typeof st.notes_meta === "object") ? st.notes_meta : {};

    // inicia com state_store para garantir que o PDF sempre mostre o que aparece no front
    let notes = { ...baseNotes };
    let notes_meta = { ...baseMeta };
    let usedDb = false;
    try {
      const rows = await fetchLancamentosForPeriod(st.period.start, st.period.end);
      const built = buildAssignmentsAndNotesFromLancamentos(rows, dates);
      if (Object.keys(built.assignments).length) {
        assignments = built.assignments;
        // DB passa a ser a fonte primária, mas fazemos merge defensivo com o state_store
        notes = (built.notes && typeof built.notes === "object") ? built.notes : {};
        notes_meta = (built.notes_meta && typeof built.notes_meta === "object") ? built.notes_meta : {};
        usedDb = true;
        // merge defensivo: se o DB não tiver observação (ou vier NULL/vazio), mantém o state_store
        for (const k of Object.keys(baseNotes)) {
          const codeNow = assignments && assignments[k] ? String(assignments[k]) : "";
          if (codeNow !== "OUTROS" && !/\*$/.test(codeNow)) continue;
          const dbVal = (notes && notes[k] != null) ? String(notes[k]).trim() : "";
          if (!dbVal) {
            const v = String(baseNotes[k] || "").trim();
            if (v) notes[k] = v;
          }
        }
        // mantém metadados do state_store quando o DB não tiver
        for (const k of Object.keys(baseMeta)) {
          if (!notes_meta[k]) notes_meta[k] = baseMeta[k];
        }
      }
    } catch (_e) {
      // mantém fallback
    }


// histórico para PDF (quando houver DB)
let changeLogs = [];
if (usedDb) {
  try {
    const rows = await fetchChangeLogsForPeriod(st.period.start, st.period.end, 500);
    changeLogs = Array.isArray(rows) ? rows : [];
  } catch (_e) {
    changeLogs = [];
  }
}

// ltimo registro (nome + data/hora) para rodap do PDF
let lastActor = (st && st.last_edit_actor) ? officerNameNoAccents(st.last_edit_actor) : "";
let lastAt = (st && st.last_edit_at) ? st.last_edit_at : (st && st.updated_at ? st.updated_at : null);

// fallback: action_logs (para ambientes antigos)
if (!lastAt || !lastActor) {
  let lastAction = null;
  try {
    lastAction = await fetchLastActionForPeriod(st.period.start, st.period.end);
  } catch (_e) {
    lastAction = null;
  }
  if (!lastActor && lastAction && lastAction.actor_name) lastActor = officerNameNoAccents(lastAction.actor_name);
  if (!lastAt && lastAction && lastAction.at) lastAt = lastAction.at;
}

const lastStamp = fmtDDMMYYYYHHmm(lastAt);
    const scoped = filterStateForUser(req.user, assignments, notes, notes_meta);
    const visibleOfficers = scoped.visibleOfficers;
    assignments = scoped.filteredAssignments;
    notes = scoped.filteredNotes;
    notes_meta = scoped.filteredNotesMeta;

    // tabela
    const left = doc.page.margins.left;
    const top = doc.y;
    const colWName = 220;
    const colWDay = 80;

    const drawPdfHeader = (yBase) => {
      doc.fontSize(9).font("Helvetica-Bold");
      doc.text("PRAA", left, yBase, { width: colWName, align: "left" });
      for (let i = 0; i < dates.length; i++) {
        doc.text(fmtDDMMYYYY(dates[i]), left + colWName + i * colWDay, yBase, { width: colWDay, align: "center" });
      }
      doc.moveTo(left, yBase + 14).lineTo(left + colWName + colWDay * dates.length, yBase + 14).stroke();
      doc.font("Helvetica");
      return yBase + 18;
    };

    let y = drawPdfHeader(top);

    doc.fontSize(8);
    let lastGroup = null;
    for (const off of visibleOfficers) {
      if (off.group_label !== lastGroup) {
        if (y > doc.page.height - 150) {
          doc.addPage({ margin: 28, size: "A4", layout: "landscape" });
          y = drawPdfHeader(doc.y);
        }
        doc.font("Helvetica-Bold").text(String(off.group_label || "SEM DIVISÃO"), left, y, { width: colWName + colWDay * dates.length, align: "left" });
        doc.moveTo(left, y + 12).lineTo(left + colWName + colWDay * dates.length, y + 12).stroke();
        y += 14;
        doc.font("Helvetica");
        lastGroup = off.group_label;
      }

      if (y > doc.page.height - 140) {
        doc.addPage({ margin: 28, size: "A4", layout: "landscape" });
        y = drawPdfHeader(doc.y);
        doc.font("Helvetica-Bold").text(String(off.group_label || "SEM DIVISÃO"), left, y, { width: colWName + colWDay * dates.length, align: "left" });
        doc.moveTo(left, y + 12).lineTo(left + colWName + colWDay * dates.length, y + 12).stroke();
        y += 14;
        doc.font("Helvetica");
      }

      const label = `${fixText(off.rank)} ${officerNameNoAccents(off.name)}`;
      doc.text(label, left, y, { width: colWName, align: "left" });
      doc.moveTo(left, y + 12).lineTo(left + colWName, y + 12).stroke();

      for (let i = 0; i < dates.length; i++) {
        const k = `${off.canonical_name}|${dates[i]}`;
        const code = assignments[k] ? String(assignments[k]) : "";
        doc.text(code || "-", left + colWName + i * colWDay, y, { width: colWDay, align: "center" });
      }

      const rowLineY = y + 12;
      doc.moveTo(left, rowLineY).lineTo(left + colWName + colWDay * dates.length, rowLineY).stroke();

      y += 14;
    }
    // assinaturas sempre na primeira pgina
    {
      const leftMargin = doc.page.margins.left;
      const usableW = doc.page.width - doc.page.margins.left - doc.page.margins.right;
      const gap = 18;
      const lineW = (usableW - (gap * 2)) / 3;
      const xLeft = leftMargin;
      const xCenter = xLeft + lineW + gap;
      const xRight = xCenter + lineW + gap;
      const yLine = doc.page.height - 100;

      const sig = (st.meta && st.meta.signatures) ? st.meta.signatures : defaultSignatures();

      doc.moveTo(xLeft, yLine).lineTo(xLeft + lineW, yLine).stroke();
      doc.moveTo(xCenter, yLine).lineTo(xCenter + lineW, yLine).stroke();
      doc.moveTo(xRight, yLine).lineTo(xRight + lineW, yLine).stroke();

      doc.fontSize(10).text(String(sig.left_name || "").toUpperCase(), xLeft, yLine + 6, { width: lineW, align: "center" });
      doc.fontSize(9).text(String(sig.left_role || "").toUpperCase(), xLeft, yLine + 22, { width: lineW, align: "center" });

      doc.fontSize(10).text(String(sig.center_name || "").toUpperCase(), xCenter, yLine + 6, { width: lineW, align: "center" });
      doc.fontSize(9).text(String(sig.center_role || "").toUpperCase(), xCenter, yLine + 22, { width: lineW, align: "center" });

      doc.fontSize(10).text(String(sig.right_name || "").toUpperCase(), xRight, yLine + 6, { width: lineW, align: "center" });
      doc.fontSize(9).text(String(sig.right_role || "").toUpperCase(), xRight, yLine + 22, { width: lineW, align: "center" });
    }

    // detalhamento de descries (OUTROS e cdigos com asterisco)
    const noteEntries = [];
    for (const k of Object.keys(notes || {})) {
      const [canonical, iso] = k.split("|");
      const off = visibleOfficers.find(o => o.canonical_name === canonical);
      if (!off) continue;
      const code = assignments[k] ? String(assignments[k]) : "";
      // s imprime descries para OUTROS e cdigos com asterisco
      if (code !== "OUTROS" && !/\*$/.test(code)) continue;
      const meta = (notes_meta && notes_meta[k]) ? notes_meta[k] : null;
      noteEntries.push({ iso, off, code, text: notes[k], meta });
    }
    noteEntries.sort((a, b) => (a.iso < b.iso ? -1 : a.iso > b.iso ? 1 : 0));

    if (noteEntries.length) {
      doc.addPage({ margin: 36, size: "A4", layout: "portrait" });
      doc.fontSize(14).text("DESCRIES (OUTROS / CDIGOS COM ASTERISCO)", { align: "center" });
      doc.moveDown(0.6);
      // registro institucional (somente aqui, conforme regra)
      if (lastStamp) {
        const line = lastActor ? `ltimo registro: ${lastActor}  ${lastStamp}` : `ltimo registro: ${lastStamp}`;
        doc.fontSize(9).text(line, { align: "center" });
        doc.moveDown(0.6);
      }

      doc.fontSize(10);

      for (const it of noteEntries) {
        const title = `${fmtDDMMYYYY(it.iso)} - ${fixText(it.off.rank)} ${officerNameNoAccents(it.off.name)} (${it.code})`;
        doc.font("Helvetica-Bold").text(title);
        doc.font("Helvetica").text(it.text, { width: doc.page.width - doc.page.margins.left - doc.page.margins.right });
        
if (it.meta && (it.meta.updated_at || it.meta.updated_by || it.meta.created_by)) {
  const dt = it.meta.updated_at ? fmtDDMMYYYYHHmm(it.meta.updated_at) : "";
  const by = it.meta.updated_by ? String(it.meta.updated_by) : (it.meta.created_by ? String(it.meta.created_by) : "");
  const suffix = [dt ? `atualizado em ${dt}` : "", by ? `por ${by}` : ""].filter(Boolean).join(" ");
  if (suffix) {
    doc.fontSize(8).fillColor("#555555").text(suffix);
    doc.fontSize(10).fillColor("black");
  }
}
doc.moveDown(0.6);
        if (doc.y > doc.page.height - 180) {
          doc.addPage({ margin: 36, size: "A4", layout: "portrait" });
          doc.fontSize(14).text("DESCRIES (OUTROS / CDIGOS COM ASTERISCO)", { align: "center" });
          doc.moveDown(0.6);
          doc.fontSize(10);
        }
      }
    }


    // folha apartada: alteraes operacionais
    const addOperationalChangesPage = () => {
      doc.addPage({ margin: 36, size: "A4", layout: "portrait" });
      doc.fontSize(14).text("ALTERAES OPERACIONAIS", { align: "center" });
      doc.moveDown(0.8);

      const weekdayLabels = [
        "SEGUNDA",
        "TERA",
        "QUARTA",
        "QUINTA",
        "SEXTA",
        "SBADO",
        "DOMINGO",
      ];

      const lineStartX = doc.page.margins.left;
      const lineEndX = doc.page.width - doc.page.margins.right;
      const lineGap = 14;

      doc.fontSize(10);
      for (let i = 0; i < dates.length && i < weekdayLabels.length; i++) {
        const heading = `${weekdayLabels[i]} - ${fmtDDMMYYYY(dates[i])}`;
        doc.font("Helvetica-Bold").text(heading);
        doc.moveDown(0.25);
        for (let j = 0; j < 4; j++) {
          const yLine = doc.y + 8;
          doc.moveTo(lineStartX, yLine).lineTo(lineEndX, yLine).stroke();
          doc.y = yLine + lineGap;
        }
        doc.moveDown(0.35);
      }
      doc.font("Helvetica");
    };

    addOperationalChangesPage();

    // sem pgina de histrico no PDF; somente DESCRIES (OUTROS / cdigos com *) quando houver contedo

    doc.end();
  } catch (err) {
    return res.status(500).json({ error: "erro ao gerar pdf", details: err.message });
  }
});

// fallback SPA
app.get("*", (_req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

// ===============================
// START
// ===============================
(async () => {
  try {
    await ensureSchema();
    app.listen(PORT, () => {
      console.log(`[OK] Escala online em :${PORT} (TZ=${process.env.TZ})`);
    });
  } catch (e) {
    console.error("[FATAL] Falha ao iniciar:", e);
    process.exit(1);
  }
})();

