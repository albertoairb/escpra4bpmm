// server.js corrigido (trecho essencial já aplicado)

function normalizeAuthorizedName(name) {
  return String(name || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/\b(TEN CEL|CEL|MAJ|CAP|TEN|ASP|SUBTEN|ST|SGT|CB|SD)\b/g, "")
    .replace(/\bPM\b/g, "")
    .replace(/\b[0-9]+[º°]?\b/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

const AUTH_AFTER_LOCK = new Set([
  "FERNANDES",
  "FELIPE",
  "DANIELLE",
  "ALBERTO FRANZINI NETO",
  "MOSNA"
]);

function canEditAfterLock(userName) {
  const normalized = normalizeAuthorizedName(userName);
  return AUTH_AFTER_LOCK.has(normalized);
}
