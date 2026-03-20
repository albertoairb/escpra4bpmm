// trecho corrigido
function canEditOfficer(officerCanonical) {
  if (!state.me) return false;

  if (state.me.is_admin) return true;

  if (!state.locked) {
    return officerCanonical === state.me.canonical_name;
  }

  if (state.locked && state.me.can_edit_after_lock) {
    return true;
  }

  return false;
}
