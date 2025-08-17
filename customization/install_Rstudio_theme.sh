#!/usr/bin/env bash
set -euo pipefail

# set_rstudio_theme.sh
# - Installs a good monospace font (Fira Code if available) or falls back
# - Sets global RStudio defaults in /etc/rstudio/rstudio-prefs.json
# - Seeds per-user prefs on first login via /etc/profile.d hook

# Optional override: export RSTUDIO_FONT="JetBrains Mono" before running
RSTUDIO_FONT="${RSTUDIO_FONT:-Fira Code}"

export DEBIAN_FRONTEND=noninteractive

need_apt_update=true
apt_update_once() {
  if $need_apt_update; then
    apt-get update
    need_apt_update=false
  fi
}

# Ensure fontconfig is present so we can detect fonts
if ! command -v fc-list >/dev/null 2>&1; then
  apt_update_once
  apt-get install -y --no-install-recommends fontconfig
fi

# Try to install Fira Code if requested and missing
have_font() { fc-list | grep -qiE "$1" ; }

if [[ "$RSTUDIO_FONT" =~ ^Fira[[:space:]]+Code$ ]]; then
  if ! have_font "Fira[[:space:]]+Code"; then
    if apt-cache show fonts-firacode >/dev/null 2>&1; then
      apt_update_once
      apt-get install -y --no-install-recommends fonts-firacode
      fc-cache -f >/dev/null 2>&1 || true
    fi
  fi
fi

# If requested font still not available, pick a solid fallback
if ! have_font "$RSTUDIO_FONT"; then
  if have_font "JetBrains[[:space:]]+Mono"; then
    RSTUDIO_FONT="JetBrains Mono"
  elif have_font "Ubuntu[[:space:]]+Mono"; then
    RSTUDIO_FONT="Ubuntu Mono"
  elif have_font "DejaVu[[:space:]]+Sans[[:space:]]+Mono"; then
    RSTUDIO_FONT="DejaVu Sans Mono"
  else
    RSTUDIO_FONT="Monospace"
  fi
fi

# Tidy apt caches if we installed anything
if ! $need_apt_update; then
  apt-get clean
  rm -rf /var/lib/apt/lists/*
fi

# 1) Global defaults (applies to all; users can override)
install -d /etc/rstudio
cat > /etc/rstudio/rstudio-prefs.json <<JSON
{
  "font_size_points": 9,
  "font": "${RSTUDIO_FONT}",
  "global_theme": "Modern",
  "ui_theme": "Modern",
  "editor_theme": "Tomorrow Night Bright",
  "show_line_numbers": true,
  "highlight_selected_line": true,
  "soft_wrap_r_files": false,
  "save_workspace": "never",
  "load_workspace": false,
  "scroll_past_end": true,
  "show_margin": true,
  "margin_column": 120,
  "enable_code_completion": true,
  "show_hidden_files": false,
  "always_save_history": false,
  "reuse_sessions_for_project_links": true,
  "enable_code_indexing": true,
  "posix_terminal_shell": "bash"
}
JSON
chmod 0644 /etc/rstudio/rstudio-prefs.json

# Optional: prefer rstudio-users group if it exists (harmless if not)
if getent group rstudio-users >/dev/null 2>&1; then
  chgrp rstudio-users /etc/rstudio/rstudio-prefs.json || true
fi

# 2) Seed on first login if user has no prefs yet
cat > /etc/profile.d/rstudio-seed-prefs.sh <<'BASH'
#!/usr/bin/env bash
# Seed RStudio prefs from /etc/rstudio/rstudio-prefs.json on first login

# Skip root and system users; require writable $HOME
if [ "$USER" != "root" ] && [ "$(id -u)" -ge 1000 ] && [ -w "$HOME" ]; then
  cfg="$HOME/.config/rstudio/rstudio-prefs.json"
  src="/etc/rstudio/rstudio-prefs.json"
  if [ ! -f "$cfg" ] && [ -r "$src" ]; then
    mkdir -p "$(dirname "$cfg")"
    cp "$src" "$cfg"
    chown "$USER":"$USER" "$cfg" 2>/dev/null || true
  fi
fi
BASH
chmod 0755 /etc/profile.d/rstudio-seed-prefs.sh

echo "✅ RStudio global prefs set. Using font: ${RSTUDIO_FONT}"
