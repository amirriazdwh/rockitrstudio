#!/usr/bin/env bash
# Ubuntu 24.04 (Noble) — Database connectors module (OS deps + R DB packages only)

set -Eeuo pipefail
umask 022
trap 'echo "❌ Error on line $LINENO"; exit 1' ERR

# Config
: "${CRAN:=https://packagemanager.posit.co/cran/__linux__/noble/latest}"   # fast prebuilt binaries
: "${INSTALL_ODBC_DRIVERS:=none}"  # options: none | postgres

# Preconditions
if [[ ${EUID:-$(id -u)} -ne 0 ]]; then
  echo "Please run as root (sudo)."; exit 1
fi
command -v Rscript >/dev/null 2>&1 || { echo "Rscript not found. Install R first."; exit 1; }

export DEBIAN_FRONTEND=noninteractive

echo "🔧 Installing system libraries for database connectivity (single apt block)…"
apt-get update -qq
apt-get install -y --no-install-recommends \
  build-essential pkg-config \
  libssl-dev libcurl4-openssl-dev libxml2-dev \
  zlib1g-dev libbz2-dev liblzma-dev libicu-dev libpcre2-dev \
  libpq-dev postgresql-client \
  libmariadb-dev \
  unixodbc unixodbc-dev odbcinst \
  ca-certificates curl wget
# Optional ODBC DB drivers (so odbcListDrivers() shows something useful)
if [[ "${INSTALL_ODBC_DRIVERS}" == "postgres" ]]; then
  apt-get install -y --no-install-recommends odbc-postgresql
fi
apt-get clean
rm -rf /var/lib/apt/lists/*

echo "📦 Installing R DB connector packages (idempotent)…"
Rscript --vanilla - <<'RS'
install_once <- function(pkgs, repos) {
  ip <- rownames(installed.packages())
  need <- setdiff(pkgs, ip)
  if (length(need)) {
    options(Ncpus = max(1L, parallel::detectCores()-1L))
    install.packages(need, repos = repos,
                     dependencies = c("Depends","Imports","LinkingTo"),
                     quiet = TRUE)
  }
}
repos <- Sys.getenv("CRAN", "https://packagemanager.posit.co/cran/__linux__/noble/latest")
pkgs  <- c("DBI","RSQLite","RPostgres","RMariaDB","duckdb","odbc","bigrquery")
install_once(pkgs, repos)
ip <- installed.packages()
keep <- intersect(pkgs, rownames(ip))
cat("\n✅ Installed DB packages:\n")
print(ip[keep, c("Package","Version"), drop = FALSE])
RS

echo "✅ DB connectors: installation complete."
