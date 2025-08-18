#!/usr/bin/env bash
# Ubuntu 24.04 (Noble) — Filesystem readers (CSV/JSON/XML/Excel/Parquet/ORC)
# Installs minimal OS libs + R packages: readr, vroom, data.table, arrow, readxl, jsonlite, xml2, bit64

set -Eeuo pipefail
umask 022
trap 'echo "❌ Error on line $LINENO"; exit 1' ERR

# Use Posit Package Manager for fast binaries on Noble
: "${CRAN:=https://packagemanager.posit.co/cran/__linux__/noble/latest}"

# Preconditions
if [[ ${EUID:-$(id -u)} -ne 0 ]]; then
  echo "Please run as root (sudo)."; exit 1
fi
command -v Rscript >/dev/null 2>&1 || { echo "Rscript not found. Install R first."; exit 1; }

export DEBIAN_FRONTEND=noninteractive

echo "🔧 Installing minimal system libraries (single apt block)…"
apt-get update -qq
apt-get install -y --no-install-recommends \
  build-essential pkg-config \
  ca-certificates curl \
  libcurl4 \
  libxml2-dev \
  libzstd1 liblz4-1 libsnappy1v5 libbrotli1
apt-get clean
rm -rf /var/lib/apt/lists/*

echo "📦 Installing R filesystem reader packages (idempotent)…"
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

pkgs <- c(
  "readr",      # CSV/TSV
  "vroom",      # fast delimited
  "data.table", # fread() for delimited files
  "arrow",      # Parquet + ORC (and more)
  "readxl",     # Excel .xlsx/.xls
  "jsonlite",   # JSON
  "xml2",       # XML
  "bit64"       # 64-bit ints for large CSVs, often used by fread/readr
)

install_once(pkgs, repos)

# Show versions
ip <- installed.packages()
keep <- intersect(pkgs, rownames(ip))
cat("\n✅ Installed file-reader packages:\n")
print(ip[keep, c("Package","Version"), drop = FALSE])
RS

echo "✅ Filesystem readers: installation complete."
