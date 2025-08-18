#!/usr/bin/env bash
# Ubuntu 24.04 (Noble) – install OS deps + R graphics packages (headless-safe)

set -Eeuo pipefail
IFS=$'\n\t'
umask 022
trap 'echo "❌ Error on line $LINENO"; exit 1' ERR

: "${CRAN:=https://cloud.r-project.org}"     # override with ENV if you like
: "${INSTALL_TEX:=no}"                        # yes/no – TinyTeX
: "${SET_HEADLESS_DEFAULT:=keep}"             # yes|no|keep – write options(bitmapType='cairo')

if [[ ${EUID:-$(id -u)} -ne 0 ]]; then
  echo "Please run as root (sudo)."; exit 1
fi

export DEBIAN_FRONTEND=noninteractive

echo "→ Installing OS libraries (Ubuntu Noble)…"
apt-get update -y
apt-get install -y --no-install-recommends build-essential pkg-config ca-certificates curl gnupg libcairo2-dev libfreetype6-dev libfontconfig1-dev libharfbuzz-dev libfribidi-dev libpng-dev libjpeg-dev libtiff-dev libpango1.0-dev libxt-dev zlib1g-dev libxml2-dev libcurl4-openssl-dev libicu-dev libopenblas-dev fonts-dejavu-core ghostscript

# --- ADD: TeX Live for tikzDevice (runs only if INSTALL_TEX=apt) ---
if [[ "${INSTALL_TEX,,}" == "apt" ]]; then
  echo "→ Installing TeX Live (apt) for tikzDevice…"
  apt-get install -y --no-install-recommends \
    texlive-latex-base texlive-latex-recommended texlive-latex-extra \
 if [[ "${INSTALL_TEX,,}" == "apt" ]]; then
  echo "→ Installing TeX Live (apt) for tikzDevice…"
  apt-get install -y --no-install-recommends \
    texlive-latex-base texlive-latex-recommended texlive-latex-extra \
    texlive-fonts-recommended texlive-pictures lmodern
fi

apt-get clean
rm -rf /var/lib/apt/lists/*

echo "→ Installing/validating R packages…"
Rscript --vanilla - <<'RS'
retry <- function(fun, tries=3, sleep=3) {
  for (i in seq_len(tries)) {
    ok <- tryCatch({ fun(); TRUE }, error=function(e){ message("WARN: ", conditionMessage(e)); FALSE })
    if (ok) return(invisible(TRUE))
    if (i < tries) Sys.sleep(sleep)
  }
  stop("giving up after retries")
}

repos <- Sys.getenv("CRAN"); if (!nzchar(repos)) repos <- "https://cloud.r-project.org"

# Optionally ensure headless-safe default for base png/jpeg/tiff
mode <- tolower(Sys.getenv("SET_HEADLESS_DEFAULT"))
if (mode %in% c("yes","keep")) {
  if (!file.exists("/etc/R/Rprofile.site")) {
    try(dir.create("/etc/R", showWarnings=FALSE), silent=TRUE)
  }
  if (file.exists("/etc/R/Rprofile.site")) {
    txt <- tryCatch(readLines("/etc/R/Rprofile.site", warn=FALSE), error=function(e) character())
    need <- !any(grepl("bitmapType\\s*=\\s*['\"]cairo['\"]", txt))
    if (mode == "yes" || (mode == "keep" && need)) {
      cat("options(bitmapType='cairo')\n", file="/etc/R/Rprofile.site", append=TRUE)
    }
  }
}

# Install in dependency-aware order
pkgs <- c("systemfonts","textshaping","ragg","svglite","ggplot2","gridExtra","gridBase","tikzDevice")
todo <- setdiff(pkgs, rownames(installed.packages()))
if (length(todo)) {
  message("Installing: ", paste(todo, collapse=", "))
  retry(function() {
    options(Ncpus = max(1L, parallel::detectCores()-1L))
    install.packages(todo, repos=repos, quiet=TRUE)
  })
} else {
  message("All requested R packages already installed.")
}

# Optional TinyTeX so tikzDevice .tex can compile to PDF
if (tolower(Sys.getenv("INSTALL_TEX")) %in% c("yes","true","1")) {
  if (!requireNamespace("tinytex", quietly=TRUE)) install.packages("tinytex", repos=repos, quiet=TRUE)
  try({ if (!tinytex::is_tinytex()) tinytex::install_tinytex() }, silent=TRUE)
}

# Probe (non-fatal warnings)
need <- c("systemfonts","textshaping","ragg","svglite","ggplot2","gridExtra","gridBase","tikzDevice")
ok <- vapply(need, function(p) requireNamespace(p, quietly=TRUE), logical(1))
if (!all(ok)) warning("Missing after install: ", paste(names(ok)[!ok], collapse=", "))
RS

if [[ "${VERIFY_GRAPHICS,,}" == "yes" ]]; then
echo "→ Verifying graphics stack end-to-end…"
Rscript --vanilla -e "suppressPackageStartupMessages({library(ragg); library(svglite); library(tikzDevice)})
  ragg::agg_png('/tmp/ragg_ok.png',800,600); plot(1:10); dev.off();
  svglite::svglite('/tmp/svglite_ok.svg',6,4); plot(1:10); dev.off();
  tikzDevice::tikz('/tmp/tikz_ok.tex', standAlone=TRUE); plot(1:10); dev.off();
  stopifnot(file.exists('/tmp/ragg_ok.png'), file.exists('/tmp/svglite_ok.svg'), file.exists('/tmp/tikz_ok.tex'))"

# Quiet, fail-fast LaTeX compile (skip if you don’t need PDF in CI)
pdflatex -interaction=nonstopmode -halt-on-error /tmp/tikz_ok.tex >/tmp/tikz_ok_build.log 2>&1 || {
  echo 'pdflatex failed; showing log tail:'; tail -n 80 /tmp/tikz_ok_build.log; exit 1; }
test -f /tmp/tikz_ok.pdf

fi

echo "✅ Graphics stack install complete."
