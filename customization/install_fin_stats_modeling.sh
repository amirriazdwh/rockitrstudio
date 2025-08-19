#!/bin/bash
# Banking / Modeling / Statistics module for R
# - NO graphics/visualization packages
# - NO filesystem I/O (readr/readxl/openxlsx/arrow/etc.)
# - NO database/ODBC/Postgres/MariaDB/duckdb
# Focus: time series, econometrics, credit scoring, ML, finance toolkits

set -euo pipefail

echo "🔧 Checking system prerequisites (Debian/Ubuntu)..."
if [ -f "/etc/debian_version" ]; then
  # Build toolchain + common headers used by many compiled CRAN packages
  sudo apt-get update -qq
  sudo apt-get install -y --no-install-recommends \
    build-essential g++ gfortran make cmake \
    libssl-dev libxml2-dev libcurl4-openssl-dev \
    libgsl-dev \
    ca-certificates curl wget git pkg-config
  sudo rm -rf /var/lib/apt/lists/*
else
  echo "⚠️ Non-Debian system detected. Install the equivalent of: build-essential/g++, gfortran, libssl-dev, libxml2-dev, libcurl4-openssl-dev, libgsl-dev."
fi

echo "📦 Installing R packages for banking, modeling, and statistics..."

Rscript -e '
# -----------------------------
# Banking / Modeling / Stats set
# -----------------------------
pkgs <- c(
  # Core wrangling (NOT filesystem I/O)
  "dplyr","data.table","tidyr","tibble","lubridate","stringr","janitor","purrr","magrittr","rlang","glue",

  # Time series & forecasting (no viz)
  "forecast","fable","tsibble","tseries","urca","zoo","xts","seasonal","tsoutliers",

  # Econometrics & statistics
  "car","lmtest","sandwich","AER","plm","strucchange","dynlm","MASS","nortest","vars",

  # Credit scoring & model validation
  "scorecard","Information","pROC","ROCR","caret","yardstick","DescTools","ModelMetrics",
  "randomForest","xgboost","glmnet","e1071",

  # Financial analysis (markets/portfolio)
  "quantmod","PerformanceAnalytics","TTR","PortfolioAnalytics","FinancialInstrument","blotter",
  "Quandl","BatchGetSymbols"
)

# Install only what is missing
inst <- setdiff(pkgs, rownames(installed.packages()))
if (length(inst)) {
  install.packages(inst, repos = getOption("repos", "https://cloud.r-project.org"), dependencies = TRUE)
}

cat("✅ Banking/modeling/statistics package set installed.\n")
'

echo "✅ Done."
