#!/bin/bash
set -e

echo "🛠 Preparing apt environment..."
apt-get update && apt-get install -y apt-utils

echo "📦 Installing core R packages for professional use..."

: "${CRAN:=https://cran.rstudio.com}"

echo "🔧 Installing required system libraries..."

# ✅ SINGLE apt-get install block — DO NOT BREAK
apt-get update -qq && apt-get install -y \
  build-essential \
  g++ \
  cmake \
  git \
  curl \
  wget \
  unzip \
  libssl-dev \
  libxml2-dev \
  libcurl4-openssl-dev \
  pkg-config \
  zlib1g-dev \
  libbz2-dev \
  libicu-dev \
  libjpeg-dev \
  libpng-dev \
  libgit2-dev \
  libssh2-1-dev \
  liblzma-dev \
  libpcre2-dev \
  libsodium-dev \
  libudunits2-dev \
  libgdal-dev \
  libhdf5-dev \
  liblapack-dev \
echo "📊 Installing data science packages..."

# Function to install R packages with optimal flags for performance
install_r_package() {
  local pkg="$1"
  echo "Installing $pkg..."
  
  R --quiet -e "options(warn = 2); \
    install.packages('$pkg', \
      repos = '$CRAN', \
      dependencies = c('Depends', 'Imports', 'LinkingTo'), \
      clean = TRUE, \
      Ncpus = min(parallel::detectCores(), 4), \
      verbose = FALSE)"
}

# Core data manipulation and visualization
install_r_package "data.table"
install_r_package "dplyr"
install_r_package "tidyr"
install_r_package "ggplot2"
install_r_package "readr"
install_r_package "vroom"

# Performance packages for large data
install_r_package "arrow"
install_r_package "fst"
install_r_package "future"
install_r_package "parallel"
install_r_package "bit64"

# Markdown and reporting
install_r_package "rmarkdown"
install_r_package "knitr"

# Database connections
install_r_package "DBI"
install_r_package "RSQLite"
install_r_package "RPostgres"

# Machine learning
install_r_package "caret"
install_r_package "randomForest"

# Python integration
install_r_package "reticulate"

# Development tools
install_r_package "devtools"
install_r_package "testthat"
install_r_package "roxygen2"

echo "✅ R package installation completed!"
), quiet = TRUE)"

Rscript -e 'if (!tinytex::is_tinytex()) message("System TeX Live is available, skipping TinyTeX install.")'

Rscript -e "install.packages(c(
  'future', 'doParallel', 'foreach', 'furrr'
), repos = '${CRAN}', quiet = TRUE)"

Rscript -e "install.packages(c(
  'RPostgres', 'RMariaDB', 'duckdb', 'arrow', 'bigrquery'
), repos = '${CRAN}', quiet = TRUE)"

echo "✅ All core packages for professional R use have been installed successfully."

Rscript -e "cat(\"Installed packages:\\n\"); print(installed.packages()[, c('Package', 'Version')])"
