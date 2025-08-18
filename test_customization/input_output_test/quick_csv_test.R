#!/usr/bin/env Rscript
# ================================================================
# Quick CSV Parallel Reading Test (Smaller Dataset)
# ================================================================
# Purpose: Quick test with smaller dataset for validation
# Environment: 8 CPU cores
# Date: August 2025
# ================================================================

# Load required libraries
suppressPackageStartupMessages({
  library(data.table)
  library(parallel)
  library(microbenchmark)
  library(pryr)
})

# Configuration
NUM_CORES <- 8
NUM_ROWS <- 1000000  # 1 million rows
NUM_COLS <- 20
TEST_RUNS <- 5

# File paths
TEST_DIR <- "/home/amirriaz/cloudera_community/community-ml-runtimes/rstudio/R4.5.1/Testing"
DATA_DIR <- file.path(TEST_DIR, "test_data")
CSV_FILE <- file.path(DATA_DIR, "quick_test.csv")

# Create directories
if (!dir.exists(DATA_DIR)) {
  dir.create(DATA_DIR, recursive = TRUE)
}

# Quick data generation
cat("Generating quick test dataset...\n")
quick_data <- data.table(
  id = 1:NUM_ROWS,
  timestamp = as.POSIXct("2025-01-01") + sample(1:86400, NUM_ROWS, replace = TRUE),
  category = sample(LETTERS[1:5], NUM_ROWS, replace = TRUE),
  value1 = rnorm(NUM_ROWS, 100, 25),
  value2 = runif(NUM_ROWS, 0, 1000),
  value3 = rpois(NUM_ROWS, 15),
  text_field = paste0("test_", sample(1:1000, NUM_ROWS, replace = TRUE)),
  flag1 = sample(c(TRUE, FALSE), NUM_ROWS, replace = TRUE),
  flag2 = sample(c(TRUE, FALSE), NUM_ROWS, replace = TRUE),
  score1 = rnorm(NUM_ROWS, 50, 15),
  score2 = rnorm(NUM_ROWS, 75, 20),
  group_id = sample(1:100, NUM_ROWS, replace = TRUE),
  amount = runif(NUM_ROWS, 1, 10000),
  percentage = runif(NUM_ROWS, 0, 100),
  count_field = sample(1:100, NUM_ROWS, replace = TRUE),
  rate = runif(NUM_ROWS, 0.1, 5.0),
  index_val = sample(1:5000, NUM_ROWS, replace = TRUE),
  status = sample(c("active", "inactive"), NUM_ROWS, replace = TRUE),
  priority = sample(c("low", "medium", "high"), NUM_ROWS, replace = TRUE),
  region = sample(c("North", "South", "East", "West"), NUM_ROWS, replace = TRUE)
)

fwrite(quick_data, CSV_FILE)
file_size <- file.info(CSV_FILE)$size
cat("Quick test file created:", round(file_size / 1024^2, 1), "MB\n\n")

# Test single vs multi-threaded reading
cat("Testing single-threaded reading...\n")
single_time <- microbenchmark(
  single = {
    dt <- fread(CSV_FILE, nThread = 1)
    nrow(dt)
  },
  times = TEST_RUNS
)

cat("Testing multi-threaded reading (", NUM_CORES, " cores)...\n")
multi_time <- microbenchmark(
  multi = {
    dt <- fread(CSV_FILE, nThread = NUM_CORES)
    nrow(dt)
  },
  times = TEST_RUNS
)

# Results
single_avg <- mean(single_time$time) / 1e9
multi_avg <- mean(multi_time$time) / 1e9
speedup <- single_avg / multi_avg

cat("\n=== QUICK TEST RESULTS ===\n")
cat("File size:", round(file_size / 1024^2, 1), "MB\n")
cat("Rows:", format(NUM_ROWS, big.mark = ","), "\n")
cat("Single-threaded time:", round(single_avg, 3), "seconds\n")
cat("Multi-threaded time:", round(multi_avg, 3), "seconds\n")
cat("Speedup:", round(speedup, 2), "x\n")
cat("Read rate (multi):", round(NUM_ROWS / multi_avg, 0), "rows/second\n")

# Memory check
mem_info <- gc()
cat("Current memory usage:", round(sum(mem_info[, "(Mb)"]), 1), "MB\n")

# R_MAX_SIZE check
r_max_size <- Sys.getenv("R_MAX_SIZE", "unset")
cat("R_MAX_SIZE:", r_max_size, "\n")

cat("\nQuick test completed!\n")
