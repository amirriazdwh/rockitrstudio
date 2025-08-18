#!/usr/bin/env Rscript
# ================================================================
# Large CSV Parallel Reading Performance Test - FIXED VERSION
# ================================================================
# Purpose: Test R_MAX_SIZE settings and parallel CSV reading performance
# Environment: 8 CPU cores, optimized for large datasets
# Date: August 2025
# ================================================================

# Load required libraries
suppressPackageStartupMessages({
  library(data.table)
  library(parallel)
  library(doParallel)
  library(foreach)
  library(microbenchmark)
  library(pryr)
})

# Configuration
NUM_CORES <- 8
NUM_ROWS <- ifelse(exists("NUM_ROWS"), NUM_ROWS, 5000000)  # Allow override
NUM_COLS <- 20
TEST_RUNS <- 3
CHUNK_SIZE <- 1000000

# File paths
TEST_DIR <- "/home/cdsw"
DATA_DIR <- file.path(TEST_DIR, "test_data")
CSV_FILE <- file.path(DATA_DIR, "large_dataset.csv")

# Create directories
if (!dir.exists(DATA_DIR)) {
  dir.create(DATA_DIR, recursive = TRUE)
}

# Utility functions
format_bytes <- function(bytes) {
  if (bytes >= 1e9) {
    paste0(round(bytes / 1e9, 2), " GB")
  } else if (bytes >= 1e6) {
    paste0(round(bytes / 1e6, 2), " MB")
  } else {
    paste0(round(bytes / 1e3, 2), " KB")
  }
}

get_memory_usage <- function() {
  mem_info <- gc(verbose = FALSE)
  used_mb <- sum(mem_info[, "used"])
  list(used_mb = used_mb)
}

print_system_info <- function() {
  cat("\n", rep("=", 70), "\n")
  cat("SYSTEM INFORMATION\n")
  cat(rep("=", 70), "\n")
  
  cat("R Version:", R.version.string, "\n")
  cat("Platform:", R.version$platform, "\n")
  
  r_max_size <- Sys.getenv("R_MAX_SIZE", "unset")
  cat("R_MAX_SIZE:", r_max_size, "\n")
  cat("Available CPU cores:", detectCores(), "\n")
  cat("Using cores for test:", NUM_CORES, "\n")
  
  mem_usage <- get_memory_usage()
  cat("Current memory usage:", format_bytes(mem_usage$used_mb * 1024^2), "\n")
  
  cat(rep("=", 70), "\n\n")
}

# Data generation
generate_large_csv <- function() {
  cat("Generating CSV file with", format(NUM_ROWS, big.mark = ","), "rows...\n")
  
  start_time <- Sys.time()
  chunk_count <- ceiling(NUM_ROWS / CHUNK_SIZE)
  
  for (i in 1:chunk_count) {
    start_row <- (i - 1) * CHUNK_SIZE + 1
    end_row <- min(i * CHUNK_SIZE, NUM_ROWS)
    current_chunk_size <- end_row - start_row + 1
    
    cat("Chunk", i, "of", chunk_count, "\n")
    
    chunk_data <- data.table(
      id = start_row:end_row,
      timestamp = as.POSIXct("2025-01-01") + sample(1:31536000, current_chunk_size, replace = TRUE),
      category = sample(LETTERS[1:10], current_chunk_size, replace = TRUE),
      value1 = rnorm(current_chunk_size, 100, 25),
      value2 = runif(current_chunk_size, 0, 1000),
      value3 = rpois(current_chunk_size, 15),
      text_field = paste0("data_", sample(1:10000, current_chunk_size, replace = TRUE)),
      flag1 = sample(c(TRUE, FALSE), current_chunk_size, replace = TRUE),
      flag2 = sample(c(TRUE, FALSE), current_chunk_size, replace = TRUE),
      score1 = rnorm(current_chunk_size, 50, 15),
      score2 = rnorm(current_chunk_size, 75, 20),
      group_id = sample(1:1000, current_chunk_size, replace = TRUE),
      amount = runif(current_chunk_size, 1, 10000),
      percentage = runif(current_chunk_size, 0, 100),
      count_field = sample(1:100, current_chunk_size, replace = TRUE),
      rate = runif(current_chunk_size, 0.1, 5.0),
      index_val = sample(1:50000, current_chunk_size, replace = TRUE),
      status = sample(c("active", "inactive", "pending"), current_chunk_size, replace = TRUE),
      priority = sample(c("low", "medium", "high", "urgent"), current_chunk_size, replace = TRUE),
      region = sample(c("North", "South", "East", "West", "Central"), current_chunk_size, replace = TRUE)
    )
    
    if (i == 1) {
      fwrite(chunk_data, CSV_FILE, append = FALSE)
    } else {
      fwrite(chunk_data, CSV_FILE, append = TRUE)
    }
    
    rm(chunk_data)
    gc(verbose = FALSE)
  }
  
  end_time <- Sys.time()
  generation_time <- as.numeric(end_time - start_time, units = "secs")
  file_size <- file.info(CSV_FILE)$size
  
  cat("Generation complete!\n")
  cat("File size:", format_bytes(file_size), "\n")
  cat("Generation time:", round(generation_time, 2), "seconds\n\n")
  
  return(list(file_size = file_size, generation_time = generation_time))
}

# Performance tests
test_single_threaded <- function() {
  cat("TEST 1: Single-threaded reading\n")
  cat(rep("-", 50), "\n")
  
  mem_before <- get_memory_usage()
  
  timing <- microbenchmark(
    single_thread = {
      dt <- fread(CSV_FILE, nThread = 1, verbose = FALSE)
      result <- nrow(dt)
      rm(dt)
      gc(verbose = FALSE)
      result
    },
    times = TEST_RUNS
  )
  
  mem_after <- get_memory_usage()
  avg_time <- mean(timing$time) / 1e9
  
  cat("Average time:", round(avg_time, 2), "seconds\n")
  cat("Read rate:", round(NUM_ROWS / avg_time, 0), "rows/second\n\n")
  
  return(list(
    method = "single_threaded",
    avg_time = avg_time,
    rows_per_second = NUM_ROWS / avg_time
  ))
}

test_multi_threaded <- function() {
  cat("TEST 2: Multi-threaded reading (", NUM_CORES, " cores)\n")
  cat(rep("-", 50), "\n")
  
  mem_before <- get_memory_usage()
  
  timing <- microbenchmark(
    multi_thread = {
      dt <- fread(CSV_FILE, nThread = NUM_CORES, verbose = FALSE)
      result <- nrow(dt)
      rm(dt)
      gc(verbose = FALSE)
      result
    },
    times = TEST_RUNS
  )
  
  mem_after <- get_memory_usage()
  avg_time <- mean(timing$time) / 1e9
  
  cat("Average time:", round(avg_time, 2), "seconds\n")
  cat("Read rate:", round(NUM_ROWS / avg_time, 0), "rows/second\n\n")
  
  return(list(
    method = "multi_threaded",
    avg_time = avg_time,
    rows_per_second = NUM_ROWS / avg_time
  ))
}

test_memory_stress <- function() {
  cat("TEST 3: Memory stress test\n")
  cat(rep("-", 50), "\n")
  
  tryCatch({
    cat("Loading full dataset into memory...\n")
    
    start_time <- Sys.time()
    dt <- fread(CSV_FILE, nThread = NUM_CORES, verbose = FALSE)
    load_time <- as.numeric(Sys.time() - start_time, units = "secs")
    
    cat("Dataset loaded successfully!\n")
    cat("Rows:", format(nrow(dt), big.mark = ","), "\n")
    cat("Columns:", ncol(dt), "\n")
    cat("Object size:", format_bytes(object.size(dt)), "\n")
    cat("Load time:", round(load_time, 2), "seconds\n")
    
    # Simple operations test
    cat("Testing operations...\n")
    
    # Aggregation
    start_time <- Sys.time()
    agg_result <- dt[, .(avg_value1 = mean(value1), count = .N), by = category]
    agg_time <- as.numeric(Sys.time() - start_time, units = "secs")
    cat("Aggregation:", round(agg_time, 3), "seconds\n")
    
    # Filtering
    start_time <- Sys.time()
    filter_result <- dt[value1 > 100 & flag1 == TRUE]
    filter_time <- as.numeric(Sys.time() - start_time, units = "secs")
    cat("Filtering:", round(filter_time, 3), "seconds\n")
    cat("Filtered rows:", format(nrow(filter_result), big.mark = ","), "\n")
    
    # Cleanup
    rm(dt, agg_result, filter_result)
    gc(verbose = FALSE)
    
    return(list(
      method = "memory_stress",
      success = TRUE,
      load_time = load_time,
      agg_time = agg_time,
      filter_time = filter_time
    ))
    
  }, error = function(e) {
    cat("Memory stress test failed:", e$message, "\n")
    return(list(
      method = "memory_stress",
      success = FALSE,
      error = e$message
    ))
  })
}

# Main execution
main <- function() {
  cat("\nLARGE CSV PARALLEL READING PERFORMANCE TEST\n")
  cat(rep("=", 70), "\n")
  
  print_system_info()
  
  results <- list()
  
  # Generate or check CSV
  if (!file.exists(CSV_FILE) || file.info(CSV_FILE)$size < (NUM_ROWS * 100)) {
    cat("Generating test dataset...\n")
    results$generation <- generate_large_csv()
  } else {
    cat("Using existing CSV file:", CSV_FILE, "\n")
    cat("File size:", format_bytes(file.info(CSV_FILE)$size), "\n\n")
  }
  
  # Run tests
  results$single_threaded <- test_single_threaded()
  results$multi_threaded <- test_multi_threaded()
  results$memory_stress <- test_memory_stress()
  
  # Results summary
  cat(rep("=", 70), "\n")
  cat("PERFORMANCE RESULTS SUMMARY\n")
  cat(rep("=", 70), "\n")
  
  cat("Dataset: ", format(NUM_ROWS, big.mark = ","), " rows, ", NUM_COLS, " columns\n")
  cat("File size:", format_bytes(file.info(CSV_FILE)$size), "\n\n")
  
  cat("Reading Performance:\n")
  cat(sprintf("%-20s %15s %20s\n", "Method", "Time (sec)", "Rows/sec"))
  cat(rep("-", 55), "\n")
  
  for (test_name in c("single_threaded", "multi_threaded")) {
    if (test_name %in% names(results)) {
      result <- results[[test_name]]
      cat(sprintf("%-20s %15.2f %20.0f\n", 
                  result$method, 
                  result$avg_time, 
                  result$rows_per_second))
    }
  }
  
  if ("single_threaded" %in% names(results) && "multi_threaded" %in% names(results)) {
    speedup <- results$single_threaded$avg_time / results$multi_threaded$avg_time
    cat("\nParallel speedup:", round(speedup, 2), "x\n")
  }
  
  if (results$memory_stress$success) {
    cat("\nMemory Test: ✓ SUCCESS\n")
    cat("Load time:", round(results$memory_stress$load_time, 2), "seconds\n")
    cat("Operations completed successfully\n")
  } else {
    cat("\nMemory Test: ✗ FAILED\n")
    cat("Error:", results$memory_stress$error, "\n")
  }
  
  r_max_size <- Sys.getenv("R_MAX_SIZE", "unset")
  cat("\nR_MAX_SIZE setting:", r_max_size, "\n")
  
  cat("\n", rep("=", 70), "\n")
  cat("Test completed!\n")
  
  return(results)
}

# Execute
if (!interactive()) {
  results <- main()
}
