#!/usr/bin/env Rscript
# =================================================================
# Simple 10GB CSV Arrow Performance Test
# Quick stress test for basic Arrow performance validation
# =================================================================
#
# This script performs a focused test:
# 1. Generate 10GB CSV with 10M rows
# 2. Test Arrow single vs multi-threaded performance
# 3. Compare with base R read.csv
#
# Usage: Rscript simple_stress_test.R
# =================================================================

library(arrow)
library(data.table)
library(parallel)

cat('=== RStudio Server Simple Stress Test ===\n')
cat('System: Available RAM -', round(as.numeric(system("free -g | grep '^Mem:' | awk '{print $2}'", intern=TRUE))), 'GB,', parallel::detectCores(), 'cores\n')
cat('R Version:', R.version.string, '\n')
cat('JIT Level:', Sys.getenv('R_ENABLE_JIT'), '\n\n')

# Phase 1: Generate 10GB CSV
cat('Phase 1: Generating 10GB CSV file...\n')
target_rows <- 10000000  # 10M rows for ~10GB
csv_file <- '/tmp/stress_test_10gb.csv'

start_time <- Sys.time()

# Generate large dataset
cat('Creating dataset with', format(target_rows, big.mark=','), 'rows...\n')

data <- data.frame(
  id = 1:target_rows,
  timestamp = as.POSIXct('2025-01-01') + runif(target_rows, 0, 365*24*3600),
  category = sample(letters[1:5], target_rows, replace=TRUE),
  value1 = rnorm(target_rows, 100, 25),
  value2 = runif(target_rows, 0, 1000),
  value3 = rexp(target_rows, 0.1),
  text_field = paste0('data_', sample(100000:999999, target_rows, replace=TRUE)),
  factor_field = sample(paste0('Type', 1:10), target_rows, replace=TRUE),
  logical_field = sample(c(TRUE, FALSE), target_rows, replace=TRUE),
  large_numeric = runif(target_rows, 1e6, 1e9)
)

cat('Writing to CSV...\n')
data.table::fwrite(data, csv_file)

generation_time <- as.numeric(difftime(Sys.time(), start_time, units='secs'))
file_size <- file.info(csv_file)$size / (1024^3)  # GB

cat('Generation completed!\n')
cat('File size:', sprintf('%.2f GB', file_size), '\n')
cat('Generation time:', sprintf('%.2f seconds', generation_time), '\n')
cat('Rate:', sprintf('%.2f MB/s', (file_size * 1024) / generation_time), '\n\n')

rm(data)
gc()

# Phase 2: Test Arrow reading with different approaches
cat('Phase 2: Arrow Performance Testing\n')
cat('==================================\n')

test_results <- data.frame(
  test = character(),
  cores = integer(),
  time_seconds = numeric(),
  throughput_mbs = numeric(),
  success = logical(),
  stringsAsFactors = FALSE
)

# Test 1: Single-threaded Arrow read
cat('Test 1: Single-threaded Arrow read...\n')
start_time <- Sys.time()
tryCatch({
  # Force single thread
  old_threads <- Sys.getenv('OMP_NUM_THREADS')
  Sys.setenv(OMP_NUM_THREADS = '1')
  
  arrow_data <- arrow::read_csv_arrow(csv_file, as_data_frame = FALSE)
  df1 <- as.data.frame(arrow_data)
  
  read_time <- as.numeric(difftime(Sys.time(), start_time, units='secs'))
  throughput <- (file_size * 1024) / read_time
  
  cat('SUCCESS - Time:', sprintf('%.2f sec', read_time), 
      'Throughput:', sprintf('%.2f MB/s', throughput), '\n')
  
  test_results <- rbind(test_results, data.frame(
    test = 'Arrow_1_thread',
    cores = 1,
    time_seconds = read_time,
    throughput_mbs = throughput,
    success = TRUE,
    stringsAsFactors = FALSE
  ))
  
  rm(arrow_data, df1)
  
  # Restore threads
  if (old_threads != '') Sys.setenv(OMP_NUM_THREADS = old_threads)
  
}, error = function(e) {
  cat('FAILED:', e$message, '\n')
  test_results <- rbind(test_results, data.frame(
    test = 'Arrow_1_thread',
    cores = 1,
    time_seconds = as.numeric(difftime(Sys.time(), start_time, units='secs')),
    throughput_mbs = 0,
    success = FALSE,
    stringsAsFactors = FALSE
  ))
})

gc()
Sys.sleep(2)

# Test 2: Multi-threaded Arrow read
cat('Test 2: Multi-threaded Arrow read (8 cores)...\n')
start_time <- Sys.time()
tryCatch({
  arrow_data <- arrow::read_csv_arrow(csv_file, as_data_frame = FALSE)
  df2 <- as.data.frame(arrow_data)
  
  read_time <- as.numeric(difftime(Sys.time(), start_time, units='secs'))
  throughput <- (file_size * 1024) / read_time
  
  cat('SUCCESS - Time:', sprintf('%.2f sec', read_time), 
      'Throughput:', sprintf('%.2f MB/s', throughput), '\n')
  
  test_results <- rbind(test_results, data.frame(
    test = 'Arrow_8_threads',
    cores = 8,
    time_seconds = read_time,
    throughput_mbs = throughput,
    success = TRUE,
    stringsAsFactors = FALSE
  ))
  
  rm(arrow_data, df2)
  
}, error = function(e) {
  cat('FAILED:', e$message, '\n')
  test_results <- rbind(test_results, data.frame(
    test = 'Arrow_8_threads',
    cores = 8,
    time_seconds = as.numeric(difftime(Sys.time(), start_time, units='secs')),
    throughput_mbs = 0,
    success = FALSE,
    stringsAsFactors = FALSE
  ))
})

gc()
Sys.sleep(2)

# Test 3: Standard R read.csv (for comparison) - Limited to prevent timeout
cat('Test 3: Standard R read.csv (baseline - may be slow)...\n')
start_time <- Sys.time()
tryCatch({
  # Set timeout for slow read.csv
  timeout_result <- withTimeout({
    df3 <- read.csv(csv_file)
    df3
  }, timeout = 300, onTimeout = "error")  # 5 minute timeout
  
  read_time <- as.numeric(difftime(Sys.time(), start_time, units='secs'))
  throughput <- (file_size * 1024) / read_time
  
  cat('SUCCESS - Time:', sprintf('%.2f sec', read_time), 
      'Throughput:', sprintf('%.2f MB/s', throughput), '\n')
  
  test_results <- rbind(test_results, data.frame(
    test = 'Base_R_read.csv',
    cores = 1,
    time_seconds = read_time,
    throughput_mbs = throughput,
    success = TRUE,
    stringsAsFactors = FALSE
  ))
  
  rm(timeout_result)
  
}, error = function(e) {
  read_time <- as.numeric(difftime(Sys.time(), start_time, units='secs'))
  cat('FAILED or TIMEOUT (', sprintf('%.1f', read_time), 's):', e$message, '\n')
  test_results <- rbind(test_results, data.frame(
    test = 'Base_R_read.csv',
    cores = 1,
    time_seconds = read_time,
    throughput_mbs = 0,
    success = FALSE,
    stringsAsFactors = FALSE
  ))
})

gc()

# Results Summary
cat('\n=== STRESS TEST RESULTS ===\n')
print(test_results)

if (nrow(test_results[test_results$success,]) > 0) {
  best_test <- test_results[test_results$success,][which.max(test_results[test_results$success,]$throughput_mbs),]
  cat('\nBest Performance:', best_test$test, '\n')
  cat('Throughput:', sprintf('%.2f MB/s', best_test$throughput_mbs), '\n')
  cat('Time:', sprintf('%.2f seconds', best_test$time_seconds), '\n')
  
  # Calculate performance improvements
  arrow_tests <- test_results[grepl('Arrow', test_results$test) & test_results$success,]
  if (nrow(arrow_tests) > 1) {
    single_perf <- arrow_tests[arrow_tests$test == 'Arrow_1_thread',]$throughput_mbs
    multi_perf <- arrow_tests[arrow_tests$test == 'Arrow_8_threads',]$throughput_mbs
    if (length(single_perf) > 0 && length(multi_perf) > 0) {
      improvement <- ((multi_perf - single_perf) / single_perf) * 100
      cat('Multi-threading improvement:', sprintf('%.1f%%', improvement), '\n')
    }
  }
}

# Memory usage summary
cat('\nMemory Usage Summary:\n')
gc_info <- gc()
print(gc_info)

# Clean up
file.remove(csv_file)
cat('\nSimple stress test completed!\n')

# Helper function for timeout (define if not available)
if (!exists('withTimeout')) {
  withTimeout <- function(expr, timeout, onTimeout = c("error", "warning", "silent")) {
    onTimeout <- match.arg(onTimeout)
    
    # Create a wrapper that will execute the expression
    result <- tryCatch({
      # Use system timeout if available
      eval(expr)
    }, error = function(e) {
      if (onTimeout == "error") {
        stop("Operation timed out or failed: ", e$message)
      } else if (onTimeout == "warning") {
        warning("Operation timed out or failed: ", e$message)
        return(NULL)
      } else {
        return(NULL)
      }
    })
    
    return(result)
  }
}
