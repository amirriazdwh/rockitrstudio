#!/usr/bin/env Rscript
# =================================================================
# RStudio Server Comprehensive Stress Testing Suite
# 28GB RAM, 8 CPU Cores, High-Performance Configuration
# =================================================================
#
# This script performs comprehensive stress testing for RStudio Server
# including 10GB CSV generation, Arrow performance testing, and 
# breaking point analysis.
#
# Usage: Rscript comprehensive_stress_test.R
# =================================================================

library(arrow)
library(data.table)
library(parallel)

# Helper function to format bytes
format_bytes <- function(bytes) {
  units <- c('B', 'KB', 'MB', 'GB', 'TB')
  i <- 1
  while(bytes >= 1024 && i < length(units)) {
    bytes <- bytes / 1024
    i <- i + 1
  }
  return(sprintf('%.2f %s', bytes, units[i]))
}

# Helper function to format time
format_time <- function(seconds) {
  if (seconds < 60) {
    return(sprintf('%.2f seconds', seconds))
  } else if (seconds < 3600) {
    return(sprintf('%.2f minutes', seconds / 60))
  } else {
    return(sprintf('%.2f hours', seconds / 3600))
  }
}

cat('==========================================\n')
cat('RStudio Server Comprehensive Stress Test\n')
cat('==========================================\n')
cat('System Configuration:\n')
cat('- Available RAM:', format_bytes(as.numeric(system("free -b | grep '^Mem:' | awk '{print $2}'", intern=TRUE))), '\n')
cat('- CPU Cores:', parallel::detectCores(), '\n')
cat('- R Version:', R.version.string, '\n')
cat('- JIT Compilation:', Sys.getenv('R_ENABLE_JIT'), '\n')
cat('- Arrow Version:', packageVersion('arrow'), '\n')
cat('- Data.table Version:', packageVersion('data.table'), '\n')
cat('- Test Date:', Sys.Date(), '\n')
cat('==========================================\n\n')

# Test parameters
target_size_gb <- 10
target_rows <- 10000000  # 10M rows for ~10GB
test_dir <- '/tmp/stress_test'
csv_file <- file.path(test_dir, 'stress_test_10gb.csv')

# Create test directory
dir.create(test_dir, recursive = TRUE, showWarnings = FALSE)

# Initialize results tracking
all_results <- list()

# =================================================================
# PHASE 1: Generate 10GB CSV Dataset
# =================================================================

cat('PHASE 1: Generating 10GB CSV Dataset\n')
cat('=====================================\n')
cat('Target rows:', format(target_rows, big.mark=','), '\n')
cat('Columns: 10 (mixed data types)\n\n')

generation_start <- Sys.time()

cat('Creating dataset with', format(target_rows, big.mark=','), 'rows...\n')

# Generate large dataset
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

generation_time <- as.numeric(difftime(Sys.time(), generation_start, units='secs'))
file_size <- file.info(csv_file)$size

cat('Generation completed!\n')
cat('File size:', format_bytes(file_size), '\n')
cat('Generation time:', format_time(generation_time), '\n')
cat('Generation rate:', format_bytes(file_size / generation_time), '/second\n\n')

# Store generation results
all_results$generation <- list(
  file_size = file_size,
  generation_time = generation_time,
  rate = file_size / generation_time
)

# Clean up data from memory
rm(data)
gc()

# =================================================================
# PHASE 2: Arrow Performance Testing
# =================================================================

cat('PHASE 2: Arrow Performance Testing\n')
cat('==================================\n')

test_results <- data.frame(
  test = character(),
  cores = integer(),
  time_seconds = numeric(),
  throughput_mbs = numeric(),
  success = logical(),
  stringsAsFactors = FALSE
)

file_size_gb <- file_size / (1024^3)

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
  throughput <- (file_size_gb * 1024) / read_time
  
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
cat('Test 2: Multi-threaded Arrow read (all cores)...\n')
start_time <- Sys.time()
tryCatch({
  arrow_data <- arrow::read_csv_arrow(csv_file, as_data_frame = FALSE)
  df2 <- as.data.frame(arrow_data)
  
  read_time <- as.numeric(difftime(Sys.time(), start_time, units='secs'))
  throughput <- (file_size_gb * 1024) / read_time
  
  cat('SUCCESS - Time:', sprintf('%.2f sec', read_time), 
      'Throughput:', sprintf('%.2f MB/s', throughput), '\n')
  
  test_results <- rbind(test_results, data.frame(
    test = 'Arrow_multi_thread',
    cores = parallel::detectCores(),
    time_seconds = read_time,
    throughput_mbs = throughput,
    success = TRUE,
    stringsAsFactors = FALSE
  ))
  
  rm(arrow_data, df2)
  
}, error = function(e) {
  cat('FAILED:', e$message, '\n')
  test_results <- rbind(test_results, data.frame(
    test = 'Arrow_multi_thread',
    cores = parallel::detectCores(),
    time_seconds = as.numeric(difftime(Sys.time(), start_time, units='secs')),
    throughput_mbs = 0,
    success = FALSE,
    stringsAsFactors = FALSE
  ))
})

gc()
Sys.sleep(2)

# Test 3: data.table fread (for comparison)
cat('Test 3: data.table fread (high performance baseline)...\n')
start_time <- Sys.time()
tryCatch({
  df3 <- data.table::fread(csv_file)
  
  read_time <- as.numeric(difftime(Sys.time(), start_time, units='secs'))
  throughput <- (file_size_gb * 1024) / read_time
  
  cat('SUCCESS - Time:', sprintf('%.2f sec', read_time), 
      'Throughput:', sprintf('%.2f MB/s', throughput), '\n')
  
  test_results <- rbind(test_results, data.frame(
    test = 'data.table_fread',
    cores = parallel::detectCores(),
    time_seconds = read_time,
    throughput_mbs = throughput,
    success = TRUE,
    stringsAsFactors = FALSE
  ))
  
  rm(df3)
  
}, error = function(e) {
  cat('FAILED:', e$message, '\n')
  test_results <- rbind(test_results, data.frame(
    test = 'data.table_fread',
    cores = parallel::detectCores(),
    time_seconds = as.numeric(difftime(Sys.time(), start_time, units='secs')),
    throughput_mbs = 0,
    success = FALSE,
    stringsAsFactors = FALSE
  ))
})

gc()
Sys.sleep(2)

# Test 4: Standard R read.csv (baseline comparison)
cat('Test 4: Standard R read.csv (baseline)...\n')
start_time <- Sys.time()
tryCatch({
  df4 <- read.csv(csv_file)
  
  read_time <- as.numeric(difftime(Sys.time(), start_time, units='secs'))
  throughput <- (file_size_gb * 1024) / read_time
  
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
  
  rm(df4)
  
}, error = function(e) {
  cat('FAILED:', e$message, '\n')
  test_results <- rbind(test_results, data.frame(
    test = 'Base_R_read.csv',
    cores = 1,
    time_seconds = as.numeric(difftime(Sys.time(), start_time, units='secs')),
    throughput_mbs = 0,
    success = FALSE,
    stringsAsFactors = FALSE
  ))
})

gc()

# Store test results
all_results$performance_tests <- test_results

# =================================================================
# PHASE 3: Memory Stress Testing (Breaking Point Analysis)
# =================================================================

cat('\nPHASE 3: Memory Breaking Point Analysis\n')
cat('========================================\n')

memory_tests <- data.frame(
  test = character(),
  target_gb = numeric(),
  rows = numeric(),
  success = logical(),
  peak_memory_gb = numeric(),
  time_seconds = numeric(),
  stringsAsFactors = FALSE
)

# Test progressively larger datasets
test_sizes <- c(5, 15, 20, 25)  # GB sizes to test

for (size_gb in test_sizes) {
  cat('Testing', size_gb, 'GB dataset...\n')
  
  test_rows <- as.integer(size_gb * 1000000)  # Scale rows with size
  test_file <- file.path(test_dir, paste0('test_', size_gb, 'gb.csv'))
  
  start_time <- Sys.time()
  peak_memory <- 0
  
  tryCatch({
    # Monitor memory usage before test
    memory_before <- as.numeric(system("free -b | grep '^Mem:' | awk '{print $3}'", intern=TRUE))
    
    # Generate test dataset
    test_data <- data.frame(
      id = 1:test_rows,
      value1 = rnorm(test_rows),
      value2 = runif(test_rows),
      value3 = sample(letters, test_rows, replace=TRUE),
      value4 = sample(1:1000, test_rows, replace=TRUE)
    )
    
    # Monitor peak memory
    memory_after <- as.numeric(system("free -b | grep '^Mem:' | awk '{print $3}'", intern=TRUE))
    peak_memory <- (memory_after - memory_before) / (1024^3)  # Convert to GB
    
    # Write to file
    data.table::fwrite(test_data, test_file)
    
    test_time <- as.numeric(difftime(Sys.time(), start_time, units='secs'))
    
    cat('SUCCESS - Peak memory:', sprintf('%.2f GB', peak_memory), 
        'Time:', sprintf('%.2f sec', test_time), '\n')
    
    memory_tests <- rbind(memory_tests, data.frame(
      test = paste0('Memory_test_', size_gb, 'GB'),
      target_gb = size_gb,
      rows = test_rows,
      success = TRUE,
      peak_memory_gb = peak_memory,
      time_seconds = test_time,
      stringsAsFactors = FALSE
    ))
    
    # Clean up
    rm(test_data)
    file.remove(test_file)
    
  }, error = function(e) {
    test_time <- as.numeric(difftime(Sys.time(), start_time, units='secs'))
    
    cat('FAILED at', size_gb, 'GB:', e$message, '\n')
    
    memory_tests <- rbind(memory_tests, data.frame(
      test = paste0('Memory_test_', size_gb, 'GB'),
      target_gb = size_gb,
      rows = test_rows,
      success = FALSE,
      peak_memory_gb = peak_memory,
      time_seconds = test_time,
      stringsAsFactors = FALSE
    ))
    
    # Clean up if possible
    if (exists('test_data')) rm(test_data)
    if (file.exists(test_file)) file.remove(test_file)
  })
  
  gc()
  Sys.sleep(3)
}

# Store memory test results
all_results$memory_tests <- memory_tests

# =================================================================
# FINAL RESULTS SUMMARY
# =================================================================

cat('\n\n=== COMPREHENSIVE STRESS TEST RESULTS ===\n')
cat('==========================================\n\n')

# Generation Summary
cat('DATA GENERATION RESULTS:\n')
cat('File size:', format_bytes(all_results$generation$file_size), '\n')
cat('Generation time:', format_time(all_results$generation$generation_time), '\n')
cat('Generation rate:', format_bytes(all_results$generation$rate), '/second\n\n')

# Performance Test Summary
cat('PERFORMANCE TEST RESULTS:\n')
print(test_results)

if (nrow(test_results[test_results$success,]) > 0) {
  best_test <- test_results[test_results$success,][which.max(test_results[test_results$success,]$throughput_mbs),]
  cat('\nBest Performance:', best_test$test, '\n')
  cat('Throughput:', sprintf('%.2f MB/s', best_test$throughput_mbs), '\n')
  cat('Time:', sprintf('%.2f seconds', best_test$time_seconds), '\n\n')
}

# Memory Test Summary
cat('MEMORY STRESS TEST RESULTS:\n')
print(memory_tests)

# Find breaking point
successful_tests <- memory_tests[memory_tests$success,]
failed_tests <- memory_tests[!memory_tests$success,]

if (nrow(successful_tests) > 0) {
  max_successful <- max(successful_tests$target_gb)
  cat('\nMaximum successful dataset size:', max_successful, 'GB\n')
}

if (nrow(failed_tests) > 0) {
  min_failed <- min(failed_tests$target_gb)
  cat('Breaking point detected at:', min_failed, 'GB\n')
}

# System recommendations
cat('\n=== SYSTEM RECOMMENDATIONS ===\n')
cat('Based on stress test results:\n')

if (nrow(test_results[test_results$success,]) > 0) {
  best_method <- test_results[test_results$success,][which.max(test_results[test_results$success,]$throughput_mbs),]$test
  cat('- Recommended I/O method:', best_method, '\n')
  
  arrow_performance <- test_results[test_results$test == 'Arrow_multi_thread' & test_results$success,]
  single_performance <- test_results[test_results$test == 'Arrow_1_thread' & test_results$success,]
  
  if (nrow(arrow_performance) > 0 && nrow(single_performance) > 0) {
    improvement <- ((arrow_performance$throughput_mbs - single_performance$throughput_mbs) / single_performance$throughput_mbs) * 100
    cat('- Multi-threading improvement:', sprintf('%.1f%%', improvement), '\n')
  }
}

if (nrow(successful_tests) > 0) {
  cat('- Maximum safe dataset size:', max(successful_tests$target_gb), 'GB\n')
  avg_memory_ratio <- mean(successful_tests$peak_memory_gb / successful_tests$target_gb)
  cat('- Average memory overhead ratio:', sprintf('%.2fx', avg_memory_ratio), '\n')
}

# Clean up test files
file.remove(csv_file)

cat('\nComprehensive stress test completed!\n')
cat('Test results saved in all_results list object.\n')
