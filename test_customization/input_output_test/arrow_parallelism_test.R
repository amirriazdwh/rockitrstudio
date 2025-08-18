#!/usr/bin/env Rscript
# =================================================================
# Arrow Parallelism Optimization Test
# Tests Arrow performance across different core configurations
# =================================================================
#
# This script tests Arrow's read performance with different
# parallelism settings to find optimal configuration
#
# Usage: Rscript arrow_parallelism_test.R
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

cat('=================================================================\n')
cat('Arrow Parallelism Optimization Test\n')
cat('=================================================================\n')

# System information
cat('System Configuration:\n')
cat('CPU Cores:', parallel::detectCores(), '\n')
cat('R Version:', R.version.string, '\n')
cat('Arrow Version:', packageVersion('arrow'), '\n')
cat('JIT Compilation:', Sys.getenv('R_ENABLE_JIT'), '\n\n')

# Test parameters
test_file_size_gb <- 2  # Start with smaller file for quick testing
target_rows <- test_file_size_gb * 1000000  # 1M rows per GB
test_dir <- '/tmp/arrow_parallel_test'
csv_file <- file.path(test_dir, 'arrow_test.csv')

# Create test directory
dir.create(test_dir, recursive = TRUE, showWarnings = FALSE)

# Generate test data
cat('Generating', test_file_size_gb, 'GB test dataset...\n')
generation_start <- Sys.time()

test_data <- data.frame(
  id = 1:target_rows,
  timestamp = as.POSIXct('2025-01-01') + runif(target_rows, 0, 365*24*3600),
  category = sample(LETTERS[1:10], target_rows, replace=TRUE),
  value1 = rnorm(target_rows, 100, 25),
  value2 = runif(target_rows, 0, 1000),
  value3 = rexp(target_rows, 0.1),
  text_field = paste0('test_', sample(100000:999999, target_rows, replace=TRUE)),
  factor_field = sample(paste0('Type', 1:5), target_rows, replace=TRUE),
  logical_field = sample(c(TRUE, FALSE), target_rows, replace=TRUE),
  large_numeric = runif(target_rows, 1e6, 1e9)
)

# Write test file
data.table::fwrite(test_data, csv_file)
rm(test_data)
gc()

file_size <- file.info(csv_file)$size
generation_time <- as.numeric(difftime(Sys.time(), generation_start, units='secs'))

cat('Test file generated:\n')
cat('Size:', format_bytes(file_size), '\n')
cat('Generation time:', sprintf('%.2f seconds', generation_time), '\n\n')

# Test different core configurations
max_cores <- parallel::detectCores()
core_configs <- c(1, 2, 4, max_cores)
if (max_cores > 8) {
  core_configs <- c(core_configs, 8)
}
core_configs <- sort(unique(core_configs))

# Results tracking
results <- data.frame(
  cores = integer(),
  threads_setting = character(),
  read_time_sec = numeric(),
  throughput_mbs = numeric(),
  cpu_efficiency = numeric(),
  success = logical(),
  stringsAsFactors = FALSE
)

cat('Testing Arrow performance with different core configurations:\n')
cat('============================================================\n\n')

file_size_gb <- file_size / (1024^3)

for (cores in core_configs) {
  cat('Testing with', cores, 'cores...\n')
  
  # Test multiple thread configurations for each core count
  thread_configs <- c(cores, cores * 2)  # Test cores and hyperthreading
  
  for (threads in thread_configs) {
    config_name <- paste0(cores, '_cores_', threads, '_threads')
    
    cat('  Configuration:', config_name, '\n')
    
    test_start <- Sys.time()
    
    tryCatch({
      # Set environment variables for Arrow parallelism
      old_omp_threads <- Sys.getenv('OMP_NUM_THREADS')
      old_arrow_cpu_count <- Sys.getenv('ARROW_CPU_COUNT')
      
      Sys.setenv(OMP_NUM_THREADS = as.character(threads))
      Sys.setenv(ARROW_CPU_COUNT = as.character(cores))
      
      # Read with Arrow
      arrow_data <- arrow::read_csv_arrow(csv_file, as_data_frame = FALSE)
      df <- as.data.frame(arrow_data)
      
      read_time <- as.numeric(difftime(Sys.time(), test_start, units='secs'))
      throughput <- (file_size_gb * 1024) / read_time
      cpu_efficiency <- throughput / cores  # MB/s per core
      
      cat('    SUCCESS - Time:', sprintf('%.3f sec', read_time), 
          'Throughput:', sprintf('%.2f MB/s', throughput),
          'Efficiency:', sprintf('%.2f MB/s/core', cpu_efficiency), '\n')
      
      results <- rbind(results, data.frame(
        cores = cores,
        threads_setting = config_name,
        read_time_sec = read_time,
        throughput_mbs = throughput,
        cpu_efficiency = cpu_efficiency,
        success = TRUE,
        stringsAsFactors = FALSE
      ))
      
      # Clean up
      rm(arrow_data, df)
      
      # Restore environment
      if (old_omp_threads != '') {
        Sys.setenv(OMP_NUM_THREADS = old_omp_threads)
      } else {
        Sys.unsetenv('OMP_NUM_THREADS')
      }
      
      if (old_arrow_cpu_count != '') {
        Sys.setenv(ARROW_CPU_COUNT = old_arrow_cpu_count)
      } else {
        Sys.unsetenv('ARROW_CPU_COUNT')
      }
      
    }, error = function(e) {
      read_time <- as.numeric(difftime(Sys.time(), test_start, units='secs'))
      
      cat('    FAILED after', sprintf('%.3f', read_time), 'sec:', e$message, '\n')
      
      results <- rbind(results, data.frame(
        cores = cores,
        threads_setting = config_name,
        read_time_sec = read_time,
        throughput_mbs = 0,
        cpu_efficiency = 0,
        success = FALSE,
        stringsAsFactors = FALSE
      ))
    })
    
    gc()
    Sys.sleep(1)  # Brief pause between tests
  }
  
  cat('\n')
}

# Additional tests: Compare Arrow with other methods using optimal settings
cat('Comparing with other I/O methods using optimal settings...\n')
cat('=========================================================\n')

# Find best Arrow configuration
successful_results <- results[results$success, ]
if (nrow(successful_results) > 0) {
  best_config <- successful_results[which.max(successful_results$throughput_mbs), ]
  
  cat('Using optimal Arrow config:', best_config$threads_setting, '\n')
  
  # Set optimal configuration
  optimal_cores <- best_config$cores
  optimal_threads <- as.numeric(strsplit(best_config$threads_setting, '_')[[1]][4])
  
  Sys.setenv(OMP_NUM_THREADS = as.character(optimal_threads))
  Sys.setenv(ARROW_CPU_COUNT = as.character(optimal_cores))
  
  # Test data.table fread
  cat('Testing data.table fread...\n')
  start_time <- Sys.time()
  tryCatch({
    df_dt <- data.table::fread(csv_file)
    dt_time <- as.numeric(difftime(Sys.time(), start_time, units='secs'))
    dt_throughput <- (file_size_gb * 1024) / dt_time
    
    cat('data.table fread - Time:', sprintf('%.3f sec', dt_time), 
        'Throughput:', sprintf('%.2f MB/s', dt_throughput), '\n')
    
    rm(df_dt)
  }, error = function(e) {
    cat('data.table fread FAILED:', e$message, '\n')
  })
  
  # Test base R read.csv (with timeout)
  cat('Testing base R read.csv (with 60s timeout)...\n')
  start_time <- Sys.time()
  tryCatch({
    # Simple timeout mechanism
    timeout_reached <- FALSE
    
    df_base <- withRestarts(
      withCallingHandlers({
        read.csv(csv_file)
      }, error = function(e) {
        if (as.numeric(difftime(Sys.time(), start_time, units='secs')) > 60) {
          timeout_reached <<- TRUE
          invokeRestart("timeout")
        } else {
          stop(e)
        }
      }),
      timeout = function() NULL
    )
    
    if (timeout_reached) {
      cat('Base R read.csv - TIMEOUT after 60 seconds\n')
    } else {
      base_time <- as.numeric(difftime(Sys.time(), start_time, units='secs'))
      base_throughput <- (file_size_gb * 1024) / base_time
      
      cat('Base R read.csv - Time:', sprintf('%.3f sec', base_time), 
          'Throughput:', sprintf('%.2f MB/s', base_throughput), '\n')
      
      rm(df_base)
    }
    
  }, error = function(e) {
    cat('Base R read.csv FAILED:', e$message, '\n')
  })
}

# Clean up
file.remove(csv_file)
unlink(test_dir, recursive = TRUE)

cat('\n=== ARROW PARALLELISM TEST RESULTS ===\n')
cat('======================================\n\n')

print(results)

# Analysis
if (nrow(successful_results) > 0) {
  cat('\n=== PERFORMANCE ANALYSIS ===\n')
  
  best_overall <- successful_results[which.max(successful_results$throughput_mbs), ]
  best_efficiency <- successful_results[which.max(successful_results$cpu_efficiency), ]
  
  cat('Best Overall Performance:\n')
  cat('  Configuration:', best_overall$threads_setting, '\n')
  cat('  Throughput:', sprintf('%.2f MB/s', best_overall$throughput_mbs), '\n')
  cat('  Time:', sprintf('%.3f seconds', best_overall$read_time_sec), '\n\n')
  
  cat('Best CPU Efficiency:\n')
  cat('  Configuration:', best_efficiency$threads_setting, '\n')
  cat('  Efficiency:', sprintf('%.2f MB/s per core', best_efficiency$cpu_efficiency), '\n')
  cat('  Throughput:', sprintf('%.2f MB/s', best_efficiency$throughput_mbs), '\n\n')
  
  # Scaling analysis
  single_core_result <- successful_results[successful_results$cores == 1, ]
  if (nrow(single_core_result) > 0) {
    single_core_throughput <- max(single_core_result$throughput_mbs)
    max_throughput <- max(successful_results$throughput_mbs)
    scaling_factor <- max_throughput / single_core_throughput
    
    cat('Parallel Scaling Analysis:\n')
    cat('  Single core throughput:', sprintf('%.2f MB/s', single_core_throughput), '\n')
    cat('  Maximum throughput:', sprintf('%.2f MB/s', max_throughput), '\n')
    cat('  Scaling factor:', sprintf('%.2fx', scaling_factor), '\n')
    cat('  Parallel efficiency:', sprintf('%.1f%%', (scaling_factor / max_cores) * 100), '\n\n')
  }
  
  # Recommendations
  cat('=== RECOMMENDATIONS ===\n')
  cat('For optimal Arrow performance in this environment:\n')
  cat('  Use', best_overall$cores, 'cores with', 
      as.numeric(strsplit(best_overall$threads_setting, '_')[[1]][4]), 'threads\n')
  cat('  Expected throughput:', sprintf('%.2f MB/s', best_overall$throughput_mbs), '\n')
  cat('  Set environment variables:\n')
  cat('    export OMP_NUM_THREADS=', as.numeric(strsplit(best_overall$threads_setting, '_')[[1]][4]), '\n')
  cat('    export ARROW_CPU_COUNT=', best_overall$cores, '\n')
}

cat('\nArrow parallelism optimization test completed!\n')
