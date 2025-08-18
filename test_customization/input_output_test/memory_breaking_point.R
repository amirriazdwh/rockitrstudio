#!/usr/bin/env Rscript
# ================================================================
# MEMORY BREAKING POINT TEST - PARALLEL DATASET LOADING
# ================================================================
# Purpose: Find the memory breaking point using data.table parallel reading
# Strategy: Load increasingly larger datasets until memory failure
# View results in RStudio with detailed memory reporting
# ================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(parallel)
  library(pryr)
})

# Configuration
NUM_CORES <- detectCores()
BASE_ROWS <- 1000000  # Start with 1M rows
MAX_ROWS <- 50000000  # Up to 50M rows (potential 10-15GB)
STEP_MULTIPLIER <- 1.5  # Increase by 50% each step
NUM_COLS <- 20

# File paths in container
TEST_DIR <- "/home/cdsw"
DATA_DIR <- file.path(TEST_DIR, "test_data")
if (!dir.exists(DATA_DIR)) dir.create(DATA_DIR, recursive = TRUE)

# Results storage
breaking_point_results <- list()

# Utility functions
format_bytes <- function(bytes) {
  units <- c('B', 'KB', 'MB', 'GB', 'TB')
  i <- 1
  while(bytes >= 1024 && i < length(units)) {
    bytes <- bytes / 1024
    i <- i + 1
  }
  return(sprintf('%.2f %s', bytes, units[i]))
}

# Function to get current memory usage
get_memory_usage <- function() {
  if (.Platform$OS.type == "unix") {
    # Linux/Unix systems
    mem_info <- system("free -b | grep '^Mem:'", intern=TRUE)
    if (length(mem_info) > 0) {
      mem_parts <- as.numeric(strsplit(mem_info, "\\s+")[[1]][-1])
      return(list(
        total = mem_parts[1],
        used = mem_parts[2],
        free = mem_parts[3],
        available = if(length(mem_parts) >= 7) mem_parts[7] else mem_parts[3]
      ))
    }
  }
  
  # Fallback: use R's memory info
  gc_info <- gc()
  return(list(
    total = sum(gc_info[, "limit (Mb)"]) * 1024^2,
    used = sum(gc_info[, "used (Mb)"]) * 1024^2,
    free = sum(gc_info[, "limit (Mb)"] - gc_info[, "used (Mb)"]) * 1024^2,
    available = sum(gc_info[, "limit (Mb)"] - gc_info[, "used (Mb)"]) * 1024^2
  ))
}

cat('=================================================================\n')
cat('Memory Breaking Point Analysis\n')
cat('=================================================================\n')

# Get initial system information
initial_memory <- get_memory_usage()
cat('System Memory Information:\n')
cat('Total RAM:', format_bytes(initial_memory$total), '\n')
cat('Available RAM:', format_bytes(initial_memory$available), '\n')
cat('CPU Cores:', parallel::detectCores(), '\n')
cat('R Version:', R.version.string, '\n\n')

# Test parameters
test_sizes_gb <- c(1, 2, 5, 10, 15, 20, 25, 30)  # Progressive sizes in GB
results <- data.frame(
  test_size_gb = numeric(),
  rows_generated = numeric(),
  columns = numeric(),
  success = logical(),
  peak_memory_gb = numeric(),
  generation_time_sec = numeric(),
  write_time_sec = numeric(),
  error_message = character(),
  stringsAsFactors = FALSE
)

test_dir <- '/tmp/memory_test'
dir.create(test_dir, recursive = TRUE, showWarnings = FALSE)

cat('Starting progressive memory tests...\n')
cat('===================================\n\n')

for (size_gb in test_sizes_gb) {
  cat('Testing', size_gb, 'GB dataset generation...\n')
  
  # Calculate approximate rows needed for target size
  # Estimate: 10 columns * 8 bytes average = 80 bytes per row
  target_rows <- as.integer((size_gb * 1024^3) / 80)
  
  test_file <- file.path(test_dir, paste0('memory_test_', size_gb, 'gb.csv'))
  
  # Monitor memory before test
  memory_before <- get_memory_usage()
  generation_start <- Sys.time()
  
  test_result <- tryCatch({
    cat('  Generating', format(target_rows, big.mark=','), 'rows...\n')
    
    # Generate progressively in chunks to monitor memory
    chunk_size <- min(500000, target_rows)  # 500K rows or less
    chunks <- ceiling(target_rows / chunk_size)
    
    all_data <- data.frame()
    max_memory_used <- 0
    
    for (chunk in 1:chunks) {
      chunk_start <- (chunk - 1) * chunk_size + 1
      chunk_end <- min(chunk * chunk_size, target_rows)
      chunk_rows <- chunk_end - chunk_start + 1
      
      if (chunk %% 10 == 0) {
        cat('    Chunk', chunk, 'of', chunks, '\n')
      }
      
      # Generate chunk
      chunk_data <- data.frame(
        id = chunk_start:chunk_end,
        timestamp = as.POSIXct('2025-01-01') + runif(chunk_rows, 0, 365*24*3600),
        category = sample(LETTERS[1:5], chunk_rows, replace=TRUE),
        value1 = rnorm(chunk_rows, 100, 25),
        value2 = runif(chunk_rows, 0, 1000),
        value3 = rexp(chunk_rows, 0.1),
        text_field = paste0('test_', sample(10000:99999, chunk_rows, replace=TRUE)),
        factor_field = sample(paste0('Type', 1:20), chunk_rows, replace=TRUE),
        logical_field = sample(c(TRUE, FALSE), chunk_rows, replace=TRUE),
        large_numeric = runif(chunk_rows, 1e6, 1e9)
      )
      
      # Combine with previous data
      if (nrow(all_data) == 0) {
        all_data <- chunk_data
      } else {
        all_data <- rbind(all_data, chunk_data)
      }
      
      # Monitor memory usage
      current_memory <- get_memory_usage()
      memory_used_gb <- (current_memory$used - memory_before$used) / (1024^3)
      max_memory_used <- max(max_memory_used, memory_used_gb)
      
      rm(chunk_data)
      
      # Check if we're approaching memory limits
      if (memory_used_gb > (initial_memory$total / (1024^3)) * 0.8) {
        warning("Approaching memory limit, stopping chunk generation")
        break
      }
    }
    
    generation_end <- Sys.time()
    generation_time <- as.numeric(difftime(generation_end, generation_start, units='secs'))
    
    cat('  Writing to disk...\n')
    write_start <- Sys.time()
    data.table::fwrite(all_data, test_file)
    write_time <- as.numeric(difftime(Sys.time(), write_start, units='secs'))
    
    # Get final file size
    final_size <- file.info(test_file)$size
    
    cat('  SUCCESS - Generated:', format_bytes(final_size), '\n')
    cat('  Rows:', format(nrow(all_data), big.mark=','), '\n')
    cat('  Peak memory:', sprintf('%.2f GB', max_memory_used), '\n')
    cat('  Generation time:', sprintf('%.2f sec', generation_time), '\n')
    cat('  Write time:', sprintf('%.2f sec', write_time), '\n\n')
    
    # Clean up immediately to free memory
    rm(all_data)
    file.remove(test_file)
    gc()
    
    return(list(
      success = TRUE,
      rows = nrow(all_data),
      peak_memory = max_memory_used,
      generation_time = generation_time,
      write_time = write_time,
      error = ""
    ))
    
  }, error = function(e) {
    generation_time <- as.numeric(difftime(Sys.time(), generation_start, units='secs'))
    
    cat('  FAILED after', sprintf('%.2f', generation_time), 'seconds\n')
    cat('  Error:', e$message, '\n\n')
    
    # Clean up on error
    if (exists('all_data')) {
      tryCatch(rm(all_data), error = function(e) {})
    }
    if (file.exists(test_file)) {
      tryCatch(file.remove(test_file), error = function(e) {})
    }
    gc()
    
    return(list(
      success = FALSE,
      rows = 0,
      peak_memory = 0,
      generation_time = generation_time,
      write_time = 0,
      error = e$message
    ))
  })
  
  # Record results
  results <- rbind(results, data.frame(
    test_size_gb = size_gb,
    rows_generated = test_result$rows,
    columns = 10,
    success = test_result$success,
    peak_memory_gb = test_result$peak_memory,
    generation_time_sec = test_result$generation_time,
    write_time_sec = test_result$write_time,
    error_message = test_result$error,
    stringsAsFactors = FALSE
  ))
  
  # Stop if we hit a failure (breaking point found)
  if (!test_result$success) {
    cat('Breaking point detected at', size_gb, 'GB. Stopping tests.\n\n')
    break
  }
  
  # Small delay between tests
  Sys.sleep(2)
}

# Clean up test directory
unlink(test_dir, recursive = TRUE)

cat('=== MEMORY BREAKING POINT ANALYSIS RESULTS ===\n')
cat('================================================\n\n')

print(results)

# Analysis summary
successful_tests <- results[results$success, ]
failed_tests <- results[!results$success, ]

cat('\n=== ANALYSIS SUMMARY ===\n')

if (nrow(successful_tests) > 0) {
  max_successful_size <- max(successful_tests$test_size_gb)
  max_successful_rows <- max(successful_tests$rows_generated)
  
  cat('Maximum successful dataset size:', max_successful_size, 'GB\n')
  cat('Maximum rows generated:', format(max_successful_rows, big.mark=','), '\n')
  cat('Peak memory usage:', sprintf('%.2f GB', max(successful_tests$peak_memory_gb)), '\n')
  
  # Memory efficiency analysis
  if (nrow(successful_tests) > 1) {
    avg_memory_ratio <- mean(successful_tests$peak_memory_gb / successful_tests$test_size_gb)
    cat('Average memory overhead ratio:', sprintf('%.2fx', avg_memory_ratio), '\n')
  }
}

if (nrow(failed_tests) > 0) {
  breaking_point <- min(failed_tests$test_size_gb)
  cat('Breaking point detected at:', breaking_point, 'GB\n')
  cat('Failure reason:', failed_tests$error_message[1], '\n')
}

# Recommendations
cat('\n=== RECOMMENDATIONS ===\n')
total_ram_gb <- initial_memory$total / (1024^3)

if (nrow(successful_tests) > 0) {
  safe_limit <- max(successful_tests$test_size_gb) * 0.8  # 80% of max successful
  cat('Recommended safe dataset limit:', sprintf('%.1f GB', safe_limit), '\n')
  cat('System RAM utilization at limit:', sprintf('%.1f%%', (max(successful_tests$peak_memory_gb) / total_ram_gb) * 100), '\n')
}

cat('For optimal performance, keep datasets under', sprintf('%.1f GB', total_ram_gb * 0.6), '\n')
cat('This allows headroom for R operations and system processes.\n')

cat('\nMemory breaking point analysis completed!\n')
