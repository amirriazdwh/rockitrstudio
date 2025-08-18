#!/usr/bin/env Rscript
# ================================================================
# Large CSV Parallel Reading Performance Test
# ================================================================
# Purpose: Test R_MAX_SIZE settings and parallel CSV reading performance
# Environment: 8 CPU cores, optimized for large datasets
# Libraries: data.table, parallel, doParallel, microbenchmark, pryr
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

# ================================================================
# CONFIGURATION PARAMETERS
# ================================================================

# Test parameters
NUM_CORES <- 8
NUM_ROWS <- 25000000  # 25 million rows for ~10GB file
NUM_COLS <- 20
TEST_RUNS <- 3
CHUNK_SIZE <- 1000000  # 1M rows per chunk

# File paths
TEST_DIR <- "/home/cdsw"
DATA_DIR <- file.path(TEST_DIR, "test_data")
CSV_FILE <- file.path(DATA_DIR, "large_dataset_10gb.csv")

# Create directories if they don't exist
if (!dir.exists(DATA_DIR)) {
  dir.create(DATA_DIR, recursive = TRUE)
}

# ================================================================
# UTILITY FUNCTIONS
# ================================================================

# Function to format bytes in human readable format
format_bytes <- function(bytes) {
  if (bytes >= 1e9) {
    paste0(round(bytes / 1e9, 2), " GB")
  } else if (bytes >= 1e6) {
    paste0(round(bytes / 1e6, 2), " MB")
  } else if (bytes >= 1e3) {
    paste0(round(bytes / 1e3, 2), " KB")
  } else {
    paste0(bytes, " bytes")
  }
}

# Function to get current memory usage
get_memory_usage <- function() {
  mem_info <- gc(verbose = FALSE)
  list(
    used_mb = sum(mem_info[, "(Mb)"]),
    total_allocated = mem_info[1, "max used (Mb)"] + mem_info[2, "max used (Mb)"]
  )
}

# Function to print system information
print_system_info <- function() {
  cat("\n" , rep("=", 70), "\n")
  cat("SYSTEM INFORMATION\n")
  cat(rep("=", 70), "\n")
  
  # R version and memory limits
  cat("R Version:", R.version.string, "\n")
  cat("Platform:", R.version$platform, "\n")
  
  # Memory limits
  r_max_size <- Sys.getenv("R_MAX_SIZE", "unset")
  cat("R_MAX_SIZE:", r_max_size, "\n")
  
  # Available cores
  cat("Available CPU cores:", detectCores(), "\n")
  cat("Using cores for test:", NUM_CORES, "\n")
  
  # Memory limits from R
  cat("Memory limit (if set):", format_bytes(memory.limit() * 1024^2), "\n")
  
  # Current memory usage
  mem_usage <- get_memory_usage()
  cat("Current memory usage:", format_bytes(mem_usage$used_mb * 1024^2), "\n")
  
  cat(rep("=", 70), "\n\n")
}

# ================================================================
# DATA GENERATION
# ================================================================

generate_large_csv <- function() {
  cat("Generating large CSV file...\n")
  cat("Target file:", CSV_FILE, "\n")
  cat("Rows:", format(NUM_ROWS, big.mark = ","), "\n")
  cat("Columns:", NUM_COLS, "\n")
  
  start_time <- Sys.time()
  
  # Generate data in chunks to avoid memory issues
  chunk_count <- ceiling(NUM_ROWS / CHUNK_SIZE)
  
  for (i in 1:chunk_count) {
    start_row <- (i - 1) * CHUNK_SIZE + 1
    end_row <- min(i * CHUNK_SIZE, NUM_ROWS)
    current_chunk_size <- end_row - start_row + 1
    
    cat("Generating chunk", i, "of", chunk_count, 
        "(rows", format(start_row, big.mark = ","), "to", format(end_row, big.mark = ","), ")\n")
    
    # Generate chunk data
    chunk_data <- data.table(
      id = start_row:end_row,
      timestamp = as.POSIXct("2025-01-01") + sample(1:31536000, current_chunk_size, replace = TRUE),
      category = sample(LETTERS[1:10], current_chunk_size, replace = TRUE),
      value1 = rnorm(current_chunk_size, mean = 100, sd = 25),
      value2 = runif(current_chunk_size, min = 0, max = 1000),
      value3 = rpois(current_chunk_size, lambda = 15),
      text_field = paste0("data_", sample(1:10000, current_chunk_size, replace = TRUE)),
      flag1 = sample(c(TRUE, FALSE), current_chunk_size, replace = TRUE),
      flag2 = sample(c(TRUE, FALSE), current_chunk_size, replace = TRUE),
      score1 = rnorm(current_chunk_size, mean = 50, sd = 15),
      score2 = rnorm(current_chunk_size, mean = 75, sd = 20),
      group_id = sample(1:1000, current_chunk_size, replace = TRUE),
      amount = runif(current_chunk_size, min = 1, max = 10000),
      percentage = runif(current_chunk_size, min = 0, max = 100),
      count_field = sample(1:100, current_chunk_size, replace = TRUE),
      rate = runif(current_chunk_size, min = 0.1, max = 5.0),
      index_val = sample(1:50000, current_chunk_size, replace = TRUE),
      status = sample(c("active", "inactive", "pending"), current_chunk_size, replace = TRUE),
      priority = sample(c("low", "medium", "high", "urgent"), current_chunk_size, replace = TRUE),
      region = sample(c("North", "South", "East", "West", "Central"), current_chunk_size, replace = TRUE)
    )
    
    # Write chunk to file
    if (i == 1) {
      fwrite(chunk_data, CSV_FILE, append = FALSE, verbose = FALSE)
    } else {
      fwrite(chunk_data, CSV_FILE, append = TRUE, verbose = FALSE)
    }
    
    # Memory cleanup
    rm(chunk_data)
    gc(verbose = FALSE)
  }
  
  end_time <- Sys.time()
  generation_time <- as.numeric(end_time - start_time, units = "secs")
  
  # Get file size
  file_size <- file.info(CSV_FILE)$size
  
  cat("\nCSV Generation Complete!\n")
  cat("File size:", format_bytes(file_size), "\n")
  cat("Generation time:", round(generation_time, 2), "seconds\n")
  cat("Generation rate:", round(NUM_ROWS / generation_time, 0), "rows/second\n\n")
  
  return(list(
    file_size = file_size,
    generation_time = generation_time,
    rows_per_second = NUM_ROWS / generation_time
  ))
}

# ================================================================
# PARALLEL READING TESTS
# ================================================================

# Test 1: Single-threaded data.table fread
test_single_threaded <- function() {
  cat("TEST 1: Single-threaded data.table fread\n")
  cat(rep("-", 50), "\n")
  
  mem_before <- get_memory_usage()
  
  timing <- microbenchmark(
    single_thread = {
      dt <- fread(CSV_FILE, nThread = 1, verbose = FALSE)
      nrow(dt)  # Force evaluation
      rm(dt)
      gc(verbose = FALSE)
    },
    times = TEST_RUNS
  )
  
  mem_after <- get_memory_usage()
  
  avg_time <- mean(timing$time) / 1e9  # Convert to seconds
  
  cat("Average time:", round(avg_time, 2), "seconds\n")
  cat("Memory used:", format_bytes((mem_after$used_mb - mem_before$used_mb) * 1024^2), "\n")
  cat("Read rate:", round(NUM_ROWS / avg_time, 0), "rows/second\n\n")
  
  return(list(
    method = "single_threaded",
    avg_time = avg_time,
    memory_used = mem_after$used_mb - mem_before$used_mb,
    rows_per_second = NUM_ROWS / avg_time
  ))
}

# Test 2: Multi-threaded data.table fread
test_multi_threaded <- function() {
  cat("TEST 2: Multi-threaded data.table fread (", NUM_CORES, " cores)\n")
  cat(rep("-", 50), "\n")
  
  mem_before <- get_memory_usage()
  
  timing <- microbenchmark(
    multi_thread = {
      dt <- fread(CSV_FILE, nThread = NUM_CORES, verbose = FALSE)
      nrow(dt)  # Force evaluation
      rm(dt)
      gc(verbose = FALSE)
    },
    times = TEST_RUNS
  )
  
  mem_after <- get_memory_usage()
  
  avg_time <- mean(timing$time) / 1e9  # Convert to seconds
  
  cat("Average time:", round(avg_time, 2), "seconds\n")
  cat("Memory used:", format_bytes((mem_after$used_mb - mem_before$used_mb) * 1024^2), "\n")
  cat("Read rate:", round(NUM_ROWS / avg_time, 0), "rows/second\n\n")
  
  return(list(
    method = "multi_threaded",
    avg_time = avg_time,
    memory_used = mem_after$used_mb - mem_before$used_mb,
    rows_per_second = NUM_ROWS / avg_time
  ))
}

# Test 3: Chunked parallel reading
test_chunked_parallel <- function() {
  cat("TEST 3: Chunked parallel reading (", NUM_CORES, " cores)\n")
  cat(rep("-", 50), "\n")
  
  # Setup parallel cluster
  cl <- makeCluster(NUM_CORES)
  registerDoParallel(cl)
  
  mem_before <- get_memory_usage()
  
  timing <- microbenchmark(
    chunked_parallel = {
      # Read file info to determine chunks
      file_size <- file.info(CSV_FILE)$size
      chunk_size <- ceiling(file_size / NUM_CORES)
      
      # Use foreach for parallel reading
      result <- foreach(i = 1:NUM_CORES, .combine = rbind, .packages = "data.table") %dopar% {
        skip_rows <- (i - 1) * (NUM_ROWS %/% NUM_CORES)
        nrows_to_read <- if (i == NUM_CORES) {
          NUM_ROWS - skip_rows
        } else {
          NUM_ROWS %/% NUM_CORES
        }
        
        if (skip_rows > 0) {
          fread(CSV_FILE, skip = skip_rows, nrows = nrows_to_read, verbose = FALSE)
        } else {
          fread(CSV_FILE, nrows = nrows_to_read, verbose = FALSE)
        }
      }
      
      nrow(result)  # Force evaluation
      rm(result)
      gc(verbose = FALSE)
    },
    times = TEST_RUNS
  )
  
  # Stop cluster
  stopCluster(cl)
  
  mem_after <- get_memory_usage()
  
  avg_time <- mean(timing$time) / 1e9  # Convert to seconds
  
  cat("Average time:", round(avg_time, 2), "seconds\n")
  cat("Memory used:", format_bytes((mem_after$used_mb - mem_before$used_mb) * 1024^2), "\n")
  cat("Read rate:", round(NUM_ROWS / avg_time, 0), "rows/second\n\n")
  
  return(list(
    method = "chunked_parallel",
    avg_time = avg_time,
    memory_used = mem_after$used_mb - mem_before$used_mb,
    rows_per_second = NUM_ROWS / avg_time
  ))
}

# ================================================================
# MEMORY STRESS TEST
# ================================================================

test_memory_limits <- function() {
  cat("TEST 4: Memory limit stress test\n")
  cat(rep("-", 50), "\n")
  
  mem_before <- get_memory_usage()
  
  tryCatch({
    cat("Attempting to read full dataset into memory...\n")
    
    # Read the full dataset
    dt <- fread(CSV_FILE, nThread = NUM_CORES, verbose = FALSE)
    
    mem_after_read <- get_memory_usage()
    
    cat("Dataset loaded successfully!\n")
    cat("Rows:", format(nrow(dt), big.mark = ","), "\n")
    cat("Columns:", ncol(dt), "\n")
    cat("Object size:", format_bytes(object.size(dt)), "\n")
    cat("Memory used:", format_bytes((mem_after_read$used_mb - mem_before$used_mb) * 1024^2), "\n")
    
    # Test some operations on the full dataset
    cat("Testing operations on full dataset...\n")
    
    operation_times <- list()
    
    # Test 1: Simple aggregation
    start_time <- Sys.time()
    result1 <- dt[, .(avg_value1 = mean(value1)), by = category]
    operation_times$aggregation <- as.numeric(Sys.time() - start_time, units = "secs")
    cat("Aggregation by category:", round(operation_times$aggregation, 3), "seconds\n")
    
    # Test 2: Filtering
    start_time <- Sys.time()
    result2 <- dt[value1 > 100 & flag1 == TRUE]
    operation_times$filtering <- as.numeric(Sys.time() - start_time, units = "secs")
    cat("Filtering operation:", round(operation_times$filtering, 3), "seconds\n")
    
    # Test 3: Sorting
    start_time <- Sys.time()
    setorder(dt, -value1)
    operation_times$sorting <- as.numeric(Sys.time() - start_time, units = "secs")
    cat("Sorting operation:", round(operation_times$sorting, 3), "seconds\n")
    
    # Final memory check
    mem_final <- get_memory_usage()
    
    # Clean up
    rm(dt, result1, result2)
    gc(verbose = FALSE)
    
    return(list(
      method = "memory_stress",
      success = TRUE,
      memory_used = mem_after_read$used_mb - mem_before$used_mb,
      operation_times = operation_times,
      peak_memory = mem_final$total_allocated
    ))
    
  }, error = function(e) {
    cat("Error during memory stress test:", e$message, "\n")
    return(list(
      method = "memory_stress",
      success = FALSE,
      error = e$message,
      memory_used = NA
    ))
  })
}

# ================================================================
# MAIN EXECUTION
# ================================================================

main <- function() {
  cat("\n")
  cat(rep("=", 70), "\n")
  cat("LARGE CSV PARALLEL READING PERFORMANCE TEST\n")
  cat(rep("=", 70), "\n")
  
  # Print system information
  print_system_info()
  
  # Initialize results list
  results <- list()
  
  # Generate CSV if it doesn't exist or is too small
  if (!file.exists(CSV_FILE) || file.info(CSV_FILE)$size < 1e9) {
    cat("Generating test dataset...\n")
    generation_result <- generate_large_csv()
    results$generation <- generation_result
  } else {
    cat("Using existing CSV file:", CSV_FILE, "\n")
    cat("File size:", format_bytes(file.info(CSV_FILE)$size), "\n\n")
  }
  
  # Run performance tests
  cat("Running performance tests...\n\n")
  
  # Test 1: Single-threaded
  results$single_threaded <- test_single_threaded()
  
  # Test 2: Multi-threaded
  results$multi_threaded <- test_multi_threaded()
  
  # Test 3: Chunked parallel
  results$chunked_parallel <- test_chunked_parallel()
  
  # Test 4: Memory stress test
  results$memory_stress <- test_memory_limits()
  
  # ================================================================
  # RESULTS SUMMARY
  # ================================================================
  
  cat("\n", rep("=", 70), "\n")
  cat("PERFORMANCE RESULTS SUMMARY\n")
  cat(rep("=", 70), "\n")
  
  # Performance comparison
  cat("Reading Performance Comparison:\n")
  cat(sprintf("%-20s %15s %15s %20s\n", "Method", "Time (sec)", "Memory (MB)", "Rows/sec"))
  cat(rep("-", 70), "\n")
  
  for (test_name in c("single_threaded", "multi_threaded", "chunked_parallel")) {
    if (test_name %in% names(results)) {
      result <- results[[test_name]]
      cat(sprintf("%-20s %15.2f %15.1f %20.0f\n", 
                  result$method, 
                  result$avg_time, 
                  result$memory_used,
                  result$rows_per_second))
    }
  }
  
  # Speed improvement
  if ("single_threaded" %in% names(results) && "multi_threaded" %in% names(results)) {
    speedup <- results$single_threaded$avg_time / results$multi_threaded$avg_time
    cat("\nMulti-threading speedup:", round(speedup, 2), "x\n")
  }
  
  # Memory stress test results
  cat("\nMemory Stress Test:\n")
  if (results$memory_stress$success) {
    cat("✓ Successfully loaded", format(NUM_ROWS, big.mark = ","), "rows into memory\n")
    cat("Peak memory usage:", format_bytes(results$memory_stress$peak_memory * 1024^2), "\n")
    if ("operation_times" %in% names(results$memory_stress)) {
      cat("Operation times:\n")
      for (op in names(results$memory_stress$operation_times)) {
        cat("  -", op, ":", round(results$memory_stress$operation_times[[op]], 3), "seconds\n")
      }
    }
  } else {
    cat("✗ Memory stress test failed:", results$memory_stress$error, "\n")
  }
  
  # R_MAX_SIZE analysis
  r_max_size <- Sys.getenv("R_MAX_SIZE", "unset")
  cat("\nR_MAX_SIZE Configuration:\n")
  cat("Current setting:", r_max_size, "\n")
  
  if (r_max_size != "unset") {
    cat("Memory limit appears to be configured\n")
  } else {
    cat("No explicit R_MAX_SIZE limit detected\n")
  }
  
  cat("\n", rep("=", 70), "\n")
  cat("Test completed successfully!\n")
  cat("Results saved to: large_csv_test_results.rds\n")
  cat(rep("=", 70), "\n\n")
  
  # Save results
  saveRDS(results, file = file.path(TEST_DIR, "large_csv_test_results.rds"))
  
  return(results)
}

# Run the main function
if (!interactive()) {
  results <- main()
}
