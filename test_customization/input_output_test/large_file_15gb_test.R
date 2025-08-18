#!/usr/bin/env Rscript
# ================================================================
# LARGE FILE (15GB) CHUNKED PROCESSING TEST
# ================================================================
# Purpose: Test strategies for processing 15GB+ CSV files
# Strategies: Chunked reading, streaming, parallel chunks
# Environment: 8 CPU cores, ~28GB RAM available
# Date: August 2025
# ================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(parallel)
  library(doParallel)
  library(foreach)
  library(microbenchmark)
})

# Configuration for 15GB simulation
NUM_CORES <- 8
TOTAL_ROWS <- 75000000  # ~75M rows for ~15GB file
NUM_COLS <- 20
CHUNK_ROWS <- 2500000   # 2.5M rows per chunk (~500MB chunks)
NUM_CHUNKS <- ceiling(TOTAL_ROWS / CHUNK_ROWS)

# File paths
TEST_DIR <- "/home/cdsw"
DATA_DIR <- file.path(TEST_DIR, "test_data")
LARGE_CSV <- file.path(DATA_DIR, "large_15gb_dataset.csv")
CHUNK_DIR <- file.path(DATA_DIR, "chunks")

# Create directories
if (!dir.exists(DATA_DIR)) dir.create(DATA_DIR, recursive = TRUE)
if (!dir.exists(CHUNK_DIR)) dir.create(CHUNK_DIR, recursive = TRUE)

# Utility functions
format_bytes <- function(bytes) {
  if (bytes >= 1e9) paste0(round(bytes / 1e9, 2), " GB")
  else if (bytes >= 1e6) paste0(round(bytes / 1e6, 2), " MB")
  else paste0(round(bytes / 1e3, 2), " KB")
}

get_memory_usage <- function() {
  mem_info <- gc(verbose = FALSE)
  list(used_mb = sum(mem_info[, "used"]))
}

print_system_info <- function() {
  cat("\n", rep("=", 70), "\n")
  cat("15GB FILE PROCESSING TEST - SYSTEM INFO\n")
  cat(rep("=", 70), "\n")
  cat("Target file size: ~15GB\n")
  cat("Total rows:", format(TOTAL_ROWS, big.mark = ","), "\n")
  cat("Chunk size:", format(CHUNK_ROWS, big.mark = ","), "rows (~500MB each)\n")
  cat("Number of chunks:", NUM_CHUNKS, "\n")
  cat("Available cores:", detectCores(), "\n")
  cat("Using cores:", NUM_CORES, "\n")
  
  mem_usage <- get_memory_usage()
  cat("Current memory:", format_bytes(mem_usage$used_mb * 1024^2), "\n")
  cat(rep("=", 70), "\n\n")
}

# ================================================================
# STRATEGY 1: GENERATE LARGE FILE IN CHUNKS
# ================================================================

generate_large_file_chunked <- function() {
  cat("STRATEGY 1: Generating 15GB file in chunks\n")
  cat(rep("-", 50), "\n")
  
  start_time <- Sys.time()
  total_size <- 0
  
  for (i in 1:NUM_CHUNKS) {
    chunk_start <- (i - 1) * CHUNK_ROWS + 1
    chunk_end <- min(i * CHUNK_ROWS, TOTAL_ROWS)
    current_chunk_size <- chunk_end - chunk_start + 1
    
    cat("Generating chunk", i, "of", NUM_CHUNKS, 
        "(", format(current_chunk_size, big.mark = ","), "rows)\n")
    
    # Generate chunk
    chunk_data <- data.table(
      id = chunk_start:chunk_end,
      timestamp = as.POSIXct("2025-01-01") + sample(1:31536000, current_chunk_size, replace = TRUE),
      category = sample(LETTERS[1:20], current_chunk_size, replace = TRUE),
      value1 = rnorm(current_chunk_size, 100, 25),
      value2 = runif(current_chunk_size, 0, 1000),
      value3 = rpois(current_chunk_size, 15),
      text_field = paste0("data_", sample(1:50000, current_chunk_size, replace = TRUE)),
      flag1 = sample(c(TRUE, FALSE), current_chunk_size, replace = TRUE),
      flag2 = sample(c(TRUE, FALSE), current_chunk_size, replace = TRUE),
      score1 = rnorm(current_chunk_size, 50, 15),
      score2 = rnorm(current_chunk_size, 75, 20),
      group_id = sample(1:5000, current_chunk_size, replace = TRUE),
      amount = runif(current_chunk_size, 1, 50000),
      percentage = runif(current_chunk_size, 0, 100),
      count_field = sample(1:1000, current_chunk_size, replace = TRUE),
      rate = runif(current_chunk_size, 0.1, 10.0),
      index_val = sample(1:100000, current_chunk_size, replace = TRUE),
      status = sample(c("active", "inactive", "pending", "archived"), current_chunk_size, replace = TRUE),
      priority = sample(c("low", "medium", "high", "urgent", "critical"), current_chunk_size, replace = TRUE),
      region = sample(c("North", "South", "East", "West", "Central", "Northeast", "Southwest"), current_chunk_size, replace = TRUE)
    )
    
    # Write chunk
    if (i == 1) {
      fwrite(chunk_data, LARGE_CSV, append = FALSE)
    } else {
      fwrite(chunk_data, LARGE_CSV, append = TRUE)
    }
    
    # Track progress
    if (file.exists(LARGE_CSV)) {
      current_size <- file.info(LARGE_CSV)$size
      total_size <- current_size
      cat("  Current file size:", format_bytes(current_size), "\n")
    }
    
    rm(chunk_data)
    gc(verbose = FALSE)
    
    # Memory check every 5 chunks
    if (i %% 5 == 0) {
      mem_usage <- get_memory_usage()
      cat("  Memory usage:", format_bytes(mem_usage$used_mb * 1024^2), "\n")
    }
  }
  
  end_time <- Sys.time()
  generation_time <- as.numeric(end_time - start_time, units = "secs")
  
  cat("\nFile generation complete!\n")
  cat("Final size:", format_bytes(total_size), "\n")
  cat("Generation time:", round(generation_time, 2), "seconds\n")
  cat("Generation rate:", round(TOTAL_ROWS / generation_time, 0), "rows/second\n\n")
  
  return(list(
    file_size = total_size,
    generation_time = generation_time
  ))
}

# ================================================================
# STRATEGY 2: CHUNKED SEQUENTIAL PROCESSING
# ================================================================

test_chunked_sequential <- function() {
  cat("STRATEGY 2: Chunked sequential processing\n")
  cat(rep("-", 50), "\n")
  
  start_time <- Sys.time()
  total_processed <- 0
  chunk_times <- numeric(NUM_CHUNKS)
  
  # Process each chunk sequentially
  for (i in 1:min(5, NUM_CHUNKS)) {  # Test first 5 chunks
    cat("Processing chunk", i, "of", NUM_CHUNKS, "\n")
    
    chunk_start_time <- Sys.time()
    
    # Read chunk
    skip_rows <- (i - 1) * CHUNK_ROWS
    chunk_data <- fread(LARGE_CSV, skip = skip_rows, nrows = CHUNK_ROWS, 
                       nThread = NUM_CORES, verbose = FALSE)
    
    # Process chunk (example: aggregation)
    result <- chunk_data[, .(
      count = .N,
      avg_value1 = mean(value1),
      sum_amount = sum(amount)
    ), by = category]
    
    chunk_end_time <- Sys.time()
    chunk_time <- as.numeric(chunk_end_time - chunk_start_time, units = "secs")
    chunk_times[i] <- chunk_time
    
    total_processed <- total_processed + nrow(chunk_data)
    
    cat("  Chunk", i, "processed:", nrow(chunk_data), "rows in", 
        round(chunk_time, 2), "seconds\n")
    cat("  Categories found:", nrow(result), "\n")
    
    rm(chunk_data, result)
    gc(verbose = FALSE)
  }
  
  end_time <- Sys.time()
  total_time <- as.numeric(end_time - start_time, units = "secs")
  
  cat("\nChunked sequential results:\n")
  cat("Chunks processed:", min(5, NUM_CHUNKS), "\n")
  cat("Total rows processed:", format(total_processed, big.mark = ","), "\n")
  cat("Total time:", round(total_time, 2), "seconds\n")
  cat("Average chunk time:", round(mean(chunk_times[1:min(5, NUM_CHUNKS)]), 2), "seconds\n")
  cat("Processing rate:", round(total_processed / total_time, 0), "rows/second\n\n")
  
  return(list(
    method = "chunked_sequential",
    total_time = total_time,
    chunks_processed = min(5, NUM_CHUNKS),
    rows_processed = total_processed,
    avg_chunk_time = mean(chunk_times[1:min(5, NUM_CHUNKS)])
  ))
}

# ================================================================
# STRATEGY 3: PARALLEL CHUNK PROCESSING
# ================================================================

test_parallel_chunks <- function() {
  cat("STRATEGY 3: Parallel chunk processing\n")
  cat(rep("-", 50), "\n")
  
  # Setup parallel cluster
  cl <- makeCluster(NUM_CORES)
  registerDoParallel(cl)
  
  start_time <- Sys.time()
  
  # Process chunks in parallel
  results <- foreach(i = 1:min(4, NUM_CHUNKS), .combine = rbind, 
                    .packages = "data.table") %dopar% {
    
    # Read chunk
    skip_rows <- (i - 1) * CHUNK_ROWS
    chunk_data <- fread(LARGE_CSV, skip = skip_rows, nrows = CHUNK_ROWS, 
                       nThread = 1, verbose = FALSE)  # Single thread per worker
    
    # Process chunk
    result <- chunk_data[, .(
      chunk_id = i,
      count = .N,
      avg_value1 = mean(value1),
      sum_amount = sum(amount),
      categories = length(unique(category))
    )]
    
    rm(chunk_data)
    gc(verbose = FALSE)
    
    result
  }
  
  stopCluster(cl)
  
  end_time <- Sys.time()
  total_time <- as.numeric(end_time - start_time, units = "secs")
  
  total_processed <- sum(results$count)
  
  cat("\nParallel chunk results:\n")
  cat("Chunks processed:", nrow(results), "\n")
  cat("Total rows processed:", format(total_processed, big.mark = ","), "\n")
  cat("Total time:", round(total_time, 2), "seconds\n")
  cat("Processing rate:", round(total_processed / total_time, 0), "rows/second\n")
  
  print(results)
  cat("\n")
  
  return(list(
    method = "parallel_chunks",
    total_time = total_time,
    chunks_processed = nrow(results),
    rows_processed = total_processed,
    results = results
  ))
}

# ================================================================
# STRATEGY 4: MEMORY-MAPPED SAMPLING
# ================================================================

test_sampling_strategy <- function() {
  cat("STRATEGY 4: Random sampling for large file analysis\n")
  cat(rep("-", 50), "\n")
  
  start_time <- Sys.time()
  
  # Sample every Nth row for analysis
  sample_rate <- 100  # Every 100th row
  
  cat("Reading every", sample_rate, "th row for analysis...\n")
  
  # Read sample
  sample_data <- fread(LARGE_CSV, nThread = NUM_CORES, verbose = FALSE,
                      skip = function(x, pos) (pos - 1) %% sample_rate != 0)
  
  end_time <- Sys.time()
  read_time <- as.numeric(end_time - start_time, units = "secs")
  
  # Analyze sample
  sample_size <- nrow(sample_data)
  
  cat("Sample analysis:\n")
  cat("Sample size:", format(sample_size, big.mark = ","), "rows\n")
  cat("Estimated total rows:", format(sample_size * sample_rate, big.mark = ","), "\n")
  cat("Sample read time:", round(read_time, 2), "seconds\n")
  
  # Quick analysis
  summary_stats <- sample_data[, .(
    avg_value1 = mean(value1),
    median_value2 = median(value2),
    categories = length(unique(category)),
    active_pct = mean(status == "active") * 100
  )]
  
  cat("Sample statistics:\n")
  print(summary_stats)
  
  rm(sample_data)
  gc(verbose = FALSE)
  
  return(list(
    method = "sampling",
    sample_size = sample_size,
    read_time = read_time,
    sample_rate = sample_rate
  ))
}

# ================================================================
# MEMORY LIMIT SIMULATION
# ================================================================

test_memory_limits <- function() {
  cat("STRATEGY 5: Memory limit testing\n")
  cat(rep("-", 50), "\n")
  
  # Try to read progressively larger chunks until memory limit
  chunk_sizes <- c(500000, 1000000, 2000000, 3000000, 5000000)  # 0.5M to 5M rows
  
  results <- list()
  
  for (chunk_size in chunk_sizes) {
    cat("Testing chunk size:", format(chunk_size, big.mark = ","), "rows\n")
    
    tryCatch({
      mem_before <- get_memory_usage()
      
      start_time <- Sys.time()
      chunk_data <- fread(LARGE_CSV, nrows = chunk_size, nThread = NUM_CORES, verbose = FALSE)
      read_time <- as.numeric(Sys.time() - start_time, units = "secs")
      
      mem_after <- get_memory_usage()
      memory_used <- mem_after$used_mb - mem_before$used_mb
      
      object_size_mb <- as.numeric(object.size(chunk_data)) / 1024^2
      
      cat("  ✓ Success:", format(nrow(chunk_data), big.mark = ","), "rows\n")
      cat("  Read time:", round(read_time, 2), "seconds\n")
      cat("  Object size:", round(object_size_mb, 1), "MB\n")
      cat("  Memory used:", round(memory_used, 1), "MB\n\n")
      
      results[[length(results) + 1]] <- list(
        chunk_size = chunk_size,
        success = TRUE,
        read_time = read_time,
        object_size_mb = object_size_mb,
        memory_used_mb = memory_used
      )
      
      rm(chunk_data)
      gc(verbose = FALSE)
      
    }, error = function(e) {
      cat("  ✗ Failed:", e$message, "\n\n")
      results[[length(results) + 1]] <- list(
        chunk_size = chunk_size,
        success = FALSE,
        error = e$message
      )
    })
  }
  
  return(results)
}

# ================================================================
# MAIN EXECUTION
# ================================================================

main <- function() {
  print_system_info()
  
  all_results <- list()
  
  # Check if large file exists or generate it
  if (!file.exists(LARGE_CSV) || file.info(LARGE_CSV)$size < 10e9) {
    cat("Generating large test file (this may take a while)...\n")
    all_results$generation <- generate_large_file_chunked()
  } else {
    cat("Using existing large file:", LARGE_CSV, "\n")
    cat("File size:", format_bytes(file.info(LARGE_CSV)$size), "\n\n")
  }
  
  # Run different strategies
  cat("Testing different processing strategies for 15GB files...\n\n")
  
  all_results$chunked_sequential <- test_chunked_sequential()
  all_results$parallel_chunks <- test_parallel_chunks()
  all_results$sampling <- test_sampling_strategy()
  all_results$memory_limits <- test_memory_limits()
  
  # Summary
  cat(rep("=", 70), "\n")
  cat("15GB FILE PROCESSING - RESULTS SUMMARY\n")
  cat(rep("=", 70), "\n")
  
  if (file.exists(LARGE_CSV)) {
    actual_size <- file.info(LARGE_CSV)$size
    cat("Actual file size:", format_bytes(actual_size), "\n")
  }
  
  cat("\nProcessing Strategy Performance:\n")
  cat(sprintf("%-25s %15s %20s\n", "Strategy", "Time (sec)", "Rate (rows/sec)"))
  cat(rep("-", 60), "\n")
  
  strategies <- c("chunked_sequential", "parallel_chunks")
  for (strategy in strategies) {
    if (strategy %in% names(all_results)) {
      result <- all_results[[strategy]]
      if ("total_time" %in% names(result) && "rows_processed" %in% names(result)) {
        rate <- result$rows_processed / result$total_time
        cat(sprintf("%-25s %15.2f %20.0f\n", 
                    result$method, result$total_time, rate))
      }
    }
  }
  
  cat("\nKey Recommendations for 15GB files:\n")
  cat("1. Use chunked processing (2-5M rows per chunk)\n")
  cat("2. Parallel chunk processing for CPU-bound operations\n")
  cat("3. Sequential for I/O-bound operations\n")
  cat("4. Sampling for exploratory analysis\n")
  cat("5. Monitor memory usage carefully\n")
  
  r_max_size <- Sys.getenv("R_MAX_SIZE", "unset")
  cat("\nCurrent R_MAX_SIZE:", r_max_size, "\n")
  
  if (r_max_size == "unset") {
    cat("Recommendation: Consider setting R_MAX_SIZE for 15GB processing\n")
    cat("Example: R_MAX_SIZE=20000000000 (20GB limit)\n")
  }
  
  cat("\n", rep("=", 70), "\n")
  
  return(all_results)
}

# Execute
if (!interactive()) {
  results <- main()
}
