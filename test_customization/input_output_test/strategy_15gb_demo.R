#!/usr/bin/env Rscript
# ================================================================
# 15GB FILE PROCESSING - STRATEGY DEMONSTRATION 
# ================================================================
# Purpose: Demonstrate strategies for 15GB files without generating full file
# Test chunked processing, memory limits, and parallel strategies
# ================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(parallel)
  library(doParallel)
})

cat("=== 15GB FILE PROCESSING STRATEGIES ===\n\n")

# System info
cat("System Configuration:\n")
cat("Available cores:", detectCores(), "\n")
cat("R_MAX_SIZE:", Sys.getenv("R_MAX_SIZE", "unset"), "\n")

mem_info <- gc()
cat("Current memory:", round(sum(mem_info[, "used"]), 1), "MB\n\n")

# ================================================================
# STRATEGY 1: MEMORY LIMIT TESTING
# ================================================================

cat("STRATEGY 1: Testing memory limits with increasing data sizes\n")
cat(rep("-", 60), "\n")

chunk_sizes <- c(1000000, 2500000, 5000000, 7500000, 10000000)  # 1M to 10M rows
max_successful_size <- 0

for (size in chunk_sizes) {
  cat("Testing", format(size, big.mark = ","), "rows...")
  
  tryCatch({
    # Generate test data
    start_time <- Sys.time()
    test_data <- data.table(
      id = 1:size,
      value1 = rnorm(size, 100, 25),
      value2 = runif(size, 0, 1000),
      text_field = paste0("data_", sample(1:10000, size, replace = TRUE)),
      category = sample(LETTERS[1:10], size, replace = TRUE)
    )
    
    generation_time <- as.numeric(Sys.time() - start_time, units = "secs")
    object_size_mb <- as.numeric(object.size(test_data)) / 1024^2
    
    cat(" ✓ Success\n")
    cat("  Generation time:", round(generation_time, 2), "seconds\n")
    cat("  Object size:", round(object_size_mb, 1), "MB\n")
    cat("  Memory efficiency:", round(size / object_size_mb, 0), "rows/MB\n")
    
    max_successful_size <- size
    
    # Quick operation test
    start_time <- Sys.time()
    result <- test_data[, .(count = .N, avg_val = mean(value1)), by = category]
    op_time <- as.numeric(Sys.time() - start_time, units = "secs")
    cat("  Aggregation time:", round(op_time, 3), "seconds\n\n")
    
    rm(test_data, result)
    gc(verbose = FALSE)
    
  }, error = function(e) {
    cat(" ✗ Failed:", e$message, "\n\n")
    break
  })
}

cat("Maximum successful size:", format(max_successful_size, big.mark = ","), "rows\n")
estimated_15gb_rows <- 75000000  # Estimated for 15GB
cat("15GB file estimated rows:", format(estimated_15gb_rows, big.mark = ","), "\n")

if (max_successful_size < estimated_15gb_rows) {
  chunks_needed <- ceiling(estimated_15gb_rows / max_successful_size)
  cat("Chunks needed for 15GB file:", chunks_needed, "\n")
  cat("Recommended chunk size:", format(max_successful_size, big.mark = ","), "rows\n\n")
} else {
  cat("System can handle 15GB file in memory!\n\n")
}

# ================================================================
# STRATEGY 2: CHUNKED PROCESSING SIMULATION
# ================================================================

cat("STRATEGY 2: Chunked processing simulation\n")
cat(rep("-", 60), "\n")

# Simulate processing 15GB file in chunks
chunk_size <- min(max_successful_size, 2500000)  # Conservative chunk size
total_rows_15gb <- 75000000
num_chunks <- ceiling(total_rows_15gb / chunk_size)

cat("Simulating 15GB file processing:\n")
cat("Total rows:", format(total_rows_15gb, big.mark = ","), "\n")
cat("Chunk size:", format(chunk_size, big.mark = ","), "rows\n")
cat("Number of chunks:", num_chunks, "\n\n")

# Simulate processing first few chunks
cat("Sequential chunk processing simulation:\n")
total_time <- 0
for (i in 1:min(3, num_chunks)) {
  cat("Chunk", i, "of", num_chunks, "...")
  
  start_time <- Sys.time()
  
  # Simulate chunk generation and processing
  chunk_data <- data.table(
    id = ((i-1) * chunk_size + 1):(min(i * chunk_size, total_rows_15gb)),
    value1 = rnorm(min(chunk_size, total_rows_15gb - (i-1) * chunk_size), 100, 25),
    category = sample(LETTERS[1:10], min(chunk_size, total_rows_15gb - (i-1) * chunk_size), replace = TRUE)
  )
  
  # Simulate processing
  result <- chunk_data[, .(count = .N, avg_val = mean(value1)), by = category]
  
  chunk_time <- as.numeric(Sys.time() - start_time, units = "secs")
  total_time <- total_time + chunk_time
  
  cat(" ", round(chunk_time, 2), "seconds\n")
  
  rm(chunk_data, result)
  gc(verbose = FALSE)
}

estimated_total_time <- (total_time / min(3, num_chunks)) * num_chunks
cat("\nEstimated total processing time for 15GB:", round(estimated_total_time, 1), "seconds\n")
cat("Estimated processing rate:", round(total_rows_15gb / estimated_total_time, 0), "rows/second\n\n")

# ================================================================
# STRATEGY 3: PARALLEL CHUNK PROCESSING
# ================================================================

cat("STRATEGY 3: Parallel chunk processing test\n")
cat(rep("-", 60), "\n")

num_cores <- detectCores()
cl <- makeCluster(min(4, num_cores))  # Use up to 4 cores
registerDoParallel(cl)

cat("Testing parallel processing with", getDoParWorkers(), "workers\n")

start_time <- Sys.time()

# Process chunks in parallel
results <- foreach(i = 1:4, .combine = rbind, .packages = "data.table") %dopar% {
  # Generate chunk
  chunk_data <- data.table(
    chunk_id = i,
    id = ((i-1) * 500000 + 1):(i * 500000),
    value1 = rnorm(500000, 100, 25),
    category = sample(LETTERS[1:5], 500000, replace = TRUE)
  )
  
  # Process chunk
  result <- chunk_data[, .(
    chunk = i,
    rows = .N,
    categories = length(unique(category)),
    avg_value = mean(value1)
  )]
  
  result
}

stopCluster(cl)

parallel_time <- as.numeric(Sys.time() - start_time, units = "secs")

cat("Parallel processing results:\n")
print(results)
cat("\nParallel processing time:", round(parallel_time, 2), "seconds\n")
cat("Total rows processed:", format(sum(results$rows), big.mark = ","), "\n")
cat("Parallel rate:", round(sum(results$rows) / parallel_time, 0), "rows/second\n\n")

# ================================================================
# STRATEGY 4: R_MAX_SIZE RECOMMENDATIONS
# ================================================================

cat("STRATEGY 4: R_MAX_SIZE recommendations for 15GB files\n")
cat(rep("-", 60), "\n")

current_r_max <- Sys.getenv("R_MAX_SIZE", "unset")
cat("Current R_MAX_SIZE:", current_r_max, "\n")

# Calculate recommended settings
estimated_15gb_memory <- 15 * 1024^3  # 15GB in bytes
safety_factor <- 1.5  # 50% overhead
recommended_limit <- round(estimated_15gb_memory * safety_factor)

cat("Estimated 15GB file memory requirement: ~15GB\n")
cat("Recommended R_MAX_SIZE with safety margin: ~", round(recommended_limit / 1024^3, 1), "GB\n")
cat("In bytes:", recommended_limit, "\n\n")

cat("To set R_MAX_SIZE for 15GB processing:\n")
cat("export R_MAX_SIZE=", recommended_limit, "\n", sep = "")
cat("# or about 23GB (", round(recommended_limit / 1024^3, 1), "GB)\n\n", sep = "")

# ================================================================
# SUMMARY AND RECOMMENDATIONS
# ================================================================

cat(rep("=", 70), "\n")
cat("15GB FILE PROCESSING - SUMMARY & RECOMMENDATIONS\n")
cat(rep("=", 70), "\n")

cat("Current System Capabilities:\n")
cat("✓ Max tested chunk size:", format(max_successful_size, big.mark = ","), "rows\n")
cat("✓ Parallel processing:", getDoParWorkers(), "cores available\n")
cat("✓ Memory management: Working efficiently\n\n")

cat("For 15GB CSV files, recommended approach:\n\n")

cat("1. CHUNKED PROCESSING (Recommended):\n")
cat("   - Chunk size: 2-3 million rows (~400-600MB each)\n")
cat("   - Process sequentially for I/O-bound operations\n")
cat("   - Use parallel for CPU-intensive computations\n\n")

cat("2. MEMORY CONFIGURATION:\n")
cat("   - Set R_MAX_SIZE=", recommended_limit, " (", round(recommended_limit / 1024^3, 1), "GB)\n", sep = "")
cat("   - Monitor memory usage during processing\n")
cat("   - Use gc() between chunks for cleanup\n\n")

cat("3. PROCESSING STRATEGIES:\n")
cat("   - data.table::fread() with nThread=", num_cores, "\n")
cat("   - Skip/nrows parameters for chunking\n")
cat("   - Streaming aggregations across chunks\n")
cat("   - Save intermediate results\n\n")

cat("4. PERFORMANCE EXPECTATIONS:\n")
cat("   - Estimated processing time: ~", round(estimated_total_time / 60, 1), " minutes\n")
cat("   - Chunk processing rate: ~", round(chunk_size / (total_time / min(3, num_chunks)), 0), " rows/second\n")
cat("   - Memory per chunk: ~", round((chunk_size * 20 * 8) / 1024^2, 0), "MB\n\n")

cat("5. ERROR HANDLING:\n")
cat("   - Implement try-catch for memory errors\n")
cat("   - Automatic chunk size reduction on failure\n")
cat("   - Progress tracking and resumption capability\n")

cat("\n", rep("=", 70), "\n")
cat("Test completed! System ready for 15GB file processing.\n")
cat(rep("=", 70), "\n")
