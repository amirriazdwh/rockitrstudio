# ================================================================
# BREAKING POINT TEST FOR RSTUDIO - 15GB FILE CHALLENGE
# ================================================================
# Purpose: Find memory breaking point using parallel data.table reading
# Usage: Run this directly in RStudio console
# Strategy: No chunking - direct parallel loading until failure
# ================================================================

library(data.table)
library(parallel)

# Configuration
NUM_CORES <- detectCores()
cat("🔥 PARALLEL READING BREAKING POINT TEST\n")
cat("Available cores:", NUM_CORES, "\n")
cat("R_MAX_SIZE:", Sys.getenv("R_MAX_SIZE", "unset"), "\n\n")

# Helper function
format_bytes <- function(bytes) {
  if (bytes >= 1e9) paste0(round(bytes/1e9, 2), " GB")
  else if (bytes >= 1e6) paste0(round(bytes/1e6, 2), " MB")
  else paste0(round(bytes/1e3, 2), " KB")
}

get_memory <- function() {
  gc_info <- gc(verbose = FALSE)
  sum(gc_info[, "used"])
}

# Test function for different sizes
test_size <- function(num_rows) {
  cat("Testing", format(num_rows, big.mark = ","), "rows...\n")
  
  mem_start <- get_memory()
  csv_file <- paste0("/home/cdsw/test_", num_rows, ".csv")
  
  tryCatch({
    # Step 1: Generate data
    cat("  Generating data...")
    start_time <- Sys.time()
    
    test_data <- data.table(
      id = 1:num_rows,
      timestamp = as.POSIXct("2025-01-01") + sample(0:31536000, num_rows, replace = TRUE),
      category = sample(LETTERS[1:20], num_rows, replace = TRUE),
      value1 = rnorm(num_rows, 100, 25),
      value2 = runif(num_rows, 0, 1000),
      value3 = rpois(num_rows, 15),
      text_col = paste0("data_", sample(1:50000, num_rows, replace = TRUE)),
      flag1 = sample(c(TRUE, FALSE), num_rows, replace = TRUE),
      flag2 = sample(c(TRUE, FALSE), num_rows, replace = TRUE),
      score1 = rnorm(num_rows, 50, 15),
      score2 = rnorm(num_rows, 75, 20),
      group_id = sample(1:5000, num_rows, replace = TRUE),
      amount = runif(num_rows, 1, 50000),
      percentage = runif(num_rows, 0, 100),
      count_val = sample(1:1000, num_rows, replace = TRUE),
      rate = runif(num_rows, 0.1, 10.0),
      index_num = sample(1:100000, num_rows, replace = TRUE),
      status = sample(c("active", "inactive", "pending", "archived"), num_rows, replace = TRUE),
      priority = sample(c("low", "medium", "high", "urgent", "critical"), num_rows, replace = TRUE),
      region = sample(c("North", "South", "East", "West", "Central"), num_rows, replace = TRUE)
    )
    
    gen_time <- as.numeric(Sys.time() - start_time, units = "secs")
    object_size <- as.numeric(object.size(test_data))
    cat(" OK (", round(gen_time, 2), "s,", format_bytes(object_size), ")\n")
    
    # Step 2: Write CSV
    cat("  Writing CSV...")
    fwrite(test_data, csv_file, nThread = NUM_CORES)
    file_size <- file.info(csv_file)$size
    cat(" OK (", format_bytes(file_size), ")\n")
    
    # Clear memory
    rm(test_data)
    gc(verbose = FALSE)
    
    # Step 3: PARALLEL READ TEST
    cat("  Parallel reading with", NUM_CORES, "cores...")
    read_start <- Sys.time()
    
    # THE MAIN TEST
    loaded_dt <- fread(csv_file, nThread = NUM_CORES)
    
    read_time <- as.numeric(Sys.time() - read_start, units = "secs")
    read_rate <- nrow(loaded_dt) / read_time
    
    cat(" SUCCESS!\n")
    cat("    Read time:", round(read_time, 2), "seconds\n")
    cat("    Read rate:", round(read_rate, 0), "rows/second\n")
    cat("    Loaded:", format(nrow(loaded_dt), big.mark = ","), "rows x", ncol(loaded_dt), "cols\n")
    
    # Quick operation test
    cat("  Testing operations...")
    agg_result <- loaded_dt[, .(count = .N, avg_val = mean(value1)), by = category]
    filter_result <- loaded_dt[value1 > 100 & status == "active"]
    cat(" OK (", nrow(agg_result), "groups,", format(nrow(filter_result), big.mark = ","), "filtered)\n")
    
    # Memory summary
    mem_end <- get_memory()
    memory_used <- mem_end - mem_start
    
    cat("  Memory used:", format_bytes(memory_used * 1024^2), "\n")
    cat("  ✅ SUCCESS!\n\n")
    
    # Cleanup
    rm(loaded_dt, agg_result, filter_result)
    gc(verbose = FALSE)
    file.remove(csv_file)
    
    return(list(
      num_rows = num_rows,
      success = TRUE,
      file_size = file_size,
      object_size = object_size,
      read_time = read_time,
      read_rate = read_rate,
      memory_used_mb = memory_used
    ))
    
  }, error = function(e) {
    cat("\n  ❌ FAILED:", e$message, "\n")
    
    # Cleanup on error
    if (exists("test_data")) try(rm(test_data), silent = TRUE)
    if (exists("loaded_dt")) try(rm(loaded_dt), silent = TRUE)
    gc(verbose = FALSE)
    if (file.exists(csv_file)) try(file.remove(csv_file), silent = TRUE)
    
    return(list(
      num_rows = num_rows,
      success = FALSE,
      error = e$message
    ))
  })
}

# Run progressive tests
cat("🚀 Starting breaking point tests...\n\n")

# Test sizes: 1M, 2M, 3M, 5M, 8M, 12M, 18M, 27M, 40M rows
test_sizes <- c(1000000, 2000000, 3000000, 5000000, 8000000, 12000000, 18000000, 27000000, 40000000)

results <- list()

for (i in seq_along(test_sizes)) {
  size <- test_sizes[i]
  cat("TEST", i, "/", length(test_sizes), ":\n")
  
  result <- test_size(size)
  results[[i]] <- result
  
  if (!result$success) {
    cat("🛑 BREAKING POINT FOUND!\n")
    break
  }
}

# Results summary
cat("\n", rep("=", 70), "\n")
cat("🎯 BREAKING POINT TEST RESULTS\n")
cat(rep("=", 70), "\n")

successful <- Filter(function(x) x$success, results)
failed <- Filter(function(x) !x$success, results)

if (length(successful) > 0) {
  cat("\n✅ SUCCESSFUL TESTS:\n")
  cat(sprintf("%-12s %-10s %-8s %-12s %-10s\n", 
              "Rows", "File Size", "Read(s)", "Rate", "Memory"))
  cat(rep("-", 55), "\n")
  
  for (s in successful) {
    cat(sprintf("%-12s %-10s %-8.1f %-12.0f %-10s\n",
                format(s$num_rows, big.mark = ","),
                format_bytes(s$file_size),
                s$read_time,
                s$read_rate,
                format_bytes(s$memory_used_mb * 1024^2)))
  }
  
  # Maximum successful
  max_rows <- max(sapply(successful, function(x) x$num_rows))
  max_result <- successful[[which.max(sapply(successful, function(x) x$num_rows))]]
  
  cat("\n🏆 MAXIMUM SUCCESSFUL:\n")
  cat("  Rows:", format(max_rows, big.mark = ","), "\n")
  cat("  File size:", format_bytes(max_result$file_size), "\n") 
  cat("  Read time:", round(max_result$read_time, 2), "seconds\n")
  cat("  Memory used:", format_bytes(max_result$memory_used_mb * 1024^2), "\n")
  
  # Estimate for 15GB
  estimated_15gb_rows <- round(15 * 1024^3 / (max_result$file_size / max_result$num_rows))
  cat("\n📊 15GB FILE ESTIMATE:\n")
  cat("  Estimated rows for 15GB:", format(estimated_15gb_rows, big.mark = ","), "\n")
  
  if (max_rows >= estimated_15gb_rows) {
    cat("  ✅ System CAN handle 15GB files!\n")
  } else {
    ratio_needed <- estimated_15gb_rows / max_rows
    cat("  ⚠️  15GB file is", round(ratio_needed, 1), "x larger than max successful\n")
    cat("  Recommendation: Use chunked processing for 15GB files\n")
  }
}

if (length(failed) > 0) {
  breaking_point <- failed[[1]]$num_rows
  cat("\n💥 BREAKING POINT:\n")
  cat("  Failed at:", format(breaking_point, big.mark = ","), "rows\n")
  cat("  Error:", failed[[1]]$error, "\n")
}

# R_MAX_SIZE recommendations
cat("\nR_MAX_SIZE CONFIGURATION:\n")
cat("  Current:", Sys.getenv("R_MAX_SIZE", "unset"), "\n")

if (length(successful) > 0) {
  max_memory <- max(sapply(successful, function(x) x$memory_used_mb * 1024^2))
  recommended <- round(max_memory * 2)  # 2x safety factor
  cat("  Max memory used:", format_bytes(max_memory), "\n")
  cat("  Recommended for 15GB:", format_bytes(recommended), "\n")
  cat("  Set with: export R_MAX_SIZE=", recommended, "\n", sep = "")
}

cat("\n", rep("=", 70), "\n")
cat("Test completed! Results stored in 'results' variable.\n")
cat("Copy this script to RStudio to see detailed results.\n")
cat(rep("=", 70), "\n")
