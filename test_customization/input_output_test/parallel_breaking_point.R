#!/usr/bin/env Rscript
# ================================================================
# PARALLEL READING BREAKING POINT TEST (No Chunking)
# ================================================================
# Purpose: Find memory breaking point using direct parallel reading
# Strategy: Generate + read increasingly larger datasets until failure
# For RStudio viewing and analysis
# ================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(parallel)
})

# Configuration
NUM_CORES <- detectCores()
START_ROWS <- 1000000    # 1M rows
MAX_ROWS <- 100000000    # 100M rows max
MULTIPLIER <- 1.8        # Increase by 80% each test
NUM_COLS <- 20

# File paths
TEST_DIR <- "/home/dev1"
DATA_DIR <- file.path(TEST_DIR, "test_data")
if (!dir.exists(DATA_DIR)) dir.create(DATA_DIR, recursive = TRUE)

# Results storage for RStudio
breaking_point_results <- list()

# Helper functions
format_bytes <- function(bytes) {
  if (bytes >= 1e9) paste0(round(bytes/1e9, 2), " GB")
  else if (bytes >= 1e6) paste0(round(bytes/1e6, 2), " MB")
  else paste0(round(bytes/1e3, 2), " KB")
}

get_memory_info <- function() {
  gc_info <- gc(verbose = FALSE)
  list(used_mb = sum(gc_info[, "used"]))
}

print_test_header <- function(test_num, rows) {
  cat("\n", rep("=", 70), "\n")
  cat("TEST", test_num, "- TESTING", format(rows, big.mark = ","), "ROWS\n")
  cat(rep("=", 70), "\n")
}

# Main test function
test_dataset_size <- function(num_rows, test_number) {
  print_test_header(test_number, num_rows)
  
  csv_file <- file.path(DATA_DIR, paste0("breaking_test_", test_number, ".csv"))
  
  # System info
  cat("System Info:\n")
  cat("  Cores:", NUM_CORES, "\n")
  cat("  R_MAX_SIZE:", Sys.getenv("R_MAX_SIZE", "unset"), "\n")
  
  mem_start <- get_memory_info()
  cat("  Start Memory:", format_bytes(mem_start$used_mb * 1024^2), "\n\n")
  
  result <- tryCatch({
    cat("⏳ Step 1: Generating", format(num_rows, big.mark = ","), "rows...\n")
    
    # Generate large dataset
    gen_start <- Sys.time()
    
    large_data <- data.table(
      id = 1:num_rows,
      timestamp = as.POSIXct("2025-01-01") + sample(0:31536000, num_rows, replace = TRUE),
      category = sample(LETTERS[1:25], num_rows, replace = TRUE),
      value1 = rnorm(num_rows, 100, 25),
      value2 = runif(num_rows, 0, 1000),
      value3 = rpois(num_rows, 15),
      text_col = paste0("data_", sample(1:100000, num_rows, replace = TRUE)),
      flag1 = sample(c(TRUE, FALSE), num_rows, replace = TRUE),
      flag2 = sample(c(TRUE, FALSE), num_rows, replace = TRUE),
      score1 = rnorm(num_rows, 50, 15),
      score2 = rnorm(num_rows, 75, 20),
      group_id = sample(1:10000, num_rows, replace = TRUE),
      amount = runif(num_rows, 1, 100000),
      percentage = runif(num_rows, 0, 100),
      count_val = sample(1:5000, num_rows, replace = TRUE),
      rate = runif(num_rows, 0.1, 50.0),
      index_num = sample(1:500000, num_rows, replace = TRUE),
      status = sample(c("active", "inactive", "pending", "archived", "deleted"), num_rows, replace = TRUE),
      priority = sample(c("low", "medium", "high", "urgent", "critical", "emergency"), num_rows, replace = TRUE),
      region = sample(c("North", "South", "East", "West", "Central", "Northeast", "Northwest", "Southeast", "Southwest"), num_rows, replace = TRUE)
    )
    
    gen_time <- as.numeric(Sys.time() - gen_start, units = "secs")
    object_size <- as.numeric(object.size(large_data))
    
    mem_after_gen <- get_memory_info()
    cat("✅ Generation SUCCESS!\n")
    cat("  Time:", round(gen_time, 2), "seconds\n")
    cat("  Object size:", format_bytes(object_size), "\n")
    cat("  Memory after:", format_bytes(mem_after_gen$used_mb * 1024^2), "\n\n")
    
    # Write to CSV
    cat("⏳ Step 2: Writing CSV with", NUM_CORES, "cores...\n")
    write_start <- Sys.time()
    fwrite(large_data, csv_file, nThread = NUM_CORES)
    write_time <- as.numeric(Sys.time() - write_start, units = "secs")
    
    file_size <- file.info(csv_file)$size
    cat("✅ Write SUCCESS!\n")
    cat("  Time:", round(write_time, 2), "seconds\n")
    cat("  File size:", format_bytes(file_size), "\n\n")
    
    # Clear from memory before reading test
    rm(large_data)
    gc(verbose = FALSE)
    
    # Parallel reading test
    cat("⏳ Step 3: PARALLEL READING TEST with", NUM_CORES, "cores...\n")
    mem_before_read <- get_memory_info()
    
    read_start <- Sys.time()
    
    # THE MAIN TEST: Parallel reading
    loaded_data <- fread(csv_file, nThread = NUM_CORES, verbose = TRUE)
    
    read_time <- as.numeric(Sys.time() - read_start, units = "secs")
    
    mem_after_read <- get_memory_info()
    memory_for_object <- mem_after_read$used_mb - mem_before_read$used_mb
    
    cat("🎯 PARALLEL READ SUCCESS!\n")
    cat("  Read time:", round(read_time, 2), "seconds\n")
    cat("  Read rate:", round(nrow(loaded_data) / read_time, 0), "rows/second\n")
    cat("  Loaded rows:", format(nrow(loaded_data), big.mark = ","), "\n")
    cat("  Loaded cols:", ncol(loaded_data), "\n")
    cat("  Memory for object:", format_bytes(memory_for_object * 1024^2), "\n\n")
    
    # Quick operations test
    cat("⏳ Step 4: Testing operations on loaded data...\n")
    
    # Aggregation test
    agg_start <- Sys.time()
    agg_result <- loaded_data[, .(
      count = .N, 
      avg_val1 = mean(value1), 
      sum_amount = sum(amount)
    ), by = category]
    agg_time <- as.numeric(Sys.time() - agg_start, units = "secs")
    
    # Filter test  
    filter_start <- Sys.time()
    filtered <- loaded_data[value1 > 100 & status == "active"]
    filter_time <- as.numeric(Sys.time() - filter_start, units = "secs")
    
    cat("✅ Operations SUCCESS!\n")
    cat("  Aggregation:", round(agg_time, 3), "sec (", nrow(agg_result), "groups)\n")
    cat("  Filtering:", round(filter_time, 3), "sec (", format(nrow(filtered), big.mark = ","), "rows)\n\n")
    
    # Final memory check
    mem_final <- get_memory_info()
    total_memory_used <- mem_final$used_mb - mem_start$used_mb
    
    cat("🏆 COMPLETE SUCCESS - ALL STEPS PASSED!\n")
    cat("  Total memory used:", format_bytes(total_memory_used * 1024^2), "\n")
    
    # Cleanup
    rm(loaded_data, agg_result, filtered)
    gc(verbose = FALSE)
    file.remove(csv_file)
    
    # Return success result
    list(
      test_number = test_number,
      num_rows = num_rows,
      success = TRUE,
      file_size = file_size,
      object_size = object_size,
      generation_time = gen_time,
      write_time = write_time,
      read_time = read_time,
      read_rate = nrow(loaded_data) / read_time,
      memory_used_mb = total_memory_used,
      agg_time = agg_time,
      filter_time = filter_time,
      categories = nrow(agg_result),
      filtered_rows = nrow(filtered)
    )
    
  }, error = function(e) {
    cat("\n❌ BREAKING POINT REACHED!\n")
    cat("💥 ERROR:", e$message, "\n")
    
    mem_error <- get_memory_info()
    cat("Memory at error:", format_bytes(mem_error$used_mb * 1024^2), "\n")
    
    # Cleanup on error
    if (exists("large_data")) try(rm(large_data), silent = TRUE)
    if (exists("loaded_data")) try(rm(loaded_data), silent = TRUE)
    gc(verbose = FALSE)
    if (file.exists(csv_file)) try(file.remove(csv_file), silent = TRUE)
    
    list(
      test_number = test_number,
      num_rows = num_rows,
      success = FALSE,
      error = e$message,
      breaking_point = TRUE
    )
  })
  
  return(result)
}

# Main execution
main_breaking_point_test <- function() {
  cat("\n", rep("=", 80), "\n")
  cat("🔥 PARALLEL READING BREAKING POINT TEST 🔥\n")
  cat(rep("=", 80), "\n")
  cat("Strategy: Direct parallel loading without chunking\n")
  cat("Goal: Find exact memory breaking point\n")
  cat("Cores:", NUM_CORES, "\n")
  cat("Starting size:", format(START_ROWS, big.mark = ","), "rows\n")
  cat("Max size:", format(MAX_ROWS, big.mark = ","), "rows\n")
  cat(rep("=", 80), "\n")
  
  # Test progressively larger sizes
  current_rows <- START_ROWS
  test_num <- 1
  
  while (current_rows <= MAX_ROWS) {
    cat("\n🚀 Starting test", test_num, "with", format(current_rows, big.mark = ","), "rows...")
    
    result <- test_dataset_size(current_rows, test_num)
    breaking_point_results[[test_num]] <- result
    
    if (!result$success) {
      cat("\n🛑 BREAKING POINT FOUND!\n")
      cat("Failed at:", format(current_rows, big.mark = ","), "rows\n")
      cat("Error:", result$error, "\n")
      break
    }
    
    cat("\n✅ Test", test_num, "PASSED!\n")
    
    # Calculate next size
    current_rows <- round(current_rows * MULTIPLIER)
    test_num <- test_num + 1
    
    if (test_num > 15) {  # Safety limit
      cat("\nReached safety limit (15 tests)\n")
      break
    }
    
    cat("Next test:", format(current_rows, big.mark = ","), "rows\n")
    Sys.sleep(1)  # Brief pause
  }
  
  # Results summary
  cat("\n", rep("=", 80), "\n")
  cat("🎯 BREAKING POINT TEST RESULTS SUMMARY\n")
  cat(rep("=", 80), "\n")
  
  successful <- Filter(function(x) x$success, breaking_point_results)
  failed <- Filter(function(x) !x$success, breaking_point_results)
  
  if (length(successful) > 0) {
    cat("\n✅ SUCCESSFUL TESTS:\n")
    cat(sprintf("%-4s %-12s %-10s %-8s %-10s %-12s\n", 
                "Test", "Rows", "File Size", "Read(s)", "Rate", "Memory"))
    cat(rep("-", 65), "\n")
    
    for (i in seq_along(successful)) {
      s <- successful[[i]]
      cat(sprintf("%-4d %-12s %-10s %-8.1f %-10.0f %-12s\n",
                  s$test_number,
                  format(s$num_rows, big.mark = ","),
                  format_bytes(s$file_size),
                  s$read_time,
                  s$read_rate,
                  format_bytes(s$memory_used_mb * 1024^2)))
    }
    
    # Maximum successful
    max_success <- successful[[which.max(sapply(successful, function(x) x$num_rows))]]
    cat("\n🏆 MAXIMUM SUCCESSFUL DATASET:\n")
    cat("  Rows:", format(max_success$num_rows, big.mark = ","), "\n")
    cat("  File Size:", format_bytes(max_success$file_size), "\n")
    cat("  Read Time:", round(max_success$read_time, 2), "seconds\n")
    cat("  Memory Used:", format_bytes(max_success$memory_used_mb * 1024^2), "\n")
    cat("  Read Rate:", round(max_success$read_rate, 0), "rows/second\n")
  }
  
  if (length(failed) > 0) {
    breaking_test <- failed[[1]]
    cat("\n💥 BREAKING POINT:\n")
    cat("  Failed at:", format(breaking_test$num_rows, big.mark = ","), "rows\n")
    cat("  Error:", breaking_test$error, "\n")
    
    if (length(successful) > 0) {
      last_good <- max(sapply(successful, function(x) x$num_rows))
      safety_margin <- breaking_test$num_rows - last_good
      cat("  Last successful:", format(last_good, big.mark = ","), "rows\n")
      cat("  Safety margin:", format(safety_margin, big.mark = ","), "rows\n")
    }
  } else {
    cat("\n🟢 NO BREAKING POINT FOUND in tested range!\n")
  }
  
  # R_MAX_SIZE info
  r_max <- Sys.getenv("R_MAX_SIZE", "unset")
  cat("\nR_MAX_SIZE Configuration:\n")
  cat("  Current:", r_max, "\n")
  
  if (length(successful) > 0) {
    max_mem <- max(sapply(successful, function(x) x$memory_used_mb * 1024^2))
    recommended <- round(max_mem * 2)  # 2x safety factor
    cat("  Max memory used:", format_bytes(max_mem), "\n")
    cat("  Recommended R_MAX_SIZE:", format_bytes(recommended), "\n")
    cat("  Set with: export R_MAX_SIZE=", recommended, "\n", sep = "")
  }
  
  cat("\n", rep("=", 80), "\n")
  cat("📊 RESULTS STORED IN: breaking_point_results\n")
  cat("💡 View in RStudio: View(breaking_point_results)\n")
  cat(rep("=", 80), "\n")
  
  return(breaking_point_results)
}

# Execute if script, provide instructions if interactive
if (!interactive()) {
  breaking_point_results <- main_breaking_point_test()
} else {
  cat("\n🎯 BREAKING POINT TEST READY!\n")
  cat("Run in RStudio: breaking_point_results <- main_breaking_point_test()\n")
  cat("Or run individual test: result <- test_dataset_size(5000000, 1)\n")
}
