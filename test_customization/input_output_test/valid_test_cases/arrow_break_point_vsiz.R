#!/usr/bin/env Rscript
# =============================================================================
# ARROW + PARQUET BREAKING POINT TEST
# =============================================================================
# Purpose:
#   • Stress-test memory limits when working with Arrow (Parquet) vs R heap.
#   • Show how R_MAX_VSIZE (R heap memory) affects collect() into R,
#     while Arrow scan/compute operations remain off-heap (C++).
#
# Test Strategy:
#   1. Generate synthetic data.table with configurable row counts.
#   2. Write dataset to Parquet using Arrow (compressed, columnar, off-heap).
#   3. Read dataset back with Arrow as a Table (off-heap).
#   4. Run group-by/aggregation + filtering operations in Arrow space.
#      - These stay in Arrow memory, not R heap → not limited by R_MAX_VSIZE.
#   5. (Optional) collect() results into R data.frame to stress R heap and
#      observe R_MAX_VSIZE breaking point.
#
# Measurements:
#   • R heap usage (MB) via gc()
#   • Arrow off-heap usage (bytes) via default_memory_pool()
#   • Execution time: generation, write, scan/compute, collect
#   • File size on disk (Parquet)
#
# Breaking Point:
#   • Successive tests grow dataset size (×1.8 multiplier each step).
#   • Script stops at first OOM / error = “breaking point”.
#   • Reports last successful size and memory usage.
#
# Usage:
#   Run non-interactively:
#       Rscript arrow_parquet_breaking_point_test.R
#
#   Run in RStudio:
#       arrow_breaking_point_results <- main_arrow_breaking_point_test()
#       View(arrow_breaking_point_results)
#
# Notes:
#   • Compare against CSV/data.table version to see difference in memory
#     behavior (heap vs off-heap).
#   • Use DO_COLLECT=FALSE to test pure Arrow pipeline with no heap materialization.
# =============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
  library(dplyr)
  library(parallel)
})

# --------------------
# Config
# --------------------
NUM_CORES   <- detectCores()
START_ROWS  <- 1e6
MAX_ROWS    <- 1e8
MULTIPLIER  <- 1.8
DO_COLLECT  <- TRUE

TEST_DIR <- "/home/dev1"
DATA_DIR <- file.path(TEST_DIR, "arrow_parquet_test")
if (!dir.exists(DATA_DIR)) dir.create(DATA_DIR, recursive = TRUE)

options(arrow.use_threads = TRUE)
if (is.function(get("set_cpu_count", asNamespace("arrow"), inherits = FALSE))) {
  arrow::set_cpu_count(NUM_CORES)
}
if (is.function(get("set_io_thread_count", asNamespace("arrow"), inherits = FALSE))) {
  arrow::set_io_thread_count(max(2, floor(NUM_CORES/2)))
}

# --------------------
# Helpers
# --------------------
fmt_bytes <- function(bytes) {
  if (is.na(bytes) || is.null(bytes)) return("NA")
  if (bytes >= 1024^3) paste0(round(bytes/1024^3, 2), " GB")
  else if (bytes >= 1024^2) paste0(round(bytes/1024^2, 2), " MB")
  else if (bytes >= 1024) paste0(round(bytes/1024, 2), " KB")
  else paste0(bytes, " B")
}

r_heap_used_mb <- function() {
  gcinfo <- gc(FALSE)
  sum(gcinfo[, "used"])
}

# Version-safe Arrow off-heap bytes probe
get_arrow_offheap_bytes <- function() {
  # Try method on memory pool (newer Arrow)
  val <- tryCatch({
    mp <- arrow::default_memory_pool()
    if (is.function(mp$bytes_allocated)) {
      as.numeric(mp$bytes_allocated())
    } else if (is.function(get("bytes_allocated", asNamespace("arrow"), inherits = TRUE))) {
      # Older Arrow may have a free function
      as.numeric(arrow::bytes_allocated(mp))
    } else {
      NA_real_
    }
  }, error = function(e) NA_real_)
  if (is.na(val)) 0 else val
}

safe_cpu_count <- function() {
  if (is.function(get("cpu_count", asNamespace("arrow"), inherits = FALSE))) {
    return(arrow::cpu_count())
  }
  detectCores()
}

print_hdr <- function(test_num, rows) {
  cat("\n", strrep("=", 72), "\n", sep = "")
  cat("ARROW TEST", test_num, "- TESTING", format(rows, big.mark=","), "ROWS\n")
  cat(strrep("=", 72), "\n")
}

# --------------------
# One test
# --------------------
test_arrow_size <- function(num_rows, test_number) {
  print_hdr(test_number, num_rows)
  pq <- file.path(DATA_DIR, sprintf("arrow_break_test_%02d.parquet", test_number))

  cat("System Info:\n")
  cat("  Cores:         ", NUM_CORES, "\n")
  cat("  R_MAX_VSIZE:   ", Sys.getenv("R_MAX_VSIZE", "unset"), "\n")
  cat("  R_MAX_SIZE:    ", Sys.getenv("R_MAX_SIZE",  "unset"), "\n")
  cat("  Arrow version: ", as.character(arrow::arrow_info()$version), "\n", sep = "")
  cat("  Arrow threads: ", safe_cpu_count(), "\n\n", sep = "")

  r_start_mb <- r_heap_used_mb()
  a_start    <- get_arrow_offheap_bytes()
  cat("  Start R heap:  ", fmt_bytes(r_start_mb * 1024^2), "\n")
  cat("  Start Arrow:   ", fmt_bytes(a_start), "\n\n")

  result <- tryCatch({
    # Generate on R heap
    cat("• Step 1: Generating ", format(num_rows, big.mark=","), " rows...\n", sep = "")
    t0 <- Sys.time()
    DT <- data.table(
      id         = 1:num_rows,
      timestamp  = as.POSIXct("2025-01-01", tz="UTC") + sample(0:31536000, num_rows, TRUE),
      category   = sample(LETTERS[1:25], num_rows, TRUE),
      value1     = rnorm(num_rows, 100, 25),
      value2     = runif(num_rows, 0, 1000),
      value3     = rpois(num_rows, 15),
      text_col   = paste0("data_", sample(1:100000, num_rows, TRUE)),
      flag1      = sample(c(TRUE, FALSE), num_rows, TRUE),
      flag2      = sample(c(TRUE, FALSE), num_rows, TRUE),
      score1     = rnorm(num_rows, 50, 15),
      score2     = rnorm(num_rows, 75, 20),
      group_id   = sample(1:10000, num_rows, TRUE),
      amount     = runif(num_rows, 1, 100000),
      percentage = runif(num_rows, 0, 100),
      count_val  = sample(1:5000, num_rows, TRUE),
      rate       = runif(num_rows, 0.1, 50.0),
      index_num  = sample(1:500000, num_rows, TRUE),
      status     = sample(c("active","inactive","pending","archived","deleted"), num_rows, TRUE),
      priority   = sample(c("low","medium","high","urgent","critical","emergency"), num_rows, TRUE),
      region     = sample(c("North","South","East","West","Central","Northeast","Northwest","Southeast","Southwest"), num_rows, TRUE)
    )
    gen_sec <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
    cat("  Generation OK in ", round(gen_sec,2), " s; object size ", fmt_bytes(as.numeric(object.size(DT))), "\n\n", sep = "")

    # Write Parquet (off-heap writer)
    cat("• Step 2: Writing Parquet (zstd; dict on)...\n")
    t1 <- Sys.time()
    # as_arrow_table() (>=10); fall back to arrow_table() if missing
    tbl_fun <- if (is.function(get("as_arrow_table", asNamespace("arrow"), inherits = TRUE)))
      arrow::as_arrow_table else arrow::arrow_table
    write_parquet(
      tbl_fun(DT),
      pq,
      compression = "zstd",
      compression_level = 3,
      use_dictionary = TRUE,
      write_statistics = TRUE
    )
    write_sec <- as.numeric(difftime(Sys.time(), t1, units="secs"))
    fsz <- file.info(pq)$size
    cat("  Write OK in ", round(write_sec,2), " s; file size ", fmt_bytes(fsz), "\n\n", sep = "")

    # Free R heap before read
    rm(DT); invisible(gc())

    # Off-heap scan/compute
    cat("• Step 3: Arrow SCAN/COMPUTE (off-heap, no collect)...\n")
    r_before_mb <- r_heap_used_mb()
    a_before    <- get_arrow_offheap_bytes()
    t2 <- Sys.time()

    arr_tbl <- read_parquet(pq, as_data_frame = FALSE)

    agg_tbl <- arr_tbl |>
      tbl_fun() |>
      dplyr::group_by(category) |>
      dplyr::summarise(
        n           = dplyr::n(),
        avg_value1  = mean(value1, na.rm = TRUE),
        sum_amount  = sum(amount, na.rm = TRUE),
        .groups = "drop"
      ) |>
      compute()

    scan_sec <- as.numeric(difftime(Sys.time(), t2, units="secs"))
    r_after_mb <- r_heap_used_mb()
    a_after    <- get_arrow_offheap_bytes()

    cat("  Scan/compute OK in ", round(scan_sec,2), " s\n", sep = "")
    cat("  R heap Δ (scan):    ", fmt_bytes((r_after_mb - r_before_mb) * 1024^2), "\n", sep = "")
    cat("  Arrow off-heap Δ:   ", fmt_bytes(a_after - a_before), "\n\n", sep = "")

    # Optional collect() to hit R_MAX_VSIZE
    collected_df <- NULL
    collect_sec <- NA
    r_before_collect_mb <- r_after_mb
    if (DO_COLLECT) {
      cat("• Step 4: collect() into R (stress R heap)...\n")
      t3 <- Sys.time()
      collected_df <- collect(agg_tbl)
      collect_sec <- as.numeric(difftime(Sys.time(), t3, units="secs"))
      r_after_collect_mb <- r_heap_used_mb()
      cat("  collect() OK in ", round(collect_sec,2), " s; rows=", nrow(collected_df), "\n", sep = "")
      cat("  R heap Δ (collect): ", fmt_bytes((r_after_collect_mb - r_before_collect_mb) * 1024^2), "\n\n", sep = "")
    }

    # Extra off-heap filter
    cat("• Step 5: Extra compute (filter) off-heap...\n")
    t4 <- Sys.time()
    filtered_tbl <- arr_tbl |>
      tbl_fun() |>
      filter(value1 > 100, status == "active") |>
      select(id, value1, status, amount) |>
      compute()
    filt_sec <- as.numeric(difftime(Sys.time(), t4, units="secs"))
    cat("  Filter OK in ", round(filt_sec,3), " s (off-heap)\n\n", sep = "")

    r_end_mb <- r_heap_used_mb()
    a_end    <- get_arrow_offheap_bytes()

    # Cleanup
    rm(arr_tbl, agg_tbl, filtered_tbl, collected_df)
    invisible(gc())
    file.remove(pq)

    list(
      test_number        = test_number,
      num_rows           = num_rows,
      success            = TRUE,
      parquet_size       = fsz,
      gen_time           = gen_sec,
      write_time         = write_sec,
      scan_time          = scan_sec,
      collect_time       = collect_sec,
      r_heap_start_mb    = r_start_mb,
      r_heap_end_mb      = r_end_mb,
      r_heap_delta_mb    = r_end_mb - r_start_mb,
      arrow_start_bytes  = a_start,
      arrow_end_bytes    = a_end,
      arrow_delta_bytes  = a_end - a_start
    )
  }, error = function(e) {
    cat("\n✖ BREAKING POINT REACHED (Arrow test)\n")
    cat("  ERROR: ", e$message, "\n", sep = "")
    cat("  R heap now: ", fmt_bytes(r_heap_used_mb() * 1024^2), "\n", sep = "")
    cat("  Arrow now:  ", fmt_bytes(get_arrow_offheap_bytes()), "\n", sep = "")

    # Cleanup
    try({
      rm(list = setdiff(ls(), c("NUM_CORES","START_ROWS","MAX_ROWS","MULTIPLIER",
                                "TEST_DIR","DATA_DIR","DO_COLLECT")))
      invisible(gc())
    }, silent = TRUE)
    if (file.exists(pq)) try(file.remove(pq), silent = TRUE)

    list(
      test_number    = test_number,
      num_rows       = num_rows,
      success        = FALSE,
      error          = e$message,
      breaking_point = TRUE
    )
  })

  result
}

# --------------------
# Main runner
# --------------------
main_arrow_breaking_point_test <- function() {
  cat("\n", strrep("=", 80), "\n", sep = "")
  cat("▶▶ ARROW + PARQUET BREAKING POINT TEST ◀◀\n")
  cat(strrep("=", 80), "\n", sep = "")
  cat("Goal: Contrast R_MAX_VSIZE (R heap) vs Arrow off-heap memory\n")
  cat("Cores: ", NUM_CORES, "\n", sep = "")
  cat("Start rows: ", format(START_ROWS, big.mark=","), "\n", sep = "")
  cat("Max rows:   ", format(MAX_ROWS,  big.mark=","), "\n", sep = "")
  cat("collect():  ", if (DO_COLLECT) "ENABLED (will stress R heap)" else "DISABLED (off-heap only)", "\n", sep = "")
  cat(strrep("=", 80), "\n\n", sep = "")

  results <- list()
  current_rows <- START_ROWS
  test_num <- 1

  while (current_rows <= MAX_ROWS) {
    cat("▶ Starting Arrow test ", test_num, " with ", format(current_rows, big.mark=","), " rows...\n", sep = "")
    res <- test_arrow_size(current_rows, test_num)
    results[[test_num]] <- res

    if (!res$success) {
      cat("\n✖ BREAKING POINT FOUND (Arrow)\n")
      cat("  Failed at: ", format(current_rows, big.mark=","), " rows\n", sep = "")
      cat("  Error: ", res$error, "\n", sep = "")
      break
    }

    cat("✔ Test ", test_num, " PASSED\n\n", sep = "")

    current_rows <- round(current_rows * MULTIPLIER)
    test_num <- test_num + 1
    if (test_num > 15) { cat("Reached safety cap (15 tests)\n"); break }
    Sys.sleep(1)
  }

  cat("\n", strrep("=", 80), "\n", sep = "")
  cat("▶▶ ARROW TEST RESULTS SUMMARY ◀◀\n")
  cat(strrep("=", 80), "\n", sep = "")

  ok  <- Filter(function(x) isTRUE(x$success), results)
  bad <- Filter(function(x) isFALSE(x$success), results)

  if (length(ok) > 0) {
    cat("\nSuccessful tests:\n")
    cat(sprintf("%-4s %-12s %-12s %-8s %-8s %-12s %-12s\n",
                "Test","Rows","Parquet","Scan(s)","Coll(s)","RheapΔ","ArrowΔ"))
    cat(strrep("-", 76), "\n")
    for (r in ok) {
      cat(sprintf("%-4d %-12s %-12s %-8.1f %-8s %-12s %-12s\n",
                  r$test_number,
                  format(r$num_rows, big.mark=","),
                  fmt_bytes(r$parquet_size),
                  r$scan_time,
                  ifelse(is.na(r$collect_time), "-", sprintf("%.1f", r$collect_time)),
                  fmt_bytes(r$r_heap_delta_mb * 1024^2),
                  fmt_bytes(r$arrow_delta_bytes)))
    }

    max_ok <- ok[[which.max(sapply(ok, function(x) x$num_rows))]]
    cat("\nMax successful dataset:\n")
    cat("  Rows:      ", format(max_ok$num_rows, big.mark=","), "\n", sep = "")
    cat("  Parquet:   ", fmt_bytes(max_ok$parquet_size), "\n", sep = "")
    cat("  Scan time: ", round(max_ok$scan_time,2), " s\n", sep = "")
    cat("  R heap Δ:  ", fmt_bytes(max_ok$r_heap_delta_mb * 1024^2), "\n", sep = "")
    cat("  Arrow Δ:   ", fmt_bytes(max_ok$arrow_delta_bytes), "\n", sep = "")
  }

  if (length(bad) > 0) {
    bp <- bad[[1]]
    cat("\nBreaking point:\n")
    cat("  Failed at: ", format(bp$num_rows, big.mark=","), " rows\n", sep = "")
    cat("  Error:     ", bp$error, "\n", sep = "")
    if (length(ok) > 0) {
      last_good <- max(sapply(ok, `[[`, "num_rows"))
      cat("  Last OK:   ", format(last_good, big.mark=","), " rows\n", sep = "")
      cat("  Margin:    ", format(bp$num_rows - last_good, big.mark=","), " rows\n", sep = "")
    }
  } else {
    cat("\nNo breaking point found in tested range.\n")
  }

  cat("\n", strrep("=", 80), "\n", sep = "")
  cat("Results in: arrow_breaking_point_results (list)\n")
  cat("View in RStudio: View(arrow_breaking_point_results)\n")
  cat(strrep("=", 80), "\n", sep = "")

  invisible(results)
}

if (!interactive()) {
  arrow_breaking_point_results <- main_arrow_breaking_point_test()
} else {
  cat("\nArrow test ready. Run:\n")
  cat("  arrow_breaking_point_results <- main_arrow_breaking_point_test()\n")
}
