#!/usr/bin/env Rscript
# =============================================================================
# ARROW OFF-HEAP PRESSURE TEST (System impact of Arrow allocations/mmap)
# =============================================================================
# Purpose:
#   Measure how Arrow's OFF-HEAP memory usage affects:
#     1) Process RSS (resident set size)
#     2) System MemAvailable
#   WITHOUT growing the R heap (so R_MAX_VSIZE shouldn't be the limiter).
#
# What it does:
#   • Iteratively generate data -> write Parquet
#   • Read via Arrow as Tables (off-heap) and run Arrow compute (off-heap)
#   • KEEP references to Arrow objects to intentionally hold off-heap memory
#   • Track per-iteration deltas: Arrow bytes, RSS, MemAvailable
#   • Optionally compare mmap on/off (mmap may not be counted by Arrow pool)
#
# Key knobs:
#   USE_MMAP:      TRUE uses memory-mapped IO (default in Arrow if supported)
#   TOUCH_DATA:    TRUE scans & computes to force real reads into memory
#   HOLD_OBJECTS:  TRUE keeps references to grow off-heap footprint across steps
#   RELEASE_EVERY: Release all held objects every N iterations to observe recovery
#
# Notes:
#   • Arrow pool counters (bytes_allocated) may be 0/low when using mmap.
#   • Even with mmap, TOUCH_DATA forces pages to be faulted in (RSS rises).
#   • System MemAvailable should drop as Arrow objects are retained/touched.
# =============================================================================
# Default (USE_MMAP=TRUE, TOUCH_DATA=TRUE, HOLD_OBJECTS=TRUE): shows how keeping Arrow Tables/compute results increases RSS and decreases MemAvailable even if Arrow’s bytes_allocated counter is small (because pages are mmapped).
# Flip USE_MMAP <- FALSE: on builds without mmap or when disabled, Arrow pool counters (ΔArrow) should grow alongside RSS.Set HOLD_OBJECTS <- FALSE: you’ll see memory return quickly after each iteration.
# Change RELEASE_EVERY: see recovery behavior after you drop references and run gc().


suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
  library(dplyr)
  library(parallel)
})

# --------------------
# Configuration
# --------------------
NUM_CORES    <- detectCores()
START_ROWS   <- 1e6         # 1M rows
MAX_ROWS     <- 1e8         # cap
MULTIPLIER   <- 1.8
USE_MMAP     <- TRUE        # Toggle memory-mapped IO
TOUCH_DATA   <- TRUE        # Force compute() to touch pages
HOLD_OBJECTS <- TRUE        # Keep references to grow off-heap pressure
RELEASE_EVERY <- 4          # Every N iterations, release all held objects
OUT_DIR      <- "/home/dev1/arrow_offheap_test"
if (!dir.exists(OUT_DIR)) dir.create(OUT_DIR, recursive = TRUE)

# Arrow settings (version-safe)
options(arrow.use_threads = TRUE, arrow.use_mmap = USE_MMAP)
if (is.function(get("set_cpu_count", asNamespace("arrow"), inherits = FALSE))) {
  arrow::set_cpu_count(NUM_CORES)
}
if (is.function(get("set_io_thread_count", asNamespace("arrow"), inherits = FALSE))) {
  arrow::set_io_thread_count(max(2, floor(NUM_CORES/2)))
}

# --------------------
# Helpers (safe across Arrow versions)
# --------------------
fmt_bytes <- function(b) {
  if (is.na(b) || is.null(b)) return("NA")
  if (b >= 1024^3) sprintf("%.2f GB", b/1024^3)
  else if (b >= 1024^2) sprintf("%.2f MB", b/1024^2)
  else if (b >= 1024) sprintf("%.2f KB", b/1024)
  else sprintf("%d B", as.integer(b))
}

# R heap probe (MB used per gc())
r_heap_used_mb <- function() { sum(gc(FALSE)[, "used"]) }

# Arrow off-heap bytes; returns 0 if unavailable
arrow_offheap_bytes <- function() {
  tryCatch({
    mp <- arrow::default_memory_pool()
    if (is.function(mp$bytes_allocated)) {
      as.numeric(mp$bytes_allocated())
    } else if (is.function(get("bytes_allocated", asNamespace("arrow"), inherits = TRUE))) {
      as.numeric(arrow::bytes_allocated(mp))
    } else 0
  }, error = function(e) 0)
}

# Process RSS (bytes) from /proc/self/status (Linux)
proc_rss_bytes <- function() {
  st <- tryCatch(readLines("/proc/self/status"), error = function(e) character())
  if (!length(st)) return(NA_real_)
  ln <- st[grepl("^VmRSS:", st)]
  if (!length(ln)) return(NA_real_)
  kb <- as.numeric(gsub("[^0-9]", "", ln))
  if (is.na(kb)) NA_real_ else kb * 1024
}

# System MemAvailable (bytes) from /proc/meminfo
mem_available_bytes <- function() {
  mi <- tryCatch(readLines("/proc/meminfo"), error = function(e) character())
  if (!length(mi)) return(NA_real_)
  ln <- mi[grepl("^MemAvailable:", mi)]
  if (!length(ln)) return(NA_real_)
  kb <- as.numeric(gsub("[^0-9]", "", ln))
  if (is.na(kb)) NA_real_ else kb * 1024
}

# Durable Arrow table constructor across versions
to_arrow_table <- function(x) {
  if (is.function(get("as_arrow_table", asNamespace("arrow"), inherits = TRUE))) {
    arrow::as_arrow_table(x)
  } else {
    arrow::arrow_table(x)
  }
}

# --------------------
# Single off-heap iteration
# --------------------
run_iteration <- function(it, nrows) {
  pq <- file.path(OUT_DIR, sprintf("offheap_%02d.parquet", it))

  cat("\n", strrep("=", 78), "\n", sep = "")
  cat("OFFHEAP TEST ", it, " - ROWS: ", format(nrows, big.mark=","), "\n", sep = "")
  cat(strrep("=", 78), "\n", sep = "")
  cat("Config: USE_MMAP=", USE_MMAP, ", TOUCH_DATA=", TOUCH_DATA,
      ", HOLD_OBJECTS=", HOLD_OBJECTS, "\n", sep = "")

  # Baseline probes
  r_heap0 <- r_heap_used_mb()
  a0      <- arrow_offheap_bytes()
  rss0    <- proc_rss_bytes()
  sys0    <- mem_available_bytes()

  cat("Start:\n")
  cat("  R heap:        ", fmt_bytes(r_heap0 * 1024^2), "\n")
  cat("  Arrow offheap: ", fmt_bytes(a0), "\n")
  cat("  Proc RSS:      ", fmt_bytes(rss0), "\n")
  cat("  MemAvailable:  ", fmt_bytes(sys0), "\n")

  # 1) Generate (on R heap)
  t0 <- Sys.time()
  DT <- data.table(
    id         = 1:nrows,
    timestamp  = as.POSIXct("2025-01-01", tz="UTC") + sample(0:31536000, nrows, TRUE),
    category   = sample(LETTERS[1:25], nrows, TRUE),
    value1     = rnorm(nrows, 100, 25),
    value2     = runif(nrows, 0, 1000),
    value3     = rpois(nrows, 15),
    txt        = paste0("data_", sample(1:100000, nrows, TRUE)),
    status     = sample(c("active","inactive","pending","archived","deleted"), nrows, TRUE),
    amount     = runif(nrows, 1, 1e5)
  )
  gen_sec <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
  cat("• Generated in ", round(gen_sec,2), "s; size ", fmt_bytes(as.numeric(object.size(DT))), "\n", sep = "")

  # 2) Write Parquet (off-heap writer)
  t1 <- Sys.time()
  write_parquet(
    to_arrow_table(DT), pq,
    compression = "zstd", compression_level = 3,
    use_dictionary = TRUE, write_statistics = TRUE
  )
  w_sec <- as.numeric(difftime(Sys.time(), t1, units = "secs"))
  fsz   <- file.info(pq)$size
  cat("• Wrote Parquet in ", round(w_sec,2), "s; file ", fmt_bytes(fsz), "\n", sep = "")

  # Free R heap before off-heap read
  rm(DT); invisible(gc())

  # 3) Read as Arrow Table (off-heap)
  r1 <- r_heap_used_mb(); a1 <- arrow_offheap_bytes(); rss1 <- proc_rss_bytes(); sys1 <- mem_available_bytes()

  t2 <- Sys.time()
  arr_tbl <- read_parquet(pq, as_data_frame = FALSE)
  # Optional: touch data via compute() to fault pages
  agg_tbl <- arr_tbl |>
    to_arrow_table() |>
    group_by(category) |>
    summarise(n = dplyr::n(),
              avg_v1 = mean(value1, na.rm = TRUE),
              sum_amt = sum(amount, na.rm = TRUE),
              .groups = "drop") |>
    compute()

  if (TOUCH_DATA) {
    # Extra pass to increase touched pages (still off-heap)
    invisible(
      arr_tbl |>
        to_arrow_table() |>
        filter(value1 > 100, status == "active") |>
        select(id, value1, amount) |>
        compute()
    )
  }
  scan_sec <- as.numeric(difftime(Sys.time(), t2, units = "secs"))

  r2   <- r_heap_used_mb()
  a2   <- arrow_offheap_bytes()
  rss2 <- proc_rss_bytes()
  sys2 <- mem_available_bytes()

  cat("• Off-heap scan/compute in ", round(scan_sec,2), "s\n", sep = "")
  cat("Deltas after read/compute:\n")
  cat("  Δ Arrow offheap: ", fmt_bytes(a2 - a1), "\n", sep = "")
  cat("  Δ Proc RSS:      ", fmt_bytes(rss2 - rss1), "\n", sep = "")
  cat("  Δ MemAvailable:  ", fmt_bytes(sys2 - sys1), "  (negative means pressure)\n", sep = "")
  cat("  Δ R heap:        ", fmt_bytes((r2 - r1) * 1024^2), "\n", sep = "")

  # Decide whether to hold or release
  held <- NULL
  if (HOLD_OBJECTS) {
    held <- list(arr_tbl = arr_tbl, agg_tbl = agg_tbl)  # keep references
  } else {
    rm(arr_tbl, agg_tbl)
  }

  # Remove file each iteration to isolate memory effects
  unlink(pq)

  list(
    it = it, rows = nrows,
    file_size = fsz,
    times = list(gen = gen_sec, write = w_sec, scan = scan_sec),
    start = list(r_heap = r_heap0, arrow = a0, rss = rss0, sys = sys0),
    pre_read = list(r_heap = r1, arrow = a1, rss = rss1, sys = sys1),
    post_read = list(r_heap = r2, arrow = a2, rss = rss2, sys = sys2),
    held = held
  )
}

# --------------------
# Main driver
# --------------------
main_offheap_pressure_test <- function() {
  cat("\n", strrep("=", 84), "\n", sep = "")
  cat("▶▶ ARROW OFF-HEAP PRESSURE TEST (System impact) ◀◀\n")
  cat(strrep("=", 84), "\n", sep = "")
  cat("Arrow version: ", as.character(arrow::arrow_info()$version), "\n", sep = "")
  cat("Cores: ", NUM_CORES, "  use_mmap=", USE_MMAP, "  touch_data=", TOUCH_DATA,
      "  hold_objects=", HOLD_OBJECTS, "\n", sep = "")

  results <- list()
  holders <- list()

  rows <- START_ROWS
  it <- 1
  while (rows <= MAX_ROWS) {
    res <- run_iteration(it, rows)
    results[[it]] <- res

    # Accumulate held Arrow objects to keep memory pressure
    if (HOLD_OBJECTS && !is.null(res$held)) {
      holders[[length(holders)+1]] <- res$held
    }

    # Periodic release to observe recovery
    if (RELEASE_EVERY > 0 && it %% RELEASE_EVERY == 0) {
      cat("\n--- Releasing held Arrow objects (iteration ", it, ") ---\n", sep = "")
      rm(holders); holders <- list(); invisible(gc())
      # Snapshot after release
      a_now <- arrow_offheap_bytes()
      rss_now <- proc_rss_bytes()
      sys_now <- mem_available_bytes()
      cat("After release:\n")
      cat("  Arrow offheap: ", fmt_bytes(a_now), "\n", sep = "")
      cat("  Proc RSS:      ", fmt_bytes(rss_now), "\n", sep = "")
      cat("  MemAvailable:  ", fmt_bytes(sys_now), "\n", sep = "")
    }

    # Next size
    rows <- round(rows * MULTIPLIER)
    it <- it + 1
    if (it > 15) { cat("Safety stop at 15 iterations.\n"); break }
    Sys.sleep(1)
  }

  # Summary table
  cat("\n", strrep("=", 84), "\n", sep = "")
  cat("▶▶ SUMMARY (per iteration deltas) ◀◀\n")
  cat(strrep("=", 84), "\n", sep = "")
  fmt <- function(x) if (is.na(x)) "NA" else fmt_bytes(x)

  cat(sprintf("%-3s %-12s %-10s %-12s %-12s %-12s %-12s\n",
              "It","Rows","Parquet","ΔArrow","ΔRSS","ΔMemAvail","ΔRheap"))
  cat(strrep("-", 84), "\n")
  for (r in results) {
    d_arrow <- (r$post_read$arrow - r$pre_read$arrow)
    d_rss   <- (r$post_read$rss   - r$pre_read$rss)
    d_sys   <- (r$post_read$sys   - r$pre_read$sys)
    d_rheap <- (r$post_read$r_heap - r$pre_read$r_heap) * 1024^2
    cat(sprintf("%-3d %-12s %-10s %-12s %-12s %-12s %-12s\n",
                r$it,
                format(r$rows, big.mark=","),
                fmt_bytes(r$file_size),
                fmt(d_arrow), fmt(d_rss), fmt(d_sys), fmt(d_rheap)))
  }

  cat("\nObservations:\n")
  cat("  • With USE_MMAP=TRUE, Arrow pool bytes may stay low while RSS grows as pages are faulted.\n")
  cat("  • With USE_MMAP=FALSE (if supported by your build), Arrow pool bytes should reflect allocations.\n")
  cat("  • If HOLD_OBJECTS=TRUE, memory stays high until references are released and gc() runs.\n")
  cat("  • MemAvailable should drop (negative Δ) during pressure and recover after release.\n\n")

  invisible(results)
}

if (!interactive()) {
  offheap_results <- main_offheap_pressure_test()
} else {
  cat("\nRun: offheap_results <- main_offheap_pressure_test()\n")
}
