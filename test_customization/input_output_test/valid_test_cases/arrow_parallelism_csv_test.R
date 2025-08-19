#!/usr/bin/env Rscript
# =================================================================
# Arrow Parallelism Optimization Test (Refined)
# =================================================================

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
  library(parallel)
})

# ---- helpers -----------------------------------------------------
format_bytes <- function(bytes) {
  units <- c("B","KB","MB","GB","TB")
  i <- 1
  while (bytes >= 1024 && i < length(units)) {
    bytes <- bytes / 1024; i <- i + 1
  }
  sprintf("%.2f %s", bytes, units[i])
}

time_it <- function(expr) {
  t0 <- Sys.time()
  value <- force(expr)
  t <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
  list(value = value, seconds = t)
}

cat('=================================================================\n')
cat('Arrow Parallelism Optimization Test\n')
cat('=================================================================\n')

# ---- system info -------------------------------------------------
cat('System Configuration:\n')
cat('CPU Cores:', parallel::detectCores(), '\n')
cat('R Version:', R.version.string, '\n')
cat('Arrow Version:', as.character(utils::packageVersion('arrow')), '\n')
cat('JIT Compilation (env R_ENABLE_JIT):', Sys.getenv('R_ENABLE_JIT', unset=""), '\n\n')

# ---- parameters --------------------------------------------------
test_file_size_gb <- 2              # adjust if you need quicker runs
rows_per_gb <- 1e6                  # ~1M rows/GB (rough heuristic)
target_rows <- as.integer(test_file_size_gb * rows_per_gb)

test_dir <- '/tmp/arrow_parallel_test'
csv_file <- file.path(test_dir, 'arrow_test.csv')
dir.create(test_dir, recursive = TRUE, showWarnings = FALSE)

# ---- data generation ---------------------------------------------
cat('Generating', test_file_size_gb, 'GB test dataset (approx)...\n')
gen <- time_it({
  set.seed(42)
  DT <- data.table(
    id = seq_len(target_rows),
    timestamp = as.POSIXct('2025-01-01', tz = "UTC") + runif(target_rows, 0, 365*24*3600),
    category = sample(LETTERS[1:10], target_rows, TRUE),
    value1 = rnorm(target_rows, 100, 25),
    value2 = runif(target_rows, 0, 1000),
    value3 = rexp(target_rows, 0.1),
    text_field   = paste0('test_', sample(100000:999999, target_rows, TRUE)),
    factor_field = sample(paste0('Type', 1:5), target_rows, TRUE),
    logical_field = sample(c(TRUE, FALSE), target_rows, TRUE),
    large_numeric = runif(target_rows, 1e6, 1e9)
  )
  data.table::fwrite(DT, csv_file)
  rm(DT); invisible(gc())
})
file_size <- file.info(csv_file)$size
cat('Test file generated:\n')
cat('Size:', format_bytes(file_size), '\n')
cat('Generation time:', sprintf('%.2f seconds', gen$seconds), '\n\n')

# ---- core configs ------------------------------------------------
max_cores <- parallel::detectCores()
core_configs <- sort(unique(c(1, 2, 4, 8, max_cores[1])))
core_configs <- core_configs[core_configs <= max_cores]

# ---- results DF --------------------------------------------------
results <- data.frame(
  cores = integer(),
  threads = integer(),
  threads_setting = character(),
  read_time_sec = numeric(),
  throughput_mbs = numeric(),
  cpu_efficiency = numeric(),
  success = logical(),
  stringsAsFactors = FALSE
)

cat('Testing Arrow performance with different core/thread configurations:\n')
cat('===================================================================\n\n')

file_size_gb <- file_size / (1024^3)

for (cores in core_configs) {
  cat('Testing with', cores, 'cores...\n')
  thread_configs <- sort(unique(c(cores, cores * 2)))  # try “physical” and “HT”
  for (threads in thread_configs) {
    config_name <- sprintf('%d_cores_%d_threads', cores, threads)
    cat('  Configuration:', config_name, '\n')
    
    # Save & set env; always restore
    old_env <- Sys.getenv(c("OMP_NUM_THREADS","ARROW_NUM_THREADS","ARROW_CPU_COUNT"), unset = NA)
    on.exit({
      if (!is.na(old_env[["OMP_NUM_THREADS"]])) Sys.setenv(OMP_NUM_THREADS = old_env[["OMP_NUM_THREADS"]]) else Sys.unsetenv("OMP_NUM_THREADS")
      if (!is.na(old_env[["ARROW_NUM_THREADS"]])) Sys.setenv(ARROW_NUM_THREADS = old_env[["ARROW_NUM_THREADS"]]) else Sys.unsetenv("ARROW_NUM_THREADS")
      if (!is.na(old_env[["ARROW_CPU_COUNT"]])) Sys.setenv(ARROW_CPU_COUNT = old_env[["ARROW_CPU_COUNT"]]) else Sys.unsetenv("ARROW_CPU_COUNT")
    }, add = TRUE)
    
    Sys.setenv(OMP_NUM_THREADS = as.character(threads))
    Sys.setenv(ARROW_NUM_THREADS = as.character(threads))
    Sys.setenv(ARROW_CPU_COUNT = as.character(cores))
    
    test_start <- Sys.time()
    ok <- TRUE; msg <- NULL; read_time <- NA_real_; throughput <- 0; cpu_eff <- 0
    
    # Run read
    tryCatch({
      timed <- time_it({
        tbl <- arrow::read_csv_arrow(csv_file, as_data_frame = FALSE)  # Arrow Table
        # Materialize to R data.frame to include conversion cost if you want end-to-end:
        df <- as.data.frame(tbl)
        rm(tbl, df)
      })
      read_time <- timed$seconds
      throughput <- (file_size_gb * 1024) / read_time # MB/s
      cpu_eff <- throughput / max(1, cores)           # MB/s per core
      cat('    SUCCESS - Time:', sprintf('%.3f sec', read_time),
          'Throughput:', sprintf('%.2f MB/s', throughput),
          'Efficiency:', sprintf('%.2f MB/s/core', cpu_eff), '\n')
    }, error = function(e) {
      ok <<- FALSE; msg <<- conditionMessage(e)
      read_time <<- as.numeric(difftime(Sys.time(), test_start, units='secs'))
      cat('    FAILED after', sprintf('%.3f', read_time), 'sec:', msg, '\n')
    })
    
    results <- rbind(results, data.frame(
      cores = cores,
      threads = threads,
      threads_setting = config_name,
      read_time_sec = read_time,
      throughput_mbs = throughput,
      cpu_efficiency = cpu_eff,
      success = ok,
      stringsAsFactors = FALSE
    ))
    
    invisible(gc()); Sys.sleep(0.5)
  }
  cat('\n')
}

# ---- comparison with other methods -------------------------------
cat('Comparing with other I/O methods using optimal Arrow settings...\n')
cat('===============================================================\n')

successful_results <- subset(results, success)
if (nrow(successful_results) > 0) {
  best_config <- successful_results[which.max(successful_results$throughput_mbs), ]
  cat('Using optimal Arrow config:', best_config$threads_setting, '\n')
  
  old_env2 <- Sys.getenv(c("OMP_NUM_THREADS","ARROW_NUM_THREADS","ARROW_CPU_COUNT"), unset = NA)
  on.exit({
    if (!is.na(old_env2[["OMP_NUM_THREADS"]])) Sys.setenv(OMP_NUM_THREADS = old_env2[["OMP_NUM_THREADS"]]) else Sys.unsetenv("OMP_NUM_THREADS")
    if (!is.na(old_env2[["ARROW_NUM_THREADS"]])) Sys.setenv(ARROW_NUM_THREADS = old_env2[["ARROW_NUM_THREADS"]]) else Sys.unsetenv("ARROW_NUM_THREADS")
    if (!is.na(old_env2[["ARROW_CPU_COUNT"]])) Sys.setenv(ARROW_CPU_COUNT = old_env2[["ARROW_CPU_COUNT"]]) else Sys.unsetenv("ARROW_CPU_COUNT")
  }, add = TRUE)
  
  Sys.setenv(OMP_NUM_THREADS = as.character(best_config$threads))
  Sys.setenv(ARROW_NUM_THREADS = as.character(best_config$threads))
  Sys.setenv(ARROW_CPU_COUNT = as.character(best_config$cores))
  
  # data.table::fread
  cat('Testing data.table fread...\n')
  try({
    timed <- time_it({ df_dt <- data.table::fread(csv_file); invisible(df_dt); rm(df_dt) })
    dt_thr <- (file_size_gb * 1024) / timed$seconds
    cat('data.table fread - Time:', sprintf('%.3f sec', timed$seconds),
        'Throughput:', sprintf('%.2f MB/s', dt_thr), '\n')
  })
  
  # base read.csv with timeout
  cat('Testing base R read.csv (60s time limit)...\n')
  tlim <- 60
  timed <- NULL
  ok_base <- TRUE
  tryCatch({
    setTimeLimit(elapsed = tlim, transient = TRUE)
    timed <- time_it({ df_base <- utils::read.csv(csv_file); invisible(df_base); rm(df_base) })
  }, error = function(e) {
    ok_base <<- FALSE
    if (grepl("reached elapsed time limit", conditionMessage(e))) {
      cat('Base R read.csv - TIMEOUT after', tlim, 'seconds\n')
    } else {
      cat('Base R read.csv FAILED:', conditionMessage(e), '\n')
    }
  }, finally = {
    setTimeLimit(cpu = Inf, elapsed = Inf, transient = FALSE)
  })
  if (ok_base && !is.null(timed)) {
    base_thr <- (file_size_gb * 1024) / timed$seconds
    cat('Base R read.csv - Time:', sprintf('%.3f sec', timed$seconds),
        'Throughput:', sprintf('%.2f MB/s', base_thr), '\n')
  }
} else {
  cat('No successful Arrow runs to compare against.\n')
}

# ---- cleanup (optional) ------------------------------------------
file.remove(csv_file)
unlink(test_dir, recursive = TRUE, force = TRUE)

cat('\n=== ARROW PARALLELISM TEST RESULTS ===\n')
cat('======================================\n\n')
print(results)

# ---- analysis ----------------------------------------------------
if (nrow(successful_results) > 0) {
  cat('\n=== PERFORMANCE ANALYSIS ===\n')
  
  best_overall <- successful_results[which.max(successful_results$throughput_mbs), ]
  best_eff     <- successful_results[which.max(successful_results$cpu_efficiency), ]
  
  cat('Best Overall Performance:\n')
  cat('  Configuration:', best_overall$threads_setting, '\n')
  cat('  Throughput:', sprintf('%.2f MB/s', best_overall$throughput_mbs), '\n')
  cat('  Time:', sprintf('%.3f seconds', best_overall$read_time_sec), '\n\n')
  
  cat('Best CPU Efficiency:\n')
  cat('  Configuration:', best_eff$threads_setting, '\n')
  cat('  Efficiency:', sprintf('%.2f MB/s per core', best_eff$cpu_efficiency), '\n')
  cat('  Throughput:', sprintf('%.2f MB/s', best_eff$throughput_mbs), '\n\n')
  
  single_core <- subset(successful_results, cores == 1)
  if (nrow(single_core) > 0) {
    single_core_thr <- max(single_core$throughput_mbs)
    max_thr <- max(successful_results$throughput_mbs)
    scaling_factor <- max_thr / single_core_thr
    cat('Parallel Scaling Analysis:\n')
    cat('  Single core throughput:', sprintf('%.2f MB/s', single_core_thr), '\n')
    cat('  Maximum throughput:', sprintf('%.2f MB/s', max_thr), '\n')
    cat('  Scaling factor:', sprintf('%.2fx', scaling_factor), '\n')
    cat('  Parallel efficiency:', sprintf('%.1f%%', (scaling_factor / max_cores) * 100), '\n\n')
  }
  
  cat('=== RECOMMENDATIONS ===\n')
  cat('For optimal Arrow performance in this environment:\n')
  cat('  Use', best_overall$cores, 'cores with', best_overall$threads, 'threads\n')
  cat('  Expected throughput:', sprintf('%.2f MB/s', best_overall$throughput_mbs), '\n')
  cat('  Set environment variables:\n')
  cat('    export OMP_NUM_THREADS=', best_overall$threads, '\n', sep = '')
  cat('    export ARROW_NUM_THREADS=', best_overall$threads, '\n', sep = '')
  cat('    export ARROW_CPU_COUNT=',  best_overall$cores,   '\n', sep = '')
}

cat('\nArrow parallelism optimization test completed!\n')


# parser that has to find row boundaries and then parse/convert bytes to columns in order. That design limits how much of one file can be processed at once.

# A bit more detail

# Arrow’s CSV options expose only use_threads and a block_size—there’s no “num_threads” knob for CSV like some other readers. Even with use_threads=TRUE, parallelism is modest for a single file. 
# Apache Arrow
# +1

# Arrow’s own notes call out that the streaming CSV reader “does not allow for much parallelism”, can’t read multiple segments concurrently, and lacks column fan-out during parsing. Work to improve this has been tracked, but the limitation still explains what you’re observing. 
# issues.apache.org

# Separate from CSV itself, Arrow has two thread pools (I/O and CPU). What you often see is one thread feeding bytes (I/O) and one parsing/converting (CPU) per file, so CPU usage looks like “~2 threads” for one big CSV. When you give Arrow many files, it can fan out across them and use many cores. 
# Apache Arrow

# Also, in R specifically, if you convert the Arrow table to an R data.frame (as.data.frame()), that conversion step is largely single-threaded and can hide any parsing parallelism you did get.

# How to scale beyond “2 threads” in practice

# Shard the CSV into many files and read the directory as a dataset:

# ds <- arrow::open_dataset("/path/to/csv_shards/", format = "csv")
# df <- ds %>% dplyr::filter(...) %>% collect()


# Per-file parallelism lets Arrow keep many CPU workers busy.

# Bump block_size (and keep use_threads = TRUE) so each chunk of a file is larger and reduces per-chunk overhead. This won’t turn 2 into 32 threads, but it can improve throughput. 
# Apache Arrow
# +1

# Prefer columnar formats (Parquet/Feather) for heavy parallel reads. Arrow can scan multiple row-groups/fragments concurrently and you’ll see near-linear scaling with cores in many cases. (CSV is fundamentally slower to parallelize because every byte has to be tokenized and type-converted in order.)

# Avoid immediate as.data.frame() if you can work on Arrow Table/Dataset and only collect() the result. The conversion to base R can become the new bottleneck.

# Use per-file parallelism on object storage (S3/ABFS) where the I/O pool can help more. Arrow’s I/O thread pool defaults are optimized for concurrency on remote stores; you can increase them if the storage benefits from more simultaneous reads. 
# Apache Arrow

# If you’d like, I can add a “multi-file” mode to your benchmark that: (a) splits the synthetic data into N shards, (b) tests open_dataset() scan/collect, and (c) compares Parquet vs CSV so you can see cores scale past 2 cleanly.
