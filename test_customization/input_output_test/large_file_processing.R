# 15GB File Processing Strategy for RStudio
# This script implements the optimized approach for handling large datasets
# Based on findings from breaking_point_analysis.html

# Load required libraries
library(data.table)
library(parallel)
library(pryr)

# Increase memory limits
mem.maxVSize <- function(max = 32 * 1024^3) {
  cat("Setting vector memory limit to", format(max, scientific = FALSE), "bytes\n")
  invisible(.Call("R_max_memory", max))
}

# Optimized large file reading function
read_large_csv <- function(file_path, 
                           use_parallel = TRUE, 
                           n_cores = NULL,
                           show_progress = TRUE,
                           verbose = TRUE) {
  
  # Set memory limit to 32GB
  mem.maxVSize(max = 32 * 1024^3)
  
  # Get file size for logging
  file_info <- file.info(file_path)
  file_size_gb <- file_info$size / 1024^3
  
  # Determine number of cores to use
  if (is.null(n_cores)) {
    n_cores <- if (use_parallel) min(parallel::detectCores(), 8) else 1
  }
  
  # Log start time and file details
  start_time <- Sys.time()
  if (verbose) {
    cat(sprintf("Reading file: %s\n", file_path))
    cat(sprintf("File size: %.2f GB\n", file_size_gb))
    cat(sprintf("Using %d cores\n", n_cores))
    cat(sprintf("Start time: %s\n", start_time))
    cat("Memory before reading: ")
    print(object.size(globalenv()), units = "auto")
  }
  
  # Use fread with appropriate parameters
  dt <- data.table::fread(
    file_path,
    nThread = n_cores,
    verbose = verbose,
    showProgress = show_progress
  )
  
  # Log end time and performance metrics
  end_time <- Sys.time()
  elapsed <- difftime(end_time, start_time, units = "secs")
  
  if (verbose) {
    cat(sprintf("End time: %s\n", end_time))
    cat(sprintf("Elapsed time: %.2f seconds\n", as.numeric(elapsed)))
    cat(sprintf("Processing speed: %.2f MB/sec\n", 
                file_size_gb * 1024 / as.numeric(elapsed)))
    cat(sprintf("Rows read: %d\n", nrow(dt)))
    cat(sprintf("Columns: %d\n", ncol(dt)))
    cat("Memory after reading: ")
    print(object.size(dt), units = "auto")
  }
  
  return(dt)
}

# Function to efficiently process chunks of a large file
process_large_file_in_chunks <- function(file_path, 
                                         chunk_size = 1000000,
                                         n_cores = 4,
                                         process_fn = NULL) {
  
  # Increase memory limits
  mem.maxVSize(max = 32 * 1024^3)
  
  # Count lines in file (can be slow for very large files)
  # Alternative: provide line count as parameter if known
  cat("Counting lines in file...\n")
  cmd <- sprintf("wc -l < %s", file_path)
  total_lines <- as.numeric(system(cmd, intern = TRUE))
  
  # Calculate number of chunks
  n_chunks <- ceiling((total_lines - 1) / chunk_size)  # -1 for header
  
  cat(sprintf("Processing file with %d lines in %d chunks\n", 
              total_lines, n_chunks))
  
  # Process each chunk
  results <- list()
  
  for (i in 1:n_chunks) {
    skip_lines <- (i - 1) * chunk_size
    
    cat(sprintf("Processing chunk %d of %d (lines %d to %d)\n", 
                i, n_chunks, skip_lines + 1, 
                min(skip_lines + chunk_size, total_lines)))
    
    # Read chunk
    chunk <- fread(file_path, 
                   skip = skip_lines, 
                   nrows = chunk_size,
                   header = (skip_lines == 0),  # Header only in first chunk
                   nThread = n_cores)
    
    # Apply processing function if provided
    if (!is.null(process_fn)) {
      chunk_result <- process_fn(chunk)
      results[[i]] <- chunk_result
    } else {
      # Default processing: just count rows and calculate memory
      cat(sprintf("  Chunk size: %d rows, %.2f MB\n", 
                  nrow(chunk), 
                  object.size(chunk) / 1024^2))
    }
    
    # Clean up to free memory
    rm(chunk)
    gc()
  }
  
  return(results)
}

# Example usage:
# 1. To read an entire large file if memory permits:
#    df <- read_large_csv("path/to/large_file.csv")
#
# 2. To process a file in chunks when memory is limited:
#    results <- process_large_file_in_chunks("path/to/large_file.csv", 
#                                           process_fn = function(chunk) {
#                                             # Process each chunk
#                                             return(summary(chunk))
#                                           })
