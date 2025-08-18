```R
library(data.table)
library(microbenchmark)

# Function to read CSV in parallel
read_csv_parallel <- function(file_path) {
  system.time({
    data <- fread(file_path, nThread = 8)
  })
  return(data)
}

# Measure memory usage
memory_before <- memory.size()
data <- read_csv_parallel("test_data/large_dataset.csv")
memory_after <- memory.size()

# Report memory usage and time taken
cat("Memory usage before: ", memory_before, "MB\n")
cat("Memory usage after: ", memory_after, "MB\n")
cat("Time taken to read the file: ", system.time(data), "seconds\n")
```