#!/usr/bin/env Rscript
# Run suite of RStudio runtime tests.
# Usage:
#   Rscript run_all_tests.R [--with-viewer] [--stop-on-fail]
#
# Default runs:
#  - cairo_smoke_test.R
#  - rstudio_config_test.R
#  - check_renviron.R
# Add --with-viewer to also run ggplot_viewer_test.R (may open a browser/Viewer in IDE).

args <- commandArgs(trailingOnly = TRUE)
with_viewer <- "--with-viewer" %in% args
stop_on_fail <- "--stop-on-fail" %in% args

`%||%` <- function(a,b) if (!is.null(a)) a else b
# Determine script directory reliably under Rscript
get_script_dir <- function() {
  args <- commandArgs(trailingOnly = FALSE)
  file_arg <- grep("^--file=", args, value = TRUE)
  if (length(file_arg)) return(normalizePath(dirname(sub("^--file=", "", file_arg[1]))))
  # Fallback to current working directory
  return(normalizePath(getwd()))
}
root <- get_script_dir()

scripts <- list(
  list(name = "cairo_smoke_test", path = file.path(root, "cairo_smoke_test.R"), include = TRUE, args = character()),
  list(name = "rstudio_config_test", path = file.path(root, "rstudio_config_test.R"), include = TRUE, args = character()),
  list(name = "check_renviron", path = file.path(root, "check_renviron.R"), include = TRUE, args = character()),
  list(name = "ggplot_viewer_test", path = file.path(root, "ggplot_viewer_test.R"), include = with_viewer, args = character())
)

run_one <- function(item) {
  cat(sprintf("\n=== RUN %s (%s) ===\n", item$name, item$path))
  if (!file.exists(item$path)) {
    cat(sprintf("[SKIP] %s not found\n", item$path))
    return(list(name = item$name, status = "SKIP", code = NA_integer_, output = "missing"))
  }
  cmd <- "Rscript"
  res <- tryCatch({
    out <- system2(cmd, c(item$path, item$args), stdout = TRUE, stderr = TRUE)
    attr(out, "status") <- 0L
    out
  }, error = function(e) {
    structure(paste0("ERROR: ", conditionMessage(e)), status = 1L)
  })
  status <- attr(res, "status"); status <- if (is.null(status)) 0L else status
  cat(paste(res, collapse = "\n"), "\n", sep = "")
  if (identical(status, 0L)) {
    cat(sprintf("[PASS] %s\n", item$name))
    list(name = item$name, status = "PASS", code = 0L, output = res)
  } else {
    cat(sprintf("[FAIL] %s (exit %s)\n", item$name, status))
    list(name = item$name, status = "FAIL", code = status, output = res)
  }
}

selected <- Filter(function(x) isTRUE(x$include), scripts)
results <- lapply(selected, run_one)

# Summary
pass <- sum(vapply(results, function(r) identical(r$status, "PASS"), logical(1)))
fail <- sum(vapply(results, function(r) identical(r$status, "FAIL"), logical(1)))
skip <- sum(vapply(results, function(r) identical(r$status, "SKIP"), logical(1)))

cat("\n=== SUMMARY ===\n")
cat(sprintf("PASS: %d  FAIL: %d  SKIP: %d\n", pass, fail, skip))

if (fail > 0 && stop_on_fail) quit(status = 1) else quit(status = 0)
