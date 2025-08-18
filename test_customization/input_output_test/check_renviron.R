# Compare variables from Renviron.site with current environment

# Simple CLI: --full to print full table, --limit=N to print first N rows,
# --strict to exit non-zero on mismatch.
args <- commandArgs(trailingOnly = TRUE)
full <- "--full" %in% args
strict <- "--strict" %in% args
limit <- NA_integer_
lim_arg <- grep('^--limit=\\d+$', args, value = TRUE)
if (length(lim_arg)) limit <- as.integer(sub('^--limit=', '', lim_arg[1]))

vars <- c(
  "R_MAX_VSIZE","R_MAX_NSIZE","LC_ALL","LANG",
  "R_ENABLE_JIT","R_COMPILE_PKGS","MALLOC_ARENA_MAX","OMP_NUM_THREADS",
  "R_LIBS_SITE","R_LIBS_USER","R_DEFAULT_PACKAGES",
  "R_DOWNLOAD_FILE_METHOD","R_TIMEOUT",
  "PAGER","TAR",
  "R_ZIPCMD","R_UNZIPCMD","R_GZIPCMD","R_LATEXCMD","R_MAKEINDEXCMD","R_DVIPSCMD",
  "JAVA_HOME","RETICULATE_PYTHON",
  "R_HISTSIZE","R_BROWSER","R_PDFVIEWER"
)

# Compute expected values by temporarily re-reading Renviron.site in isolation
expected <- local({
  old <- Sys.getenv()              # snapshot
  on.exit({
    # restore snapshot
    do.call(Sys.setenv, as.list(old))
  }, add = TRUE)
  # Unset the vars so defaults in file (e.g., ${X:-default}) take effect
  try(sapply(vars, Sys.unsetenv), silent = TRUE)
  readRenviron("/usr/local/lib/R/etc/Renviron.site")
  Sys.getenv(vars, NA)
})

# Actual values in current session
actual <- Sys.getenv(vars, NA)

res <- data.frame(var = vars, expected = unname(expected), actual = unname(actual), stringsAsFactors = FALSE)
# Elementwise equality, respecting NAs
res$match <- (res$expected == res$actual) | (is.na(res$expected) & is.na(res$actual))

# Print table with controlled size to avoid external piping/SIGPIPE
if (isTRUE(full)) {
  print(res, row.names = FALSE)
} else if (!is.na(limit)) {
  print(utils::head(res, limit), row.names = FALSE)
} else {
  print(utils::head(res, 25), row.names = FALSE)
}

# Summarize mismatches
mismatch <- res[!res$match, ]
if (nrow(mismatch)) {
  cat("\nMISMATCHES (", nrow(mismatch), "):\n", sep = "")
  apply(mismatch, 1, function(r) {
    cat(sprintf(" - %s: expected=\"%s\" actual=\"%s\"\n", r[["var"]], r[["expected"]], r[["actual"]]))
  })
} else {
  cat("\nAll variables match the definitions in Renviron.site\n")
}

if (strict && nrow(mismatch)) quit(status = 1) else quit(status = 0)
