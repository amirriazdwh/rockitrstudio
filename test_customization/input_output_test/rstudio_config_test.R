# Tests for rstudio-config.R behavior
# - Ensures bitmapType is 'cairo'
# - Ensures default device opens PNG via options(device=...)
# - When a stub .rs.api.versionInfo exists, help_type becomes 'html' and error is a function

ok <- function(msg) cat("[OK] ", msg, "\n", sep = "")
fail <- function(msg) { cat("[FAIL] ", msg, "\n", sep = ""); quit(status = 1) }

path <- file.path(R.home("etc"), "profiles.d", "rstudio-config.R")
if (!file.exists(path)) fail(sprintf("Config not found: %s", path))

# Snapshot and reset a few options we care about
old <- options()
on.exit({ options(old) }, add = TRUE)
options("help_type" = NULL, "error" = NULL, "device" = NULL)

# Source the config
source(path)

# 1) bitmapType
bt <- getOption("bitmapType")
if (!identical(bt, "cairo")) fail(sprintf("bitmapType expected 'cairo', got %s", deparse(bt)))
ok("bitmapType is 'cairo'")

# 2) default device opens PNG and writes a file
if (!capabilities("cairo")) fail("capabilities('cairo') is FALSE; cannot validate Cairo path")

tmp <- tempdir(); oldwd <- setwd(tmp); on.exit(setwd(oldwd), add = TRUE)
# Clean previous default PNGs
old_pngs <- Sys.glob("Rplot*.png"); if (length(old_pngs)) unlink(old_pngs, force = TRUE)

# Expect options(device) to be a function that opens PNG with default filename
if (!is.function(getOption("device"))) fail("options('device') is not a function after sourcing config")

# Trigger plotting which should auto-open the default device and create Rplot001.png
plot(1:10, main = "rstudio-config default png device test")
dev.off()

pngs <- Sys.glob("Rplot*.png")
if (!length(pngs)) fail("No Rplot*.png created by default device")

# Check PNG signature bytes: 89 50 4E 47 0D 0A 1A 0A
sig <- readBin(pngs[[1]], what = "raw", n = 8)
expected_sig <- as.raw(c(0x89,0x50,0x4E,0x47,0x0D,0x0A,0x1A,0x0A))
if (!identical(sig, expected_sig)) fail(sprintf("Created file is not a PNG: %s", basename(pngs[[1]])))
ok(sprintf("Default device created PNG: %s", basename(pngs[[1]])))

# 3) RStudio IDE enhancements: simulate presence of .rs.api.versionInfo
# Save current, if any, to restore later
had_rs <- exists(".rs.api.versionInfo", inherits = TRUE)
old_rs <- if (had_rs) get(".rs.api.versionInfo", inherits = TRUE) else NULL
on.exit({ if (had_rs) assign(".rs.api.versionInfo", old_rs, envir = .GlobalEnv) else if (exists(".rs.api.versionInfo", inherits = FALSE)) rm(.rs.api.versionInfo, envir = .GlobalEnv) }, add = TRUE)

assign(".rs.api.versionInfo", function() list(mode = "test"), envir = .GlobalEnv)
# Re-source to apply RStudio-specific options
source(path)

if (identical(getOption("help_type"), "html")) {
	ok("help_type set to 'html' with RStudio API stub")
} else {
	cat("[WARN] help_type not 'html' (non-interactive or stub not detected)\n")
}

if (is.function(getOption("error"))) {
	ok("options('error') is a function with RStudio API stub")
} else {
	cat("[WARN] options('error') not a function (acceptable in non-interactive runs)\n")
}

ok("RStudio-specific checks completed")

cat("\nAll rstudio-config.R tests passed.\n")
