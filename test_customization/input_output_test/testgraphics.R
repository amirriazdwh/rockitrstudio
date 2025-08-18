#!/usr/bin/env Rscript

# Check base graphics capabilities
cat("🔍 Verifying R graphics capabilities...\n\n")
caps <- capabilities()
graphics_keys <- c("X11", "cairo", "png", "jpeg", "tiff", "X11cairo", "aqua", "svg", "profmem", "tikz")

for (k in graphics_keys) {
  value <- if (k %in% names(caps)) caps[[k]] else NA
  status <- if (isTRUE(value)) "✅ Available" else "❌ Missing"
  cat(sprintf("  %-12s : %s\n", k, status))
}

# Check if Cairo package is usable
cat("\n📦 Checking if 'Cairo' package is installed and working...\n")
if (!requireNamespace("Cairo", quietly = TRUE)) {
  cat("❌ 'Cairo' package not installed\n")
} else {
  tryCatch({
    Cairo::CairoPNG(file = tempfile(fileext = ".png"), width = 400, height = 400)
    plot(1:10, main = "Test Cairo Plot")
    dev.off()
    cat("✅ 'Cairo' package is functional\n")
  }, error = function(e) {
    cat("❌ 'Cairo' failed: ", conditionMessage(e), "\n")
  })
}

# Check if svglite is usable
cat("\n📦 Checking if 'svglite' package is installed and working...\n")
if (!requireNamespace("svglite", quietly = TRUE)) {
  cat("❌ 'svglite' package not installed\n")
} else {
  tryCatch({
    svglite::svglite(file = tempfile(fileext = ".svg"), width = 5, height = 5)
    plot(1:5, main = "SVG Test")
    dev.off()
    cat("✅ 'svglite' package is functional\n")
  }, error = function(e) {
    cat("❌ 'svglite' failed: ", conditionMessage(e), "\n")
  })
}
