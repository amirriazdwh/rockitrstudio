# Render the sample ggplot in the RStudio Viewer
# - Uses plotly::ggplotly when available (shows in Viewer automatically)
# - Falls back to saving a Cairo PNG and opening a tiny HTML in Viewer

# Ensure ggplot2
if (!requireNamespace("ggplot2", quietly = TRUE)) {
  install.packages("ggplot2")
}
library(ggplot2)

# Your sample data and plot
plot_data <- data.frame(
  x = 1:10,
  y = c(2, 5, 3, 8, 7, 9, 6, 10, 12, 15)
)

p <- ggplot(plot_data, aes(x = x, y = y)) +
  geom_point(size = 3, color = "blue") +
  geom_line(color = "red", linetype = "dashed") +
  geom_smooth(method = "lm", se = FALSE) +
  labs(title = "Sample ggplot2 Graph", x = "X-axis Label", y = "Y-axis Label") +
  theme_minimal()

# Prefer interactive Viewer via plotly if available
if (requireNamespace("plotly", quietly = TRUE)) {
  v <- plotly::ggplotly(p)
  print(v)  # In RStudio, htmlwidgets render in the Viewer pane
  message("Opened ggplot in Viewer via plotly::ggplotly")
} else {
  # Fallback: save PNG using Cairo and open an HTML page in Viewer
  if (!capabilities("cairo")) stop("Cairo is not available in this R session")
  tmpdir <- tempdir()
  png_path <- file.path(tmpdir, "ggplot_viewer_fallback.png")
  html_path <- file.path(tmpdir, "ggplot_viewer_fallback.html")

  png(png_path, width = 1000, height = 700, res = 120, type = "cairo")
  print(p)
  dev.off()

  html <- sprintf(
    "<!doctype html><meta charset='utf-8'><title>ggplot Viewer</title>\n<style>body{font-family:sans-serif;margin:1rem;}img{max-width:100%%;height:auto;border:1px solid #ddd;}</style>\n<h1>Sample ggplot2 Graph (PNG)</h1><img src='%s'>",
    basename(png_path)
  )

  old <- setwd(tmpdir); on.exit(setwd(old), add = TRUE)
  writeLines(html, basename(html_path))

  if (requireNamespace("rstudioapi", quietly = TRUE) && rstudioapi::isAvailable()) {
    rstudioapi::viewer(html_path)
  } else {
    utils::browseURL(html_path)
  }
  message("Opened fallback PNG in Viewer: ", html_path)
}
