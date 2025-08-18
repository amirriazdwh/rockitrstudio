ragg::agg_png("plot@144dpi.png", width = 1200, height = 800, res = 144)
plot(1:10, main = "ragg @ 144 DPI")
dev.off()
