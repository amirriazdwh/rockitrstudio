ragg::agg_png("gridextra.png", width = 1200, height = 800, res = 144)
g <- ggplot2::ggplot(iris, ggplot2::aes(Sepal.Length, Sepal.Width, color = Species)) +
  ggplot2::geom_point()
gridExtra::grid.arrange(ggplot2::ggplotGrob(g))
dev.off()
file.exists("gridextra.png")