ragg::agg_png("ggplot_ragg.png", width = 1200, height = 800, res = 144)
print( ggplot2::ggplot(mtcars, ggplot2::aes(mpg, wt)) + ggplot2::geom_point() +
         ggplot2::labs(title = "ggplot2 on ragg") )
dev.off()
file.exists("ggplot_ragg.png")
