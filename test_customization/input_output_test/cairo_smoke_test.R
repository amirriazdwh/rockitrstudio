# Cairo graphics smoke test: line graph and QQ plot

if (!capabilities("cairo")) {
  stop("Cairo is not available in this R session")
}

# Line graph
png("/tmp/test_cairo_line.png", width = 1000, height = 700, res = 120, type = "cairo")
set.seed(123)
y <- cumsum(rnorm(300))
plot(y, type = "l", lwd = 2, col = "steelblue",
     main = "Line Graph (Cairo PNG)", xlab = "Index", ylab = "Value")
grid()
dev.off()

# QQ plot
png("/tmp/test_cairo_qq.png", width = 1000, height = 700, res = 120, type = "cairo")
set.seed(123)
x <- rnorm(2000)
qqnorm(x, main = "QQ Plot (Cairo PNG)")
qqline(x, col = "firebrick", lwd = 2)
dev.off()

cat("OK: wrote /tmp/test_cairo_line.png and /tmp/test_cairo_qq.png\n")
print(capabilities())
