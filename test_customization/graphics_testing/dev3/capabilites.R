capabilities(c("cairo","png","jpeg","tiff","X11"))
getOption("bitmapType")  # should be "cairo"

ok <- sapply(c("ragg","svglite","ggplot2","gridExtra","gridBase","tikzDevice"),
             requireNamespace, quietly = TRUE)
