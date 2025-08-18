# Graphics packages we care about
pkgs <- c("systemfonts","textshaping","ragg","svglite","ggplot2","gridExtra","gridBase","tikzDevice")

ip <- installed.packages()
have <- pkgs %in% rownames(ip)
ver  <- ifelse(have, ip[pkgs,"Version"], NA_character_)

cat("\n==== Graphics packages ====\n")
print(data.frame(package = pkgs, installed = have, version = ver), row.names = FALSE)

cat("\n==== Graphics capabilities ====\n")
caps <- c(cairo = capabilities("cairo"),
          png   = capabilities("png"),
          jpeg  = capabilities("jpeg"),
          tiff  = capabilities("tiff"))
print(caps)
cat("bitmapType =", getOption("bitmapType"), "\n")
pdflatex <- Sys.which("pdflatex")
cat("pdflatex   =", if (nzchar(pdflatex)) pdflatex else "<not found>", "\n")

# Exit 0 if all installed, 1 if anything missing (has no effect in RStudio; informative for CI)
if (!all(have)) {
  cat("\nMissing packages:", paste(pkgs[!have], collapse = ", "), "\n")
}
invisible()
