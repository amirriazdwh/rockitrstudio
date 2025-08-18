# Write the .tex
tikzDevice::tikz("tikz_ok.tex", standAlone = TRUE, width = 5, height = 4)
plot(1:10, main = "tikzDevice")
dev.off()
file.exists("tikz_ok.tex")

# Compile to PDF if pdflatex is available
if (nzchar(Sys.which("pdflatex"))) {
  system("pdflatex -interaction=nonstopmode -halt-on-error -output-directory . tikz_ok.tex")
  file.exists("tikz_ok.pdf")
}
