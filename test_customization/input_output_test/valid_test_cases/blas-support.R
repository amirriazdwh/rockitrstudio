install.packages("RhpcBLASctl")
library(RhpcBLASctl)

blas_get_vendor()
blas_get_num_procs()


# 1) Confirm BLAS path (you already saw this)
sessionInfo()$BLAS

# 2) See what BLAS/LAPACK R loaded at runtime
getLoadedDLLs()[c("Rlapack","Rblas")]

# 3) OS-level linkage (path may vary)
system('ldd $(R RHOME)/lib/libRblas.so')


# Your R is under /usr/lib/R, not /usr/local/lib/R. Use R.home() to build the correct path:

R.home()                             # should print "/usr/lib/R"
rb <- file.path(R.home("lib"), "libRblas.so")
rb
file.exists(rb)                      # TRUE on Debian/Ubuntu builds
system(paste("ldd", shQuote(rb)))    # shows which BLAS it links to