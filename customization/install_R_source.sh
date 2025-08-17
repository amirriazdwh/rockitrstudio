#!/usr/bin/env bash
# Minimal, base-package-style R source install for Docker (Jammy/Noble)
# - Uses defaults via: VAR="${VAR:-default}"
# - Headless + Cairo, OpenBLAS, LTO, safe -O2 (or AVX2 tuning if you flip the flag)
# - Keeps deps minimal; optional TEX/TclTk via flags

set -euo pipefail

R_VERSION="${R_VERSION:-latest}"
PURGE_BUILDDEPS="${PURGE_BUILDDEPS:-true}"
AVX2_FLEET="${AVX2_FLEET:-false}"
WITH_TEX="${WITH_TEX:-false}"
WITH_TCLTK="${WITH_TCLTK:-false}"
R_HOME="${R_HOME:-/usr/local/lib/R}"
LANG="${LANG:-en_US.UTF-8}"
export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get -y install --no-install-recommends locales ca-certificates
/usr/sbin/locale-gen --lang "${LANG}"
/usr/sbin/update-locale --reset LANG="${LANG}"

# ------- helper: pick the first available package name -------
pick_pkg() {
  for p in "$@"; do
    if apt-cache show "$p" >/dev/null 2>&1; then
      echo "$p"; return 0
    fi
  done
  return 1
}

# ------- runtime libs (minimal) -------
TIFF_RT="$(pick_pkg libtiff6 libtiff5 || true)"
ICU_RT="$(pick_pkg libicu76 libicu75 libicu74 libicu72 libicu71 libicu70 || true)"
OPENBLAS_RT="$(pick_pkg libopenblas0-openmp libopenblas0-pthread libopenblas0 || true)"
LAPACK_RT="$(pick_pkg liblapack3 liblapack0 || true)"

RUNTIME_PKGS=(bash-completion file tzdata unzip zip
  g++ gfortran make
  libcurl4 libbz2-1.0 zlib1g liblzma5
  libpcre2-8-0 libreadline8
  libcairo2 libpangocairo-1.0-0
  libjpeg-turbo8 libpng16-16)

[[ -n "${TIFF_RT}"     ]] && RUNTIME_PKGS+=("${TIFF_RT}")
[[ -n "${ICU_RT}"      ]] && RUNTIME_PKGS+=("${ICU_RT}")
[[ -n "${OPENBLAS_RT}" ]] && RUNTIME_PKGS+=("${OPENBLAS_RT}")
[[ -n "${LAPACK_RT}"   ]] && RUNTIME_PKGS+=("${LAPACK_RT}")

apt-get install -y --no-install-recommends "${RUNTIME_PKGS[@]}"

# ------- build deps (purged later) -------
BUILDDEPS=(curl wget rsync perl devscripts subversion
  libcurl4-openssl-dev libbz2-dev zlib1g-dev liblzma-dev
  libpcre2-dev libpng-dev libjpeg-dev libtiff-dev
  libreadline-dev libicu-dev
  libcairo2-dev libpango1.0-dev
  libopenblas-dev liblapack-dev)

if [[ "${WITH_TEX}" == "true" ]]; then
  BUILDDEPS+=(texinfo texlive-extra-utils texlive-fonts-recommended
              texlive-fonts-extra texlive-latex-recommended texlive-latex-extra)
fi
if [[ "${WITH_TCLTK}" == "true" ]]; then
  BUILDDEPS+=(tcl-dev tk-dev libx11-dev libxt-dev xauth xfonts-base)
fi

apt-get install -y --no-install-recommends "${BUILDDEPS[@]}"

# ------- fetch R source -------
download_r_src () {
  wget "https://cloud.r-project.org/src/$1" -O R.tar.gz || \
  wget "https://cran.r-project.org/src/$1"  -O R.tar.gz
}
case "${R_VERSION}" in
  devel)   download_r_src "base-prerelease/R-devel.tar.gz" ;;
  patched) download_r_src "base-prerelease/R-latest.tar.gz" ;;
  latest)  download_r_src "base/R-latest.tar.gz" ;;
  *)       download_r_src "base/R-${R_VERSION%%.*}/R-${R_VERSION}.tar.gz" ;;
esac

tar xzf R.tar.gz
cd R-*/

# ------- compiler/linker flags -------
if [[ "${AVX2_FLEET}" == "true" ]]; then
  CFLAGS="-O3 -pipe -march=x86-64-v3 -mtune=generic -fno-plt"
  CXXFLAGS="${CFLAGS}"
  FFLAGS="-O3 -pipe -march=x86-64-v3 -mtune=generic"
  FCFLAGS="${FFLAGS}"
else
  CFLAGS="-O2 -pipe -fstack-protector-strong -D_FORTIFY_SOURCE=2 -fno-plt"
  CXXFLAGS="${CFLAGS}"
  FFLAGS="-O2 -pipe"
  FCFLAGS="${FFLAGS}"
fi
LDFLAGS="-Wl,-O1 -Wl,--as-needed"

# ------- configure options (base feature set) -------
CONFIG_OPTS=(
  --prefix=/usr/local
  --enable-R-shlib
  --enable-lto
  --enable-memory-profiling
  --with-blas="-lopenblas"
  --with-lapack
  --with-readline
  --with-recommended-packages
  --with-cairo
)
if [[ "${WITH_TCLTK}" == "true" ]]; then
  CONFIG_OPTS+=(--with-tcltk)
else
  CONFIG_OPTS+=(--without-x)
fi

R_PAPERSIZE=letter LIBnn=lib \
  ./configure "${CONFIG_OPTS[@]}" \
    CFLAGS="${CFLAGS}" CXXFLAGS="${CXXFLAGS}" \
    FFLAGS="${FFLAGS}" FCFLAGS="${FCFLAGS}" LDFLAGS="${LDFLAGS}"

# ------- build & install -------
if command -v nproc >/dev/null 2>&1; then
  J=$(( $(nproc) > 1 ? $(nproc --ignore=1) : 1 ))
else
  J=1
fi
make -j"${J}"
make install
make clean

# ------- site-library & defaults -------
mkdir -p "${R_HOME}/site-library"
chgrp -R staff "${R_HOME}/site-library" 2>/dev/null || true
chmod g+ws "${R_HOME}/site-library"
echo "R_LIBS=\${R_LIBS-'${R_HOME}/site-library:${R_HOME}/library'}" >> "${R_HOME}/etc/Renviron.site"

# keep checkbashisms before purging devscripts (if present)
if command -v checkbashisms >/dev/null 2>&1; then
  cp /usr/bin/checkbashisms /usr/local/bin/checkbashisms || true
fi

# ------- cleanup -------
cd ..
rm -rf /tmp/* R-*/ R.tar.gz

if [[ "${PURGE_BUILDDEPS}" != "false" ]]; then
  apt-get purge -y "${BUILDDEPS[@]}" || true
fi
apt-get autoremove -y
apt-get autoclean -y
rm -rf /var/lib/apt/lists/*

# ------- summary -------
R -q -e "sessionInfo(); \
          cat('\ncapabilities(cairo)=', capabilities('cairo'), '\n'); \
          cat('BLAS:', tryCatch(extSoftVersion()[['BLAS']], error=function(e) 'n/a'), \
              '\nLAPACK:', tryCatch(extSoftVersion()[['LAPACK']], error=function(e) 'n/a'), '\n')"

echo -e "\n✅ R source install complete."
