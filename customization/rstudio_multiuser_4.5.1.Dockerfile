# syntax=docker/dockerfile:1

FROM docker.io/library/ubuntu:noble

ENV R_VERSION="4.5.1"
ENV R_HOME="/usr/local/lib/R"
ENV TZ="Etc/UTC"

COPY scripts/install_R_source.sh /rocker_scripts/install_R_source.sh
RUN /rocker_scripts/install_R_source.sh

ENV CRAN="https://p3m.dev/cran/__linux__/noble/latest"
ENV LANG=en_US.UTF-8

COPY scripts/bin/ /rocker_scripts/bin/
COPY scripts/setup_R.sh /rocker_scripts/setup_R.sh
RUN <<EOF
if grep -q "1000" /etc/passwd; then
    userdel --remove "$(id -un 1000)";
fi
/rocker_scripts/setup_R.sh
EOF

ENV S6_VERSION="v2.1.0.2"
ENV RSTUDIO_VERSION="2025.05.1+513"
ENV DEFAULT_USER="rstudio"
ENV DEFAULT_GROUP="rstudio-users"

COPY scripts/install_rstudio.sh /rocker_scripts/install_rstudio.sh
COPY scripts/install_s6init.sh /rocker_scripts/install_s6init.sh
COPY customization/default_users.sh /rocker_scripts/default_users.sh
COPY scripts/init_set_env.sh /rocker_scripts/init_set_env.sh
COPY customization/init_userconf.sh /rocker_scripts/init_userconf.sh
COPY scripts/pam-helper.sh /rocker_scripts/pam-helper.sh

# Make scripts executable
RUN chmod +x /rocker_scripts/default_user.sh /rocker_scripts/default_users.sh

# Install RStudio (this will create default rstudio user via default_user.sh)
RUN /rocker_scripts/install_rstudio.sh

COPY scripts/install_pandoc.sh /rocker_scripts/install_pandoc.sh
RUN /rocker_scripts/install_pandoc.sh

# Create all users: rstudio, dev1, dev2, dev3 with rstudio-users group (GID 8500)
RUN DEFAULT_USER=dev1 DEFAULT_GROUP=rstudio-users /rocker_scripts/default_users.sh
RUN DEFAULT_USER=dev2 DEFAULT_GROUP=rstudio-users /rocker_scripts/default_users.sh
RUN DEFAULT_USER=dev3 DEFAULT_GROUP=rstudio-users /rocker_scripts/default_users.sh

COPY scripts/install_quarto.sh /rocker_scripts/install_quarto.sh
RUN /rocker_scripts/install_quarto.sh

EXPOSE 8787
CMD ["/init"]
