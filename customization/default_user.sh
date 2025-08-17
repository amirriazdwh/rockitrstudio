#!/bin/bash
set -e

DEFAULT_USER=${1:-${DEFAULT_USER:-"rstudio"}}
DEFAULT_GROUP=${2:-${DEFAULT_GROUP:-"rstudio-users"}}

# Use GID 8500 for rstudio-users group, auto-assign for others
if [ "${DEFAULT_GROUP}" = "rstudio-users" ]; then
    GROUP_ID=8500
else
    GROUP_ID=""  # Let system auto-assign
fi

# Use UID 1000 for rstudio user, auto-assign for others
if [ "${DEFAULT_USER}" = "rstudio" ]; then
    USER_ID=1000
else
    USER_ID=""  # Let system auto-assign
fi

if getent group "${DEFAULT_GROUP}" >/dev/null 2>&1; then
    echo "Group ${DEFAULT_GROUP} already exists"
else
    if [ -n "$GROUP_ID" ]; then
        groupadd -g "$GROUP_ID" -r "${DEFAULT_GROUP}"
    else
        groupadd -r "${DEFAULT_GROUP}"
    fi
fi

if id -u "${DEFAULT_USER}" >/dev/null 2>&1; then
    echo "User ${DEFAULT_USER} already exists"
else
    ## Need to configure non-root user for RStudio
    if [ -n "$USER_ID" ]; then
        useradd -s /bin/bash -m -u "$USER_ID" -g "${DEFAULT_GROUP}" "$DEFAULT_USER"
    else
        useradd -s /bin/bash -m -g "${DEFAULT_GROUP}" "$DEFAULT_USER"
    fi
    echo "${DEFAULT_USER}:${DEFAULT_USER}" | chpasswd

    ## Rocker's default RStudio settings, for better reproducibility
    mkdir -p "/home/${DEFAULT_USER}/.config/rstudio/"
    cat <<EOF >"/home/${DEFAULT_USER}/.config/rstudio/rstudio-prefs.json"
{
    "save_workspace": "never",
    "always_save_history": false,
    "reuse_sessions_for_project_links": true,
    "posix_terminal_shell": "bash"
}
EOF
    chown -R "${DEFAULT_USER}:${DEFAULT_GROUP}" "/home/${DEFAULT_USER}"
fi

# If shiny server installed, make the user part of the shiny group
if [ -x "$(command -v shiny-server)" ]; then
    adduser "${DEFAULT_USER}" shiny
fi

## configure git not to request password each time
if [ -x "$(command -v git)" ]; then
    git config --system credential.helper 'cache --timeout=3600'
    git config --system push.default simple
fi
