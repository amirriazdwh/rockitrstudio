#!/bin/bash
# Script to create users dev1, dev2, dev3 with the same password

PASSWORD="changeme"
for USER in dev1 dev2 dev3; do
    if ! id "$USER" &>/dev/null; then
        useradd -m "$USER"
        echo "$USER:$PASSWORD" | chpasswd
        echo "User $USER created."
    else
        echo "User $USER already exists."
    fi
done
