# syntax=docker/dockerfile:1

FROM docker.io/library/ubuntu:noble

# Install basic utilities for testing
RUN apt-get update && apt-get install -y \
    bash \
    passwd \
    && rm -rf /var/lib/apt/lists/*

# Copy the custom user creation script
COPY customization/default_users_custom.sh /tmp/default_users_custom.sh
RUN chmod +x /tmp/default_users_custom.sh

# Create test users: testuser1, dev1, dev2, dev3 with rstudio-users group (GID 8500)
RUN DEFAULT_USER=testuser1 DEFAULT_GROUP=rstudio-users /tmp/default_users_custom.sh
RUN DEFAULT_USER=dev1 DEFAULT_GROUP=rstudio-users /tmp/default_users_custom.sh
RUN DEFAULT_USER=dev2 DEFAULT_GROUP=rstudio-users /tmp/default_users_custom.sh
RUN DEFAULT_USER=dev3 DEFAULT_GROUP=rstudio-users /tmp/default_users_custom.sh

# Verify the setup
RUN echo "=== User Creation Test Results ===" && \
    echo "Group created:" && getent group rstudio-users && \
    echo "Users created:" && \
    id testuser1 && \
    id dev1 && \
    id dev2 && \
    id dev3 && \
    echo "Home directories:" && \
    ls -la /home/ && \
    echo "=== Test completed successfully ==="

# Simple command to keep container running for inspection
CMD ["bash", "-c", "echo 'User creation test completed. Container ready for inspection.'; tail -f /dev/null"]


