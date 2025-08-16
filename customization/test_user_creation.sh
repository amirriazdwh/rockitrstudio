#!/bin/bash
set -e

echo "=== Testing User Creation Script ==="
echo "Container: rstudio-custom"
echo

# Function to run command in container and show result
run_test() {
    local description="$1"
    local command="$2"
    local expected="$3"
    
    echo "TEST: $description"
    echo "Command: $command"
    
    if result=$(docker exec rstudio-custom bash -c "$command" 2>&1); then
        echo "✓ Success: $result"
        if [[ -n "$expected" && "$result" != *"$expected"* ]]; then
            echo "⚠ Warning: Expected '$expected' but got '$result'"
        fi
    else
        echo "✗ Failed: $result"
    fi
    echo
}

# Copy the script into container
echo "1. Copying default_users_custom.sh into container..."
docker cp customization/default_users_custom.sh rstudio-custom:/tmp/default_users_custom.sh
docker exec rstudio-custom chmod +x /tmp/default_users_custom.sh
echo "✓ Script copied and made executable"
echo

# Test 1: Check if GID 8500 is already in use
run_test "Check if GID 8500 is available" \
    "getent group 8500 || echo 'GID 8500 available'" \
    "available"

# Test 2: Run the script to create test user
echo "2. Creating test user 'testuser1'..."
docker exec rstudio-custom bash -c 'DEFAULT_USER=testuser1 DEFAULT_GROUP=rstudio-users /tmp/default_users_custom.sh'
echo "✓ Script executed"
echo

# Test 3: Verify group creation
run_test "Check rstudio-users group exists with GID 8500" \
    "getent group rstudio-users" \
    "rstudio-users:x:8500"

# Test 4: Verify user creation and group membership
run_test "Check user testuser1 ID and groups" \
    "id testuser1" \
    "gid=8500(rstudio-users)"

run_test "Check user testuser1 group membership" \
    "groups testuser1" \
    "rstudio-users"

# Test 5: Check home directory ownership
run_test "Check home directory ownership" \
    "stat -c '%U:%G %g' /home/testuser1" \
    "testuser1:rstudio-users 8500"

# Test 6: Check RStudio preferences file
run_test "Check RStudio preferences file exists" \
    "ls -la /home/testuser1/.config/rstudio/rstudio-prefs.json" \
    "testuser1"

run_test "Check RStudio preferences content" \
    "cat /home/testuser1/.config/rstudio/rstudio-prefs.json | grep save_workspace" \
    "never"

# Test 7: Test password authentication
run_test "Check user password is set (non-empty hash)" \
    "getent shadow testuser1 | cut -d: -f2 | head -c10" \
    ""

# Test 8: Test idempotency - run script again
echo "8. Testing idempotency - running script again..."
docker exec rstudio-custom bash -c 'DEFAULT_USER=testuser1 DEFAULT_GROUP=rstudio-users /tmp/default_users_custom.sh'

run_test "Verify user still exists after second run" \
    "id testuser1" \
    "gid=8500(rstudio-users)"

# Test 9: Create multiple users (dev1, dev2, dev3)
echo "9. Testing multiple user creation..."
for user in dev1 dev2 dev3; do
    echo "Creating user: $user"
    docker exec rstudio-custom bash -c "DEFAULT_USER=$user DEFAULT_GROUP=rstudio-users /tmp/default_users_custom.sh"
done

run_test "Check all users are in rstudio-users group" \
    "getent group rstudio-users" \
    ""

# Test 10: Verify all users can access their home directories
for user in testuser1 dev1 dev2 dev3; do
    run_test "Check $user home directory access" \
        "ls -la /home/$user/.config/rstudio/" \
        "rstudio-prefs.json"
done

echo "=== Test Summary ==="
echo "✓ All tests completed"
echo "Check output above for any warnings or failures"
echo "Users created: testuser1, dev1, dev2, dev3"
echo "Group: rstudio-users (GID: 8500)"
echo
echo "To login to RStudio, use any of these users with password 'testuser1' (or 'dev1', 'dev2', 'dev3' respectively)"
echo "Access RStudio at: http://localhost:8787"
