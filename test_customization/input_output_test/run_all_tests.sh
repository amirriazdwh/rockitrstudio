#!/bin/bash
# =================================================================
# RStudio Container Stress Test Runner
# Automated script to run all stress tests in sequence
# =================================================================
#
# This script runs all stress testing scripts in the correct order
# and generates a comprehensive report
#
# Usage: ./run_all_tests.sh [container_name]
# Example: ./run_all_tests.sh rstudio-optimized
# =================================================================

CONTAINER_NAME=${1:-"rstudio-optimized"}
TEST_USER=${2:-"cdsw"}
TEST_DIR="/home/$TEST_USER/stress_tests"
RESULTS_DIR="./test_results_$(date +%Y%m%d_%H%M%S)"

echo "================================================================="
echo "RStudio Container Comprehensive Stress Test Suite"
echo "================================================================="
echo "Container: $CONTAINER_NAME"
echo "Test User: $TEST_USER"
echo "Results Directory: $RESULTS_DIR"
echo "================================================================="

# Create results directory
mkdir -p "$RESULTS_DIR"

# Function to run test in container
run_container_test() {
    local test_name="$1"
    local script_path="$2"
    local output_file="$RESULTS_DIR/${test_name}_output.txt"
    local timing_file="$RESULTS_DIR/${test_name}_timing.txt"
    
    echo "Running $test_name..."
    echo "Output will be saved to: $output_file"
    
    # Record start time
    start_time=$(date +%s)
    echo "Test started: $(date)" > "$timing_file"
    
    # Copy test script to container and run
    docker cp "$script_path" "$CONTAINER_NAME:/tmp/$(basename $script_path)"
    
    # Run the test
    docker exec -u "$TEST_USER" "$CONTAINER_NAME" Rscript "/tmp/$(basename $script_path)" > "$output_file" 2>&1
    test_exit_code=$?
    
    # Record end time
    end_time=$(date +%s)
    duration=$((end_time - start_time))
    echo "Test completed: $(date)" >> "$timing_file"
    echo "Duration: ${duration} seconds" >> "$timing_file"
    echo "Exit code: $test_exit_code" >> "$timing_file"
    
    if [ $test_exit_code -eq 0 ]; then
        echo "✅ $test_name completed successfully (${duration}s)"
    else
        echo "❌ $test_name failed with exit code $test_exit_code (${duration}s)"
    fi
    
    return $test_exit_code
}

# Function to start system monitoring
start_monitoring() {
    local monitor_output="$RESULTS_DIR/system_monitor.csv"
    echo "Starting system monitoring..."
    
    # Copy monitoring script to container
    docker cp "./monitor_system.sh" "$CONTAINER_NAME:/tmp/monitor_system.sh"
    docker exec "$CONTAINER_NAME" chmod +x "/tmp/monitor_system.sh"
    
    # Start monitoring in background
    docker exec -d "$CONTAINER_NAME" bash -c "/tmp/monitor_system.sh $monitor_output 5" > /dev/null 2>&1
    
    echo "System monitoring started, output: $monitor_output"
}

# Function to stop monitoring
stop_monitoring() {
    echo "Stopping system monitoring..."
    docker exec "$CONTAINER_NAME" pkill -f "monitor_system.sh" 2>/dev/null || true
    
    # Copy monitoring results
    docker cp "$CONTAINER_NAME:$TEST_DIR/system_monitor.csv" "$RESULTS_DIR/" 2>/dev/null || true
}

# Check if container is running
if ! docker ps | grep -q "$CONTAINER_NAME"; then
    echo "❌ Container '$CONTAINER_NAME' is not running!"
    echo "Please start the container first:"
    echo "  docker start $CONTAINER_NAME"
    exit 1
fi

# Verify test scripts exist
test_scripts=(
    "./simple_stress_test.R"
    "./arrow_parallelism_test.R"
    "./memory_breaking_point.R"
    "./comprehensive_stress_test.R"
)

missing_scripts=()
for script in "${test_scripts[@]}"; do
    if [ ! -f "$script" ]; then
        missing_scripts+=("$script")
    fi
done

if [ ${#missing_scripts[@]} -ne 0 ]; then
    echo "❌ Missing test scripts:"
    printf '%s\n' "${missing_scripts[@]}"
    echo "Please ensure all test scripts are in the current directory."
    exit 1
fi

# Create test directory in container
docker exec -u "$TEST_USER" "$CONTAINER_NAME" mkdir -p "$TEST_DIR"

# Start system monitoring
start_monitoring

# Create overall test summary
summary_file="$RESULTS_DIR/test_summary.txt"
{
    echo "================================================================="
    echo "RStudio Container Stress Test Summary"
    echo "================================================================="
    echo "Test Date: $(date)"
    echo "Container: $CONTAINER_NAME"
    echo "Test User: $TEST_USER"
    echo "================================================================="
    echo
} > "$summary_file"

# Run tests in sequence
total_tests=0
passed_tests=0
failed_tests=0

echo
echo "Starting test sequence..."
echo "========================="

# Test 1: Simple Stress Test
echo
echo "Test 1/4: Simple 10GB CSV Arrow Performance Test"
echo "================================================"
total_tests=$((total_tests + 1))
if run_container_test "simple_stress" "./simple_stress_test.R"; then
    passed_tests=$((passed_tests + 1))
    echo "✅ Simple Stress Test: PASSED" >> "$summary_file"
else
    failed_tests=$((failed_tests + 1))
    echo "❌ Simple Stress Test: FAILED" >> "$summary_file"
fi

sleep 5  # Brief pause between tests

# Test 2: Arrow Parallelism Test
echo
echo "Test 2/4: Arrow Parallelism Optimization Test"
echo "============================================="
total_tests=$((total_tests + 1))
if run_container_test "arrow_parallelism" "./arrow_parallelism_test.R"; then
    passed_tests=$((passed_tests + 1))
    echo "✅ Arrow Parallelism Test: PASSED" >> "$summary_file"
else
    failed_tests=$((failed_tests + 1))
    echo "❌ Arrow Parallelism Test: FAILED" >> "$summary_file"
fi

sleep 5

# Test 3: Memory Breaking Point
echo
echo "Test 3/4: Memory Breaking Point Analysis"
echo "======================================="
total_tests=$((total_tests + 1))
if run_container_test "memory_breaking_point" "./memory_breaking_point.R"; then
    passed_tests=$((passed_tests + 1))
    echo "✅ Memory Breaking Point Test: PASSED" >> "$summary_file"
else
    failed_tests=$((failed_tests + 1))
    echo "❌ Memory Breaking Point Test: FAILED" >> "$summary_file"
fi

sleep 5

# Test 4: Comprehensive Stress Test
echo
echo "Test 4/4: Comprehensive Stress Test"
echo "=================================="
total_tests=$((total_tests + 1))
if run_container_test "comprehensive_stress" "./comprehensive_stress_test.R"; then
    passed_tests=$((passed_tests + 1))
    echo "✅ Comprehensive Stress Test: PASSED" >> "$summary_file"
else
    failed_tests=$((failed_tests + 1))
    echo "❌ Comprehensive Stress Test: FAILED" >> "$summary_file"
fi

# Stop monitoring
stop_monitoring

# Generate final summary
{
    echo
    echo "================================================================="
    echo "FINAL TEST SUMMARY"
    echo "================================================================="
    echo "Total Tests: $total_tests"
    echo "Passed: $passed_tests"
    echo "Failed: $failed_tests"
    echo "Success Rate: $(( passed_tests * 100 / total_tests ))%"
    echo
    echo "Test completion time: $(date)"
    echo "================================================================="
} >> "$summary_file"

echo
echo "================================================================="
echo "ALL TESTS COMPLETED"
echo "================================================================="
echo "Total Tests: $total_tests"
echo "Passed: $passed_tests"
echo "Failed: $failed_tests"
echo "Success Rate: $(( passed_tests * 100 / total_tests ))%"
echo
echo "Results saved in: $RESULTS_DIR"
echo "Summary: $summary_file"
echo

# Display summary
cat "$summary_file"

# Generate HTML report if possible
if command -v pandoc >/dev/null 2>&1; then
    echo "Generating HTML report..."
    {
        echo "# RStudio Container Stress Test Report"
        echo
        echo "Generated on: $(date)"
        echo
        echo "## Test Summary"
        echo
        echo "- **Container**: $CONTAINER_NAME"
        echo "- **Test User**: $TEST_USER"
        echo "- **Total Tests**: $total_tests"
        echo "- **Passed**: $passed_tests"
        echo "- **Failed**: $failed_tests"
        echo "- **Success Rate**: $(( passed_tests * 100 / total_tests ))%"
        echo
        echo "## Test Results"
        echo
        for test_output in "$RESULTS_DIR"/*_output.txt; do
            if [ -f "$test_output" ]; then
                test_name=$(basename "$test_output" _output.txt)
                echo "### $test_name"
                echo
                echo '```'
                head -50 "$test_output"
                echo '```'
                echo
            fi
        done
    } > "$RESULTS_DIR/report.md"
    
    pandoc "$RESULTS_DIR/report.md" -o "$RESULTS_DIR/stress_test_report.html" 2>/dev/null && \
        echo "HTML report generated: $RESULTS_DIR/stress_test_report.html"
fi

echo
echo "To view individual test results:"
echo "  ls -la $RESULTS_DIR/"
echo
echo "To view test output:"
echo "  cat $RESULTS_DIR/*_output.txt"

exit $failed_tests
