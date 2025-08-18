# RStudio Container Stress Testing Suite

This testing suite provides comprehensive performance analysis and stress testing for RStudio Server Docker containers. The suite is designed to be completely self-contained and can run in any environment, including on-premises systems without AI assistance.

## 📁 Test Scripts Overview

### Core Stress Tests

1. **`simple_stress_test.R`** - Quick 10GB CSV generation and Arrow performance validation
   - Generates 10 million row dataset with mixed data types
   - Tests Arrow single vs multi-threaded performance
   - Compares with base R read.csv performance
   - **Runtime**: ~2-5 minutes

2. **`comprehensive_stress_test.R`** - Complete system stress analysis
   - 10GB dataset generation with detailed metrics
   - Multiple I/O method comparisons (Arrow, data.table, base R)
   - Memory breaking point analysis with progressive dataset sizes
   - System resource monitoring and recommendations
   - **Runtime**: ~10-20 minutes

3. **`arrow_parallelism_test.R`** - Arrow optimization analysis
   - Tests different core/thread configurations
   - Identifies optimal parallelism settings
   - CPU efficiency analysis
   - Scaling factor measurements
   - **Runtime**: ~5-10 minutes

4. **`memory_breaking_point.R`** - Memory limit analysis
   - Progressive memory stress testing
   - Tests datasets from 1GB to 30GB
   - Identifies system breaking points
   - Memory efficiency analysis
   - **Runtime**: ~15-30 minutes (stops at breaking point)

### System Monitoring

5. **`monitor_system.sh`** - Real-time system monitoring
   - Tracks CPU, memory, disk I/O during tests
   - CSV output for analysis
   - Configurable monitoring intervals
   - **Usage**: `./monitor_system.sh [output_file] [interval_seconds]`

6. **`run_all_tests.sh`** - Automated test suite runner
   - Runs all tests in optimal sequence
   - Automatic system monitoring
   - Generates comprehensive reports
   - HTML report generation (if pandoc available)
   - **Usage**: `./run_all_tests.sh [container_name] [test_user]`

## 🚀 Quick Start Guide

### Prerequisites

Ensure your RStudio container has these R packages installed:
```r
install.packages(c("arrow", "data.table", "parallel"))
```

### Running Individual Tests

```bash
# Quick performance check
Rscript simple_stress_test.R

# Full system analysis
Rscript comprehensive_stress_test.R

# Arrow optimization
Rscript arrow_parallelism_test.R

# Memory limits
Rscript memory_breaking_point.R
```

### Running Complete Test Suite

```bash
# Make scripts executable
chmod +x *.sh

# Run all tests with monitoring
./run_all_tests.sh rstudio-optimized cdsw
```

### Docker Container Testing

```bash
# Copy tests to container
docker cp Testing/ container_name:/home/user/

# Run inside container
docker exec -u user container_name bash -c "cd /home/user/Testing && ./run_all_tests.sh"
```

## 📊 Expected Test Results

### Performance Benchmarks

Based on our 28GB RAM, 8-core optimized environment:

| Test | Expected Result | Optimal Setting |
|------|----------------|-----------------|
| **CSV Generation** | 50-60 MB/s | data.table::fwrite |
| **Arrow Single-thread** | 200-220 MB/s | 1 core |
| **Arrow Multi-thread** | 220-250 MB/s | 8 cores |
| **Memory Limit** | 20-25 GB | Before breaking point |
| **CPU Efficiency** | 25-35 MB/s/core | Multi-threaded Arrow |

### System Optimization Indicators

✅ **Good Performance**: 
- Arrow throughput > 200 MB/s
- Multi-threading improvement > 5%
- Memory utilization < 80% at breaking point

⚠️ **Needs Optimization**:
- Arrow throughput < 150 MB/s
- No multi-threading benefit
- Memory breaking point < 15GB

❌ **System Issues**:
- Frequent test failures
- Arrow throughput < 100 MB/s
- Breaking point < 10GB

## 🔧 Customization Options

### Adjusting Test Parameters

Edit these variables in the test scripts:

```r
# In simple_stress_test.R
target_rows <- 10000000  # Change dataset size

# In comprehensive_stress_test.R
test_sizes_gb <- c(5, 15, 20, 25)  # Memory test progression

# In arrow_parallelism_test.R
test_file_size_gb <- 2  # Test file size
core_configs <- c(1, 2, 4, 8)  # Core configurations to test
```

### Environment Variables

Set these for optimal Arrow performance:
```bash
export OMP_NUM_THREADS=8
export ARROW_CPU_COUNT=8
export R_ENABLE_JIT=3
```

## 📈 Interpreting Results

### Performance Metrics

- **Throughput (MB/s)**: Higher is better, indicates I/O performance
- **CPU Efficiency (MB/s/core)**: Parallel processing effectiveness
- **Memory Overhead**: Ratio of peak memory to dataset size
- **Scaling Factor**: Multi-core vs single-core performance improvement

### Breaking Point Analysis

- **Safe Limit**: 80% of maximum successful dataset size
- **Memory Ratio**: Typical overhead is 1.2-1.5x dataset size
- **System Capacity**: Maximum dataset before failure

### Optimization Recommendations

The tests automatically generate recommendations for:
- Optimal Arrow configuration
- Safe dataset size limits
- Memory management settings
- CPU utilization strategies

## 🛠 Troubleshooting

### Common Issues

**Arrow Installation Problems**:
```bash
# Reinstall with all dependencies
docker exec container R -e "install.packages('arrow', dependencies=TRUE)"
```

**Memory Errors**:
```r
# Increase R memory limits
options(java.parameters = "-Xmx8g")
```

**Performance Issues**:
```bash
# Check JIT compilation
docker exec container R -e "cat(Sys.getenv('R_ENABLE_JIT'))"
```

### Test Failures

1. **Script Permission Errors**: Run `chmod +x *.sh`
2. **Package Missing**: Install arrow, data.table, parallel
3. **Memory Insufficient**: Reduce test dataset sizes
4. **Container Access**: Verify user permissions

## 📋 Test Validation Checklist

- [ ] All 4 core test scripts run without errors
- [ ] Arrow throughput > 200 MB/s
- [ ] Multi-threading shows improvement
- [ ] Memory breaking point identified
- [ ] System monitoring captures data
- [ ] Comprehensive report generated

## 🎯 Performance Targets

### Minimum Acceptable Performance
- Arrow read: > 150 MB/s
- Memory capacity: > 15GB datasets
- Multi-threading benefit: > 3%

### High Performance Targets  
- Arrow read: > 250 MB/s
- Memory capacity: > 25GB datasets
- Multi-threading benefit: > 10%

### Exceptional Performance
- Arrow read: > 300 MB/s
- Memory capacity: > 30GB datasets
- Multi-threading benefit: > 15%

## 📝 Output Files

Each test generates specific output files:

- `*_output.txt`: Complete test output and results
- `*_timing.txt`: Test duration and performance metrics
- `system_monitor.csv`: Real-time system resource usage
- `test_summary.txt`: Overall test results summary
- `stress_test_report.html`: Formatted HTML report (if pandoc available)

## 🔄 Continuous Testing

For ongoing performance monitoring:

```bash
# Daily performance check
0 2 * * * /path/to/Testing/simple_stress_test.R

# Weekly comprehensive analysis
0 1 * * 0 /path/to/Testing/run_all_tests.sh
```

This testing suite provides everything needed for comprehensive RStudio performance validation in any environment. All scripts are self-contained and designed for reliable execution without external dependencies beyond the base R packages.
