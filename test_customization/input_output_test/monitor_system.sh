#!/bin/bash
# =================================================================
# System Resource Monitoring Script for RStudio Stress Testing
# Monitors CPU, Memory, Disk I/O during stress tests
# =================================================================
#
# Usage: ./monitor_system.sh [output_file] [interval_seconds]
# Example: ./monitor_system.sh stress_monitor.csv 5
# =================================================================

# Default parameters
OUTPUT_FILE=${1:-"system_monitor.csv"}
INTERVAL=${2:-10}

echo "=== System Resource Monitor for RStudio Stress Testing ==="
echo "Output file: $OUTPUT_FILE"
echo "Monitoring interval: ${INTERVAL} seconds"
echo "Press Ctrl+C to stop monitoring"
echo "=================================================="

# Create CSV header
echo "Timestamp,Memory_Used_GB,Memory_Free_GB,Memory_Available_GB,Memory_Percent,CPU_Load_1m,CPU_Load_5m,CPU_Load_15m,Disk_IO_Read_MB,Disk_IO_Write_MB,R_Processes,RStudio_Processes" > "$OUTPUT_FILE"

# Function to get disk I/O stats
get_disk_io() {
    # Get disk stats from /proc/diskstats or iostat if available
    if command -v iostat >/dev/null 2>&1; then
        iostat -d 1 1 | grep -E '^[sv]d[a-z]|^nvme' | head -1 | awk '{print $(NF-1)","$NF}'
    else
        # Fallback to /proc/diskstats
        awk '/sda|nvme/ {read_mb=$6/2048; write_mb=$10/2048; print read_mb","write_mb; exit}' /proc/diskstats
    fi
}

# Function to format timestamp
get_timestamp() {
    date '+%Y-%m-%d %H:%M:%S'
}

# Main monitoring loop
while true; do
    timestamp=$(get_timestamp)
    
    # Memory information (in GB and percentage)
    mem_info=$(free -g | grep '^Mem:')
    mem_total=$(echo $mem_info | awk '{print $2}')
    mem_used=$(echo $mem_info | awk '{print $3}')
    mem_free=$(echo $mem_info | awk '{print $4}')
    mem_available=$(echo $mem_info | awk '{print $7}')
    mem_percent=$(echo "$mem_used $mem_total" | awk '{printf "%.1f", ($1/$2)*100}')
    
    # CPU load averages
    load_avg=$(uptime | awk -F'load average:' '{print $2}' | tr -d ' ' | tr ',' ' ')
    load_1m=$(echo $load_avg | awk '{print $1}')
    load_5m=$(echo $load_avg | awk '{print $2}')
    load_15m=$(echo $load_avg | awk '{print $3}')
    
    # Disk I/O information
    disk_io=$(get_disk_io)
    if [ -z "$disk_io" ]; then
        disk_io="0,0"
    fi
    
    # Count R and RStudio processes
    r_procs=$(ps aux | grep -c '[Rr]script\|[Rr] --' || echo 0)
    rstudio_procs=$(ps aux | grep -c '[Rr]studio\|rserver\|rsession' || echo 0)
    
    # Output to CSV
    echo "$timestamp,$mem_used,$mem_free,$mem_available,$mem_percent,$load_1m,$load_5m,$load_15m,$disk_io,$r_procs,$rstudio_procs" >> "$OUTPUT_FILE"
    
    # Display current status
    echo "$(date '+%H:%M:%S') | Memory: ${mem_used}/${mem_total}GB (${mem_percent}%) | Load: $load_1m | R Procs: $r_procs | RStudio Procs: $rstudio_procs"
    
    sleep $INTERVAL
done
