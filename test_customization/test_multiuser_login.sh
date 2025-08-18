#!/bin/bash

echo "=== MultiUser RStudio Test ==="
echo ""

echo "Container Status:"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep rstudio

echo ""
echo "Created Users:"
docker exec rstudio cat /etc/passwd | grep -E "(dev1|dev2|dev3|rstudio)" | grep -v rstudio-server

echo ""
echo "Groups:"
docker exec rstudio getent group | grep -E "(rstudio|dev)"

echo ""
echo "RStudio Server Process:"
docker exec rstudio ps aux | grep rstudio-server | head -1

echo ""
echo "RStudio Server Port Status:"
docker exec rstudio netstat -tlnp | grep :8787 || echo "Port 8787 not found"

echo ""
echo "Home Directories:"
docker exec rstudio ls -la /home/

echo ""
echo "Test Login Information:"
echo "URL: http://localhost:8787"
echo "Available Users:"
echo "- dev1 (password: dev1)"
echo "- dev2 (password: dev2)"  
echo "- dev3 (password: dev3)"
echo "All users are members of the 'rstudio-users' group (GID 8500)"
