#!/bin/bash
set -e

echo "🛠 Installing Java and configuring environment..."

# Install OpenJDK (using Java 11 for best compatibility with R packages)
apt-get update
apt-get install -y --no-install-recommends \
    openjdk-11-jdk \
    openjdk-11-jre \
    ca-certificates-java

# Ensure certificates are properly setup
update-ca-certificates -f

# Define JAVA_HOME explicitly
export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"

# Add Java to global profile for all users
cat > /etc/profile.d/java_home.sh << 'EOF'
#!/bin/bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$PATH:$JAVA_HOME/bin
EOF

chmod +x /etc/profile.d/java_home.sh

# Verify Java installation
echo "✓ Verifying Java installation:"
java -version

echo "✓ JAVA_HOME is set to: $JAVA_HOME"

# Clean up
apt-get clean && rm -rf /var/lib/apt/lists/*

echo "✅ Java installation and configuration completed!"
