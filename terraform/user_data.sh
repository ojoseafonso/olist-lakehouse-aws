#!/bin/bash
# Update all installed packages
yum update -y || apt-get update -y

# Install unzip (required for the AWS CLI installation package)
yum install unzip -y || apt-get install unzip -y

# Navigate to a temporary directory
cd /tmp

# Download the AWS CLI v2 installation file
curl "https://awscli.amazonaws.com" -o "awscliv2.zip"

# Unzip the installer package
unzip awscliv2.zip

# Run the install program (this installs to /usr/local/aws and creates a symlink in /usr/local/bin)
./aws/install

# Optional: Verify the installation
aws --version