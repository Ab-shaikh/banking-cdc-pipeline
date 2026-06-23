#!/bin/bash
set -e

apt-get update -y
apt-get install -y docker.io docker-compose-v2 postgresql-client unzip curl

usermod -aG docker ubuntu

# Install AWS CLI v2
curl -s "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/tmp/awscliv2.zip"
cd /tmp && unzip -q awscliv2.zip && ./aws/install
rm -rf /tmp/awscliv2.zip /tmp/aws

# Download the full setup script from S3 (IAM role s3_access_ec2 gives read access)
aws s3 cp s3://${s3_bucket_name}/scripts/setup_cdc_lab.sh /home/ubuntu/setup_cdc_lab.sh
chmod +x /home/ubuntu/setup_cdc_lab.sh
chown ubuntu:ubuntu /home/ubuntu/setup_cdc_lab.sh

# Run it as the ubuntu user, log everything for debugging
sudo -u ubuntu bash -c "cd /home/ubuntu && ./setup_cdc_lab.sh" > /home/ubuntu/setup_log.txt 2>&1