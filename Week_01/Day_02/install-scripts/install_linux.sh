#!/bin/bash
# Day 2 Environment Setup Script for Ubuntu 24.04 LTS
#
# This script prepares your workstation to complete the Day 2 Kafka
# exercises.  It installs Docker and the Docker Compose plugin, Python 3,
# pip and Git.  Docker is used to run a single-node Kafka cluster via
# KRaft mode.  After running this script, log out and back in so that
# your user is added to the `docker` group.

set -e

echo "Updating package lists..."
sudo apt-get update -y

echo "Installing prerequisite packages..."
sudo apt-get install -y apt-transport-https ca-certificates curl gnupg lsb-release software-properties-common

echo "Adding Docker’s official GPG key..."
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

echo "Setting up the stable Docker repository..."
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] \" \
https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

echo "Installing Docker Engine and CLI..."
sudo apt-get update -y
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

echo "Adding your user to the docker group..."
sudo usermod -aG docker $USER

echo "Installing Python and Git..."
sudo apt-get install -y python3 python3-pip git

echo "Installation complete.  Please log out and log back in to apply Docker group membership."