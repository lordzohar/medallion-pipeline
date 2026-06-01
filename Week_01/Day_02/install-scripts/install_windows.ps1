<#
Day 2 Environment Setup Script for Windows 10/11

This PowerShell script prepares a Windows workstation to run the
Day 2 Kafka exercises.  It installs Windows Subsystem for Linux 2 (WSL2)
with Ubuntu, downloads Docker Desktop and installs Python and Git using
winget.  Run this script from an elevated PowerShell session (Run as
Administrator).  You may need to reboot once WSL and Docker are
installed.  After rebooting, launch the Ubuntu app to finish the
distribution setup.
#>

Write-Host "Enabling required Windows features..." -ForegroundColor Cyan
Enable-WindowsOptionalFeature -Online -FeatureName Microsoft-Windows-Subsystem-Linux -NoRestart -ErrorAction Stop
Enable-WindowsOptionalFeature -Online -FeatureName VirtualMachinePlatform -NoRestart -ErrorAction Stop

Write-Host "Installing Ubuntu distribution via winget..." -ForegroundColor Cyan
winget install -e --id Canonical.Ubuntu || Write-Host "Ubuntu may already be installed."
wsl --set-default-version 2

Write-Host "Downloading and installing Docker Desktop..." -ForegroundColor Cyan
$installer = "$env:TEMP\DockerInstaller.exe"
Invoke-WebRequest -UseBasicParsing -Uri "https://desktop.docker.com/win/main/amd64/Docker%20Desktop%20Installer.exe" -OutFile $installer
Start-Process -FilePath $installer -Wait

Write-Host "Installing Git and Python via winget..." -ForegroundColor Cyan
winget install -e --id Git.Git || Write-Host "Git may already be installed."
winget install -e --id Python.Python.3.10 || Write-Host "Python may already be installed."
