#!/bin/bash

# Exit immediately if a command fails
set -e

# 1. Install Python requirements (Ensure certifi is in requirements.txt)
pip install -r requirements.txt

# 1b. Force install the bleeding-edge master branch of yt-dlp directly from GitHub
# This bypasses PyPI release lag to fix immediate SoundCloud extraction breakages
pip install -U "yt-dlp[default]@git+https://github.com/yt-dlp/yt-dlp.git"

# 2. Fix SSL Certificate issues for the build environment
# This helps during the build process if any python scripts need web access
pip install certifi
export SSL_CERT_FILE=$(python -m certifi)

# 3. Create folder for ffmpeg
# We need this so yt-dlp can convert the audio to MP3
mkdir -p ffmpeg_bin
cd ffmpeg_bin

# 4. Download and extract FFmpeg
# We use -O to ensure the filename is consistent
wget https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz -O ffmpeg.tar.xz
tar xf ffmpeg.tar.xz --strip-components=1

# CRITICAL: Permissions so the app can actually run the binary files
chmod +x ffmpeg ffprobe

# Clean up the compressed file to save space
cd ..
rm ffmpeg_bin/ffmpeg.tar.xz

echo "Build successful with SSL fix, FFmpeg setup, and bleeding-edge yt-dlp!"