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

# 4. Download and extract FFmpeg & FFprobe (GitHub CDN prebuilt static binaries)
wget https://github.com/ffbinaries/ffbinaries-prebuilt/releases/download/v6.1/ffmpeg-6.1-linux-64.zip -O ffmpeg.zip
python -m zipfile -e ffmpeg.zip .
wget https://github.com/ffbinaries/ffbinaries-prebuilt/releases/download/v6.1/ffprobe-6.1-linux-64.zip -O ffprobe.zip
python -m zipfile -e ffprobe.zip .

# 4b. Download and extract AcoustID Chromaprint fpcalc binary
wget https://github.com/acoustid/chromaprint/releases/download/v1.6.0/chromaprint-fpcalc-1.6.0-linux-x86_64.tar.gz -O fpcalc.tar.gz
tar xf fpcalc.tar.gz --strip-components=1

# CRITICAL: Permissions so the app can actually run the binary files
chmod +x ffmpeg ffprobe fpcalc

# Clean up the compressed files to save space
cd ..
rm -f ffmpeg_bin/ffmpeg.zip ffmpeg_bin/ffprobe.zip ffmpeg_bin/fpcalc.tar.gz

echo "Build successful with SSL fix, FFmpeg setup, and bleeding-edge yt-dlp!"