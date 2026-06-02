#!/bin/sh
# Render every PlantUML diagram in this directory to PNG.
# GitHub renders the committed PNGs; the .puml sources live here so the
# diagrams can be regenerated.
#
# Install PlantUML:
#   macOS:         brew install plantuml
#   Debian/Ubuntu: apt-get install plantuml
#
# Then, from this directory:
#   ./render.sh
#
# Commit both the .puml source and the resulting .png.

set -e
cd "$(dirname "$0")"
plantuml -tpng *.puml
