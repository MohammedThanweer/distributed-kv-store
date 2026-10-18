#!/bin/bash

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"

osascript <<EOF
tell application "Terminal"
    activate
    do script "cd '$PROJECT_DIR' && ./startServer.sh"
    delay 2
    do script "cd '$PROJECT_DIR' && ./startClient.sh"
end tell
EOF
