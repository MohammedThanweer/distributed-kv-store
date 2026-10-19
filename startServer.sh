
#!/bin/bash

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
NODE_ID="${1:-node1}"
PORT="${2:-7001}"

osascript <<EOF
tell application "Terminal"
    activate
    do script "cd '$PROJECT_DIR' && mvn org.codehaus.mojo:exec-maven-plugin:3.5.0:java -Dexec.mainClass=\"com.raftkv.KVServer\" -Dexec.args=\"$NODE_ID $PORT\""
end tell
EOF

