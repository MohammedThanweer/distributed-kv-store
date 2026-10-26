
#!/bin/bash

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
PORT="${1:-7001}"

osascript <<EOF
tell application "Terminal"
    activate
    do script "cd '${PROJECT_DIR}' && mvn org.codehaus.mojo:exec-maven-plugin:3.5.0:java -Dexec.mainClass=\"com.raftkv.KVClient\" -Dexec.args=\"localhost ${PORT}\""
end tell
EOF

