
#!/bin/bash

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"

osascript <<EOF
tell application "Terminal"
    activate
    do script "cd '$PROJECT_DIR' && mvn org.codehaus.mojo:exec-maven-plugin:3.5.0:java -Dexec.mainClass=\"com.raftkv.KVServer\""
end tell
EOF

