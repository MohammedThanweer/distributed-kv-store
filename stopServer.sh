
#!/bin/bash

PORT="${1:-7000}"

PID="$(lsof -tiTCP:"$PORT" -sTCP:LISTEN 2>/dev/null | head -n 1)"

if [ -z "$PID" ]; then
    echo "No server is listening on port $PORT"
    exit 0
fi

COMMAND="$(ps -p "$PID" -o command= 2>/dev/null)"

if ! echo "$COMMAND" | grep -q "com.raftkv.KVServer"; then
    echo "Port $PORT belongs to another process. Nothing was stopped."
    echo "$COMMAND"
    exit 1
fi

echo "Stopping KV server on port $PORT (PID $PID)..."
kill -TERM "$PID"

for i in {1..10}; do
    if ! kill -0 "$PID" 2>/dev/null; then
        echo "Server stopped."
        exit 0
    fi
    sleep 1
done

echo "Server has not exited yet. Check its Terminal window."
exit 1

