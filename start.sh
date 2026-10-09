#!/data/data/com.termux/files/usr/bin/bash
# Auto-restart forever
termux-wake-lock 2>/dev/null
cd "$(dirname "$0")"
while true; do
  python main.py
  echo "Crashed, restarting in 3s..."
  sleep 3
done

