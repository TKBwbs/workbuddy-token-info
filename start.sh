#!/usr/bin/env bash
# WorkBuddy Token 监控 - macOS / Linux 启动脚本
cd "$(dirname "$0")" || exit 1
if command -v python3 >/dev/null 2>&1; then
  exec python3 wb_usage_server.py "$@"
elif command -v python >/dev/null 2>&1; then
  exec python wb_usage_server.py "$@"
else
  echo "未找到 Python，请先安装 Python 3.8+（https://www.python.org/downloads/）"
  exit 1
fi
