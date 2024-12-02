#!/bin/bash

# 清理旧的构建文件
rm -rf ./dist ./build

# 安装依赖
pip install -r requirements.txt

# 使用 PyInstaller 打包 Python 脚本，并确保 dotenv 被包含
pyinstaller --onefile --hidden-import=dotenv monitor_keywords.py || { echo "打包失败"; exit 1; }

echo "打包成功"

# 可执行文件的路径
EXECUTABLE_PATH="./dist/monitor_keywords"

# 检查是否已有实例在运行
PID=$(pgrep -f "$EXECUTABLE_PATH")

if [ -n "$PID" ]; then
    echo "已发现正在运行的实例，PID: $PID"
    kill $PID || { echo "结束进程失败"; exit 1; }
    echo "已结束之前运行的实例"
else
    echo "没有发现正在运行的实例"
fi

# 确保可执行文件存在
if [ ! -f "$EXECUTABLE_PATH" ]; then
    echo "找不到可执行文件，启动失败"
    exit 1
fi

# 启动新的程序，并将输出重定向到 nohup.out 文件
nohup "$EXECUTABLE_PATH" > nohup.out 2>&1 &

echo "新的程序已在后台启动，PID: $!"
