@echo off
setlocal
cd /d D:\RobotTest\workspace\trade_agent
set TRADE_AGENT_PROXY=http://127.0.0.1:7897
python app\main.py
endlocal
