# KosterDataTool

Windows 电化学文本数据处理工具（GUI + CLI）。

## 开发态运行
```bash
python -m pip install -r requirements.txt
python main.py --no-gui --root "<某个根目录>" --scan-only
python main.py --no-gui --selftest
python main.py
```

## 运行目录说明
- 运行态目录固定写入：`程序所在文件夹/KosterData`
- 日志：`KosterData/logs/run_<run_id>.log` 与 `KosterData/logs/run_<run_id>.jsonl`
- 报告：`KosterData/reports/run_<run_id>_report.txt`
- 跳过清单：`KosterData/reports/skipped_paths-<run_id>.txt`
