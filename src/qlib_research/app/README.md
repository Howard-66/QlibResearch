# QlibResearch App Layer

这里现在承载独立研究工作台的 API / service 层：

- `main.py`: FastAPI 入口
- `contracts.py`: Pydantic DTO 与任务模型
- `services.py`: artifacts / panels / task store 聚合服务
- `task_worker.py`: 文件落盘任务 worker

默认启动方式：

```bash
uv sync --extra qlib --extra app
uv run uvicorn qlib_research.app.main:app --host 0.0.0.0 --port 8010 --reload
```

前端位于项目根目录的 `web/`，默认运行在 `3010`，通过 `/backend/*` 代理访问这里提供的 API。
