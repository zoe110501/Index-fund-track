# 金融热点投研终端

本项目新增一个本地/内网单机使用的金融热点 MVP，面向 A股、港股、指数、ETF 和基金的投研/销售支持工作流。它参照 AI HOT 的精选流、日报和 API 形态，但默认不做公网部署、不做登录、不输出投资建议。

## 快速运行

```powershell
python -m pip install -e ".[dev]"
python -m financial_hot_terminal --host 127.0.0.1 --port 8765 --seed-demo
```

打开：

```text
http://127.0.0.1:8765
```

## 功能

- FastAPI + Jinja + SQLite 的独立本地应用。
- 首页展示精选热点、待复核数量、源状态、产品关联和证据片段。
- `/daily` 展示日报，`/review` 处理社媒/KOL 候选线索。
- 内部 API：
  - `GET /api/items`
  - `GET /api/items/{id}`
  - `GET /api/daily?date=YYYY-MM-DD`
  - `GET /api/dailies`
  - `GET /api/sources/status`
  - `POST /api/jobs/ingest`
  - `POST /api/review/{id}`
- 社媒/KOL 源只作为线索；进入精选前需要官方/可信源交叉验证或人工复核。
- OpenAI 兼容接口配置位于 `financial_hot_terminal.llm`，读取 `FINHOT_OPENAI_BASE_URL`、`FINHOT_OPENAI_API_KEY`、`FINHOT_OPENAI_MODEL`。

## 配置

示例源在 `config/source_registry.json`。首版使用可配置示例项，不抓登录态、验证码、付费墙或受限页面。后续可把公开 RSS、授权 API、人工维护的 KOL feed 接入该 registry。

## 测试

```powershell
python -m pytest tests\financial_hot_terminal -q
```

合规边界：所有页面和 API 都定位为资讯线索与材料草稿，必须保留来源链接和“非投资建议”提示。
