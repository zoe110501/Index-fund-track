from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from .core import DISCLAIMER, REVIEW_APPROVED, REVIEW_CANDIDATE, REVIEW_REJECTED, REVIEW_SELECTED, SHANGHAI_TZ
from .database import initialize_database, open_connection
from .ingestion import DEFAULT_REGISTRY_PATH, run_ingestion
from .repository import HotspotRepository
from .seed import seed_demo_data


PACKAGE_ROOT = Path(__file__).resolve().parent
DEFAULT_DB_PATH = Path("data/financial_hot_terminal.sqlite")
TEMPLATES = Jinja2Templates(directory=str(PACKAGE_ROOT / "templates"))


class ReviewRequest(BaseModel):
    status: str = Field(pattern=f"^({REVIEW_CANDIDATE}|{REVIEW_SELECTED}|{REVIEW_APPROVED}|{REVIEW_REJECTED})$")
    reviewer: str = "local"
    comment: str = ""


def create_repository(db_path: str | Path = DEFAULT_DB_PATH) -> HotspotRepository:
    connection = open_connection(db_path)
    initialize_database(connection)
    return HotspotRepository(connection)


def create_app(repository: HotspotRepository | None = None, *, seed_demo: bool = False) -> FastAPI:
    repository = repository or create_repository()
    if seed_demo:
        from datetime import datetime

        seed_demo_data(repository, now=datetime.now(SHANGHAI_TZ))

    app = FastAPI(title="金融热点投研终端", version="0.1.0")
    app.state.repository = repository
    app.mount("/static", StaticFiles(directory=str(PACKAGE_ROOT / "static")), name="static")

    @app.get("/favicon.ico", include_in_schema=False)
    def favicon() -> Response:
        return Response(status_code=204)

    @app.get("/", response_class=HTMLResponse)
    def home(
        request: Request,
        status: str = Query("selected"),
        q: str | None = None,
        market: str | None = None,
        category: str | None = None,
    ) -> HTMLResponse:
        items = repository.list_hotspots(status=status, q=q, market=market, category=category, limit=100)
        candidates = repository.list_hotspots(status=REVIEW_CANDIDATE, limit=20)
        latest_daily = repository.get_daily_report()
        source_status = repository.source_status()
        return TEMPLATES.TemplateResponse(
            request,
            "index.html",
            {
                "items": items,
                "status": status,
                "q": q or "",
                "market": market or "",
                "category": category or "",
                "candidates": candidates,
                "latest_daily": latest_daily,
                "source_status": source_status,
                "disclaimer": DISCLAIMER,
            },
        )

    @app.get("/daily", response_class=HTMLResponse)
    def daily_page(request: Request, date: str | None = None) -> HTMLResponse:
        report = repository.get_daily_report(date)
        if not report:
            raise HTTPException(status_code=404, detail="daily report not found")
        return TEMPLATES.TemplateResponse(request, "daily.html", {"report": report})

    @app.get("/review", response_class=HTMLResponse)
    def review_page(request: Request) -> HTMLResponse:
        candidates = repository.list_hotspots(status=REVIEW_CANDIDATE, limit=100)
        return TEMPLATES.TemplateResponse(request, "review.html", {"items": candidates, "disclaimer": DISCLAIMER})

    @app.get("/api/items")
    def api_items(
        status: str | None = Query(None),
        q: str | None = None,
        market: str | None = None,
        category: str | None = None,
        limit: int = Query(100, ge=1, le=200),
    ) -> dict[str, Any]:
        return {
            "items": repository.list_hotspots(status=status, q=q, market=market, category=category, limit=limit),
            "disclaimer": DISCLAIMER,
        }

    @app.get("/api/items/{hotspot_id}")
    def api_item(hotspot_id: str) -> dict[str, Any]:
        item = repository.get_hotspot(hotspot_id)
        if not item:
            raise HTTPException(status_code=404, detail="hotspot not found")
        return item

    @app.get("/api/daily")
    def api_daily(date: str | None = None) -> dict[str, Any]:
        report = repository.get_daily_report(date)
        if not report:
            raise HTTPException(status_code=404, detail="daily report not found")
        return report

    @app.get("/api/dailies")
    def api_dailies(take: int = Query(30, ge=1, le=120)) -> dict[str, Any]:
        return {"items": repository.list_dailies(take)}

    @app.get("/api/sources/status")
    def api_source_status() -> dict[str, Any]:
        return {"sources": repository.source_status()}

    @app.post("/api/jobs/ingest")
    def api_ingest() -> JSONResponse:
        result = run_ingestion(repository, registry_path=DEFAULT_REGISTRY_PATH)
        return JSONResponse(result)

    @app.post("/api/review/{hotspot_id}")
    def api_review(hotspot_id: str, payload: ReviewRequest) -> dict[str, Any]:
        item = repository.review_hotspot(
            hotspot_id,
            status=payload.status,
            reviewer=payload.reviewer,
            comment=payload.comment,
        )
        if not item:
            raise HTTPException(status_code=404, detail="hotspot not found")
        return item

    return app


app = create_app(seed_demo=True)
