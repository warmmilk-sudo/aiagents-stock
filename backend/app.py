from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi import HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from backend.api import register_exception_handlers, success_payload
from backend.routers import (
    analysis,
    analysis_history,
    auth_router,
    config_router,
    database_admin,
    exports,
    followup_assets,
    investment_activity,
    macro_cycle,
    low_price_bull,
    longhubang,
    main_force,
    news_flow,
    portfolio,
    price_alerts,
    profit_growth,
    sector_strategy,
    small_cap,
    smart_monitor,
    system,
    tasks,
    value_stock,
)
from backend.services import ensure_runtime_started


ROOT_DIR = Path(__file__).resolve().parents[1]
FRONTEND_DIST_DIR = ROOT_DIR / "frontend" / "dist"
NO_CACHE_HTML_HEADERS = {
    "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
    "Pragma": "no-cache",
    "Expires": "0",
}


@asynccontextmanager
async def lifespan(_app: FastAPI):
    ensure_runtime_started()
    yield


def create_app() -> FastAPI:
    app = FastAPI(
        title="aiagents-stock API",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "http://127.0.0.1:5173",
            "http://localhost:5173",
            "http://127.0.0.1:8503",
            "http://localhost:8503",
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    register_exception_handlers(app)

    app.include_router(auth_router.router)
    app.include_router(system.router)
    app.include_router(config_router.router)
    app.include_router(database_admin.router)
    app.include_router(tasks.router)
    app.include_router(analysis.router)
    app.include_router(analysis_history.router)
    app.include_router(followup_assets.router)
    app.include_router(exports.router)
    app.include_router(portfolio.router)
    app.include_router(price_alerts.router)
    app.include_router(smart_monitor.router)
    app.include_router(investment_activity.router)
    app.include_router(low_price_bull.router)
    app.include_router(main_force.router)
    app.include_router(small_cap.router)
    app.include_router(profit_growth.router)
    app.include_router(value_stock.router)
    app.include_router(sector_strategy.router)
    app.include_router(longhubang.router)
    app.include_router(news_flow.router)
    app.include_router(macro_cycle.router)

    if (FRONTEND_DIST_DIR / "assets").is_dir():
        app.mount("/assets", StaticFiles(directory=FRONTEND_DIST_DIR / "assets"), name="assets")

    @app.get("/health")
    def health() -> dict:
        return success_payload({"status": "ok"})

    @app.get("/")
    def root():
        index_file = FRONTEND_DIST_DIR / "index.html"
        if index_file.is_file():
            return FileResponse(index_file, headers=NO_CACHE_HTML_HEADERS)
        return success_payload(
            {
                "service": "aiagents-stock-backend",
                "frontend_built": False,
            },
            message="frontend dist not found",
        )

    @app.get("/{full_path:path}")
    def spa_fallback(full_path: str):
        if full_path.startswith("api/"):
            raise HTTPException(status_code=404, detail="Not Found")
        if FRONTEND_DIST_DIR.is_dir():
            candidate = FRONTEND_DIST_DIR / full_path
            if candidate.is_file():
                if candidate.suffix == ".html":
                    return FileResponse(candidate, headers=NO_CACHE_HTML_HEADERS)
                return FileResponse(candidate)
            index_file = FRONTEND_DIST_DIR / "index.html"
            if index_file.is_file():
                return FileResponse(index_file, headers=NO_CACHE_HTML_HEADERS)
        return success_payload(
            {
                "service": "aiagents-stock-backend",
                "path": full_path,
            },
            message="frontend dist not found",
        )

    return app
