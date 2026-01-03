from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.v1.router import api_router
from app.config import get_settings
from app.core.cache import close_redis
from app.core.database import close_db, init_db
from app.middleware import TelemetryMiddleware
from app.models.data_source import DataSource  # noqa: F401
from app.models.experiment import Experiment, VariantResult  # noqa: F401
from app.models.feedback import Feedback  # noqa: F401
from app.models.report import Report  # noqa: F401
from app.models.usage_metric import UsageMetric  # noqa: F401

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("Starting up Echo...")
    await init_db()
    print("Database initialized")
    yield
    # Shutdown
    print("Shutting down Echo...")
    await close_redis()
    await close_db()
    print("Cleanup complete")


app = FastAPI(
    title=settings.APP_NAME,
    description="AI Data Scientist for SMB Analytics - Powered by DeepSeek",
    version="0.1.0",
    docs_url=f"{settings.API_V1_PREFIX}/docs",
    redoc_url=f"{settings.API_V1_PREFIX}/redoc",
    lifespan=lifespan,
)

# CORS middleware
if settings.CORS_ORIGINS:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

app.add_middleware(TelemetryMiddleware)

# Include routers
app.include_router(api_router, prefix=settings.API_V1_PREFIX)


@app.get("/")
async def root():
    return {
        "app": settings.APP_NAME,
        "version": "0.1.0",
        "environment": settings.ENVIRONMENT,
        "llm": "DeepSeek 3.2 Exp",
    }
