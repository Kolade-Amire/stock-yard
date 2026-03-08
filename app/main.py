from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.requests import Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError

from app.api.routers import analytics, chat, health, tickers
from app.core.config import get_settings
from app.core.errors import ApiError
from app.core.logging import configure_logging, get_logger
from app.schemas.common import ErrorBody, ErrorResponse

settings = get_settings()
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(_: FastAPI):
    configure_logging()
    logger = get_logger(__name__)
    logger.info("Starting %s (%s)", settings.app_name, settings.app_env)
    yield


app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(ApiError)
async def api_error_handler(_: Request, exc: ApiError) -> JSONResponse:
    payload = ErrorResponse(
        error=ErrorBody(code=exc.code, message=exc.message, details=exc.details)
    )
    return JSONResponse(
        status_code=exc.status_code,
        content=payload.model_dump(),
        headers=exc.headers,
    )


@app.exception_handler(RequestValidationError)
async def validation_error_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
    payload = ErrorResponse(
        error=ErrorBody(
            code="VALIDATION_ERROR",
            message="Request validation failed.",
            details={"errors": exc.errors()},
        )
    )
    return JSONResponse(status_code=422, content=payload.model_dump())


@app.exception_handler(Exception)
async def unhandled_error_handler(_: Request, exc: Exception) -> JSONResponse:
    logger.exception("Unhandled application error", exc_info=exc)
    payload = ErrorResponse(
        error=ErrorBody(
            code="INTERNAL_ERROR",
            message="An unexpected error occurred.",
        )
    )
    return JSONResponse(status_code=500, content=payload.model_dump())


app.include_router(health.router, prefix=settings.api_prefix)
app.include_router(tickers.router, prefix=settings.api_prefix)
app.include_router(analytics.router, prefix=settings.api_prefix)
app.include_router(chat.router, prefix=settings.api_prefix)
