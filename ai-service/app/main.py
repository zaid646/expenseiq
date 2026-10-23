"""ExpenseIQ AI service — FastAPI application entrypoint."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from . import __version__
from .config import get_settings
from .graph.router import router as graph_router
from .llm import LLMClient
from .metrics import STORE
from .routers import (
    admin,
    assistant,
    code,
    corrections,
    extract,
    ops,
    policy,
    search,
    workflows,
)
from .schemas import HealthResponse
from .tracing import get_tracer

settings = get_settings()
logging.basicConfig(level=settings.log_level.upper())
log = logging.getLogger("expenseiq")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    log.info(
        "ExpenseIQ AI service v%s | model=%s | langfuse=%s",
        __version__,
        settings.vllm_model,
        get_tracer().enabled,
    )
    yield


app = FastAPI(
    title="ExpenseIQ AI service",
    version=__version__,
    lifespan=lifespan,
    description=(
        "Receipt extraction, GL coding and policy checks with schema-guided LLM output, "
        "PII redaction, a self-healing harness and Langfuse tracing. Inference is served "
        "by a self-hosted vLLM endpoint."
    ),
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(extract.router)
app.include_router(admin.router)
app.include_router(ops.router)
app.include_router(code.router)
app.include_router(policy.router)
app.include_router(corrections.router)
app.include_router(search.router)
app.include_router(assistant.router)
app.include_router(workflows.router)
app.include_router(graph_router, prefix="/graphql")


@app.get("/health", response_model=HealthResponse, tags=["meta"])
async def health() -> HealthResponse:
    reachable = False
    try:
        await LLMClient().list_models()
        reachable = True
    except Exception:  # noqa: BLE001 - health must not raise
        reachable = False
    return HealthResponse(
        status="ok",
        llm_reachable=reachable,
        llm_model=settings.vllm_model,
        langfuse=get_tracer().enabled,
        version=__version__,
    )


@app.get("/metrics", tags=["meta"])
async def metrics() -> dict:
    return STORE.snapshot()
