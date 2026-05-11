"""FastAPI app factory for swap-api. Wires routers, CORS, and lifespan state."""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from allways.swap_api.deps import build_app_state
from allways.swap_api.routes import chains, health, miners, proofs, swap


def create_app() -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.allways = build_app_state()
        yield

    app = FastAPI(
        title='Allways Swap API',
        description='HTTP wrapper around the swap CLI — see docs/swap-api/browser-swap-spec.md',
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=['*'],
        allow_methods=['GET', 'POST', 'OPTIONS'],
        allow_headers=['*'],
    )
    app.include_router(health.router)
    app.include_router(chains.router)
    app.include_router(miners.router)
    app.include_router(proofs.router)
    app.include_router(swap.router)
    return app


app = create_app()
