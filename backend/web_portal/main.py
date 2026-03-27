from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from backend.mentor_module.router import router as mentor_router
from backend.mentor_module.schema import init_mentor_schema
from backend.roadmap_engine.storage.schema import init_roadmap_schema
from backend.web_portal.routers.pages import router as pages_router


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_roadmap_schema()
    init_mentor_schema()
    yield


app = FastAPI(
    title="Career Roadmap AI",
    lifespan=lifespan,
)

app.include_router(pages_router)
app.include_router(mentor_router)

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

