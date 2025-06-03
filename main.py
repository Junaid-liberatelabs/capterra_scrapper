from fastapi import FastAPI
import uvicorn

from app.api.endpoints import capterra as capterra_api_router
from app.core.config import APP_TITLE, APP_DESCRIPTION, APP_VERSION


app = FastAPI(
    title=APP_TITLE,
    description=APP_DESCRIPTION,
    version=APP_VERSION
)

app.include_router(capterra_api_router.router, prefix="/api/v1", tags=["Capterra"])


@app.get("/", tags=["Root"])
async def read_root():
    return {
        "message": f"Welcome to the {APP_TITLE}",
        "version": APP_VERSION,
        "docs_url": "/docs",
        "redoc_url": "/redoc"
    }
