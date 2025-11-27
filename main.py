# app/__init__.py
from fastapi import FastAPI
from router import router as api_router
from utils.database import init_db_pool

app = FastAPI(title="VenusDB API Demo", version="0.2.0")
app.include_router(api_router)

@app.on_event("startup")
async def startup():
    await init_db_pool()
@app.get("/")
async def root():
    return {"message": "VenusDB API - ok"}
