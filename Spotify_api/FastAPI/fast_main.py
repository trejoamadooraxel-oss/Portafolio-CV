from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, HTTPException
from Spotify_api.FastAPI.routers import artista, album, tracks, mas_inf

app = FastAPI(
    title="Main End Point Spotify API",
    description="Estructura limpia usando APIRouter",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(artista.router)
app.include_router(album.router)
app.include_router(tracks.router)
app.include_router(mas_inf.router)

@app.get("/", tags=["Root"])
async def root():
    return {
        "status": "online",
        "message": "Spotify API FastAPI Service is running smoothly"
    }


