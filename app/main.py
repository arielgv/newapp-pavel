from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.core.config import settings
from app.file_processing.router import router as file_router
from app.utils.logger import logger

logger.info("Starting the COSA Core Engine")


# Create the FastAPI application and run migrations on startup
app = FastAPI()


# CORS origins
origins = [
    "http://localhost",
    "http://localhost:8080",
]

# Attach CORS middleware to the application
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Include routers
app.include_router(file_router, prefix="/file-tasks", tags=["File Processing"])
