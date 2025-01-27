from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from tests.benchmark import Benchmark
from .routes import router
import os
import logging

app = FastAPI()

app.include_router(router)

origins = [
    "http://localhost:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

logging.basicConfig(
    format="> %(message)s",
    level=logging.INFO
)

@app.on_event("startup")
async def startup_event():
    app.state.scale_factor = os.getenv("SCALE_FACTOR")

    if app.state.scale_factor:
        logging.info("Scale factor set to {}".format(app.state.scale_factor))
    else:
        app.state.scale_factor = 1
        logging.info("Default scale factor set to {}".format(app.state.scale_factor))

    Benchmark.init_tpch(app.state.scale_factor)


@app.on_event("shutdown")
async def shutdown_event():
    print("Shutting down...")
