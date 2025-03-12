from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from tests.benchmark import Benchmark
from .routes import router
import os
import logging

app = FastAPI()

app.include_router(router)
app.mount("/static", StaticFiles(directory="static"), name="static")

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
    Benchmark.init_imdb()
    #Benchmark.init_stackoverflow()
    Benchmark.init_tpcds(app.state.scale_factor)


@app.on_event("shutdown")
async def shutdown_event():
    print("Shutting down...")
    static_folder = "static"

    if os.path.exists(static_folder):
        for filename in os.listdir(static_folder):
            file_path = os.path.join(static_folder, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f"Failed to delete {file_path}: {e}")

    print("Static folder cleaned up.")
