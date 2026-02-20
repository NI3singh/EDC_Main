from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException

from edc_main.mongo   import mongo
from edc_main.schemas import EDCRequest, EDCResponse
from edc_main.engine  import run_edc


@asynccontextmanager
async def lifespan(app: FastAPI):
    mongo.connect()
    yield
    mongo.close()


app = FastAPI(
    title="EDC — Early Damage Control",
    version="1.0.0",
    description=(
        "Single endpoint that runs all financial-pattern checks "
        "(structuring, rapid round-trip, velocity/baseline) against "
        "the i-betting platform MongoDB and returns one unified verdict."
    ),
    lifespan=lifespan,
)


@app.post("/edc", response_model=EDCResponse, summary="Run all EDC checks on a transaction")
async def edc(req: EDCRequest):
    try:
        result = await run_edc(req.transaction_id, mongo.db())
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"EDC check failed: {str(exc)}")

    return result


