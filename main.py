import datetime
import uuid
from typing import Dict, Any
import inngest
import inngest.fast_api
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel

app = FastAPI()

# 1. In-memory storage for reports
reports_db: Dict[str, Dict[str, Any]] = {}

inngest_client = inngest.Inngest(
    app_id="report-api",
    is_production=False,
)

# Schema for incoming request
class ReportRequest(BaseModel):
    topic: str

# 2. Inngest Function: make-report
@inngest_client.create_function(
    fn_id="make-report",
    trigger=inngest.TriggerEvent(event="report/requested"),
)
async def make_report(ctx: inngest.Context) -> Dict[str, Any]:
    report_id = ctx.event.data["id"]
    topic = ctx.event.data["topic"]

    # Step 1: Simulate the slow work (8 seconds)
    await ctx.step.sleep("do-the-slow-work", datetime.timedelta(seconds=8))

    # Step 2: Build the report and update in-memory DB
    async def build_report():
        result = f"Summary report on {topic}: Detailed findings and analysis."
        if report_id in reports_db:
            reports_db[report_id]["status"] = "done"
            reports_db[report_id]["result"] = result
        return {"status": "done", "result": result}

    return await ctx.step.run("build-report", build_report)

@inngest_client.create_function(
    fn_id="say-hello",
    trigger=inngest.TriggerEvent(event="test/hello"),
)
async def say_hello(ctx: inngest.Context) -> str:
    await ctx.step.sleep("wait-a-bit", datetime.timedelta(seconds=5))
    return "Hello from the background!"

# Serve functions
inngest.fast_api.serve(
    app,
    inngest_client,
    [say_hello, make_report],
)

@app.get("/health")
def health_check():
    return {"status": "ok"}

# 3. Fast Door: POST /reports
@app.post("/reports", status_code=status.HTTP_202_ACCEPTED)
async def create_report(body: ReportRequest):
    report_id = str(uuid.uuid4())
    reports_db[report_id] = {
        "id": report_id,
        "topic": body.topic,
        "status": "pending",
        "result": None,
    }

    # Send event to Inngest to initiate the background task
    await inngest_client.send(
        inngest.Event(
            name="report/requested",
            data={"id": report_id, "topic": body.topic},
        )
    )

    return {"id": report_id, "status": "pending"}

# 4. Status Endpoint: GET /reports/{report_id}
@app.get("/reports/{report_id}")
def get_report(report_id: str):
    if report_id not in reports_db:
        raise HTTPException(status_code=404, detail="Report not found")
    return reports_db[report_id]