import datetime
import uuid
from typing import Dict, Any
import inngest
import inngest.fast_api
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, field_validator

app = FastAPI()

reports_db: Dict[str, Dict[str, Any]] = {}

inngest_client = inngest.Inngest(
    app_id="report-api",
    is_production=False,
)

# 1. Reject invalid input at the door
class ReportRequest(BaseModel):
    topic: str

    @field_validator("topic")
    @classmethod
    def validate_topic(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("topic cannot be empty")
        return v.strip()

# 2. Configure make-report with retries=2
@inngest_client.create_function(
    fn_id="make-report",
    trigger=inngest.TriggerEvent(event="report/requested"),
    retries=2,
)
async def make_report(ctx: inngest.Context) -> Dict[str, Any]:
    report_id = ctx.event.data["id"]
    topic = ctx.event.data["topic"]

    # Step 1: Slow work simulation
    await ctx.step.sleep("do-the-slow-work", datetime.timedelta(seconds=8))

    # Step 2: Build report or simulate crash
    async def build_report():
        if topic.lower() == "fail":
            if report_id in reports_db:
                reports_db[report_id]["status"] = "failed"
            raise RuntimeError("The report oven is broken!")

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

inngest.fast_api.serve(
    app,
    inngest_client,
    [say_hello, make_report],
)

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.post("/reports", status_code=status.HTTP_202_ACCEPTED)
async def create_report(body: ReportRequest):
    report_id = str(uuid.uuid4())
    reports_db[report_id] = {
        "id": report_id,
        "topic": body.topic,
        "status": "pending",
        "result": None,
    }

    await inngest_client.send(
        inngest.Event(
            name="report/requested",
            data={"id": report_id, "topic": body.topic},
        )
    )

    return {"id": report_id, "status": "pending"}

@app.get("/reports/{report_id}")
def get_report(report_id: str):
    if report_id not in reports_db:
        raise HTTPException(status_code=404, detail="Report not found")
    return reports_db[report_id]