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

class ReportRequest(BaseModel):
    topic: str

    @field_validator("topic")
    @classmethod
    def validate_topic(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("topic cannot be empty")
        return v.strip()

@inngest_client.create_function(
    fn_id="make-report",
    trigger=inngest.TriggerEvent(event="report/requested"),
    retries=2,
)
async def make_report(ctx: inngest.Context) -> Dict[str, Any]:
    report_id = ctx.event.data["id"]
    topic = ctx.event.data["topic"]

    await ctx.step.sleep("do-the-slow-work", datetime.timedelta(seconds=8))

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

# 1. New cron function running every minute
@inngest_client.create_function(
    fn_id="heartbeat",
    trigger=inngest.TriggerCron(cron="* * * * *"),
)
async def heartbeat(ctx: inngest.Context) -> Dict[str, int]:
    def compute_summary():
        counts = {"pending": 0, "done": 0, "failed": 0}
        for item in reports_db.values():
            st = item.get("status", "pending")
            counts[st] = counts.get(st, 0) + 1
        print(f"[HEARTBEAT CRON] Status summary -> Pending: {counts['pending']} | Done: {counts['done']} | Failed: {counts['failed']}")
        return counts

    return await ctx.step.run("log-summary", compute_summary)

# 2. Register all three functions
inngest.fast_api.serve(
    app,
    inngest_client,
    [say_hello, make_report, heartbeat],
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