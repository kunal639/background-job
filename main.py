import datetime
import inngest
import inngest.fast_api
from fastapi import FastAPI

app = FastAPI()

inngest_client = inngest.Inngest(
    app_id="report-api",
    is_production=False,
)

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
    [say_hello],
)

@app.get("/health")
def health_check():
    return {"status": "ok"}