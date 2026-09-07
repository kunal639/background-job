# Background Job Processing Service

An asynchronous background job system built with **FastAPI**, **uv**, and **Inngest**.

Heavy workloads are immediately offloaded to background workflows using the **accept-fast / poll-status** pattern, with automatic backoff retries and scheduled cron reporting.

---

## How to Run

### 1. Start the API Server

```powershell
uv run uvicorn main:app --port 8000 --reload
```

### 2. Start the Inngest Dev Server

```powershell
npx inngest-cli@latest dev -u http://localhost:8000/api/inngest
```

Dev Dashboard: [http://localhost:8288](http://localhost:8288)

---

## Endpoints & Background Functions

| Door / Worker | Type | Trigger / Method | Description |
|---|---|---|---|
| `GET /health` | Endpoint | HTTP GET | API health check |
| `POST /reports` | Endpoint | HTTP POST | Accepts a report topic, dispatches an event, and returns `202 Accepted` |
| `GET /reports/{id}` | Endpoint | HTTP GET | Status endpoint returning report progress (`pending`, `done`, `failed`) |
| `say-hello` | Function | Event `test/hello` | Introductory 5-second sleeping worker |
| `make-report` | Function | Event `report/requested` | Sleeps 8 seconds, builds a report, or retries on transient errors |
| `heartbeat` | Function | Cron `* * * * *` | Runs every minute to log report state counts |

---

## Proof: 202 Accepted Followed by Eventual Consistency

**Step 1 — Submit a job (returns immediately with `202 Accepted`):**

```powershell
C:\Users\kunal>curl.exe -i -X POST http://localhost:8000/reports -H "Content-Type: application/json" -d "{\"topic\":\"cats\"}"

HTTP/1.1 202 Accepted
date: Mon, 07 Sep 2026 18:35:50 GMT
server: uvicorn
content-length: 64
content-type: application/json

{"id":"686b3d27-19ab-4979-8445-a7fe8a42c649","status":"pending"}
```

**Step 2 — Poll for the result once it's done:**

```powershell
C:\Users\kunal>curl.exe -i http://localhost:8000/reports/686b3d27-19ab-4979-8445-a7fe8a42c649

HTTP/1.1 200 OK
date: Mon, 07 Sep 2026 18:36:25 GMT
server: uvicorn
content-length: 143
content-type: application/json

{"id":"686b3d27-19ab-4979-8445-a7fe8a42c649","topic":"cats","status":"done","result":"Summary report on cats: Detailed findings and analysis."}
```

This shows the core idea of the system: the API responds instantly with a `pending` status, and the actual work happens in the background — the client checks back later to get the finished result.

---

## Architecture Answers

### Stage 3: Input Validation vs. Job Retries

A malformed request (such as a missing or empty topic) is a **deterministic error** — it will never succeed no matter how many times it's retried. These must be rejected right away at the API doorway with an HTTP `4xx` status, before any event is even created.

Background retries, on the other hand, exist for **transient operational failures** — things like database locks or network drops — where waiting a bit and trying again can actually succeed.

### Stage 4: Cron Schedules

- **Every day at 08:00:** `0 8 * * *`
- **Every Sunday at 22:00:** `0 22 * * 0` (or `0 22 * * 7`)

### Inngest Dashboard Runs

*(Take a screenshot of `http://localhost:8288/runs` showing a completed `make-report` run, a retried failed run, and recurring `heartbeat` executions, and paste it here.)*

---

## Save and Commit — Stage 5

```powershell
git add README.md
git commit -m "Stage 5: publish and docs"
```