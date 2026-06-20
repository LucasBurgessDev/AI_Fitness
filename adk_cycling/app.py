from __future__ import annotations

import logging
import os
import struct
import zlib
from typing import Optional
from uuid import uuid4

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from google_auth_oauthlib.flow import Flow
from itsdangerous import BadSignature, URLSafeSerializer

import profile as profile_store

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
GOOGLE_CLIENT_ID = os.environ["GOOGLE_CLIENT_ID"]
GOOGLE_CLIENT_SECRET = os.environ["GOOGLE_CLIENT_SECRET"]
ALLOWED_EMAILS = {e.strip().lower() for e in os.environ["ALLOWED_EMAIL"].split(",") if e.strip()}
SECRET_KEY = os.environ["SECRET_KEY"]
PROJECT_ID = os.environ.get("PROJECT_ID", "health-data-482722")
REDIRECT_URI = os.getenv("REDIRECT_URI", "http://localhost:8080/auth/callback")
GARMIN_JOB_NAME = os.getenv("GARMIN_JOB_NAME", "garmin-fitness-daily")
GARMIN_JOB_REGION = os.getenv("GARMIN_JOB_REGION", "europe-west2")
MORNING_SHEET_ID = os.getenv("MORNING_SHEET_ID", "")
EVENING_SHEET_ID = os.getenv("EVENING_SHEET_ID", "")

SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/calendar",
]

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------
app = FastAPI(title="Cycling Coach AI")
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))
signer = URLSafeSerializer(SECRET_KEY)


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

def _get_flow() -> Flow:
    client_config = {
        "web": {
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": [REDIRECT_URI],
        }
    }
    flow = Flow.from_client_config(client_config, scopes=SCOPES)
    flow.redirect_uri = REDIRECT_URI
    return flow


def _get_session(request: Request) -> Optional[dict]:
    cookie = request.cookies.get("session")
    if not cookie:
        return None
    try:
        return signer.loads(cookie)
    except BadSignature:
        return None


def _require_session(request: Request) -> dict:
    session = _get_session(request)
    if not session:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return session


# ---------------------------------------------------------------------------
# Auth routes
# ---------------------------------------------------------------------------

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})


@app.get("/auth/start")
async def auth_start():
    import hashlib
    import secrets

    code_verifier = secrets.token_urlsafe(64)
    code_challenge = (
        __import__("base64")
        .urlsafe_b64encode(hashlib.sha256(code_verifier.encode()).digest())
        .rstrip(b"=")
        .decode()
    )

    flow = _get_flow()
    auth_url, _ = flow.authorization_url(
        prompt="consent",
        access_type="offline",
        code_challenge=code_challenge,
        code_challenge_method="S256",
    )

    redirect = RedirectResponse(auth_url)
    redirect.set_cookie(
        "pkce_cv",
        signer.dumps(code_verifier),
        httponly=True,
        samesite="lax",
        max_age=600,
    )
    return redirect


@app.get("/auth/callback")
async def auth_callback(request: Request, code: str):
    code_verifier: str | None = None
    signed_cv = request.cookies.get("pkce_cv")
    if signed_cv:
        try:
            code_verifier = signer.loads(signed_cv)
        except BadSignature:
            pass

    flow = _get_flow()
    fetch_kwargs: dict = {"code": code}
    if code_verifier:
        fetch_kwargs["code_verifier"] = code_verifier
    flow.fetch_token(**fetch_kwargs)
    credentials = flow.credentials

    async with httpx.AsyncClient() as client:
        resp = await client.get(
            "https://www.googleapis.com/oauth2/v2/userinfo",
            headers={"Authorization": f"Bearer {credentials.token}"},
        )
    userinfo = resp.json()
    email = userinfo.get("email", "")

    if email.lower() not in ALLOWED_EMAILS:
        raise HTTPException(status_code=403, detail=f"Access denied for {email}")

    import calendar_store
    calendar_store.save_tokens(email, credentials)

    session_cookie = signer.dumps({"email": email})
    response = RedirectResponse("/")
    response.set_cookie("session", session_cookie, httponly=True, samesite="lax", max_age=86400 * 7)
    response.delete_cookie("pkce_cv")
    return response


@app.get("/logout")
async def logout():
    response = RedirectResponse("/login")
    response.delete_cookie("session")
    return response


# ---------------------------------------------------------------------------
# Main routes
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    session = _get_session(request)
    if not session:
        return RedirectResponse("/login")
    return templates.TemplateResponse("chat.html", {"request": request, "email": session["email"]})


@app.post("/chat")
async def chat(request: Request):
    session = _require_session(request)

    body = await request.json()
    message = body.get("message", "").strip()
    session_id = body.get("session_id", "").strip()
    if not message:
        raise HTTPException(status_code=400, detail="message is required")
    if not session_id:
        raise HTTPException(status_code=400, detail="session_id is required")

    from agent import run_agent

    try:
        response_text = await run_agent(message, session_id=session_id, user_email=session["email"])
    except Exception as e:
        LOGGER.exception("Agent error: %s", e)
        raise HTTPException(status_code=500, detail="Agent error — please try again")

    return JSONResponse({"response": response_text})


# ---------------------------------------------------------------------------
# Session routes
# ---------------------------------------------------------------------------

@app.get("/sessions")
async def list_sessions(request: Request):
    session = _require_session(request)
    import session_store
    sessions = session_store.list_sessions(session["email"])
    return JSONResponse(sessions)


@app.post("/sessions")
async def create_session(request: Request):
    session = _require_session(request)
    import session_store
    new_id = str(uuid4())
    sess = session_store.create_session(session["email"], new_id)
    return JSONResponse({"session_id": sess["session_id"], "title": sess["title"]})


@app.get("/sessions/{session_id}")
async def get_session(request: Request, session_id: str):
    session = _require_session(request)
    import session_store
    data = session_store.load_session(session["email"], session_id)
    if data is None:
        raise HTTPException(status_code=404, detail="Session not found")
    return JSONResponse({
        "session_id": data.get("session_id"),
        "title": data.get("title"),
        "messages": data.get("messages", []),
    })


@app.patch("/sessions/{session_id}")
async def rename_session(request: Request, session_id: str):
    session = _require_session(request)
    body = await request.json()
    new_title = body.get("title", "").strip()
    if not new_title:
        raise HTTPException(status_code=400, detail="title is required")
    import session_store
    session_store.rename_session(session["email"], session_id, new_title)
    return JSONResponse({"session_id": session_id, "title": new_title[:80]})


@app.delete("/sessions/{session_id}")
async def delete_session(request: Request, session_id: str):
    session = _require_session(request)
    import session_store
    from agent import evict_session
    session_store.delete_session(session["email"], session_id)
    evict_session(session_id)
    return JSONResponse({"deleted": session_id})


# ---------------------------------------------------------------------------
# Settings routes
# ---------------------------------------------------------------------------

@app.get("/settings", response_class=HTMLResponse)
async def settings_get(request: Request):
    session = _get_session(request)
    if not session:
        return RedirectResponse("/login")
    p = profile_store.load()
    return templates.TemplateResponse(
        "settings.html",
        {"request": request, "email": session["email"], "profile": p, "saved": False, "error": None},
    )


@app.post("/settings", response_class=HTMLResponse)
async def settings_post(request: Request):
    session = _get_session(request)
    if not session:
        return RedirectResponse("/login")

    form = await request.form()

    _KPI_KEYS = [
        "weekly_cycling_km", "weekly_running_km", "weekly_hours",
        "weekly_active_days", "target_weight_kg", "target_body_fat_pct",
    ]
    kpis = {}
    for k in _KPI_KEYS:
        enabled = form.get(f"kpi_{k}_enabled") == "true"
        try:
            target = float(form.get(f"kpi_{k}_target", "0") or "0")
        except ValueError:
            target = 0.0
        kpis[k] = {"target": target, "enabled": enabled}

    new_profile = {
        "ftp": float(form.get("ftp", 191)),
        "weight_kg": float(form.get("weight_kg", 90)),
        "height_cm": int(float(form.get("height_cm", 178))),
        "age": int(float(form.get("age", 31))),
        "stats_date": form.get("stats_date", "").strip(),
        "goals": (form.get("goals", "") or "").strip(),
        "equipment": (form.get("equipment", "") or "").strip(),
        "reminders": {
            "morning_checkin_enabled": form.get("morning_checkin_enabled") == "true",
            "morning_checkin_time": form.get("morning_checkin_time", "07:30"),
            "training_reminder_enabled": form.get("training_reminder_enabled") == "true",
            "training_reminder_time": form.get("training_reminder_time", "17:00"),
            "evening_checkin_enabled": form.get("evening_checkin_enabled") == "true",
            "evening_checkin_time": form.get("evening_checkin_time", "21:00"),
        },
        "kpis": kpis,
    }

    error = None
    saved = False
    try:
        profile_store.save(new_profile)
        # Invalidate cached runners so the agent picks up the new prompt immediately
        from agent import invalidate_sessions
        invalidate_sessions()
        saved = True
    except Exception as exc:
        error = str(exc)

    return templates.TemplateResponse(
        "settings.html",
        {
            "request": request,
            "email": session["email"],
            "profile": new_profile,
            "saved": saved,
            "error": error,
        },
    )


# ---------------------------------------------------------------------------
# Analytics routes
# ---------------------------------------------------------------------------

@app.get("/health", response_class=HTMLResponse)
async def health_analytics_page(request: Request):
    session = _get_session(request)
    if not session:
        return RedirectResponse("/login")
    return templates.TemplateResponse("health_analytics.html", {"request": request, "email": session["email"]})


@app.get("/training", response_class=HTMLResponse)
async def training_analytics_page(request: Request):
    session = _get_session(request)
    if not session:
        return RedirectResponse("/login")
    return templates.TemplateResponse("training_analytics.html", {"request": request, "email": session["email"]})


@app.get("/goals", response_class=HTMLResponse)
async def goals_page(request: Request):
    session = _get_session(request)
    if not session:
        return RedirectResponse("/login")
    return templates.TemplateResponse("goals.html", {"request": request, "email": session["email"]})


@app.get("/checkin", response_class=HTMLResponse)
async def checkin_page(request: Request):
    session = _get_session(request)
    if not session:
        return RedirectResponse("/login")
    return templates.TemplateResponse("checkin.html", {"request": request, "email": session["email"]})


# ---------------------------------------------------------------------------
# Check-in helpers
# ---------------------------------------------------------------------------

_MOOD_SCORE = {"Terrible": 1, "Bad": 2, "Fine": 3, "Good": 4, "Great!": 5}


def _sheets_service():
    from googleapiclient.discovery import build
    from google.auth import default as _gauth
    creds, _ = _gauth(scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return build("sheets", "v4", credentials=creds)


def _append_to_sheet(sheet_id: str, values: list) -> None:
    if not sheet_id:
        LOGGER.warning("Sheet ID not configured — skipping Sheets write")
        return
    try:
        svc = _sheets_service()
        svc.spreadsheets().values().append(
            spreadsheetId=sheet_id,
            range="Form Responses!A:M",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": [values]},
        ).execute()
    except Exception as exc:
        LOGGER.error("Sheets append failed: %s", exc)


def _bq_insert_checkin(table_id: str, row: dict) -> None:
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT_ID)
    errors = client.insert_rows_json(f"{PROJECT_ID}.garmin.{table_id}", [row])
    if errors:
        LOGGER.error("BQ insert errors for %s: %s", table_id, errors)


# ---------------------------------------------------------------------------
# Check-in API routes
# ---------------------------------------------------------------------------

@app.get("/api/checkin/today")
async def api_checkin_today(request: Request):
    """Return whether morning and/or evening check-in has been submitted today."""
    _require_session(request)
    from google.cloud import bigquery
    from datetime import date
    today = date.today().isoformat()
    client = bigquery.Client(project=PROJECT_ID)
    result = {"morning": False, "evening": False, "date": today}
    try:
        for kind, table in [("morning", "morning_checkin"), ("evening", "evening_checkin")]:
            sql = f"""
            SELECT COUNT(*) AS n FROM `{PROJECT_ID}.garmin.{table}`
            WHERE date = '{today}' AND source = 'app'
            """
            rows = list(client.query(sql).result())
            result[kind] = rows[0].n > 0 if rows else False
    except Exception as exc:
        LOGGER.warning("checkin/today BQ error (table may not exist yet): %s", exc)
    return JSONResponse(result)


@app.post("/api/checkin/morning")
async def api_checkin_morning(request: Request):
    _require_session(request)
    from datetime import datetime, timezone, date
    body = await request.json()
    now = datetime.now(timezone.utc)
    sub_id = str(uuid4())
    feeling = body.get("feeling", "")
    row = {
        "date": date.today().isoformat(),
        "submitted_at": now.isoformat(),
        "feeling": feeling,
        "mood_score": _MOOD_SCORE.get(feeling),
        "working_out": bool(body.get("working_out")),
        "stretching": bool(body.get("stretching")),
        "drinks_tonight": bool(body.get("drinks_tonight")),
        "notes": body.get("notes", "") or "",
        "priority": body.get("priority", "") or "",
        "fill_in_blank": body.get("fill_in_blank", "") or "",
        "source": "app",
        "submission_id": sub_id,
    }
    _bq_insert_checkin("morning_checkin", row)
    _append_to_sheet(MORNING_SHEET_ID, [
        now.strftime("%Y-%m-%d %H:%M:%S"), "",
        feeling,
        "YES" if row["working_out"] else "NO",
        "YES" if row["stretching"] else "NO",
        "YES" if row["drinks_tonight"] else "NO",
        row["notes"], row["priority"], row["fill_in_blank"],
        "", sub_id,
    ])
    return JSONResponse({"ok": True, "submission_id": sub_id})


@app.post("/api/checkin/evening")
async def api_checkin_evening(request: Request):
    _require_session(request)
    from datetime import datetime, timezone, date
    body = await request.json()
    now = datetime.now(timezone.utc)
    sub_id = str(uuid4())
    feeling = body.get("feeling", "")
    row = {
        "date": date.today().isoformat(),
        "submitted_at": now.isoformat(),
        "did_workout": bool(body.get("did_workout")),
        "alcohol_drinks": float(body.get("alcohol_drinks", 0) or 0),
        "tracked_eating": bool(body.get("tracked_eating")),
        "feeling": feeling,
        "mood_score": _MOOD_SCORE.get(feeling),
        "worked_late": bool(body.get("worked_late")),
        "notes": body.get("notes", "") or "",
        "gratitude": body.get("gratitude", "") or "",
        "chocolate": body.get("chocolate", "None") or "None",
        "source": "app",
        "submission_id": sub_id,
    }
    _bq_insert_checkin("evening_checkin", row)
    _append_to_sheet(EVENING_SHEET_ID, [
        now.strftime("%Y-%m-%d %H:%M:%S"), "",
        "YES" if row["did_workout"] else "NO",
        row["alcohol_drinks"],
        "YES" if row["tracked_eating"] else "NO",
        feeling,
        "YES" if row["worked_late"] else "NO",
        row["notes"], row["gratitude"], row["chocolate"],
        "", sub_id,
    ])
    return JSONResponse({"ok": True, "submission_id": sub_id})


@app.get("/api/analytics/health")
async def api_health_analytics(request: Request, days: int = 90):
    _require_session(request)
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT_ID)
    sql = f"""
    SELECT date, weight_lbs, body_fat_pct, muscle_mass_lbs,
           sleep_total_hr, sleep_deep_hr, sleep_rem_hr, sleep_light_hr, sleep_score,
           rhr, hrv_avg, avg_stress, body_battery, body_battery_high, body_battery_low,
           vo2_max, steps, step_goal, cals_total
    FROM `{PROJECT_ID}.garmin.garmin_stats`
    WHERE date >= FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY))
    QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY run_date DESC, timestamp DESC) = 1
    ORDER BY date
    """
    try:
        rows = list(client.query(sql).result())
    except Exception as exc:
        LOGGER.exception("Health analytics BQ error: %s", exc)
        return JSONResponse({"error": str(exc)}, status_code=500)

    _LBS_TO_KG = 1 / 2.20462
    fields = [
        "weight_lbs", "body_fat_pct", "muscle_mass_lbs",
        "sleep_total_hr", "sleep_deep_hr", "sleep_rem_hr", "sleep_light_hr", "sleep_score",
        "rhr", "hrv_avg", "avg_stress", "body_battery", "body_battery_high", "body_battery_low",
        "vo2_max", "steps", "step_goal", "cals_total",
    ]
    data: dict = {"dates": []}
    for f in fields:
        # Expose weight/muscle in kg
        key = f.replace("_lbs", "_kg")
        data[key] = []

    for row in rows:
        data["dates"].append(str(row["date"]))
        for f in fields:
            key = f.replace("_lbs", "_kg")
            v = row[f]
            if v is not None:
                fv = float(v)
                # Convert lbs → kg
                if f.endswith("_lbs"):
                    fv = round(fv * _LBS_TO_KG, 1)
                data[key].append(fv)
            else:
                data[key].append(None)

    # Latest non-null value for each field (for stat cards)
    latest: dict = {}
    remaining = set(fields)
    for row in reversed(rows):
        for f in list(remaining):
            if row[f] is not None:
                key = f.replace("_lbs", "_kg")
                fv = float(row[f])
                if f.endswith("_lbs"):
                    fv = round(fv * _LBS_TO_KG, 1)
                latest[key] = fv
                remaining.discard(f)
        if not remaining:
            break
    data["latest"] = latest
    return JSONResponse(data)


@app.get("/api/analytics/training")
async def api_training_analytics(request: Request, days: int = 90):
    _require_session(request)
    from google.cloud import bigquery
    from collections import Counter
    from datetime import timedelta

    client = bigquery.Client(project=PROJECT_ID)

    stats_sql = f"""
    SELECT date, atl, ctl, tsb
    FROM `{PROJECT_ID}.garmin.garmin_stats`
    WHERE date >= FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY))
    QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY run_date DESC, timestamp DESC) = 1
    ORDER BY date
    """
    acts_sql = f"""
    SELECT date, activity_type, title, tss, duration_s, distance_m,
           normalized_power_w, ftp_watts, avg_hr,
           hr_zone_1_secs, hr_zone_2_secs, hr_zone_3_secs, hr_zone_4_secs, hr_zone_5_secs,
           power_zone_1_secs, power_zone_2_secs, power_zone_3_secs, power_zone_4_secs, power_zone_5_secs
    FROM `{PROJECT_ID}.garmin.garmin_activities`
    WHERE date >= FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY))
    QUALIFY ROW_NUMBER() OVER (PARTITION BY activity_id ORDER BY run_date DESC) = 1
    ORDER BY date
    """
    try:
        stats_rows = list(client.query(stats_sql).result())
        act_rows = list(client.query(acts_sql).result())
    except Exception as exc:
        LOGGER.exception("Training analytics BQ error: %s", exc)
        return JSONResponse({"error": str(exc)}, status_code=500)

    pmc = {
        "dates": [str(r["date"]) for r in stats_rows],
        "atl": [float(r["atl"]) if r["atl"] is not None else None for r in stats_rows],
        "ctl": [float(r["ctl"]) if r["ctl"] is not None else None for r in stats_rows],
        "tsb": [float(r["tsb"]) if r["tsb"] is not None else None for r in stats_rows],
    }

    cycling_types = {
        "cycling", "road_cycling", "gravel_cycling", "mountain_biking",
        "indoor_cycling", "virtual_ride", "spinning",
    }

    activities = []
    hr_zones = [0.0] * 5
    power_zones = [0.0] * 5
    type_counts: Counter = Counter()
    ftp_by_date: dict = {}
    weekly: dict = {}

    for r in act_rows:
        d_str = str(r["date"])
        atype = (r["activity_type"] or "unknown").lower()
        type_counts[atype] += 1

        activities.append({
            "date": d_str,
            "activity_type": atype,
            "title": r["title"] or "",
            "tss": float(r["tss"]) if r["tss"] is not None else None,
            "duration_s": float(r["duration_s"]) if r["duration_s"] is not None else None,
            "distance_m": float(r["distance_m"]) if r["distance_m"] is not None else None,
            "normalized_power_w": float(r["normalized_power_w"]) if r["normalized_power_w"] is not None else None,
            "ftp_watts": float(r["ftp_watts"]) if r["ftp_watts"] is not None else None,
            "avg_hr": float(r["avg_hr"]) if r["avg_hr"] is not None else None,
        })

        for i, col in enumerate(["hr_zone_1_secs", "hr_zone_2_secs", "hr_zone_3_secs", "hr_zone_4_secs", "hr_zone_5_secs"]):
            v = r[col]
            if v is not None:
                hr_zones[i] += float(v)

        if atype in cycling_types:
            for i, col in enumerate(["power_zone_1_secs", "power_zone_2_secs", "power_zone_3_secs", "power_zone_4_secs", "power_zone_5_secs"]):
                v = r[col]
                if v is not None:
                    power_zones[i] += float(v)

        if r["ftp_watts"] is not None:
            ftp_by_date[d_str] = float(r["ftp_watts"])

        d_obj = r["date"]
        week_start = str(d_obj - timedelta(days=d_obj.weekday())) if hasattr(d_obj, "weekday") else d_str[:10]
        if week_start not in weekly:
            weekly[week_start] = {"week_start": week_start, "tss": 0.0, "hours": 0.0, "count": 0}
        weekly[week_start]["tss"] += float(r["tss"]) if r["tss"] is not None else 0.0
        weekly[week_start]["hours"] += (float(r["duration_s"]) / 3600.0) if r["duration_s"] is not None else 0.0
        weekly[week_start]["count"] += 1

    ftp_sorted = sorted(ftp_by_date.items())

    return JSONResponse({
        "pmc": pmc,
        "activities": list(reversed(activities)),  # most recent first for the table
        "hr_zones": hr_zones,
        "power_zones": power_zones,
        "activity_type_counts": dict(type_counts),
        "ftp_trend": {
            "dates": [p[0] for p in ftp_sorted],
            "values": [p[1] for p in ftp_sorted],
        },
        "weekly": sorted(weekly.values(), key=lambda x: x["week_start"]),
        "latest_ctl": pmc["ctl"][-1] if pmc["ctl"] else None,
        "latest_atl": pmc["atl"][-1] if pmc["atl"] else None,
        "latest_tsb": pmc["tsb"][-1] if pmc["tsb"] else None,
    })


@app.get("/api/analytics/goals")
async def api_goals_analytics(request: Request):
    _require_session(request)
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT_ID)
    p = profile_store.load()
    kpis = p.get("kpis", {})

    CYCLING_TYPES = "('cycling','road_cycling','gravel_cycling','mountain_biking','indoor_cycling','virtual_ride','spinning')"
    RUNNING_TYPES = "('running','treadmill_running','trail_running')"

    actuals_sql = f"""
    WITH deduped AS (
      SELECT date, activity_type, distance_m, duration_s
      FROM `{PROJECT_ID}.garmin.garmin_activities`
      WHERE date >= FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)))
      QUALIFY ROW_NUMBER() OVER (PARTITION BY activity_id ORDER BY run_date DESC) = 1
    )
    SELECT
      COALESCE(SUM(CASE WHEN activity_type IN {CYCLING_TYPES} THEN distance_m ELSE 0 END), 0) / 1000.0 AS cycling_km,
      COALESCE(SUM(CASE WHEN activity_type IN {RUNNING_TYPES} THEN distance_m ELSE 0 END), 0) / 1000.0 AS running_km,
      COALESCE(SUM(duration_s), 0) / 3600.0 AS hours,
      COUNT(DISTINCT date) AS active_days
    FROM deduped
    """

    history_sql = f"""
    WITH deduped AS (
      SELECT date, activity_type, distance_m, duration_s
      FROM `{PROJECT_ID}.garmin.garmin_activities`
      WHERE date >= FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE(), INTERVAL 12 WEEK))
      QUALIFY ROW_NUMBER() OVER (PARTITION BY activity_id ORDER BY run_date DESC) = 1
    )
    SELECT
      FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(CAST(date AS DATE), WEEK(MONDAY))) AS week_start,
      COALESCE(SUM(CASE WHEN activity_type IN {CYCLING_TYPES} THEN distance_m ELSE 0 END), 0) / 1000.0 AS cycling_km,
      COALESCE(SUM(CASE WHEN activity_type IN {RUNNING_TYPES} THEN distance_m ELSE 0 END), 0) / 1000.0 AS running_km,
      COALESCE(SUM(duration_s), 0) / 3600.0 AS hours,
      COUNT(DISTINCT date) AS active_days
    FROM deduped
    GROUP BY week_start
    ORDER BY week_start
    """

    weight_sql = f"""
    SELECT weight_lbs, body_fat_pct, date
    FROM `{PROJECT_ID}.garmin.garmin_stats`
    WHERE weight_lbs IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY run_date DESC, timestamp DESC) = 1
    ORDER BY date DESC
    LIMIT 1
    """

    this_week_sql = f"""
    WITH deduped AS (
      SELECT date, title, activity_type, duration_s, distance_m, avg_hr
      FROM `{PROJECT_ID}.garmin.garmin_activities`
      WHERE date >= FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)))
      QUALIFY ROW_NUMBER() OVER (PARTITION BY activity_id ORDER BY run_date DESC) = 1
    )
    SELECT date, title, activity_type, duration_s, distance_m, avg_hr
    FROM deduped
    ORDER BY date DESC
    """

    try:
        actuals_rows = list(client.query(actuals_sql).result())
        history_rows = list(client.query(history_sql).result())
        weight_rows = list(client.query(weight_sql).result())
        week_act_rows = list(client.query(this_week_sql).result())
    except Exception as exc:
        LOGGER.exception("Goals analytics BQ error: %s", exc)
        return JSONResponse({"error": str(exc)}, status_code=500)

    _LBS_TO_KG = 1 / 2.20462
    actuals = {}
    if actuals_rows:
        r = actuals_rows[0]
        actuals = {
            "cycling_km": round(float(r["cycling_km"]), 1),
            "running_km": round(float(r["running_km"]), 1),
            "hours": round(float(r["hours"]), 1),
            "active_days": int(r["active_days"]),
        }

    weight_latest = None
    if weight_rows:
        r = weight_rows[0]
        weight_latest = {
            "weight_kg": round(float(r["weight_lbs"]) * _LBS_TO_KG, 1) if r["weight_lbs"] else None,
            "body_fat_pct": round(float(r["body_fat_pct"]), 1) if r["body_fat_pct"] else None,
            "date": str(r["date"]),
        }

    history = [
        {
            "week_start": str(r["week_start"]),
            "cycling_km": round(float(r["cycling_km"]), 1),
            "running_km": round(float(r["running_km"]), 1),
            "hours": round(float(r["hours"]), 1),
            "active_days": int(r["active_days"]),
        }
        for r in history_rows
    ]

    this_week_activities = [
        {
            "date": str(r["date"]),
            "title": r["title"] or "",
            "activity_type": (r["activity_type"] or "unknown").lower(),
            "duration_s": float(r["duration_s"]) if r["duration_s"] is not None else None,
            "distance_m": float(r["distance_m"]) if r["distance_m"] is not None else None,
            "avg_hr": float(r["avg_hr"]) if r["avg_hr"] is not None else None,
        }
        for r in week_act_rows
    ]

    return JSONResponse({
        "kpis": kpis,
        "actuals": actuals,
        "weight_latest": weight_latest,
        "history": history,
        "this_week_activities": this_week_activities,
    })


# ---------------------------------------------------------------------------
# Garmin sync
# ---------------------------------------------------------------------------

def _cloud_run_token() -> str:
    """Return a short-lived access token for the Cloud Run API using ADC."""
    import google.auth
    import google.auth.transport.requests
    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    creds.refresh(google.auth.transport.requests.Request())
    return creds.token


@app.post("/api/garmin/sync")
async def garmin_sync(request: Request):
    """Trigger the Garmin data-pull Cloud Run Job (max 1 concurrent execution)."""
    _require_session(request)

    base = (
        f"https://run.googleapis.com/v2/projects/{PROJECT_ID}"
        f"/locations/{GARMIN_JOB_REGION}/jobs/{GARMIN_JOB_NAME}"
    )
    try:
        token = _cloud_run_token()
    except Exception as exc:
        LOGGER.error("Could not obtain Cloud Run token: %s", exc)
        return JSONResponse({"status": "error", "message": "Auth error — check service account permissions."}, status_code=500)

    headers = {"Authorization": f"Bearer {token}"}

    async with httpx.AsyncClient(timeout=15) as client:
        # Check for an already-running execution
        list_resp = await client.get(f"{base}/executions", headers=headers, params={"pageSize": 10})
        if list_resp.status_code == 200:
            executions = list_resp.json().get("executions", [])
            for ex in executions:
                if ex.get("completionTime") is None and ex.get("runningCount", 0) > 0:
                    return JSONResponse({"status": "running", "message": "A sync is already in progress. Check back in a few minutes."})
        elif list_resp.status_code != 404:
            LOGGER.warning("Could not list executions: %s %s", list_resp.status_code, list_resp.text)

        # Trigger a new execution
        run_resp = await client.post(f"{base}:run", headers=headers, json={})
        if run_resp.status_code in (200, 202):
            import threading
            import bq_cache
            from agent import warm_bq_cache
            bq_cache.clear()
            # Re-warm the cache ~3 minutes later, once the pipeline has finished
            threading.Thread(target=warm_bq_cache, kwargs={"delay_seconds": 180}, daemon=True).start()
            return JSONResponse({"status": "triggered", "message": "Garmin data sync started. This usually takes 1–2 minutes."})
        else:
            LOGGER.error("Cloud Run job trigger failed: %s %s", run_resp.status_code, run_resp.text)
            return JSONResponse(
                {"status": "error", "message": f"Failed to start job ({run_resp.status_code}). Check Cloud Run permissions."},
                status_code=500,
            )


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def startup_event():
    """Pre-warm the BQ cache in the background so the first query is fast."""
    import threading
    from agent import warm_bq_cache
    threading.Thread(target=warm_bq_cache, daemon=True).start()


@app.get("/health")
async def health():
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# PWA assets
# ---------------------------------------------------------------------------

def _make_png(size: int) -> bytes:
    """Generate a solid #1a73e8 PNG at the given square size (stdlib only)."""
    r, g, b = 26, 115, 232
    raw = b"".join(b"\x00" + bytes([r, g, b] * size) for _ in range(size))

    def chunk(tag: bytes, data: bytes) -> bytes:
        c = tag + data
        return struct.pack(">I", len(data)) + c + struct.pack(">I", zlib.crc32(c) & 0xFFFFFFFF)

    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", size, size, 8, 2, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(raw))
        + chunk(b"IEND", b"")
    )


@app.get("/manifest.json")
async def manifest():
    data = {
        "name": "Cycling Coach AI",
        "short_name": "CycleCoach",
        "description": "Your personal data-driven cycling coach",
        "id": "/",
        "start_url": "/",
        "scope": "/",
        "display": "standalone",
        "background_color": "#1e40af",
        "theme_color": "#2563eb",
        "orientation": "portrait-primary",
        "icons": [
            {"src": "/icon-192.png", "sizes": "192x192", "type": "image/png", "purpose": "any"},
            {"src": "/icon-512.png", "sizes": "512x512", "type": "image/png", "purpose": "any"},
            {"src": "/icon-512.png", "sizes": "512x512", "type": "image/png", "purpose": "maskable"},
        ],
    }
    return JSONResponse(data, headers={"Cache-Control": "public, max-age=86400"})


@app.get("/icon-192.png")
async def icon_192():
    return Response(_make_png(192), media_type="image/png",
                    headers={"Cache-Control": "public, max-age=86400"})


@app.get("/icon-512.png")
async def icon_512():
    return Response(_make_png(512), media_type="image/png",
                    headers={"Cache-Control": "public, max-age=86400"})


@app.get("/icon.svg")
async def icon():
    svg = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512">
  <rect width="512" height="512" rx="112" fill="#1a73e8"/>
  <circle cx="160" cy="320" r="80" fill="none" stroke="white" stroke-width="28"/>
  <circle cx="160" cy="320" r="14" fill="white"/>
  <circle cx="352" cy="320" r="80" fill="none" stroke="white" stroke-width="28"/>
  <circle cx="352" cy="320" r="14" fill="white"/>
  <polyline points="160,320 230,180 352,320" fill="none" stroke="white" stroke-width="28" stroke-linejoin="round" stroke-linecap="round"/>
  <line x1="230" y1="180" x2="210" y2="320" stroke="white" stroke-width="22" stroke-linecap="round"/>
  <line x1="230" y1="180" x2="290" y2="168" stroke="white" stroke-width="22" stroke-linecap="round"/>
  <line x1="280" y1="168" x2="310" y2="185" stroke="white" stroke-width="18" stroke-linecap="round"/>
  <line x1="190" y1="200" x2="165" y2="192" stroke="white" stroke-width="18" stroke-linecap="round"/>
  <line x1="155" y1="192" x2="210" y2="192" stroke="white" stroke-width="18" stroke-linecap="round"/>
</svg>"""
    return Response(svg, media_type="image/svg+xml", headers={"Cache-Control": "public, max-age=86400"})


@app.get("/sw.js")
async def service_worker():
    js = """
const CACHE = 'cycling-coach-v1';

self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', e => e.waitUntil(clients.claim()));

self.addEventListener('fetch', e => {
  if (e.request.method !== 'GET') return;
  e.respondWith(fetch(e.request).catch(() => caches.match(e.request)));
});

self.addEventListener('push', e => {
  let payload = { title: 'Cycling Coach AI', body: 'Tap to open your coaching dashboard.', url: '/' };
  try { payload = { ...payload, ...e.data.json() }; } catch (_) {}
  e.waitUntil(
    self.registration.showNotification(payload.title, {
      body: payload.body,
      icon: '/icon-192.png',
      badge: '/icon-192.png',
      data: { url: payload.url },
    })
  );
});

self.addEventListener('notificationclick', e => {
  e.notification.close();
  e.waitUntil(clients.openWindow(e.notification.data?.url || '/'));
});
"""
    return Response(js, media_type="application/javascript", headers={"Cache-Control": "no-cache"})


# ---------------------------------------------------------------------------
# Push notification routes
# ---------------------------------------------------------------------------

@app.get("/api/push/vapid-key")
async def push_vapid_key(request: Request):
    _require_session(request)
    import vapid_store
    public_key, _ = vapid_store.get_keys()
    return JSONResponse({"publicKey": public_key})


@app.post("/api/push/subscribe")
async def push_subscribe(request: Request):
    session = _require_session(request)
    body = await request.json()
    import push_store
    push_store.save_subscription(session["email"], body)
    return JSONResponse({"status": "subscribed"})


@app.post("/api/push/unsubscribe")
async def push_unsubscribe(request: Request):
    session = _require_session(request)
    body = await request.json()
    endpoint = body.get("endpoint", "")
    import push_store
    push_store.remove_subscription(session["email"], endpoint)
    return JSONResponse({"status": "unsubscribed"})


@app.post("/api/push/test")
async def push_test(request: Request):
    """Send a test notification to all subscriptions for the logged-in user."""
    session = _require_session(request)
    import push_store, vapid_store
    _, private_key = vapid_store.get_keys()
    subs = push_store.get_subscriptions(session["email"])
    if not subs:
        return JSONResponse({"sent": 0, "message": "No push subscriptions found. Enable notifications first."})
    sent = _send_push_to_subs(
        subs, session["email"],
        "Test notification",
        "Push notifications are working for Cycling Coach AI!",
        private_key,
    )
    return JSONResponse({"sent": sent})


@app.post("/api/send-reminders")
async def send_reminders():
    """Called by Cloud Scheduler every 30 min. Sends due reminder notifications."""
    import push_store, vapid_store
    from datetime import datetime
    try:
        from zoneinfo import ZoneInfo
        now = datetime.now(ZoneInfo("Europe/London"))
    except Exception:
        now = datetime.utcnow()

    current_hhmm = now.strftime("%H:%M")
    _, private_key = vapid_store.get_keys()
    total_sent = 0

    for email in push_store.list_all_emails():
        p = profile_store.load()
        reminders = p.get("reminders", {})
        subs = push_store.get_subscriptions(email)
        if not subs:
            continue

        if reminders.get("morning_checkin_enabled") and \
                current_hhmm == reminders.get("morning_checkin_time", "07:30"):
            total_sent += _send_push_to_subs(
                subs, email,
                "Morning Recovery Check",
                "Check your HRV, sleep score, and body battery to plan today's training.",
                private_key,
            )

        if reminders.get("training_reminder_enabled") and \
                current_hhmm == reminders.get("training_reminder_time", "17:00"):
            total_sent += _send_push_to_subs(
                subs, email,
                "Training Reminder",
                "Time to train! Open your coaching dashboard to see today's plan.",
                private_key,
            )

        if reminders.get("evening_checkin_enabled") and \
                current_hhmm == reminders.get("evening_checkin_time", "21:00"):
            total_sent += _send_push_to_subs(
                subs, email,
                "Evening check-in 🌙",
                "How did today go? Take a moment to log your day.",
                private_key,
                url="/checkin",
            )

    LOGGER.info("send-reminders: sent %d notifications at %s", total_sent, current_hhmm)
    return JSONResponse({"sent": total_sent, "time": current_hhmm})


def _send_push_to_subs(
    subs: list,
    email: str,
    title: str,
    body: str,
    private_key_pem: str,
    url: str = "/",
) -> int:
    """Send a push notification to all subscriptions. Returns count of successful sends."""
    import json as _json
    import push_store

    sent = 0
    expired = []
    for sub in subs:
        try:
            from pywebpush import webpush, WebPushException
            webpush(
                subscription_info={
                    "endpoint": sub["endpoint"],
                    "keys": {"p256dh": sub["keys"]["p256dh"], "auth": sub["keys"]["auth"]},
                },
                data=_json.dumps({"title": title, "body": body, "url": url}),
                vapid_private_key=private_key_pem,
                vapid_claims={"sub": "mailto:coach@cycling-coach.app"},
            )
            sent += 1
        except Exception as exc:
            # 404/410 = subscription expired; remove it
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if status in (404, 410):
                expired.append(sub.get("endpoint", ""))
            else:
                LOGGER.error("Push send error for %s: %s", email, exc)

    for endpoint in expired:
        push_store.remove_subscription(email, endpoint)
        LOGGER.info("Removed expired subscription for %s", email)

    return sent
