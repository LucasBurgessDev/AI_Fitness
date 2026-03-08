from __future__ import annotations

import logging
import os
import struct
import zlib
from typing import Optional
from uuid import uuid4

import httpx
from fastapi import FastAPI, Form, HTTPException, Request
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
async def settings_post(
    request: Request,
    ftp: float = Form(...),
    weight_kg: float = Form(...),
    height_cm: int = Form(...),
    age: int = Form(...),
    stats_date: str = Form(...),
    goals: str = Form(...),
    equipment: str = Form(...),
    morning_checkin_enabled: bool = Form(default=False),
    morning_checkin_time: str = Form(default="07:30"),
    training_reminder_enabled: bool = Form(default=False),
    training_reminder_time: str = Form(default="17:00"),
):
    session = _get_session(request)
    if not session:
        return RedirectResponse("/login")

    new_profile = {
        "ftp": ftp,
        "weight_kg": weight_kg,
        "height_cm": height_cm,
        "age": age,
        "stats_date": stats_date,
        "goals": goals.strip(),
        "equipment": equipment.strip(),
        "reminders": {
            "morning_checkin_enabled": morning_checkin_enabled,
            "morning_checkin_time": morning_checkin_time,
            "training_reminder_enabled": training_reminder_enabled,
            "training_reminder_time": training_reminder_time,
        },
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

    LOGGER.info("send-reminders: sent %d notifications at %s", total_sent, current_hhmm)
    return JSONResponse({"sent": total_sent, "time": current_hhmm})


def _send_push_to_subs(
    subs: list,
    email: str,
    title: str,
    body: str,
    private_key_pem: str,
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
                data=_json.dumps({"title": title, "body": body, "url": "/"}),
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
