from __future__ import annotations

import logging
import os
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
    flow = _get_flow()
    auth_url, _ = flow.authorization_url(prompt="consent", access_type="offline")
    return RedirectResponse(auth_url)


@app.get("/auth/callback")
async def auth_callback(request: Request, code: str):
    flow = _get_flow()
    flow.fetch_token(code=code)
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
# Health check
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# PWA assets
# ---------------------------------------------------------------------------

@app.get("/manifest.json")
async def manifest():
    data = {
        "name": "Cycling Coach AI",
        "short_name": "CycleCoach",
        "description": "Your personal data-driven cycling coach",
        "start_url": "/",
        "display": "standalone",
        "background_color": "#1a73e8",
        "theme_color": "#1a73e8",
        "orientation": "portrait-primary",
        "icons": [
            {"src": "/icon.svg", "sizes": "any", "type": "image/svg+xml", "purpose": "any maskable"},
        ],
    }
    return JSONResponse(data, headers={"Cache-Control": "public, max-age=86400"})


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
  // Only cache GET requests for the app shell; pass everything else through
  if (e.request.method !== 'GET') return;
  e.respondWith(fetch(e.request).catch(() => caches.match(e.request)));
});
"""
    return Response(js, media_type="application/javascript", headers={"Cache-Control": "no-cache"})
