from __future__ import annotations

import logging
import os
from typing import Optional

import httpx
from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
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


def _session_id_for(email: str) -> str:
    return email.replace("@", "_").replace(".", "_")


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
    if not message:
        raise HTTPException(status_code=400, detail="message is required")

    from agent import run_agent

    session_id = _session_id_for(session["email"])
    try:
        response_text = await run_agent(message, session_id=session_id)
    except Exception as e:
        LOGGER.exception("Agent error: %s", e)
        raise HTTPException(status_code=500, detail="Agent error — please try again")

    return JSONResponse({"response": response_text})


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
