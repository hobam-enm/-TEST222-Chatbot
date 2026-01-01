
# region [Imports & Setup]
import streamlit as st
from io import BytesIO
from functools import lru_cache

# PDF export (lazy import inside functions)
import pandas as pd
import os
import re
import gc
import time
import json
import base64
import requests
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from uuid import uuid4
import io
import threading

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import google.generativeai as genai
from google.generativeai import caching  # [추가] 캐싱 모듈
from streamlit.components.v1 import html as st_html

# 경로 및 GitHub 설정
BASE_DIR = "/tmp"
SESS_DIR = os.path.join(BASE_DIR, "sessions")
os.makedirs(SESS_DIR, exist_ok=True)

GITHUB_TOKEN = st.secrets.get("GITHUB_TOKEN", "")
GITHUB_REPO = st.secrets.get("GITHUB_REPO", "")
GITHUB_BRANCH = st.secrets.get("GITHUB_BRANCH", "main")


# (추가) 1차 분석 프롬프트 파일 (레포에 함께 커밋해두면 자동 적용)
FIRST_TURN_PROMPT_FILE = "1차 질문 프롬프트.md"
REPO_DIR = os.path.dirname(os.path.abspath(__file__))
PGC_CACHE_DIR = os.path.join(REPO_DIR, "pgc_cache")

def load_first_turn_system_prompt() -> str:
    """레포의 '1차 질문 프롬프트.md'만 사용한다(폴백 없음)."""
    if not os.path.exists(FIRST_TURN_PROMPT_FILE):
        raise RuntimeError(f"프롬프트 파일이 없습니다: {FIRST_TURN_PROMPT_FILE}")
    with open(FIRST_TURN_PROMPT_FILE, "r", encoding="utf-8") as f:
        txt = f.read().strip()
    if not txt:
        raise RuntimeError(f"프롬프트 파일이 비어있습니다: {FIRST_TURN_PROMPT_FILE}")
    return txt


KST = timezone(timedelta(hours=9))

def now_kst() -> datetime:
    return datetime.now(tz=KST)

def to_iso_kst(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=KST)
    return dt.astimezone(KST).isoformat(timespec="seconds")

def kst_to_rfc3339_utc(dt_kst: datetime) -> str:
    if dt_kst.tzinfo is None:
        dt_kst = dt_kst.replace(tzinfo=KST)
    return dt_kst.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
# endregion



# region [Page Config & CSS]
st.set_page_config(
    page_title="(테스트)유튜브 댓글분석: 챗봇",
    layout="wide",
    initial_sidebar_state="expanded"
)

GLOBAL_CSS = r'''
<style>
  /* ===== App chrome ===== */
  header, footer, #MainMenu { visibility: hidden; }

  /* ===== Main padding ===== */
  .main .block-container{
    padding-top: 1.6rem;
    padding-right: 1rem;
    padding-left: 1rem;
    padding-bottom: 5rem;
    max-width: 1300px;
  }

  /* ===== Sidebar width ===== */
  [data-testid="stSidebar"]{
    width: 360px !important;
    min-width: 360px !important;
    max-width: 360px !important;
  }
  [data-testid="stSidebar"] + div[class*="resizer"]{ display:none; }

  /* ===== Sidebar base spacing reset ===== */
  [data-testid="stSidebarContent"]{
    padding: 12px 14px 12px 14px !important;
  }
  [data-testid="stSidebar"] .element-container,
  [data-testid="stSidebar"] .stMarkdown,
  [data-testid="stSidebar"] .stButton,
  [data-testid="stSidebar"] .stDownloadButton,
  [data-testid="stSidebar"] [data-testid="stVerticalBlock"] > div{
    margin: 0 !important;
    padding: 0 !important;
  }
  [data-testid="stSidebar"] [data-testid="stHorizontalBlock"]{
    gap: 8px !important;
    margin: 0 !important;
    padding: 0 !important;
  }

  /* Divider / section title spacing */
  [data-testid="stSidebar"] hr{
    margin: 10px 0 !important;
  }
  [data-testid="stSidebar"] h4,
  [data-testid="stSidebar"] .stMarkdown h4{
    margin: 8px 0 8px 0 !important;
    padding: 0 !important;
    font-weight: 700;
  }

  /* ===== Session list (대화 기록) ===== */
  .session-list{
    margin-top: 6px !important;
  }
  .session-list .sess-name .stButton button{
    background: #ffffff !important;
    border: 1px solid #e5e7eb !important;
    border-radius: 14px !important;
    padding: 0.52rem 0.72rem !important;
    box-shadow: none !important;
    color: #111827 !important;
    font-size: 0.90rem !important;
    font-weight: 650 !important;
    text-align: left !important;
    width: 100% !important;
    overflow: hidden !important;
    text-overflow: ellipsis !important;
    white-space: nowrap !important;
  }
  .session-list .sess-name .stButton button:hover{
    border-color: #d1d5db !important;
    background: #f9fafb !important;
  }

  /* ⋯ 버튼 */
  .session-list .more-menu .stButton button{
    background: #ffffff !important;
    border: 1px solid #e5e7eb !important;
    border-radius: 12px !important;
    padding: 0.48rem 0.55rem !important;
    min-height: 2.15rem !important;
    line-height: 1 !important;
    box-shadow: none !important;
  }
  .session-list .more-menu .stButton button:hover{
    border-color: #d1d5db !important;
    background: #f9fafb !important;
  }

  /* Rename input compact */
  [data-testid="stSidebar"] input{
    padding-top: 0.38rem !important;
    padding-bottom: 0.38rem !important;
  }

  /* Assistant message font */
  [data-testid="stChatMessage"]:has(span[data-testid="chat-avatar-assistant"]) p,
  [data-testid="stChatMessage"]:has(span[data-testid="chat-avatar-assistant"]) li{
    font-size: 0.95rem;
  }
</style>
'''
st.markdown(GLOBAL_CSS, unsafe_allow_html=True)
# endregion


# region [Constants & State Management]
_YT_FALLBACK, _GEM_FALLBACK = [], []
YT_API_KEYS       = list(st.secrets.get("YT_API_KEYS", [])) or _YT_FALLBACK
GEMINI_API_KEYS   = list(st.secrets.get("GEMINI_API_KEYS", [])) or _GEM_FALLBACK
GEMINI_MODEL      = "gemini-3-flash-preview"  
GEMINI_TIMEOUT    = 120
GEMINI_MAX_TOKENS = 8192
MAX_TOTAL_COMMENTS   = 120_000
MAX_COMMENTS_PER_VID = 4_000
CACHE_TTL_MINUTES    = 20  # [추가] 캐시 수명 (분)

# [추가] Gemini 동시 호출(In-flight) 제한
MAX_GEMINI_INFLIGHT = max(1, int(st.secrets.get("MAX_GEMINI_INFLIGHT", 3) or 3))
GEMINI_INFLIGHT_WAIT_SEC = int(st.secrets.get("GEMINI_INFLIGHT_WAIT_SEC", 120) or 120)

# 프로세스 전역 세마포어 (동시 Gemini 호출 제한)
_GEMINI_SEM = threading.BoundedSemaphore(MAX_GEMINI_INFLIGHT)
_GEMINI_TLOCAL = threading.local()

class GeminiInflightSlot:
    """Gemini API 호출 동시성 제한.

    - 동일 스레드에서 중첩 호출(캐시 -> 폴백 등) 시 데드락 방지.
    - 슬롯이 없으면 최대 GEMINI_INFLIGHT_WAIT_SEC 동안 대기.
    """
    def __init__(self, wait_sec: int = None):
        self.wait_sec = GEMINI_INFLIGHT_WAIT_SEC if wait_sec is None else int(wait_sec)
        self.acquired = False

    def __enter__(self):
        if getattr(_GEMINI_TLOCAL, "held", False):
            # 재진입: 이미 슬롯 보유 (중첩 호출 데드락 방지)
            return self

        deadline = time.time() + max(0, self.wait_sec)
        while True:
            if _GEMINI_SEM.acquire(timeout=0.2):
                self.acquired = True
                _GEMINI_TLOCAL.held = True
                return self
            if time.time() >= deadline:
                raise TimeoutError("GEMINI_INFLIGHT_TIMEOUT")

    def __exit__(self, exc_type, exc, tb):
        if self.acquired:
            _GEMINI_TLOCAL.held = False
            _GEMINI_SEM.release()
        return False


def ensure_state():
    defaults = {
        "chat": [],
        "last_schema": None,
        "last_csv": "",
        "last_df": None,
        "sample_text": "",
        "loaded_session_name": None,
        "own_ip_mode": False,
        "own_ip_toggle_prev": None,
        "current_cache": None, # [추가] 캐시 정보 저장용
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def _reset_chat_only(keep_auth: bool = True):
    """전체 clear() 대신, 대화/분석 상태만 안전하게 초기화."""
    auth_keys = {
        "auth_ok", "auth_user_id", "auth_role", "auth_display_name",
        "client_instance_id", "_auth_users_cache"
    }
    # 흐름 제어 키가 있다면 여기 유지(앱 내에서 사용 중이면)
    safe_flow_keys = {"session_to_load", "session_to_delete"}
    keep = set()
    if keep_auth:
        keep |= auth_keys
    keep |= safe_flow_keys

    for k in list(st.session_state.keys()):
        if k in keep:
            continue
        del st.session_state[k]

    ensure_state()

ensure_state()
# endregion


# region [PDF Export: current session -> PDF]
@lru_cache(maxsize=1)
def _pdf_font_name() -> str:
    """Return a registered font name for Korean text if available. Fallback: Helvetica."""
    try:
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.ttfonts import TTFont
    except ModuleNotFoundError:
        return "Helvetica"

    candidates = [
        ("NanumGothic", "./fonts/NanumGothic.ttf"),
        ("NanumGothic", "./NanumGothic.ttf"),
        ("NanumGothic", "/usr/share/fonts/truetype/nanum/NanumGothic.ttf"),
        ("NanumGothicCoding", "/usr/share/fonts/truetype/nanum/NanumGothicCoding.ttf"),
        ("UnDotum", "/usr/share/fonts/truetype/unfonts-core/UnDotum.ttf"),
        ("UnBatang", "/usr/share/fonts/truetype/unfonts-core/UnBatang.ttf"),
        ("NotoSansCJKkr", "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"),
        ("NotoSansKR", "/usr/share/fonts/truetype/noto/NotoSansKR-Regular.ttf"),
    ]

    for name, fp in candidates:
        if os.path.exists(fp):
            try:
                # Avoid duplicate registration errors
                if name not in pdfmetrics.getRegisteredFontNames():
                    pdfmetrics.registerFont(TTFont(name, fp))
                return name
            except Exception:
                continue
    return "Helvetica"


def _strip_html_to_text(s: str) -> str:
    """Best-effort: convert HTML-ish strings to readable text for PDF."""
    if not s:
        return ""
    # Common line breaks
    s = re.sub(r"<\s*br\s*/?\s*>", "\n", s, flags=re.I)
    s = re.sub(r"</\s*p\s*>", "\n\n", s, flags=re.I)
    s = re.sub(r"<\s*li\s*>", "• ", s, flags=re.I)
    s = re.sub(r"</\s*li\s*>", "\n", s, flags=re.I)
    # Remove all tags
    s = re.sub(r"<[^>]+>", "", s)
    # Unescape entities
    try:
        import html as _html
        s = _html.unescape(s)
    except Exception:
        pass
    # Normalize whitespace
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"\n{3,}", "\n\n", s).strip()
    return s


def build_session_pdf_bytes(session_title: str, user_label: str, chat: list) -> bytes:
    """
    Export the current session as a 'captured-like' chat PDF:
    - user/assistant speech bubbles
    - left/right alignment
    - HTML tags stripped (so raw HTML won't leak into PDF)
    """
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.pdfgen import canvas
        from reportlab.lib.utils import simpleSplit
        from reportlab.lib.colors import HexColor
    except ModuleNotFoundError:
        return b""  # reportlab missing

    font = _pdf_font_name()

    buf = BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    w, h = A4

    # Layout
    margin_l, margin_r = 18 * 2.8346, 18 * 2.8346  # ~18mm
    margin_t, margin_b = 18 * 2.8346, 18 * 2.8346
    max_bubble_w = (w - margin_l - margin_r) * 0.78
    pad_x, pad_y = 10, 8
    line_h = 13

    y = h - margin_t

    def new_page():
        nonlocal y
        c.showPage()
        y = h - margin_t

    def draw_title():
        nonlocal y
        c.setFont(font, 16)
        c.drawString(margin_l, y, f"대화 기록: {session_title}")
        y -= 22
        c.setFont(font, 10)
        ts = datetime.now().strftime("%Y-%m-%d %H:%M")
        label = (user_label or "").strip()
        c.drawString(margin_l, y, f"사용자: {label}   ·   생성: {ts}")
        y -= 18
        y -= 8

    def draw_bubble(role: str, text: str):
        nonlocal y
        role = (role or "").lower()
        is_user = role == "user"

        # Colors
        fill = HexColor("#EAFBF2") if is_user else HexColor("#F3F4F6")
        stroke = HexColor("#CDEEDB") if is_user else HexColor("#E5E7EB")
        text_color = HexColor("#0F172A")

        # Clean text
        t = _strip_html_to_text(text or "")
        if not t:
            t = " "

        # Split lines by paragraphs then wrap
        c.setFont(font, 10.5)
        wrapped = []
        for para in t.split("\n"):
            if para.strip() == "":
                wrapped.append("")
                continue
            wrapped.extend(simpleSplit(para, font, 10.5, max_bubble_w - pad_x * 2))
        if not wrapped:
            wrapped = [" "]

        bubble_h = pad_y * 2 + line_h * len(wrapped) + 10  # +label space
        # Page break if needed
        if y - bubble_h < margin_b:
            new_page()

        # Bubble width: fit to longest line (capped)
        max_line_w = 0
        for ln in wrapped:
            try:
                max_line_w = max(max_line_w, c.stringWidth(ln, font, 10.5))
            except Exception:
                pass
        bubble_w = min(max_bubble_w, max(220, max_line_w + pad_x * 2))  # minimum width

        x = (w - margin_r - bubble_w) if is_user else margin_l

        # Draw label
        c.setFillColor(HexColor("#64748B"))
        c.setFont(font, 9)
        who = "나" if is_user else "AI"
        c.drawString(x + pad_x, y, who)
        y -= 12

        # Bubble rect
        c.setFillColor(fill)
        c.setStrokeColor(stroke)
        c.roundRect(x, y - (bubble_h - 12), bubble_w, bubble_h - 12, 10, fill=1, stroke=1)

        # Text
        c.setFillColor(text_color)
        c.setFont(font, 10.5)
        tx = x + pad_x
        ty = y - pad_y - 2
        for ln in wrapped:
            c.drawString(tx, ty, ln)
            ty -= line_h

        # Advance y
        y = y - (bubble_h - 12) - 12

    draw_title()

    for m in chat or []:
        draw_bubble(m.get("role"), m.get("content", ""))

    c.save()
    return buf.getvalue()


def _session_title_for_pdf() -> str:

    return st.session_state.get("loaded_session_name") or "현재대화"

def _get_cached_session_pdf_bytes() -> bytes:
    title = _session_title_for_pdf()
    user_label = st.session_state.get("auth_display_name") or st.session_state.get("auth_user_id") or ""
    raw = json.dumps(st.session_state.get("chat", []), ensure_ascii=False, sort_keys=True).encode("utf-8")
    h = hashlib.sha256(raw).hexdigest()
    if st.session_state.get("_pdf_chat_hash") != h:
        st.session_state["_pdf_chat_hash"] = h
        st.session_state["_pdf_bytes"] = build_session_pdf_bytes(title, user_label, st.session_state.get("chat", []))
    return st.session_state.get("_pdf_bytes") or b""
# endregion


# region [Auth: ID/PW in secrets.toml]
import hmac
import hashlib
from typing import Dict, Optional

def _load_auth_users_from_secrets() -> Dict[str, dict]:
    """Load users from Streamlit secrets.

    Supports:
      - [[users]] ... at root
      - [auth] ... with users list (depending on secrets layout)
    """
    users = []
    try:
        if "users" in st.secrets:
            users = list(st.secrets.get("users") or [])
        elif "auth" in st.secrets and isinstance(st.secrets.get("auth"), dict) and "users" in st.secrets["auth"]:
            users = list(st.secrets["auth"].get("users") or [])
    except Exception:
        users = []

    out = {}
    for u in users:
        if not isinstance(u, dict):
            continue
        uid = (u.get("id") or "").strip()
        if not uid:
            continue
        out[uid] = u
    return out

def _get_auth_pepper() -> str:
    try:
        if "AUTH_PEPPER" in st.secrets:
            return str(st.secrets.get("AUTH_PEPPER") or "")
        if "auth" in st.secrets and isinstance(st.secrets.get("auth"), dict):
            return str(st.secrets["auth"].get("pepper") or "")
    except Exception:
        pass
    return ""

def _pbkdf2_sha256_verify(password: str, encoded: str, pepper: str = "") -> bool:
    """Verify 'pbkdf2_sha256$iters$salt_b64$dk_b64'"""
    try:
        parts = encoded.split("$")
        if len(parts) != 4 or parts[0] != "pbkdf2_sha256":
            return False
        iters = int(parts[1])
        salt = base64.b64decode(parts[2].encode("utf-8"))
        expect = base64.b64decode(parts[3].encode("utf-8"))
        dk = hashlib.pbkdf2_hmac("sha256", (password + pepper).encode("utf-8"), salt, iters, dklen=len(expect))
        return hmac.compare_digest(dk, expect)
    except Exception:
        return False

def verify_user_password(user_rec: dict, password: str) -> bool:
    pepper = _get_auth_pepper()
    pw_hash = (user_rec.get("pw_hash") or "").strip()

    # Recommended: pbkdf2_sha256$iters$salt_b64$dk_b64
    if pw_hash.startswith("pbkdf2_sha256$"):
        return _pbkdf2_sha256_verify(password, pw_hash, pepper=pepper)

    # Legacy fallback: plain 'pw'
    pw_plain = user_rec.get("pw")
    if isinstance(pw_plain, str) and pw_plain:
        return hmac.compare_digest(pw_plain, password)

    return False

def get_current_user() -> Optional[dict]:
    uid = st.session_state.get("auth_user_id")
    users = st.session_state.get("_auth_users_cache") or _load_auth_users_from_secrets()
    st.session_state["_auth_users_cache"] = users
    return users.get(uid) if uid else None

def is_authenticated() -> bool:
    return bool(st.session_state.get("auth_ok") and st.session_state.get("auth_user_id"))

def _qp_get() -> dict:
    """Query params helper (supports both old/new Streamlit APIs)."""
    try:
        # Streamlit >= 1.30
        return dict(st.query_params)
    except Exception:
        try:
            return st.experimental_get_query_params()
        except Exception:
            return {}

def _qp_set(**kwargs):
    """Set query params helper (supports both old/new Streamlit APIs)."""
    # Normalize: remove keys with None/""/[] values
    cleaned = {}
    for k, v in kwargs.items():
        if v is None:
            continue
        if isinstance(v, (list, tuple)):
            if len(v) == 0:
                continue
            cleaned[k] = list(v)
        else:
            s = str(v).strip()
            if s == "":
                continue
            cleaned[k] = s

    try:
        st.query_params.clear()
        for k, v in cleaned.items():
            st.query_params[k] = v
        return
    except Exception:
        pass

    try:
        st.experimental_set_query_params(**cleaned)
    except Exception:
        return

def _b64url_encode(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode("utf-8").rstrip("=")

def _b64url_decode(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode((s + pad).encode("utf-8"))

def _auth_signing_secret() -> bytes:
    # Prefer pepper; fallback to repo token; fallback to a fixed dev string (last resort).
    pepper = _get_auth_pepper()
    secret = pepper or str(st.secrets.get("AUTH_SIGNING_SECRET", "") or "") or (GITHUB_TOKEN or "") or "dev-secret"
    return secret.encode("utf-8")

def _make_auth_token(user_id: str, ttl_hours: int = None) -> str:
    ttl = ttl_hours if ttl_hours is not None else int(st.secrets.get("AUTH_TOKEN_TTL_HOURS", 24*14) or (24*14))
    exp = int(time.time() + max(60, ttl * 3600))
    payload = {"uid": user_id, "exp": exp}
    raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    body = _b64url_encode(raw)
    sig = hmac.new(_auth_signing_secret(), body.encode("utf-8"), hashlib.sha256).digest()
    return f"{body}.{_b64url_encode(sig)}"

def _verify_auth_token(token: str) -> Optional[dict]:
    try:
        if not token or "." not in token:
            return None
        body, sig = token.split(".", 1)
        expect = hmac.new(_auth_signing_secret(), body.encode("utf-8"), hashlib.sha256).digest()
        if not hmac.compare_digest(_b64url_decode(sig), expect):
            return None
        payload = json.loads(_b64url_decode(body).decode("utf-8"))
        if not isinstance(payload, dict):
            return None
        if int(payload.get("exp", 0)) < int(time.time()):
            return None
        uid = (payload.get("uid") or "").strip()
        if not uid:
            return None
        return payload
    except Exception:
        return None

def _logout_and_clear():
    # Keep query params clean
    _qp_set()  # clears all
    # clear in-memory session
    _reset_chat_only(keep_auth=False)

def require_auth():
    """Gate the app behind a login screen if users are configured in secrets.

    - 로그인 성공 시 URL query param(auth=...)에 서명 토큰을 심어서 새로고침에도 로그인 유지
    - 로그아웃은 사이드바 '로그아웃' 버튼으로 처리(같은 창에서 상태 초기화)
    """
    users = st.session_state.get("_auth_users_cache") or _load_auth_users_from_secrets()
    st.session_state["_auth_users_cache"] = users
    auth_enabled = bool(users)

    if not auth_enabled:
        return  # no users configured -> open access

    # Handle logout via query param
    qp = _qp_get()
    if "logout" in qp:
        _logout_and_clear()

    # Already authenticated in this session?
    if is_authenticated():
        u = get_current_user() or {}
        if u and (u.get("active") is False):
            st.session_state.pop("auth_ok", None)
            st.session_state.pop("auth_user_id", None)
        else:
            return

    # Try restore from signed token in query params
    token = None
    try:
        if "auth" in qp:
            token = qp.get("auth")
            if isinstance(token, list):
                token = token[0] if token else None
    except Exception:
        token = None

    payload = _verify_auth_token(str(token or ""))
    if payload:
        uid = payload["uid"]
        rec = users.get(uid)
        if rec and (rec.get("active") is not False):
            st.session_state["auth_ok"] = True
            st.session_state["auth_user_id"] = uid
            st.session_state["auth_role"] = rec.get("role", "user")
            st.session_state["auth_display_name"] = rec.get("display_name", uid)
            st.session_state["client_instance_id"] = st.session_state.get("client_instance_id") or uuid4().hex[:10]
            return

    # --- Login Screen (Centered / Clean) ---
    c1, c2, c3 = st.columns([1.2, 1.0, 1.2])
    with c2:
        st.markdown("<div style='height:10vh;'></div>", unsafe_allow_html=True)

        # 앱 이름
        st.markdown(
            """
            <div style="text-align:center;">
              <h1 style="font-size:2.0rem; font-weight:700; margin:0;
                         background:-webkit-linear-gradient(45deg,#4285F4,#9B72CB,#D96570,#F2A60C);
                         -webkit-background-clip:text; -webkit-text-fill-color:transparent;">
                💬 유튜브 댓글분석: AI 챗봇
              </h1>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # 로그인 타이틀
        st.markdown(
            "<div style='text-align:center; margin-top:0.9rem; margin-bottom:0.6rem;'>"
            "<h2 style='font-size:1.3rem; font-weight:650; margin:0; color:#111827;'>로그인</h2>"
            "</div>",
            unsafe_allow_html=True,
        )

        with st.form("login_form", clear_on_submit=False):
            uid = st.text_input("ID", value="", placeholder="아이디")
            pw = st.text_input("Password", value="", type="password", placeholder="비밀번호")
            submitted = st.form_submit_button("로그인", use_container_width=True)

        if submitted:
            uid = (uid or "").strip()
            rec = users.get(uid)
            if (not rec) or rec.get("active") is False:
                st.error("ID 또는 비밀번호가 올바르지 않습니다.")
                st.stop()
            if not verify_user_password(rec, pw):
                st.error("ID 또는 비밀번호가 올바르지 않습니다.")
                st.stop()

            st.session_state["auth_ok"] = True
            st.session_state["auth_user_id"] = uid
            st.session_state["auth_role"] = rec.get("role", "user")
            st.session_state["auth_display_name"] = rec.get("display_name", uid)
            st.session_state["client_instance_id"] = st.session_state.get("client_instance_id") or uuid4().hex[:10]

            # persist login in query params
            tok = _make_auth_token(uid)
            _qp_set(auth=tok)
            return

    st.stop()
# endregion



# region [Helper Classes]
class RotatingKeys:
    def __init__(self, keys, state_key: str, on_rotate=None):
        self.keys = [k.strip() for k in (keys or []) if isinstance(k, str) and k.strip()][:10]
        self.state_key = state_key
        self.on_rotate = on_rotate

        idx = st.session_state.get(state_key, 0)
        self.idx = 0 if not self.keys else (idx % len(self.keys))
        st.session_state[state_key] = self.idx

    def current(self):
        return self.keys[self.idx % len(self.keys)] if self.keys else None

    def rotate(self):
        if not self.keys:
            return
        self.idx = (self.idx + 1) % len(self.keys)
        st.session_state[self.state_key] = self.idx
        if callable(self.on_rotate):
            self.on_rotate(self.idx, self.current())

class RotatingYouTube:
    def __init__(self, keys, state_key="yt_key_idx"):
        self.rot = RotatingKeys(keys, state_key)
        self.service = None
        self._build()

    def _build(self):
        key = self.rot.current()
        if not key:
            raise RuntimeError("YouTube API Key가 비어 있습니다.")
        self.service = build("youtube", "v3", developerKey=key)

    def execute(self, factory):
        # [수정] 키 개수만큼 반복해서 재시도 (모든 키를 다 찔러봄)
        max_retries = len(self.rot.keys)
        last_error = None

        for _ in range(max_retries + 1):
            try:
                return factory(self.service).execute()
            except HttpError as e:
                last_error = e
                status = getattr(getattr(e, 'resp', None), 'status', None)
                msg = (getattr(e, 'content', b'').decode('utf-8', 'ignore') or '').lower()
                
                # 403(Quota) 또는 429(Rate Limit) 발생 시에만 로테이션
                if status in (403, 429) and any(t in msg for t in ["quota", "rate", "limit"]):
                    print(f"⚠️ [YouTube API] 키 만료/제한 감지. 다음 키로 교체 시도... (Current: {self.rot.idx})")
                    self.rot.rotate() # 다음 키로 인덱스 변경
                    self._build()     # 서비스 재구축
                    continue          # 루프 다시 실행 (재시도)
                
                # 쿼터 문제가 아닌 다른 에러(400, 404 등)면 즉시 에러 발생
                raise e
        
        # 모든 키를 다 써봤는데도 안 되면 마지막 에러 발생
        raise last_error
# endregion

# region [GitHub & Session Management]
def _gh_headers(token: str):
    # Fine-grained PAT 호환을 위해 Bearer 우선
    auth = f"Bearer {token}" if token else ""
    return {
        "Authorization": auth,
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "ytcc-chatbot"
    }

def github_upload_file(repo, branch, path_in_repo, local_path, token):
    url = f"https://api.github.com/repos/{repo}/contents/{path_in_repo}"
    with open(local_path, "rb") as f:
        content = base64.b64encode(f.read()).decode()

    headers = _gh_headers(token)
    get_resp = requests.get(url + f"?ref={branch}", headers=headers)
    sha = get_resp.json().get("sha") if get_resp.ok else None

    data = {
        "message": f"archive: {os.path.basename(path_in_repo)}",
        "content": content,
        "branch": branch
    }
    if sha:
        data["sha"] = sha

    resp = requests.put(url, headers=headers, json=data)
    resp.raise_for_status()
    return resp.json()


def github_list_dir(repo, branch, folder, token):
    url = f"https://api.github.com/repos/{repo}/contents/{folder}?ref={branch}"
    resp = requests.get(url, headers=_gh_headers(token))
    if resp.ok:
        return [item['name'] for item in resp.json() if item['type'] == 'dir']
    return []

def github_download_file(repo, branch, path_in_repo, token, local_path):
    url = f"https://api.github.com/repos/{repo}/contents/{path_in_repo}?ref={branch}"
    resp = requests.get(url, headers=_gh_headers(token))
    if resp.ok:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "wb") as f:
            f.write(base64.b64decode(resp.json()["content"]))
        return True
    return False


# region [PGC Cache: Auto Sync & Search]
def _cache_local_dir() -> str:
    """PGC 캐시 폴더(레포 내 pgc_cache/)."""
    os.makedirs(PGC_CACHE_DIR, exist_ok=True)
    return PGC_CACHE_DIR

def _extract_vid_from_cache_item(obj):
    """캐시 JSON 내부 구조가 달라도 최대한 video_id를 뽑아냅니다."""
    if not isinstance(obj, dict):
        return None, None, None
    vid = obj.get("video_id") or obj.get("videoId") or obj.get("id") or obj.get("videoId ")
    title = obj.get("title") or (obj.get("snippet") or {}).get("title") or ""
    desc = obj.get("description") or (obj.get("snippet") or {}).get("description") or ""
    return (vid, title or "", desc or "")

def normalize_text_for_search(text: str) -> str:
    """[핵심] 띄어쓰기/특수문자 무시하고 검색 (ytan 스타일)"""
    if not text: return ""
    return re.sub(r'[^a-zA-Z0-9가-힣]', '', text).lower()

def hashtagify_keyword(keyword: str) -> str:
    """UGC 검색용: 키워드 앞에 #을 붙여 검색 정확도를 높임."""
    kw = (keyword or "").strip()
    if not kw:
        return ""
    return kw if kw.startswith("#") else f"#{kw}"

def load_pgc_video_ids_by_keyword(keyword: str, start_dt: datetime = None, end_dt: datetime = None):
    """
    로컬 캐시 JSON에서 keyword로 PGC 영상 후보 찾기.
    [수정] start_dt, end_dt가 있으면 'publishedAt'을 확인하여 기간 필터링 수행.
    """
    keyword = (keyword or "").strip()
    if not keyword:
        return []

    cache_dir = _cache_local_dir()
    files = []
    for fn in os.listdir(cache_dir):
        if re.fullmatch(r"cache_token_.*\.json", fn):
            files.append(os.path.join(cache_dir, fn))

    vids = []
    kw_norm = normalize_text_for_search(keyword)

    for fp in files:
        try:
            with open(fp, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue

        candidates = []
        if isinstance(data, list):
            candidates = data
        elif isinstance(data, dict):
            if isinstance(data.get("videos"), list):
                candidates = data.get("videos", [])
            elif isinstance(data.get("items"), list):
                candidates = data.get("items", [])
            else:
                candidates = [data]

        for it in candidates:
            # 1. 날짜 필터링 (publishedAt 확인) - 여기가 핵심 수정 사항
            if start_dt or end_dt:
                pub_str = it.get("date")  # ✅ publishedAt → date
                if not pub_str:
                    continue  
                try:
                    pub_dt = datetime.fromisoformat(pub_str.replace("Z", "+00:00"))

                    if start_dt and pub_dt < start_dt:
                        continue
                    if end_dt and pub_dt > end_dt:
                        continue
                except Exception:
                    continue

            # 2. 키워드 매칭
            vid, title, desc = _extract_vid_from_cache_item(it)
            if not vid or not YTB_ID_RE.fullmatch(str(vid)):
                continue
            
            title_norm = normalize_text_for_search(title)
            desc_norm = normalize_text_for_search(desc)
            
            if (kw_norm in title_norm) or (kw_norm in desc_norm):
                vids.append(str(vid))

    return list(dict.fromkeys(vids))
# endregion


def github_delete_folder(repo, branch, folder_path, token):
    contents_url = f"https://api.github.com/repos/{repo}/contents/{folder_path}?ref={branch}"
    headers = _gh_headers(token)
    resp = requests.get(contents_url, headers=headers)
    if not resp.ok:
        return
    for item in resp.json():
        delete_url = f"https://api.github.com/repos/{repo}/contents/{item['path']}"
        data = {"message": f"delete: {item['name']}", "sha": item['sha'], "branch": branch}
        requests.delete(delete_url, headers=headers, json=data).raise_for_status()

def github_rename_session(user_id: str, old_name: str, new_name: str, token):
    contents_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/sessions/{user_id}/{old_name}?ref={GITHUB_BRANCH}"
    resp = requests.get(contents_url, headers=_gh_headers(token))
    resp.raise_for_status()
    files_to_move = resp.json()

    for item in files_to_move:
        filename = item['name']
        local_path = os.path.join(SESS_DIR, filename)
        if not github_download_file(GITHUB_REPO, GITHUB_BRANCH, item['path'], token, local_path):
            raise Exception(f"Failed to download {filename} from {old_name}")
        github_upload_file(GITHUB_REPO, GITHUB_BRANCH, f"sessions/{user_id}/{new_name}/{filename}", local_path, token)

    github_delete_folder(GITHUB_REPO, GITHUB_BRANCH, f"sessions/{user_id}/{old_name}", token)

def _session_base_keyword() -> str:
    schema = st.session_state.get("last_schema", {}) or {}
    kw = (schema.get("keywords") or ["세션"])[0]
    kw = (kw or "").strip()
    # 한글/영문/숫자만 남기고(공백 제거), 너무 길면 컷
    base = re.sub(r"[^0-9A-Za-z가-힣]", "", kw)
    base = base[:12] if base else "세션"
    return base

def _next_session_number(user_id: str, base: str) -> int:
    """같은 키워드(base)로 저장된 세션이 있으면 뒤 숫자를 증가."""
    try:
        if not all([GITHUB_TOKEN, GITHUB_REPO]):
            return 1
        sessions = github_list_dir(GITHUB_REPO, GITHUB_BRANCH, f"sessions/{user_id}", GITHUB_TOKEN) or []
    except Exception:
        sessions = []

    pat = re.compile(rf"^{re.escape(base)}(\d+)$")
    max_n = 0
    for s in sessions:
        m = pat.match(str(s))
        if m:
            try:
                max_n = max(max_n, int(m.group(1)))
            except Exception:
                pass
    return max_n + 1 if max_n > 0 else 1

def _build_session_name() -> str:
    # 이미 불러온 세션이면 같은 이름 유지
    if st.session_state.get("loaded_session_name"):
        return st.session_state.loaded_session_name

    user_id = st.session_state.get('auth_user_id') or 'public'
    base = _session_base_keyword()
    n = _next_session_number(user_id, base)
    return f"{base}{n}"


def save_current_session_to_github():
    if not all([GITHUB_REPO, GITHUB_TOKEN, st.session_state.chat, st.session_state.last_csv]):
        return False, "저장할 데이터가 없거나 GitHub 설정이 누락되었습니다."

    sess_name = _build_session_name()
    user_id = st.session_state.get('auth_user_id') or 'public'
    local_dir = os.path.join(SESS_DIR, user_id, sess_name)
    os.makedirs(local_dir, exist_ok=True)

    try:
        meta_path = os.path.join(local_dir, "qa.json")
        meta_data = {
            "chat": st.session_state.chat,
            "last_schema": st.session_state.last_schema,
            "sample_text": st.session_state.sample_text
        }
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta_data, f, ensure_ascii=False, indent=2)

        comments_path = os.path.join(local_dir, "comments.csv")
        videos_path = os.path.join(local_dir, "videos.csv")

        os.system(f'cp "{st.session_state.last_csv}" "{comments_path}"')
        if st.session_state.last_df is not None:
            st.session_state.last_df.to_csv(videos_path, index=False, encoding="utf-8-sig")

        github_upload_file(GITHUB_REPO, GITHUB_BRANCH, f"sessions/{user_id}/{sess_name}/qa.json", meta_path, GITHUB_TOKEN)
        github_upload_file(GITHUB_REPO, GITHUB_BRANCH, f"sessions/{user_id}/{sess_name}/comments.csv", comments_path, GITHUB_TOKEN)
        if os.path.exists(videos_path):
            github_upload_file(GITHUB_REPO, GITHUB_BRANCH, f"sessions/{user_id}/{sess_name}/videos.csv", videos_path, GITHUB_TOKEN)

        st.session_state.loaded_session_name = sess_name
        return True, sess_name

    except Exception as e:
        return False, f"저장 실패: {e}"

def load_session_from_github(sess_name: str):
    with st.spinner(f"세션 '{sess_name}' 불러오는 중..."):
        try:
            user_id = st.session_state.get('auth_user_id') or 'public'
            local_dir = os.path.join(SESS_DIR, user_id, sess_name)
            qa_ok = github_download_file(GITHUB_REPO, GITHUB_BRANCH, f"sessions/{user_id}/{sess_name}/qa.json", GITHUB_TOKEN, os.path.join(local_dir, "qa.json"))
            comments_ok = github_download_file(GITHUB_REPO, GITHUB_BRANCH, f"sessions/{user_id}/{sess_name}/comments.csv", GITHUB_TOKEN, os.path.join(local_dir, "comments.csv"))
            videos_ok = github_download_file(GITHUB_REPO, GITHUB_BRANCH, f"sessions/{user_id}/{sess_name}/videos.csv", GITHUB_TOKEN, os.path.join(local_dir, "videos.csv"))

            if not (qa_ok and comments_ok):
                st.error("세션 핵심 파일을 불러오는 데 실패했습니다.")
                return
            # 로그인 상태는 유지한 채로, 대화/분석 상태만 초기화
            _reset_chat_only(keep_auth=True)

            with open(os.path.join(local_dir, "qa.json"), "r", encoding="utf-8") as f:
                meta = json.load(f)

            st.session_state.update({
                "chat": meta.get("chat", []),
                "last_schema": meta.get("last_schema", None),
                "last_csv": os.path.join(local_dir, "comments.csv"),
                "last_df": pd.read_csv(os.path.join(local_dir, "videos.csv")) if videos_ok and os.path.exists(os.path.join(local_dir, "videos.csv")) else pd.DataFrame(),
                "loaded_session_name": sess_name,
                "sample_text": meta.get("sample_text", "")
            })
        except Exception as e:
            st.error(f"세션 로드 실패: {e}")

# 세션 로드/삭제/이름변경 트리거 처리
if 'session_to_load' in st.session_state:
    load_session_from_github(st.session_state.pop('session_to_load'))
    st.rerun()

if 'session_to_delete' in st.session_state:
    sess_name = st.session_state.pop('session_to_delete')
    with st.spinner(f"세션 '{sess_name}' 삭제 중..."):
        user_id = st.session_state.get("auth_user_id") or "public"
        github_delete_folder(GITHUB_REPO, GITHUB_BRANCH, f"sessions/{user_id}/{sess_name}", GITHUB_TOKEN)
    st.success("세션 삭제 완료.")
    time.sleep(1)
    st.rerun()

if 'session_to_rename' in st.session_state:
    old, new = st.session_state.pop('session_to_rename')
    if old and new and old != new:
        with st.spinner("이름 변경 중..."):
            try:
                user_id = st.session_state.get("auth_user_id") or "public"
                github_rename_session(user_id, old, new, GITHUB_TOKEN)
                st.success("이름 변경 완료!")
            except Exception as e:
                st.error(f"변경 실패: {e}")
        time.sleep(1)
        st.rerun()
# endregion

# region [Data Processing & Utils]
def serialize_comments_for_llm_from_file(csv_path: str,
                                         max_chars_per_comment=280,
                                         max_total_chars=420_000,
                                         top_n=1000,
                                         random_n=1000,
                                         dedup_key="text"):
    """CSV(댓글)에서 LLM 입력용 샘플 텍스트를 생성한다.

    - 추출 기준(기본):
      1) likeCount 상위 top_n개 + 2) 나머지에서 random_n개 랜덤
    - LLM 입력 안정화를 위해:
      - 댓글당 max_chars_per_comment 글자 컷
      - 전체 max_total_chars 글자 컷(이 선에서 라인 생성 중단)

    Returns:
      (sample_text, sample_line_count, sample_total_chars, meta_dict)
    """
    if not os.path.exists(csv_path):
        return "", 0, 0, {"error": "csv_not_found"}

    try:
        df_all = pd.read_csv(csv_path)
    except Exception:
        return "", 0, 0, {"error": "csv_read_failed"}

    if df_all.empty:
        return "", 0, 0, {"error": "csv_empty"}

    # 총 수집 댓글 수(=CSV rows)
    total_rows = len(df_all)

    # (선택) 중복 제거 기준(기본: text)
    unique_rows = None
    try:
        if dedup_key in df_all.columns:
            unique_rows = df_all[dedup_key].astype(str).str.strip().replace("", pd.NA).dropna().nunique()
    except Exception:
        unique_rows = None

    # 인기 댓글 + 랜덤 댓글 샘플링
    df_top_likes = df_all.sort_values("likeCount", ascending=False).head(top_n)
    df_remaining = df_all.drop(df_top_likes.index)

    if not df_remaining.empty:
        take_n = min(random_n, len(df_remaining))
        df_random = df_remaining.sample(n=take_n, random_state=42)
    else:
        df_random = pd.DataFrame()

    df_sample = pd.concat([df_top_likes, df_random], ignore_index=True)
    sampled_target = len(df_sample)

    lines, total_chars = [], 0
    used_top = len(df_top_likes)
    used_random = len(df_random)

    for _, r in df_sample.iterrows():
        if total_chars >= max_total_chars:
            break

        raw_text = str(r.get("text", "") or "").replace("\n", " ")
        prefix = f"[{'R' if int(r.get('isReply', 0)) == 1 else 'T'}|♥{int(r.get('likeCount', 0))}] "
        author_clean = str(r.get('author', '')).replace('\n', ' ')
        prefix += f"{author_clean}: "

        body = raw_text[:max_chars_per_comment] + '…' if len(raw_text) > max_chars_per_comment else raw_text

        line = prefix + body
        lines.append(line)
        total_chars += len(line) + 1

    meta = {
        "total_rows": total_rows,
        "unique_rows": unique_rows,
        "top_n": int(top_n),
        "random_n": int(random_n),
        "used_top": int(used_top),
        "used_random": int(used_random),
        "sampled_target": int(sampled_target),
        "llm_input_lines": int(len(lines)),
        "llm_input_chars": int(total_chars),
        "max_chars_per_comment": int(max_chars_per_comment),
        "max_total_chars": int(max_total_chars),
        "dedup_key": str(dedup_key),
    }
    return "\n".join(lines), len(lines), total_chars, meta


def tidy_answer(text: str) -> str:
    """
    1. 마크다운 코드 블록(```html) 제거
    2. [핵심] HTML 태그 앞의 들여쓰기(공백)를 강제로 제거하여 코드 블록으로 인식되는 것을 방지
    3. 불필요한 제목 제거
    """
    if not text:
        return ""
    
    # 1. ```html, ``` 제거
    text = re.sub(r"^```html", "", text, flags=re.MULTILINE | re.IGNORECASE)
    text = re.sub(r"^```", "", text, flags=re.MULTILINE)
    
    # 2. [신규 기능] HTML 태그로 시작하는 줄의 앞 공백 제거 (들여쓰기 삭제)
    #    예: "    <div..." -> "<div..."
    #    이게 없으면 Streamlit이 '코드 블록'으로 오해해서 Raw HTML을 보여줌
    text = re.sub(r"^\s+(?=<)", "", text, flags=re.MULTILINE)
    
    lines = text.splitlines()
    cleaned = []
    
    # 3. 불필요한 제목 제거
    REMOVE_PATTERN = re.compile(r"유튜브\s*댓글\s*분석|보고서\s*작성|분석\s*결과", re.IGNORECASE)

    for line in lines:
        if not line.strip():
            cleaned.append(line)
            continue
        if REMOVE_PATTERN.search(line) and len(line) < 50:
            continue
        cleaned.append(line)

    return "\n".join(cleaned).strip()

YTB_ID_RE = re.compile(r"[A-Za-z0-9_-]{11}")

def extract_video_ids_from_text(text: str) -> list:
    if not text:
        return []
    ids = set()
    for m in re.finditer(r"https?://youtu\.be/([A-Za-z0-9_-]{11})", text):
        ids.add(m.group(1))
    for m in re.finditer(r"https?://(?:www\.)?youtube\.com/shorts/([A-Za-z0-9_-]{11})", text):
        ids.add(m.group(1))
    for m in re.finditer(r"https?://(?:www\.)?youtube\.com/watch\?[^ \n]+", text):
        url = m.group(0)
        try:
            qs = dict((kv.split("=", 1) + [""])[:2] for kv in url.split("?", 1)[1].split("&"))
            v = qs.get("v", "")
            if YTB_ID_RE.fullmatch(v):
                ids.add(v)
        except Exception:
            pass
    return list(ids)

def strip_urls(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"https?://\S+", " ", s)
    return re.sub(r"\s+", " ", s).strip()
# endregion

# region [API Integrations: Gemini & YouTube]
# ==============================================================================
# [Gemini 호출 함수] - 일반 호출 & 스마트 캐싱 호출
# ==============================================================================
def call_gemini_rotating(model_name, keys, system_instruction, user_payload,
                         timeout_s=120, max_tokens=8192) -> str:
    """기존의 일반(Non-Cached) 호출 함수"""
    rk = RotatingKeys(keys, "gem_key_idx")
    if not rk.current():
        raise RuntimeError("Gemini API Key가 비어 있습니다.")

    real_sys_inst = None if (not system_instruction or not system_instruction.strip()) else system_instruction
    
    from google.generativeai.types import HarmCategory, HarmBlockThreshold
    safety_settings = {
        HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
    }

    for _ in range(len(rk.keys) or 1):
        try:
            genai.configure(api_key=rk.current())
            model = genai.GenerativeModel(
                model_name,
                generation_config={"temperature": 0.2, "max_output_tokens": max_tokens},
                system_instruction=real_sys_inst 
            )
            with GeminiInflightSlot():
                resp = model.generate_content(
                    user_payload,
                    request_options={"timeout": timeout_s},
                    safety_settings=safety_settings 
                )
            
            if not resp: return "⚠️ AI 응답 없음"
            try:
                if getattr(resp, "text", None): return resp.text
            except ValueError:
                if resp.prompt_feedback: return f"⚠️ [차단] {resp.prompt_feedback}"
            
            if c0 := (getattr(resp, "candidates", None) or [None])[0]:
                if p0 := (getattr(c0, "content", None) and getattr(c0.content, "parts", None) or [None])[0]:
                    if hasattr(p0, "text"): return p0.text
            return "⚠️ [시스템] 내용 과다 또는 차단으로 답변 생성 실패"

        except Exception as e:
            if isinstance(e, TimeoutError) or "GEMINI_INFLIGHT_TIMEOUT" in str(e):
                return "⚠️ 현재 요청이 많아 AI 분석 대기열이 꽉 찼습니다. 잠시 후 다시 시도해주세요."
            msg = str(e).lower()
            if "429" in msg or "quota" in msg:
                if len(rk.keys) > 1:
                    rk.rotate()
                    continue
            print(f"Gemini API Error: {e}")
            raise e
    return ""

def call_gemini_smart_cache(model_name, keys, system_instruction, user_query, 
                            large_context_text=None, cache_key_in_session="current_cache"):
    """
    [스마트 캐싱 로직]
    1. 캐시가 있으면 -> 불러오기 & 수명연장(TTL Update)
    2. 캐시가 없거나 만료(404) -> 새로 생성(Resurrection)
    3. 텍스트가 너무 짧으면 -> 일반 호출로 자동 전환
    """
    rk = RotatingKeys(keys, "gem_key_idx")
    cached_info = st.session_state.get(cache_key_in_session, None)
    
    active_cache = None
    final_model = None
    
    from google.generativeai.types import HarmCategory, HarmBlockThreshold
    safety_settings = {
        HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
    }

    # [Case A] 기존 캐시 활용 시도 (Keep-Alive)
    if cached_info and not large_context_text:
        cache_name = cached_info.get("name")
        creator_key = cached_info.get("key")
        
        # 캐시는 만든 키로만 접근 가능
        genai.configure(api_key=creator_key)
        try:
            active_cache = caching.CachedContent.get(cache_name)
            # 수명 연장 (+20분)
            with GeminiInflightSlot():
                active_cache.update(ttl=timedelta(minutes=CACHE_TTL_MINUTES))
            
            final_model = genai.GenerativeModel.from_cached_content(
                cached_content=active_cache,
                generation_config={"temperature": 0.2, "max_output_tokens": GEMINI_MAX_TOKENS}
            )
            # print(f"✅ [Cache] 수명 연장 성공: {cache_name}")
        except Exception as e:
            # 404(만료), 403(권한) -> 재생성 필요
            # print(f"⚠️ [Cache] 만료/오류로 재생성 필요: {e}")
            active_cache = None
            
            # 재생성을 위한 원본 데이터 복구
            large_context_text = st.session_state.get("sample_text_full_context", "")
            if not large_context_text:
                return "⚠️ [오류] 세션이 만료되어 복구할 데이터가 없습니다. 새로고침 해주세요."

    # [Case B] 신규 생성 또는 재생성 (Resurrection)
    if not active_cache and large_context_text:
        # 세션에 원본 백업 (재생성용)
        st.session_state["sample_text_full_context"] = large_context_text

        for _ in range(len(rk.keys)):
            current_key = rk.current()
            genai.configure(api_key=current_key)
            try:
                with GeminiInflightSlot():
                    active_cache = caching.CachedContent.create(
                        model=model_name,
                        display_name=f"ytcc_{uuid4().hex[:8]}",
                        system_instruction=system_instruction,
                        contents=[large_context_text],
                        ttl=timedelta(minutes=CACHE_TTL_MINUTES)
                    )
                
                st.session_state[cache_key_in_session] = {
                    "name": active_cache.name,
                    "key": current_key
                }
                
                final_model = genai.GenerativeModel.from_cached_content(
                    cached_content=active_cache,
                    generation_config={"temperature": 0.2, "max_output_tokens": GEMINI_MAX_TOKENS}
                )
                # print(f"🆕 [Cache] 생성 완료: {active_cache.name}")
                break
            except Exception as e:
                msg = str(e).lower()
                # 내용이 너무 짧음 -> 캐싱 포기하고 일반 호출
                if "too short" in msg or "argument" in msg:
                    # print("ℹ️ [Cache] 텍스트가 짧아 일반 호출로 전환")
                    active_cache = None
                    break
                if "429" in msg or "quota" in msg:
                    rk.rotate()
                    continue
                raise e

    # [Execution]
    try:
        if final_model:
            with GeminiInflightSlot():
                resp = final_model.generate_content(user_query, safety_settings=safety_settings)
        else:
            # 캐싱 실패/미사용 시 일반 호출 (Fallback)
            full_payload = f"{system_instruction}\n\n{large_context_text or ''}\n\n{user_query}"
            return call_gemini_rotating(model_name, keys, None, full_payload)

        if resp and resp.text: return resp.text
        return "⚠️ [시스템] AI 응답 없음 (빈 내용)"
    except Exception as e:
        if isinstance(e, TimeoutError) or "GEMINI_INFLIGHT_TIMEOUT" in str(e):
            return "⚠️ 현재 요청이 많아 AI 분석 대기열이 꽉 찼습니다. 잠시 후 다시 시도해주세요."
        return f"⚠️ [시스템] 처리 중 에러: {e}"

def yt_search_videos(rt, keyword, max_results, order="viewCount",
                     published_after=None, published_before=None):
    video_ids, token = [], None
    while len(video_ids) < max_results:
        params = {
            "q": keyword, "part": "id", "type": "video", "order": order,
            "maxResults": min(50, max_results - len(video_ids))
        }
        if published_after: params["publishedAfter"] = published_after
        if published_before: params["publishedBefore"] = published_before
        if token: params["pageToken"] = token

        resp = rt.execute(lambda s: s.search().list(**params))
        video_ids.extend(it["id"]["videoId"] for it in resp.get("items", [])
                         if it["id"]["videoId"] not in video_ids)
        if not (token := resp.get("nextPageToken")):
            break
        time.sleep(0.25)
    return video_ids

def yt_video_statistics(rt, video_ids):
    rows = []
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        if not batch: continue

        resp = rt.execute(lambda s: s.videos().list(part="statistics,snippet,contentDetails", id=",".join(batch)))
        for item in resp.get("items", []):
            stats, snip, cont = item.get("statistics", {}), item.get("snippet", {}), item.get("contentDetails", {})
            dur = cont.get("duration", "")
            h, m, s = re.search(r"(\d+)H", dur), re.search(r"(\d+)M", dur), re.search(r"(\d+)S", dur)
            dur_sec = (int(h.group(1))*3600 if h else 0) + (int(m.group(1))*60 if m else 0) + (int(s.group(1)) if s else 0)

            vid_id = item.get("id")
            rows.append({
                "video_id": vid_id,
                "video_url": f"https://www.youtube.com/watch?v={vid_id}",
                "title": snip.get("title", ""),
                "channelTitle": snip.get("channelTitle", ""),
                "publishedAt": snip.get("publishedAt", ""),
                "duration": dur,
                "shortType": "Shorts" if dur_sec <= 60 else "Clip",
                "viewCount": int(stats.get("viewCount", 0) or 0),
                "likeCount": int(stats.get("likeCount", 0) or 0),
                "commentCount": int(stats.get("commentCount", 0) or 0)
            })
        time.sleep(0.25)
    return rows

def yt_all_replies(rt, parent_id, video_id, title="", short_type="Clip", cap=None):
    replies, token = [], None
    while not (cap is not None and len(replies) >= cap):
        try:
            resp = rt.execute(lambda s: s.comments().list(part="snippet", parentId=parent_id, maxResults=100, pageToken=token, textFormat="plainText"))
        except HttpError: break

        for c in resp.get("items", []):
            sn = c["snippet"]
            replies.append({
                "video_id": video_id, "video_title": title, "shortType": short_type,
                "comment_id": c.get("id", ""), "parent_id": parent_id, "isReply": 1,
                "author": sn.get("authorDisplayName", ""), "text": sn.get("textDisplay", "") or "",
                "publishedAt": sn.get("publishedAt", ""), "likeCount": int(sn.get("likeCount", 0) or 0)
            })
        if not (token := resp.get("nextPageToken")): break
        time.sleep(0.2)
    return replies[:cap] if cap is not None else replies

def yt_all_comments_sync(rt, video_id, title="", short_type="Clip",
                         include_replies=True, max_per_video=None):
    rows, token = [], None
    while not (max_per_video is not None and len(rows) >= max_per_video):
        try:
            resp = rt.execute(lambda s: s.commentThreads().list(part="snippet,replies", videoId=video_id, maxResults=100, pageToken=token, textFormat="plainText"))
        except HttpError: break

        for it in resp.get("items", []):
            top = it["snippet"]["topLevelComment"]["snippet"]
            thread_id = it["snippet"]["topLevelComment"]["id"]
            rows.append({
                "video_id": video_id, "video_title": title, "shortType": short_type,
                "comment_id": thread_id, "parent_id": "", "isReply": 0,
                "author": top.get("authorDisplayName", ""), "text": top.get("textDisplay", "") or "",
                "publishedAt": top.get("publishedAt", ""), "likeCount": int(top.get("likeCount", 0) or 0)
            })
            if include_replies and int(it["snippet"].get("totalReplyCount", 0) or 0) > 0:
                cap = None if max_per_video is None else max(0, max_per_video - len(rows))
                if cap == 0: break
                rows.extend(yt_all_replies(rt, thread_id, video_id, title, short_type, cap=cap))
        if not (token := resp.get("nextPageToken")): break
        time.sleep(0.2)
    return rows[:max_per_video] if max_per_video is not None else rows

def parallel_collect_comments_streaming(video_list, rt_keys, include_replies,
                                        max_total_comments, max_per_video, prog_bar):
    out_csv = os.path.join(BASE_DIR, f"collect_{uuid4().hex}.csv")
    wrote_header, total_written, done, total_videos = False, 0, 0, len(video_list)

    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {
            ex.submit(yt_all_comments_sync, RotatingYouTube(rt_keys), v["video_id"], v.get("title", ""),
                      v.get("shortType", "Clip"), include_replies, max_per_video): v for v in video_list
        }
        for f in as_completed(futures):
            try:
                if comm := f.result():
                    dfc = pd.DataFrame(comm)
                    dfc.to_csv(out_csv, index=False, mode="a" if wrote_header else "w", header=not wrote_header, encoding="utf-8-sig")
                    wrote_header = True
                    total_written += len(dfc)
            except Exception: pass
            done += 1
            prog_bar.progress(min(0.90, 0.50 + (done / total_videos) * 0.40 if total_videos > 0 else 0.50), text="댓글 수집중…")
            if total_written >= max_total_comments: break
    return out_csv, total_written
# endregion


# region [UI Components]
def scroll_to_bottom():
    st_html(
        "<script> "
        "let last_message = document.querySelectorAll('.stChatMessage'); "
        "if (last_message.length > 0) { "
        "  last_message[last_message.length - 1].scrollIntoView({behavior: 'smooth'}); "
        "} "
        "</script>",
        height=0
    )

def render_capture_pdf_button(file_basename: str, label: str = "PDF저장"):
    """
    대화창(채팅 메시지 영역)만 '스크린샷 기반'으로 PDF 저장한다.
    - reportlab 한글/레이아웃 이슈 회피
    - 스크롤 끝까지(전체 메시지) 포함
    - 우측 잘림 방지(캡쳐 폭을 동적으로 확보 + 줄바꿈 강제)
    """

    # 파일명 안전화
    safe = re.sub(r'[\\/:*?"<>|]+', "_", (file_basename or "chat")).strip()
    safe = re.sub(r"\s+", "_", safe) or "chat"

    # Streamlit rerun마다 id가 바뀌도록 (DOM 충돌 방지)
    btn_id = f"ytcc_cap_pdf_{uuid4().hex[:8]}"

    st_html(
        f"""
        <style>
          html, body {{
            margin: 0; padding: 0;
            background: transparent;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Noto Sans KR", Arial, sans-serif;
          }}
          .ytcc-cap-wrap {{ width: 100%; margin: 0; padding: 0; }}
          /* ✅ '세션저장' 톤과 동일하게 */
          .ytcc-cap-btn {{
            width: 100%;
            padding: 0.30rem 0.55rem;
            border-radius: 12px;
            border: 1px solid #cdeedb;
            background: #eafaf1;
            color: #127a3a;
            cursor: pointer;
            font-size: 0.72rem;
            font-weight: 650;
            line-height: 1.0;
            box-sizing: border-box;
          }}
          .ytcc-cap-btn:hover {{
            background: #d6f3e4;
            border-color: #bfe8d3;
            color: #0f6a32;
          }}
          .ytcc-cap-btn:disabled {{
            opacity: 0.70;
            cursor: default;
          }}
</style>

        <div class="ytcc-cap-wrap">
          <button id="{btn_id}" class="ytcc-cap-btn">{label}</button>
        </div>

        <script>
        (function() {{
          const btn = document.getElementById("{btn_id}");
          if (!btn) return;

          const PARENT = window.parent;
          const DOC = PARENT.document;

          function ensureScript(src, globalName) {{
            return new Promise((resolve, reject) => {{
              try {{
                if (globalName && PARENT[globalName]) return resolve(true);
                if (DOC.querySelector('script[data-ytcc-src="'+src+'"]')) return resolve(true);
                const s = DOC.createElement('script');
                s.src = src;
                s.async = true;
                s.setAttribute('data-ytcc-src', src);
                s.onload = () => resolve(true);
                s.onerror = () => reject(new Error('load failed: ' + src));
                DOC.head.appendChild(s);
              }} catch (e) {{
                reject(e);
              }}
            }});
          }}

          async function captureToPdf() {{
            btn.disabled = true;
            const originalText = btn.innerText;
            btn.innerText = "캡쳐중...";

            try {{
              await ensureScript("https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js", "html2canvas");
              await ensureScript("https://cdnjs.cloudflare.com/ajax/libs/jspdf/2.5.1/jspdf.umd.min.js", "jspdf");

              const msgs = Array.from(DOC.querySelectorAll('[data-testid="stChatMessage"]'));
              if (!msgs.length) {{
                alert("캡쳐할 대화가 없습니다.");
                return;
              }}

              // ✅ 첫 채팅 메시지 폭을 기준으로 캡쳐 폭 확장 (우측 잘림 방지)
              const r = msgs[0].getBoundingClientRect();
              const capW = Math.max(1200, Math.min(1700, (r.width || 1200) + 140));

              const tmp = DOC.createElement('div');
              tmp.id = "ytcc_capture_tmp";
              tmp.style.position = "absolute";
              tmp.style.left = "0px";
              tmp.style.top = "0px";
              tmp.style.transform = "translateX(-20000px)";
              tmp.style.width = capW + "px";
              tmp.style.background = "#ffffff";
              tmp.style.padding = "18px 22px";
              tmp.style.borderRadius = "12px";
              tmp.style.color = "#111827";
              tmp.style.boxSizing = "border-box";
              tmp.style.overflow = "visible";
              tmp.style.overflowWrap = "anywhere";
              tmp.style.wordBreak = "break-word";

              const title = DOC.createElement('div');
              title.style.fontSize = "14px";
              title.style.fontWeight = "800";
              title.style.marginBottom = "6px";
              title.innerText = "유튜브 댓글분석: AI 챗봇 — 대화 캡쳐";
              tmp.appendChild(title);

              const meta = DOC.createElement('div');
              meta.style.fontSize = "12px";
              meta.style.color = "#6b7280";
              meta.style.marginBottom = "10px";
              meta.innerText = "생성일시: " + (new Date()).toLocaleString();
              tmp.appendChild(meta);

              msgs.forEach(m => tmp.appendChild(m.cloneNode(true)));
              DOC.body.appendChild(tmp);

              // ✅ 긴 URL/코드블럭도 강제로 줄바꿈 (우측 잘림 방지)
              tmp.querySelectorAll("*").forEach(el => {{
                try {{
                  el.style.boxSizing = "border-box";
                  el.style.maxWidth = "100%";
                  el.style.overflowWrap = "anywhere";
                  el.style.wordBreak = "break-word";
                }} catch (e) {{}}
              }});

              const canvas = await PARENT.html2canvas(tmp, {{
                scale: 2,
                useCORS: true,
                backgroundColor: "#ffffff",
                logging: false,
                windowWidth: capW,
              }});

              DOC.body.removeChild(tmp);

              const imgData = canvas.toDataURL("image/png");
              const {{ jsPDF }} = PARENT.jspdf;

              const pdf = new jsPDF("p", "pt", "a4");
              const pageWidth = pdf.internal.pageSize.getWidth();
              const pageHeight = pdf.internal.pageSize.getHeight();

              const imgWidth = pageWidth;
              const imgHeight = (canvas.height * imgWidth) / canvas.width;

              let heightLeft = imgHeight;
              let position = 0;

              pdf.addImage(imgData, "PNG", 0, position, imgWidth, imgHeight);
              heightLeft -= pageHeight;

              while (heightLeft > 0) {{
                position = heightLeft - imgHeight;
                pdf.addPage();
                pdf.addImage(imgData, "PNG", 0, position, imgWidth, imgHeight);
                heightLeft -= pageHeight;
              }}

              pdf.save("{safe}.pdf");
            }} catch (err) {{
              console.error(err);
              alert("PDF 캡쳐 중 오류가 발생했습니다. 콘솔 로그를 확인해주세요.");
            }} finally {{
              btn.disabled = false;
              btn.innerText = originalText;
            }}
          }}

          btn.addEventListener("click", captureToPdf);
        }})();
        </script>
        """,
        height=44,
    )

def render_metadata_and_downloads():
    if not (schema := st.session_state.get("last_schema")):
        return

    kw_main = schema.get("keywords", [])
    start_iso, end_iso = schema.get('start_iso', ''), schema.get('end_iso', '')
    try:
        start_dt_str = datetime.fromisoformat(start_iso).astimezone(KST).strftime('%Y-%m-%d %H:%M')
        end_dt_str   = datetime.fromisoformat(end_iso).astimezone(KST).strftime('%Y-%m-%d %H:%M')
    except (ValueError, TypeError):
        start_dt_str, end_dt_str = (start_iso.split('T')[0] if start_iso else ""), (end_iso.split('T')[0] if end_iso else "")

    with st.container(border=True):
        st.markdown(f"""
            <div style="font-size:14px; color:#4b5563; line-height:1.8;">
              <span style='font-weight:600;'>키워드:</span> {', '.join(kw_main) if kw_main else '(없음)'}<br>
              <span style='font-weight:600;'>기간:</span> {start_dt_str} ~ {end_dt_str} (KST)
            </div>
            """, unsafe_allow_html=True)

        csv_path, df_videos = st.session_state.get("last_csv"), st.session_state.get("last_df")
        if csv_path and os.path.exists(csv_path) and df_videos is not None and not df_videos.empty:
            with open(csv_path, "rb") as f: comment_csv_data = f.read()
            buffer = io.BytesIO()
            df_videos.to_csv(buffer, index=False, encoding="utf-8-sig")
            video_csv_data = buffer.getvalue()
            keywords_str = "_".join(kw_main).replace(" ", "_") if kw_main else "data"
            now_str = now_kst().strftime('%Y%m%d')

            col1, col2, col3, col4, _ = st.columns([1.1, 1.2, 1.2, 1.6, 5.0])
            col1.markdown("<div style='font-size:14px; color:#4b...ght:600; padding-top:5px;'>다운로드:</div>", unsafe_allow_html=True)

            with col2:
                st.download_button("전체댓글", comment_csv_data, f"comments_{keywords_str}_{now_str}.csv", "text/csv")

            with col3:
                st.download_button("영상목록", video_csv_data, f"videos_{keywords_str}_{now_str}.csv", "text/csv")

            # ✅ LLM에 실제로 들어간 댓글 샘플(그대로) 다운로드
            sample_text = (st.session_state.get("sample_text") or "").strip()
            if sample_text:
                sample_bytes = sample_text.encode("utf-8-sig")
                with col4:
                    st.download_button(
                        "AI샘플(LLM입력)",
                        sample_bytes,
                        f"llm_sample_{keywords_str}_{now_str}.txt",
                        "text/plain"
                    )

                sample_cnt = st.session_state.get("sample_count")
                sample_chars = st.session_state.get("sample_chars")
                if sample_cnt is not None and sample_chars is not None:
                    st.caption(f"AI 입력 샘플: {sample_cnt:,}줄 / {sample_chars:,} chars")

def render_chat():
    for msg in st.session_state.chat:
        with st.chat_message(msg.get("role", "user")):
            content = msg.get("content", "")
            
            # AI 답변이고, 내용이 HTML 태그(<div, <style 등)를 포함하는 경우
            if isinstance(content, str) and msg.get("role") == "assistant" and ("<div" in content or "<style" in content):
                # 스타일 정의 (가독성 확보)
                report_style = """
                <style>
                .yt-report { font-family: "Helvetica Neue", Arial, sans-serif; line-height: 1.6; color: #333; }
                .yt-report .header { border-bottom: 2px solid #eee; padding-bottom: 10px; margin-bottom: 15px; }
                .yt-report .badge { background: #f0f2f6; color: #31333F; padding: 2px 8px; border-radius: 4px; font-size: 0.85em; margin-right: 5px; font-weight: 600; }
                .yt-report .card { background: white; border: 1px solid #ddd; border-radius: 8px; padding: 15px; margin-bottom: 15px; box-shadow: 0 1px 3px rgba(0,0,0,0.05); }
                .yt-report h3 { font-size: 1.1em; margin-top: 0; margin-bottom: 10px; color: #000; font-weight: 700; }
                .yt-report .quote { border-left: 3px solid #ff4b4b; padding-left: 10px; color: #555; font-style: italic; margin: 5px 0; font-size: 0.95em; background: #fafafa; padding: 5px 10px; }
                .yt-report table { width: 100%; border-collapse: collapse; font-size: 0.9em; margin: 10px 0; }
                .yt-report th { text-align: left; border-bottom: 2px solid #ddd; padding: 5px; color: #555; background-color: #f9fafb; }
                .yt-report td { border-bottom: 1px solid #eee; padding: 8px 5px; vertical-align: top; }
</style>
                """
                
                # [안전장치] 잘린 태그 방지를 위해 div로 감쌈 (브라우저가 웬만하면 닫아줌)
                # unsafe_allow_html=True로 렌더링해야 코드가 안 보이고 디자인이 적용됨
                full_html = f"<div class='yt-report'>{report_style}{content}</div>"
                st.markdown(full_html, unsafe_allow_html=True)
                
            else:
                # 일반 텍스트 대화
                st.markdown(content)


def render_sidebar_controls_html(display_name: str, role: str, show_actions: bool, pdf_basename: str):
    """사이드바 상단 UI를 Streamlit 위젯이 아니라 '단일 HTML 블록'으로 렌더링한다.

    - Streamlit은 마크다운 HTML로 위젯을 감쌀 수 없어서(=CSS가 안 먹음),
      상단 컨트롤(로그아웃/새채팅/세션저장/PDF저장)을 하나의 HTML로 묶어 스타일/간격을 완전 고정한다.
    - 새채팅/세션저장/로그아웃은 query param으로 액션을 전달해 서버에서 처리한다.
    - PDF저장은 클라이언트에서 대화 DOM을 캡쳐해 PDF로 저장한다.
    """

    disp = (display_name or "").strip() or "사용자"
    role = (role or "user").strip() or "user"

    safe_pdf = re.sub(r'[\\/:*?"<>|]+', "_", (pdf_basename or "chat")).strip()
    safe_pdf = re.sub(r"\s+", "_", safe_pdf) or "chat"

    # iframe 내부에서만 쓰는 랜덤 id
    rid = uuid4().hex[:8]

    tpl = r'''
<div class="ytcc-sb-wrap" id="ytcc_sb__RID__">
  <div class="ytcc-sb-title">💬 유튜브 댓글분석: <span>AI 챗봇</span></div>

  <div class="ytcc-sb-userrow">
    <div class="ytcc-sb-userleft">
      <div class="ytcc-sb-user">👤 __DISP__ <span class="ytcc-sb-role">(__ROLE__)</span></div>
    </div>
    <div class="ytcc-sb-userright">
      <a href="javascript:void(0)" class="ytcc-sb-logout" id="ytcc_logout__RID__">로그아웃</a>
    </div>
  </div>

  <div class="ytcc-sb-gap"></div>

  <button class="ytcc-btn ytcc-btn-secondary" id="ytcc_newchat__RID__">새채팅</button>

  __ACTIONS__

</div>

<style>
  /* 이 블록(iframe) 안에서만 적용되는, 완전 독립 스타일 */
  html, body { margin:0; padding:0; background:transparent; font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Noto Sans KR",Arial,sans-serif; }

  .ytcc-sb-wrap{
    width:100%;
    box-sizing:border-box;
  }

  .ytcc-sb-title{
    font-weight:800;
    font-size: 1.55rem;
    line-height: 1.15;
    margin: 0 0 8px 0;
    background: -webkit-linear-gradient(45deg, #4285F4, #9B72CB, #D96570, #F2A60C);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
  }
  .ytcc-sb-title span{ font-weight:800; }

  .ytcc-sb-userrow{
    display:flex;
    align-items:flex-start;
    justify-content:space-between;
    gap: 10px;
    margin: 0;
  }
  .ytcc-sb-user{
    font-size: 0.95rem;
    font-weight: 700;
    color:#111827;
    line-height:1.25;
    margin: 0;
  }
  .ytcc-sb-role{
    font-size: 0.78rem;
    font-weight: 600;
    color:#6b7280;
  }
  .ytcc-sb-logout{
    font-size: 0.78rem;
    color: #6b7280;
    text-decoration: underline;
    font-weight: 600;
    line-height:1.1;
  }
  .ytcc-sb-logout:hover{ color:#374151; }

  .ytcc-sb-gap{ height: 10px; }

  .ytcc-btn{
    width:100%;
    border-radius: 14px;
    padding: 0.55rem 0.65rem;
    font-size: 0.90rem;
    font-weight: 750;
    line-height: 1.1;
    border: 1px solid #e5e7eb;
    background: #ffffff;
    color:#111827;
    cursor:pointer;
    box-sizing:border-box;
  }
  .ytcc-btn:hover{
    background:#f9fafb;
    border-color:#d1d5db;
  }

  .ytcc-btn-secondary{
    background:#e8f0fe;
    border-color:#d2e3fc;
    color:#0052CC;
  }
  .ytcc-btn-secondary:hover{
    background:#d2e3fc;
    border-color:#c2d8f8;
    color:#0041A3;
  }

  .ytcc-row{
    display:flex;
    gap: 10px;
    margin-top: 10px;
  }
  .ytcc-row .ytcc-btn{ width: 50%; }

  .ytcc-btn-success{
    background:#eafaf1;
    border-color:#cdeedb;
    color:#127a3a;
  }
  .ytcc-btn-success:hover{
    background:#d6f3e4;
    border-color:#bfe8d3;
    color:#0f6a32;
  }

  /* iframe 자체 높이로 공백 최소화 */
  .ytcc-bottom-gap{ height: 4px; }
</style>

<script>
(function(){
  // ✅ Streamlit components iframe 깊이에 상관없이 최상위(같은 origin) window 찾기
  const P = (function(){
    try{
      let w = window;
      while (w.parent && w.parent !== w) w = w.parent;
      return w;
    }catch(e){
      return window.parent;
    }
  })();
  const DOC = P.document;

  // ✅ 한 번에 query param 세팅/삭제 후, 같은 창에서 확실히 리로드
  function navigate(setObj, delKeys){
    try{
      const url = new URL(P.location.href);
      const sp = url.searchParams;

      (delKeys || []).forEach(k => { try{ sp.delete(k); }catch(e){} });
      Object.entries(setObj || {}).forEach(([k,v])=>{
        if(v === null || v === undefined || String(v).trim()===""){
          sp.delete(k);
        } else {
          sp.set(k, v);
        }
      });

      url.search = sp.toString();

      // 1) URL만 교체(동일 페이지) → 2) 리로드 (sandbox/top-nav 제약 회피에 유리)
      try{
        P.history.replaceState(null, "", url.toString());
        P.location.reload();
        return;
      }catch(e){}

      // fallback
      try{ P.location.assign(url.toString()); return; }catch(e){}
      try{ P.location.href = url.toString(); }catch(e){}
    }catch(e){
      console.error(e);
    }
  }

  // 액션 버튼들
  const btnLogout = document.getElementById("ytcc_logout__RID__");
  const btnNew = document.getElementById("ytcc_newchat__RID__");
  const btnSave = document.getElementById("ytcc_save__RID__");
  const btnPdf  = document.getElementById("ytcc_pdf__RID__");

  if(btnLogout){
    btnLogout.addEventListener("click", ()=>{ navigate({logout:"1"}, ["action"]); });
  }
  if(btnNew){
    btnNew.addEventListener("click", ()=>{ navigate({action:"new_chat"}, ["logout"]); });
  }
  if(btnSave){
    btnSave.addEventListener("click", ()=>{ navigate({action:"save_session"}, ["logout"]); });
  }

  // --- PDF 캡쳐 (대화창만 / 스크롤 끝까지) ---
  function ensureScript(src, globalName){
    return new Promise((resolve, reject)=>{
      try{
        if(globalName && P[globalName]) return resolve(true);
        if(DOC.querySelector('script[data-ytcc-src="'+src+'"]')) return resolve(true);
        const s = DOC.createElement("script");
        s.src = src;
        s.async = true;
        s.setAttribute("data-ytcc-src", src);
        s.onload = ()=>resolve(true);
        s.onerror = ()=>reject(new Error("load failed: "+src));
        DOC.head.appendChild(s);
      }catch(e){ reject(e); }
    });
  }

  async function captureToPdf(){
    if(!btnPdf) return;
    const original = btnPdf.innerText;
    btnPdf.disabled = true;
    btnPdf.innerText = "캡쳐중...";

    try{
      await ensureScript("https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js", "html2canvas");
      await ensureScript("https://cdnjs.cloudflare.com/ajax/libs/jspdf/2.5.1/jspdf.umd.min.js", "jspdf");

      const msgs = Array.from(DOC.querySelectorAll('[data-testid="stChatMessage"]'));
      if(!msgs.length){
        alert("캡쳐할 대화가 없습니다.");
        return;
      }

      // 첫 메시지 폭 기반으로 캡쳐 컨테이너 폭 확보 (우측 잘림 방지)
      const r = msgs[0].getBoundingClientRect();
      let capW = Math.max(1200, Math.min(1700, (r.width || 1200) + 160));

      const tmp = DOC.createElement("div");
      tmp.id = "ytcc_capture_tmp__RID__";
      tmp.style.position = "absolute";
      tmp.style.left = "0px";
      tmp.style.top = "0px";
      tmp.style.transform = "translateX(-24000px)";
      tmp.style.width = capW + "px";
      tmp.style.background = "#ffffff";
      tmp.style.padding = "18px 22px";
      tmp.style.borderRadius = "12px";
      tmp.style.color = "#111827";
      tmp.style.boxSizing = "border-box";
      tmp.style.overflow = "visible";
      tmp.style.overflowWrap = "anywhere";
      tmp.style.wordBreak = "break-word";

      const title = DOC.createElement("div");
      title.style.fontSize = "14px";
      title.style.fontWeight = "800";
      title.style.marginBottom = "6px";
      title.innerText = "유튜브 댓글분석: AI 챗봇 — 대화 캡쳐";
      tmp.appendChild(title);

      const meta = DOC.createElement("div");
      meta.style.fontSize = "12px";
      meta.style.color = "#6b7280";
      meta.style.marginBottom = "10px";
      meta.innerText = "생성일시: " + (new Date()).toLocaleString();
      tmp.appendChild(meta);

      msgs.forEach(m => tmp.appendChild(m.cloneNode(true)));
      DOC.body.appendChild(tmp);

      // 하위 요소 폭/줄바꿈 강제
      tmp.querySelectorAll("*").forEach(el => {
        el.style.boxSizing = "border-box";
        el.style.maxWidth = "100%";
        el.style.overflowWrap = "anywhere";
        el.style.wordBreak = "break-word";
      });

      const canvas = await P.html2canvas(tmp, {
        scale: 2,
        useCORS: true,
        backgroundColor: "#ffffff",
        logging: false,
        windowWidth: capW,
      });

      DOC.body.removeChild(tmp);

      const imgData = canvas.toDataURL("image/png");
      const { jsPDF } = P.jspdf;

      const pdf = new jsPDF("p", "pt", "a4");
      const pageWidth = pdf.internal.pageSize.getWidth();
      const pageHeight = pdf.internal.pageSize.getHeight();

      const imgWidth = pageWidth;
      const imgHeight = (canvas.height * imgWidth) / canvas.width;

      let heightLeft = imgHeight;
      let position = 0;

      pdf.addImage(imgData, "PNG", 0, position, imgWidth, imgHeight);
      heightLeft -= pageHeight;

      while (heightLeft > 0) {
        position = heightLeft - imgHeight;
        pdf.addPage();
        pdf.addImage(imgData, "PNG", 0, position, imgWidth, imgHeight);
        heightLeft -= pageHeight;
      }

      pdf.save("__PDFNAME__.pdf");
    }catch(err){
      console.error(err);
      alert("PDF 캡쳐 중 오류가 발생했습니다. 콘솔 로그를 확인해주세요.");
    }finally{
      btnPdf.disabled = false;
      btnPdf.innerText = original;
    }
  }

  if(btnPdf){
    btnPdf.addEventListener("click", captureToPdf);
  }
})();
</script>
'''

    actions_html = ""
    if show_actions:
        actions_html = r'''
  <div class="ytcc-row">
    <button class="ytcc-btn ytcc-btn-success" id="ytcc_save__RID__">세션저장</button>
    <button class="ytcc-btn ytcc-btn-success" id="ytcc_pdf__RID__">PDF저장</button>
  </div>
  <div class="ytcc-bottom-gap"></div>
'''
    else:
        actions_html = '<div class="ytcc-bottom-gap"></div>'

    html_str = (tpl
        .replace("__RID__", rid)
        .replace("__DISP__", disp.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;"))
        .replace("__ROLE__", role.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;"))
        .replace("__ACTIONS__", actions_html.replace("__RID__", rid))
        .replace("__PDFNAME__", safe_pdf)
    )

    st_html(html_str, height=210 if show_actions else 170)

# endregion

# region [Main Pipeline]

LIGHT_PROMPT = (
    "역할: 유튜브 댓글 반응 분석기의 자연어 해석가.\n"
    "목표: 한국어 입력에서 [기간(KST)]과 [키워드/옵션]만 정확히 추출.\n"
    "규칙:\n"
    "- 기간은 Asia/Seoul 기준, 상대기간의 종료는 지금.\n"
    "- '키워드'는 검색에 사용할 핵심 주제 1개로 한정.\n"
    "- 옵션: include_replies, channel_filter(any|official|unofficial), lang(ko|en|auto).\n\n"
    "출력(5줄 고정):\n"
    "- 한 줄 요약: <문장>\n"
    "- 기간(KST): <YYYY-MM-DDTHH:MM:SS+09:00> ~ <YYYY-MM-DDTHH:MM:SS+09:00>\n"
    "- 키워드: [<핵심 키워드 1개>]\n"
    "- 옵션: { include_replies: true|false, channel_filter: \"any|official|unofficial\", lang: \"ko|en|auto\" }\n"
    "- 원문: {USER_QUERY}\n\n"
    f"현재 KST: {to_iso_kst(now_kst())}\n"
    "입력:\n{USER_QUERY}"
)

def parse_light_block_to_schema(light_text: str) -> dict:
    """LIGHT_PROMPT 결과(5줄)를 schema로 파싱."""
    raw = (light_text or "").strip()

    m_time = re.search(r"기간\(KST\)\s*:\s*([^~]+)~\s*([^\n]+)", raw)
    start_iso, end_iso = (m_time.group(1).strip(), m_time.group(2).strip()) if m_time else (None, None)

    m_kw = re.search(r"키워드\s*:\s*\[(.*?)\]", raw, flags=re.DOTALL)
    keywords = [p.strip() for p in re.split(r"\s*,\s*", m_kw.group(1)) if p.strip()] if m_kw else []

    m_opt = re.search(r"옵션\s*:\s*\{(.*?)\}", raw, flags=re.DOTALL)
    options = {"include_replies": False, "channel_filter": "any", "lang": "auto"}
    if m_opt:
        blob = m_opt.group(1)
        ir = re.search(r"include_replies\s*:\s*(true|false)", blob, re.I)
        if ir:
            options["include_replies"] = (ir.group(1).lower() == "true")
        cf = re.search(r"channel_filter\s*:\s*\"(any|official|unofficial)\"", blob, re.I)
        if cf:
            options["channel_filter"] = cf.group(1)
        lg = re.search(r"lang\s*:\s*\"(ko|en|auto)\"", blob, re.I)
        if lg:
            options["lang"] = lg.group(1)

    if not (start_iso and end_iso):
        end_dt = now_kst()
        start_dt = end_dt - timedelta(hours=24)
        start_iso, end_iso = to_iso_kst(start_dt), to_iso_kst(end_dt)

    if not keywords:
        tokens = re.findall(r"[가-힣A-Za-z0-9]{2,}", raw)
        keywords = [tokens[0]] if tokens else ["유튜브"]

    return {"start_iso": start_iso, "end_iso": end_iso, "keywords": keywords, "options": options, "raw": raw}


def run_pipeline_first_turn(user_query: str, extra_video_ids=None, only_these_videos: bool = False):
    extra_video_ids = list(dict.fromkeys(extra_video_ids or []))
    prog_bar = st.progress(0, text="준비 중…")

    if not GEMINI_API_KEYS: return "오류: Gemini API Key가 설정되지 않았습니다."
    prog_bar.progress(0.05, text="해석중…")
    
    light = call_gemini_rotating(GEMINI_MODEL, GEMINI_API_KEYS, "", LIGHT_PROMPT.replace("{USER_QUERY}", user_query))
    schema = parse_light_block_to_schema(light)
    st.session_state["last_schema"] = schema

    prog_bar.progress(0.10, text="영상 수집중…")
    if not YT_API_KEYS: return "오류: YouTube API Key가 설정되지 않았습니다."
    
    rt = RotatingYouTube(YT_API_KEYS)
    start_dt, end_dt = datetime.fromisoformat(schema["start_iso"]), datetime.fromisoformat(schema["end_iso"])
    kw_main = schema.get("keywords", [])

    own_mode = bool(st.session_state.get("own_ip_mode", False))
    pgc_ids = []
    
    # [수정] 자사 IP 모드 처리
    if own_mode:
        cache_dir = _cache_local_dir()
        cache_files = [fn for fn in os.listdir(cache_dir) if re.fullmatch(r"cache_token_.*\.json", fn)]
        if not cache_files:
            return f"자사모드 캐시 파일을 찾지 못했습니다: {os.path.join(cache_dir, 'cache_token_*.json')}"
        
        for base_kw in (kw_main or []):
            # [핵심] 키워드와 함께 start_dt, end_dt를 넘겨 기간 필터링 적용
            pgc_ids.extend(load_pgc_video_ids_by_keyword(base_kw, start_dt, end_dt))
        pgc_ids = list(dict.fromkeys(pgc_ids))

    if only_these_videos and extra_video_ids:
        all_ids = extra_video_ids
    else:
        all_ids = []
        # UGC 검색
        for base_kw in (kw_main or ["유튜브"]):
            search_kw = hashtagify_keyword(base_kw)
            if search_kw:
                all_ids.extend(yt_search_videos(rt, search_kw, 60, "viewCount", kst_to_rfc3339_utc(start_dt), kst_to_rfc3339_utc(end_dt)))
        
        if extra_video_ids:
            all_ids.extend(extra_video_ids)
            
        # PGC 아이디 합치기
        if own_mode and pgc_ids:
            all_ids.extend(pgc_ids)

    all_ids = list(dict.fromkeys(all_ids))
    prog_bar.progress(0.40, text="댓글 수집 준비중…")

    df_stats = pd.DataFrame(yt_video_statistics(rt, all_ids))
    
    if bool(st.session_state.get("own_ip_mode", False)) and (not df_stats.empty) and ("title" in df_stats.columns):
        df_stats = df_stats[~df_stats["title"].astype(str).str.contains(r"\bOST\b", case=False, na=False)]
    
    st.session_state["last_df"] = df_stats

    csv_path, total_cnt = parallel_collect_comments_streaming(
        df_stats.to_dict('records'), YT_API_KEYS, bool(schema.get("options", {}).get("include_replies")),
        MAX_TOTAL_COMMENTS, MAX_COMMENTS_PER_VID, prog_bar
    )
    st.session_state["last_csv"] = csv_path

    if total_cnt == 0:
        prog_bar.empty()
        return "지정 조건에서 댓글을 찾을 수 없습니다. 다른 조건으로 시도해 보세요."

    prog_bar.progress(0.90, text="AI 분석중…")

    sample_text, sample_cnt, sample_chars, sample_meta = serialize_comments_for_llm_from_file(csv_path)

    st.session_state["sample_text"] = sample_text
    st.session_state["sample_count"] = sample_cnt
    st.session_state["sample_chars"] = sample_chars
    st.session_state["sample_meta"] = sample_meta

    sys = load_first_turn_system_prompt()

    used_top = sample_meta.get("used_top", 0)
    used_random = sample_meta.get("used_random", 0)
    max_per = sample_meta.get("max_chars_per_comment", 0)
    max_total = sample_meta.get("max_total_chars", 0)

    analysis_scope_line = (
        f"{sample_cnt:,}개 (추출: 인기댓글 {used_top:,}개 + 랜덤 {used_random:,}개, "
        f"댓글당 {max_per}자 컷, 총 {max_total:,}자 컷)"
    )
    st.session_state["analysis_scope_line"] = analysis_scope_line

    metrics_block = (
        "[METRICS]\n"
        f"TOTAL_COLLECTED_COMMENTS={sample_meta.get('total_rows', 'NA')}\n"
        f"UNIQUE_COMMENTS_BY_{str(sample_meta.get('dedup_key','text')).upper()}={sample_meta.get('unique_rows', 'NA')}\n"
        f"SAMPLE_RULE=top_like:{used_top}/{sample_meta.get('top_n', 1000)}, random:{used_random}/{sample_meta.get('random_n', 1000)}\n"
        f"LLM_INPUT_LINES={sample_cnt}\n"
        f"LLM_INPUT_CHARS={sample_chars}\n"
        f"ANALYSIS_COMMENT_COUNT_LINE={analysis_scope_line}\n"
    )

    # [핵심] 스마트 캐싱을 위한 Context 구성 (대용량 데이터)
    large_context_text = (
        f"{metrics_block}\n"
        f"[키워드]: {', '.join(kw_main)}\n"
        f"[기간(KST)]: {schema['start_iso']} ~ {schema['end_iso']}\n\n"
        f"[댓글 샘플]:\n{sample_text}\n"
    )
    user_query_part = f"[사용자 원본 질문]: {user_query}"

    # 캐시 초기화 (새 질문이므로)
    if "current_cache" in st.session_state:
        del st.session_state["current_cache"]

    # [수정] call_gemini_smart_cache 사용 (기존 기능 유지)
    answer_md_raw = call_gemini_smart_cache(
        GEMINI_MODEL, GEMINI_API_KEYS, sys, user_query_part,
        large_context_text=large_context_text,
        cache_key_in_session="current_cache"
    )

    prog_bar.progress(1.0, text="완료")
    time.sleep(0.5)
    prog_bar.empty()
    gc.collect()

    return tidy_answer(answer_md_raw)


# [복구] Smart Cache를 활용하는 run_followup_turn
def run_followup_turn(user_query: str):
    if not (schema := st.session_state.get("last_schema")):
        return "오류: 이전 분석 기록이 없습니다. 새 채팅을 시작해주세요."

    context = "\n".join(f"[이전 {'Q' if m['role'] == 'user' else 'A'}]: {m['content']}" for m in st.session_state["chat"][-10:])

    followup_instruction = (
        "🛑 [지시사항 변경] 🛑\n"
        "지금부터는 전체 요약가가 아니라, 사용자의 질문 하나하나를 파고드는 **'심층 분석가'**로서 행동해.\n"
        "이전의 요약 미션은 잊어. 오직 아래 [현재 질문]에만 집중해서 답해.\n\n"
        "=== 답변 전략 ===\n"
        "1. 질문의 의도(속성/대상)를 먼저 파악해라.\n"
        "2. 네 기억 속에 있는 [댓글 샘플]에서 그와 관련된 구체적인 증거(댓글)를 찾아라.\n"
        "3. 뭉뚱그려 말하지 말고, `> 댓글 내용` 형식으로 직접 인용하며 근거를 대라.\n"
        "4. 질문과 관련 없는 TMI(다른 배우, 다른 이슈 등)는 절대 말하지 마라.\n"
        "5. 만약 관련 내용이 데이터에 없으면 '데이터에서 확인되지 않는다'고 딱 잘라 말해라.\n"
    )

    user_payload = (
        f"{followup_instruction}\n\n"
        f"{context}\n\n"
        f"[현재 질문]: {user_query}\n"
        f"[기간(KST)]: {schema.get('start_iso', '?')} ~ {schema.get('end_iso', '?')}\n"
    )

    with st.spinner("💬 심층 분석 중... (Smart Cache)"):
        # call_gemini_smart_cache 호출 (large_context_text=None -> 기존 캐시 사용)
        response_raw = call_gemini_smart_cache(GEMINI_MODEL, GEMINI_API_KEYS, "", user_payload, large_context_text=None)
        response = tidy_answer(response_raw)

    return response
# endregion


# region [Main Execution]
require_auth()

# --- Sidebar actions (HTML controls -> query params) ---
_qp = _qp_get()

def _qp_first(val):
    if val is None:
        return None
    if isinstance(val, (list, tuple)):
        return val[0] if val else None
    return val

_auth_tok = _qp_first(_qp.get("auth"))
_action = _qp_first(_qp.get("action"))

if _action == "new_chat":
    _reset_chat_only(keep_auth=True)
    _qp_set(auth=_auth_tok)  # keep auth only
    st.rerun()

if _action == "save_session":
    if st.session_state.get("chat") and st.session_state.get("last_csv"):
        with st.spinner("세션 저장 중..."):
            ok, result = save_current_session_to_github()
        if ok:
            st.session_state["_toast_msg"] = {"text": f"'{result}' 저장 완료!", "icon": "✅"}
        else:
            st.session_state["_toast_msg"] = {"text": str(result), "icon": "⚠️"}
    _qp_set(auth=_auth_tok)  # keep auth only (and clear action)
    st.rerun()

with st.sidebar:
    disp = st.session_state.get("auth_display_name", st.session_state.get("auth_user_id") or "사용자")
    role = st.session_state.get("auth_role", "user")

    show_actions = bool(st.session_state.get("chat") and st.session_state.get("last_csv"))
    pdf_title = _session_title_for_pdf()
    render_sidebar_controls_html(disp, role, show_actions, pdf_title)

    # one-time toast (레이아웃 깨지지 않게 overlay 우선)
    _t = st.session_state.pop("_toast_msg", None)
    if _t and isinstance(_t, dict):
        try:
            st.toast(_t.get("text", ""), icon=_t.get("icon", ""))
        except Exception:
            st.caption(_t.get("text", ""))

    st.markdown("---")
    st.markdown("#### 대화 기록")

    if not all([GITHUB_TOKEN, GITHUB_REPO]):
        st.caption("GitHub 설정이 Secrets에 없습니다.")
    else:
        try:
            user_id = st.session_state.get("auth_user_id") or "public"
            sessions = sorted(github_list_dir(GITHUB_REPO, GITHUB_BRANCH, f"sessions/{user_id}", GITHUB_TOKEN), reverse=True)
            if not sessions: st.caption("저장된 기록이 없습니다.")
            else:
                editing_session = st.session_state.get("editing_session", None)
                st.markdown('<div class="session-list">', unsafe_allow_html=True)
                for sess in sessions:
                    if sess == editing_session:
                        new_name = st.text_input("새 이름:", value=sess, key=f"new_name_{sess}")
                        c1, c2 = st.columns(2)
                        if c1.button("✅", key=f"save_{sess}"):
                            st.session_state.session_to_rename = (sess, new_name)
                            st.session_state.pop('editing_session', None)
                            st.rerun()
                        if c2.button("❌", key=f"cancel_{sess}"):
                            st.session_state.pop('editing_session', None)
                            st.rerun()
                    else:
                        c1, c2 = st.columns([0.84, 0.16], gap="small")
                        with c1:
                            st.markdown('<div class="sess-name">', unsafe_allow_html=True)
                            if st.button(sess, key=f"sess_{sess}", use_container_width=True):
                                st.session_state.session_to_load = sess
                                st.rerun()
                            st.markdown('</div>', unsafe_allow_html=True)
                        with c2:
                            st.markdown('<div class="more-menu">', unsafe_allow_html=True)
                            if hasattr(st, "popover"):
                                with st.popover("⋯"):
                                    if st.button("이름 변경", key=f"more_edit_{sess}", use_container_width=True):
                                        st.session_state.editing_session = sess
                                        st.rerun()
                                    if st.button("삭제", key=f"more_del_{sess}", use_container_width=True):
                                        st.session_state.session_to_delete = sess
                                        st.rerun()
                            else:
                                with st.expander("⋯"):
                                    if st.button("이름 변경", key=f"more_edit_{sess}", use_container_width=True):
                                        st.session_state.editing_session = sess
                                        st.rerun()
                                    if st.button("삭제", key=f"more_del_{sess}", use_container_width=True):
                                        st.session_state.session_to_delete = sess
                                        st.rerun()
                            st.markdown('</div>', unsafe_allow_html=True)
                st.markdown('</div>', unsafe_allow_html=True)
        except Exception: st.error("기록 로딩 실패")
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="sidebar-bottom-section">', unsafe_allow_html=True)
    st.markdown("""<hr><h3>📞 문의</h3><p>미디어)디지털마케팅 데이터파트</p>""", unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

# [UI 분기]
if not st.session_state.chat:
    # 1. 메인 화면 (채팅 전): 여기에만 토글이 존재해야 함
    st.markdown(
        """
<div style="display:flex; flex-direction:column; align-items:center; justify-content:center;
            text-align:center; height:70vh;">
  <h1 style="font-size:3.5rem; font-weight:600;
             background:-webkit-linear-gradient(45deg, #4285F4, #9B72CB, #D96570, #F2A60C);
             -webkit-background-clip:text; -webkit-text-fill-color:transparent;">
    유튜브 댓글분석: AI 챗봇
  </h1>
  <p style="font-size:1.2rem; color:#4b5563;">관련영상 유튜브 댓글반응을 AI가 요약해줍니다</p>
  <div style="margin-top:3rem; padding:1rem 1.5rem; border:1px solid #e5e7eb; border-radius:12px;
              background-color:#fafafa; max-width:600px; text-align:left;">
    <h4 style="margin-bottom:1rem; font-weight:600;">⚠️ 사용 주의사항</h4>
    <ol style="padding-left:20px;">
      <li><strong>첫 질문 시</strong> 댓글 수집 및 AI 분석에 시간이 소요될 수 있습니다.</li>
      <li>한 세션에서는 <strong>하나의 주제</strong>만 진행해야 분석 정확도가 유지됩니다.</li>
      <li>첫 질문에는 <strong>기간을 명시</strong>해주세요 (예: 최근 48시간 / 5월 1일부터).</li>
    </ol>
  </div>
</div>
""", unsafe_allow_html=True)

    # [토글 버튼] 주의사항 박스 바로 아래 & 가운데 정렬
    _, col_toggle, _ = st.columns([1.3, 1, 1.3])
    with col_toggle:
        st.write("") # 상단 여백
        st.toggle(
            "🧩 자사 IP 모드",
            key="own_ip_mode",
            help="ON: 자사(PGC) 캐시로 공식 영상 후보를 확보하고, 동시에 YouTube 검색으로 외부(UGC)까지 함께 수집합니다."
        )

        # [자사모드 캐시 체크] (원격 동기화 없음: 레포의 pgc_cache/를 그대로 사용)
        cur_toggle = bool(st.session_state.get("own_ip_mode", False))
        prev_toggle = st.session_state.get("own_ip_toggle_prev", None)

        if cur_toggle and (prev_toggle is None or prev_toggle is False):
            cache_dir = _cache_local_dir()
            cache_files = [fn for fn in os.listdir(cache_dir) if re.fullmatch(r"cache_token_.*\.json", fn)]
            if cache_files:
                st.success(f"자사(PGC) 캐시 준비됨 ({len(cache_files)}개 파일).")
            else:
                st.error(f"자사(PGC) 캐시 파일이 없습니다: {os.path.join(cache_dir, 'cache_token_*.json')}")

        st.session_state["own_ip_toggle_prev"] = cur_toggle

else:
    render_metadata_and_downloads()
    render_chat()
    scroll_to_bottom()


if prompt := st.chat_input("예) 최근 24시간 태풍상사 반응 요약해줘 / 또는 영상 URL 붙여도 OK"):
    st.session_state.chat.append({"role": "user", "content": prompt})
    st.rerun()

if st.session_state.chat and st.session_state.chat[-1]["role"] == "user":
    user_query = st.session_state.chat[-1]["content"]
    url_ids = extract_video_ids_from_text(user_query)
    natural_text = strip_urls(user_query)
    has_urls = len(url_ids) > 0
    has_natural = len(natural_text) > 0

    if not st.session_state.get("last_csv"):
        if has_urls and not has_natural:
            response = run_pipeline_first_turn(user_query, extra_video_ids=url_ids, only_these_videos=True)
        elif has_urls and has_natural:
            response = run_pipeline_first_turn(user_query, extra_video_ids=url_ids, only_these_videos=False)
        else:
            response = run_pipeline_first_turn(user_query)
    else:
        response = run_followup_turn(user_query)

    st.session_state.chat.append({"role": "assistant", "content": response})
    st.rerun()
# endregion