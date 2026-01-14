# region [Imports & Setup]
import streamlit as st
from io import BytesIO
from functools import lru_cache

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
from google.generativeai import caching  
from streamlit.components.v1 import html as st_html

# Optional: browser storage bridge (used for login persistence without URL params)
try:
    from streamlit_js_eval import streamlit_js_eval  # pip: streamlit-js-eval
    _SJE_AVAILABLE = True
except Exception:
    streamlit_js_eval = None
    _SJE_AVAILABLE = False

import pymongo
from pymongo import MongoClient
import certifi

# 경로 및 GitHub 설정
BASE_DIR = "/tmp"
SESS_DIR = os.path.join(BASE_DIR, "sessions")
os.makedirs(SESS_DIR, exist_ok=True)

GITHUB_TOKEN = st.secrets.get("GITHUB_TOKEN", "")
GITHUB_REPO = st.secrets.get("GITHUB_REPO", "")
GITHUB_BRANCH = st.secrets.get("GITHUB_BRANCH", "main")

FIRST_TURN_PROMPT_FILE = "1차 질문 프롬프트.md"
REPO_DIR = os.path.dirname(os.path.abspath(__file__))

def load_first_turn_system_prompt() -> str:
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
    page_title="유튜브 댓글분석: AI챗봇",
    layout="wide",
    initial_sidebar_state="expanded"
)

GLOBAL_CSS = r"""
<style>
  /* ===== App chrome ===== */
  header, footer, #MainMenu { visibility: hidden; }

  /* ===== Main padding ===== */
  .main .block-container{
    padding-top: 2rem;
    padding-bottom: 5rem;
    max-width: 1200px;
  }

  /* ===== Sidebar Layout Control ===== */
  [data-testid="stSidebar"]{
    background-color: #f9fafb;
    border-right: 1px solid #efefef;
  }
  [data-testid="stSidebarUserContent"] {
    padding: 1rem 0.8rem !important;
  }
  [data-testid="stSidebar"] [data-testid="stVerticalBlock"] {
    gap: 0rem !important;
  }
  [data-testid="stSidebar"] .element-container {
    margin-bottom: 0.5rem !important;
  }
  [data-testid="stSidebar"] [data-testid="column"] {
    padding: 0 !important;
  }

  /* Sidebar Titles */
  .ytcc-sb-title{
    font-family: 'Helvetica Neue', sans-serif;
    font-weight: 800;
    font-size: 1.25rem;
    margin-bottom: 0.8rem;
    background: linear-gradient(90deg, #4285F4, #DB4437, #F4B400, #0F9D58);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    letter-spacing: -0.5px;
    white-space: nowrap; 
  }

  /* User Profile */
  .user-info-text {
    font-size: 0.85rem;
    font-weight: 700;
    color: #374151;
    white-space: nowrap;
  }
  .user-role-text {
    font-size: 0.75rem;
    color: #9ca3af;
    font-weight: 500;
    margin-left: 4px;
  }

  /* ===== Button Styling Strategy ===== */
  
  /* 1. Default (Secondary) Buttons: Save, Session List, etc. */
  div[data-testid="stButton"] button[kind="secondary"] {
    background-color: #f3f4f6 !important; 
    border: none !important;
    border-radius: 8px !important;
    color: #374151 !important;
    font-weight: 600 !important;
    font-size: 0.82rem !important;
    padding: 0.45rem 0.5rem !important;
    box-shadow: none !important;
    width: 100% !important;
    transition: all 0.15s ease !important;
    box-sizing: border-box !important;
    font-family: inherit !important;
    min-height: 2.15rem !important;
  }
  div[data-testid="stButton"] button[kind="secondary"]:hover {
    background-color: #e5e7eb !important; 
    color: #111827 !important;
  }
  div[data-testid="stButton"] button[kind="secondary"]:active {
    background-color: #d1d5db !important;
    box-shadow: none !important;
  }

  /* 2. Primary Button: New Analysis Start */
  div[data-testid="stButton"] button[kind="primary"] {
    background-color: #111827 !important; 
    border: 1px solid #1f2937 !important;
    border-radius: 8px !important;
    color: #ffffff !important;
    font-weight: 600 !important;
    font-size: 0.82rem !important;
    padding: 0.45rem 0.5rem !important;
    box-shadow: none !important;
    width: 100% !important;
    min-height: 2.15rem !important;
    transition: all 0.15s ease !important;
  }
  div[data-testid="stButton"] button[kind="primary"]:hover {
    background-color: #374151 !important; 
    color: #ffffff !important;
    border-color: #4b5563 !important;
  }
  div[data-testid="stButton"] button[kind="primary"]:active {
    background-color: #000000 !important;
  }

  /* Disabled State */
  button:disabled, .ytcc-cap-btn:disabled {
    background-color: #f9fafb !important;
    color: #e5e7eb !important;
    cursor: not-allowed !important;
    border-color: transparent !important;
  }

  /* ===== Session List Styling ===== */
  .session-list-container {
    margin-top: 5px !important;
    border-top: 1px solid #efefef;
    padding-top: 8px !important;
  }
  .session-header {
    font-size: 0.75rem;
    font-weight: 700;
    color: #9ca3af;
    margin-bottom: 4px !important;
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }
  
  /* List Items */
  .sess-name div[data-testid="stButton"] button {
    background: transparent !important;
    text-align: left !important;
    padding: 0.2rem 0.3rem !important;
    color: #4b5563 !important;
    font-weight: 500 !important;
    box-shadow: none !important;
  }
  .sess-name div[data-testid="stButton"] button:hover {
    background: #f3f4f6 !important;
    color: #111827 !important;
  }
  
  /* More Menu (...) Button - Remove Border */
  .more-menu div[data-testid="stButton"] button {
    background: transparent !important;
    border: none !important;
    padding: 0 !important;
    color: #9ca3af !important;
    box-shadow: none !important;
    min-height: auto !important;
  }
  .more-menu div[data-testid="stButton"] button:hover {
    color: #4b5563 !important;
    background: transparent !important;
  }
  
  /* Login & Main Title */
  .ytcc-login-title, .ytcc-main-title {
    font-weight: 800;
    font-size: clamp(1.4rem, 2.2vw, 2.5rem); 
    background: linear-gradient(45deg, #4285F4, #9B72CB, #D96570, #F2A60C);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    margin-bottom: 0.5rem;
    white-space: nowrap !important;
  }
</style>
"""
st.markdown(GLOBAL_CSS, unsafe_allow_html=True)
# endregion


# region [Constants & State Management]
_YT_FALLBACK, _GEM_FALLBACK = [], []
YT_API_KEYS       = list(st.secrets.get("YT_API_KEYS", [])) or _YT_FALLBACK
GEMINI_API_KEYS   = list(st.secrets.get("GEMINI_API_KEYS", [])) or _GEM_FALLBACK
GEMINI_MODEL      = "gemini-3-flash-preview"  
GEMINI_TIMEOUT    = 240
GEMINI_MAX_TOKENS = 8192
MAX_TOTAL_COMMENTS   = 120_000
MAX_COMMENTS_PER_VID = 4_000
CACHE_TTL_MINUTES    = 20 

# Gemini 동시 호출 제한
MAX_GEMINI_INFLIGHT = max(1, int(st.secrets.get("MAX_GEMINI_INFLIGHT", 3) or 3))
GEMINI_INFLIGHT_WAIT_SEC = int(st.secrets.get("GEMINI_INFLIGHT_WAIT_SEC", 120) or 120)

_GEMINI_SEM = threading.BoundedSemaphore(MAX_GEMINI_INFLIGHT)
_GEMINI_TLOCAL = threading.local()

class GeminiInflightSlot:
    def __init__(self, wait_sec: int = None):
        self.wait_sec = GEMINI_INFLIGHT_WAIT_SEC if wait_sec is None else int(wait_sec)
        self.acquired = False

    def __enter__(self):
        if getattr(_GEMINI_TLOCAL, "held", False):
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
        "current_cache": None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def _reset_chat_only(keep_auth: bool = True):
    auth_keys = {
        "auth_ok", "auth_user_id", "auth_role", "auth_display_name",
        "client_instance_id", "_auth_users_cache"
    }
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


# region [MongoDB Integration: Sync & Load]
# ==========================================
# 몽고DB에 저장한 데이터를 읽어옵니다.
# ==========================================

@st.cache_resource
def init_mongo():
    """몽고DB 클라이언트 연결"""
    try:
        if "mongo" not in st.secrets: return None
        uri = st.secrets["mongo"]["uri"]
        # certifi: SSL 인증서 오류 방지용
        return MongoClient(uri, tlsCAFile=certifi.where())
    except Exception as e:
        print(f"MongoDB Init Error: {e}")
        return None

@st.cache_data(ttl=3600, show_spinner=False)
def load_from_mongodb(file_name):
    try:
        client = init_mongo()
        if not client: return []
        
        db = client.get_database("yt_dashboard")
        col = db.get_collection("videos")
        
        # source_file이 일치하는 문서 검색 (_id 필드는 제외하고 가져옴)
        cursor = col.find({"source_file": file_name}, {"_id": 0, "source_file": 0})
        return list(cursor)

    except Exception as e:
        print(f"Load Error: {e}")
        return []

# 전역 타임스탬프 추적기 (새로고침 해도 유지)
@st.cache_resource
def get_global_time_tracker():
    return {}

def get_all_pgc_data():
    """
    몽고DB에 있는 '모든' 수집 데이터를 가져옵니다. (자사 IP 모드용)
    metadata 컬렉션을 확인하여 변경사항이 있을 때만 캐시를 갱신합니다.
    """
    client = init_mongo()
    if not client: return []

    all_pgc_videos = []
    
    try:
        db = client.get_database("yt_dashboard")
        meta_col = db.get_collection("metadata")
        
        # 1. 메타데이터(업데이트 시간) 전체 조회
        #    문서 구조: {"_id": "cache_token_xxx.json", "updated_at": ..., "count": ...}
        docs = meta_col.find({})
        
        tracker = get_global_time_tracker()
        need_refresh = False
        file_list = []

        for doc in docs:
            file_name = doc["_id"]
            updated_at = doc.get("updated_at")
            
            file_list.append(file_name)
            
            # 타임스탬프 비교
            current_ts_str = str(updated_at) if updated_at else "none"
            last_ts_str = tracker.get(file_name)
            
            if last_ts_str != current_ts_str:
                need_refresh = True
                tracker[file_name] = current_ts_str
        
        # 2. 변경사항 있으면 캐시 비우기
        if need_refresh:
            load_from_mongodb.clear()
        
        # 3. 데이터 로드 (캐시 활용)
        for f_name in file_list:
            vids = load_from_mongodb(f_name)
            all_pgc_videos.extend(vids)

    except Exception as e:
        print(f"Sync Error: {e}")
        return []
        
    return all_pgc_videos

def _extract_vid_from_item(obj):
    # 몽고DB 문서는 'id', 'title', 'date' 등의 필드를 가짐
    vid = obj.get("id") or obj.get("videoId")
    title = obj.get("title", "")
    desc = obj.get("description", "")
    pub_at = obj.get("date") or obj.get("publishedAt") 
    return vid, title, desc, pub_at

def normalize_text_for_search(text: str) -> str:
    if not text: return ""
    return re.sub(r'[^a-zA-Z0-9가-힣]', '', text).lower()

def filter_pgc_data_by_keyword(all_data, keyword, start_dt=None, end_dt=None):
    if not keyword or not all_data: return []
    
    kw_norm = normalize_text_for_search(keyword)
    matched_ids = []
    
    for item in all_data:
        vid, title, desc, pub_str = _extract_vid_from_item(item)
        if not vid: continue
        
        # 날짜 필터
        if (start_dt or end_dt) and pub_str:
            try:
                # 몽고DB date 필드가 이미 datetime 객체일 수도 있고 문자열일 수도 있음
                if isinstance(pub_str, datetime):
                    pub_dt = pub_str
                else:
                    pub_dt = datetime.fromisoformat(str(pub_str).replace("Z", "+00:00"))
                
                # 타임존 보정 (KST 기준 비교를 위해 naive로 변환하거나 tz 맞춤)
                if pub_dt.tzinfo:
                    pub_dt = pub_dt.astimezone(timezone(timedelta(hours=9)))
                
                # 비교 (start_dt, end_dt는 이미 KST aware라고 가정)
                if start_dt and pub_dt < start_dt: continue
                if end_dt and pub_dt > end_dt: continue
            except: pass
            
        # 키워드 필터
        if kw_norm in normalize_text_for_search(title) or kw_norm in normalize_text_for_search(desc):
            matched_ids.append(vid)
            
    return list(dict.fromkeys(matched_ids))
# endregion


# region [PDF Export: current session -> PDF]
@lru_cache(maxsize=1)
def _pdf_font_name() -> str:
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
                if name not in pdfmetrics.getRegisteredFontNames():
                    pdfmetrics.registerFont(TTFont(name, fp))
                return name
            except Exception:
                continue
    return "Helvetica"


def _strip_html_to_text(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"<\s*br\s*/?\s*>", "\n", s, flags=re.I)
    s = re.sub(r"</\s*p\s*>", "\n\n", s, flags=re.I)
    s = re.sub(r"<\s*li\s*>", "• ", s, flags=re.I)
    s = re.sub(r"</\s*li\s*>", "\n", s, flags=re.I)
    s = re.sub(r"<[^>]+>", "", s)
    try:
        import html as _html
        s = _html.unescape(s)
    except Exception as e:
        print(f"⚠️ [_qp_set] failed: {e}")
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"\n{3,}", "\n\n", s).strip()
    return s


def build_session_pdf_bytes(session_title: str, user_label: str, chat: list) -> bytes:
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.pdfgen import canvas
        from reportlab.lib.utils import simpleSplit
        from reportlab.lib.colors import HexColor
    except ModuleNotFoundError:
        return b"" 

    font = _pdf_font_name()

    buf = BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    w, h = A4

    margin_l, margin_r = 18 * 2.8346, 18 * 2.8346 
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

        fill = HexColor("#EAFBF2") if is_user else HexColor("#F3F4F6")
        stroke = HexColor("#CDEEDB") if is_user else HexColor("#E5E7EB")
        text_color = HexColor("#0F172A")

        t = _strip_html_to_text(text or "")
        if not t:
            t = " "

        c.setFont(font, 10.5)
        wrapped = []
        for para in t.split("\n"):
            if para.strip() == "":
                wrapped.append("")
                continue
            wrapped.extend(simpleSplit(para, font, 10.5, max_bubble_w - pad_x * 2))
        if not wrapped:
            wrapped = [" "]

        bubble_h = pad_y * 2 + line_h * len(wrapped) + 10  
        if y - bubble_h < margin_b:
            new_page()

        max_line_w = 0
        for ln in wrapped:
            try:
                max_line_w = max(max_line_w, c.stringWidth(ln, font, 10.5))
            except Exception:
                pass
        bubble_w = min(max_bubble_w, max(220, max_line_w + pad_x * 2)) 

        x = (w - margin_r - bubble_w) if is_user else margin_l

        c.setFillColor(HexColor("#64748B"))
        c.setFont(font, 9)
        who = "나" if is_user else "AI"
        c.drawString(x + pad_x, y, who)
        y -= 12

        c.setFillColor(fill)
        c.setStrokeColor(stroke)
        c.roundRect(x, y - (bubble_h - 12), bubble_w, bubble_h - 12, 10, fill=1, stroke=1)

        c.setFillColor(text_color)
        c.setFont(font, 10.5)
        tx = x + pad_x
        ty = y - pad_y - 2
        for ln in wrapped:
            c.drawString(tx, ty, ln)
            ty -= line_h

        y = y - (bubble_h - 12) - 12

    draw_title()

    for m in chat or []:
        draw_bubble(m.get("role"), m.get("content", ""))

    c.save()
    return buf.getvalue()


def _session_title_for_pdf() -> str:
    return st.session_state.get("loaded_session_name") or "현재대화"


def render_pdf_capture_button(label: str, pdf_filename_base: str) -> None:
    safe = re.sub(r'[^0-9A-Za-z가-힣 _\-\(\)\[\]]+', '', (pdf_filename_base or 'chat')).strip() or "chat"
    safe = safe.replace(" ", "_")[:80]
    btn_id = f"ytcc-cap-{uuid4().hex[:8]}"

    st_html(f"""
    <div style="width:100%;">
      <button id="{btn_id}" class="ytcc-cap-btn" type="button">{label}</button>
    </div>

    <script>
    (function(){{
      const BTN_ID = "{btn_id}";
      const FILE_BASE = "{safe}";
      const btn = document.getElementById(BTN_ID);
      if(!btn) return;

      function loadScriptOnce(src, id){{
        return new Promise((resolve, reject) => {{
          const d = window.parent.document;
          if(id && d.getElementById(id)) return resolve();
          const s = d.createElement("script");
          if(id) s.id = id;
          s.src = src;
          s.onload = () => resolve();
          s.onerror = () => reject(new Error("failed: " + src));
          d.head.appendChild(s);
        }});
      }}

      async function ensureLibs(){{
        if(!window.parent.html2canvas){{
          await loadScriptOnce("https://cdn.jsdelivr.net/npm/html2canvas@1.4.1/dist/html2canvas.min.js", "ytcc-html2canvas");
        }}
        if(!window.parent.jspdf){{
          await loadScriptOnce("https://cdn.jsdelivr.net/npm/jspdf@2.5.1/dist/jspdf.umd.min.js", "ytcc-jspdf");
        }}
      }}

      async function captureToPdf(){{
        const doc = window.parent.document;
        const msgs = Array.from(doc.querySelectorAll('div[data-testid="stChatMessage"]'));
        if(!msgs || msgs.length === 0){{
          alert("저장할 대화가 없습니다.");
          return;
        }}

        const tmp = doc.createElement("div");
        tmp.style.position = "fixed";
        tmp.style.left = "-99999px";
        tmp.style.top = "0";
        tmp.style.background = "white";
        tmp.style.padding = "24px";
        tmp.style.borderRadius = "16px";
        tmp.style.boxSizing = "border-box";

        let maxW = 0;
        msgs.forEach(m => {{
          const r = m.getBoundingClientRect();
          if(r.width) maxW = Math.max(maxW, r.width);
        }});
        const capW = Math.max(1200, Math.min(1700, Math.ceil(maxW + 140)));
        tmp.style.width = capW + "px";

        msgs.forEach(m => {{
          const clone = m.cloneNode(true);
          clone.style.width = "100%";
          clone.style.maxWidth = "100%";
          clone.style.boxSizing = "border-box";
          clone.querySelectorAll("*").forEach(el => {{
            el.style.maxWidth = "100%";
            el.style.boxSizing = "border-box";
            el.style.overflowWrap = "anywhere";
            el.style.wordBreak = "break-word";
          }});
          tmp.appendChild(clone);
        }});

        doc.body.appendChild(tmp);

        try {{
          await ensureLibs();
          const canvas = await window.parent.html2canvas(tmp, {{
            scale: 2,
            backgroundColor: "#ffffff",
            useCORS: true,
            allowTaint: true,
            windowWidth: capW
          }});

          const imgData = canvas.toDataURL("image/png", 1.0);
          const {{ jsPDF }} = window.parent.jspdf;
          const pdf = new jsPDF("p", "mm", "a4");

          const pageW = pdf.internal.pageSize.getWidth();
          const pageH = pdf.internal.pageSize.getHeight();
          const imgW = pageW;
          const imgH = (canvas.height * imgW) / canvas.width;

          let y = 0;
          let remaining = imgH;

          while (remaining > 0) {{
            pdf.addImage(imgData, "PNG", 0, y, imgW, imgH, undefined, "FAST");
            remaining -= pageH;
            if (remaining > 0) {{
              pdf.addPage();
              y -= pageH;
            }}
          }}

          pdf.save(FILE_BASE + ".pdf");
        }} catch(e) {{
          console.error(e);
          alert("PDF 저장 중 오류가 발생했습니다.");
        }} finally {{
          try {{ tmp.remove(); }} catch(e) {{}}
        }}
      }}

      btn.addEventListener("click", () => {{
        if(btn.dataset.busy === "1") return;
        btn.dataset.busy = "1";
        const old = btn.innerText;
        btn.innerText = "저장중...";
        btn.disabled = true;
        captureToPdf().finally(() => {{
          btn.dataset.busy = "0";
          btn.innerText = old;
          btn.disabled = false;
        }});
      }});
    }})();
    </script>

    <style>
      .ytcc-cap-btn {{
        width: 100%;
        border-radius: 8px; 
        padding: 0.45rem 0.5rem;
        font-size: 0.82rem;
        font-weight: 600;
        line-height: 1.2;
        min-height: 2.15rem; 
        border: none;
        background: #f3f4f6; 
        color: #374151;
        cursor: pointer;
        box-sizing: border-box;
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
        transition: background-color 0.15s ease;
        display: flex;
        align-items: center;
        justify-content: center;
      }}
      .ytcc-cap-btn:hover {{
        background: #e5e7eb; 
        color: #111827;
      }}
      .ytcc-cap-btn:disabled {{
        background: #f9fafb;
        color: #e5e7eb;
        cursor: not-allowed;
      }}
    </style>
    """, height=46)

# endregion


# region [Auth: ID/PW in secrets.toml]
import hmac
import hashlib
from typing import Dict, Optional

def _load_auth_users_from_secrets() -> Dict[str, dict]:
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
    if pw_hash.startswith("pbkdf2_sha256$"):
        return _pbkdf2_sha256_verify(password, pw_hash, pepper=pepper)
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
    try:
        return dict(st.query_params)
    except Exception:
        return {}

def _qp_set(**kwargs):
    try:
        st.query_params.clear()
        cleaned = {}
        for k, v in kwargs.items():
            if v is None: continue
            if isinstance(v, (list, tuple)):
                if len(v) == 0: continue
                cleaned[k] = v[0] 
            else:
                s = str(v).strip()
                if s == "": continue
                cleaned[k] = s
        
        for k, v in cleaned.items():
            st.query_params[k] = v
    except Exception:
        pass

def _b64url_encode(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode("utf-8").rstrip("=")

def _b64url_decode(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode((s + pad).encode("utf-8"))

def _auth_signing_secret() -> bytes:
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
        if not token or "." not in token: return None
        body, sig = token.split(".", 1)
        expect = hmac.new(_auth_signing_secret(), body.encode("utf-8"), hashlib.sha256).digest()
        if not hmac.compare_digest(_b64url_decode(sig), expect): return None
        payload = json.loads(_b64url_decode(body).decode("utf-8"))
        if not isinstance(payload, dict): return None
        if int(payload.get("exp", 0)) < int(time.time()): return None
        uid = (payload.get("uid") or "").strip()
        if not uid: return None
        return payload
    except Exception:
        return None


# --- MongoDB (URI / PyMongo) based session store (auth only) ---
def _mongo_uri() -> str:
    # Support existing secrets layout:
    #   [mongo]
    #   uri = "mongodb+srv://..."
    # and also common flat keys: MONGO_URI / MONGODB_URI
    try:
        mongo_block = st.secrets.get("mongo", {}) or {}
        uri = mongo_block.get("uri", "") or ""
    except Exception:
        uri = ""
    uri = str(uri or st.secrets.get("MONGO_URI", "") or st.secrets.get("MONGODB_URI", "") or "").strip()
    return uri

def _mongo_enabled() -> bool:
    return bool(_mongo_uri())

@st.cache_resource
def _mongo_client():
    # Keep this isolated to auth; if pymongo isn't installed or connection fails, we gracefully fallback.
    try:
        from pymongo import MongoClient  # type: ignore
        uri = _mongo_uri()
        if not uri:
            return None
        return MongoClient(uri, serverSelectionTimeoutMS=3000, connectTimeoutMS=3000, socketTimeoutMS=3000)
    except Exception as e:
        print(f"⚠️ [mongo] client init failed: {e}")
        return None

def _mongo_db_name() -> str:
    # Prefer explicit secret, else try to parse default DB from URI path, else fallback to a safe name.
    try:
        mongo_block = st.secrets.get("mongo", {}) or {}
        mongo_db = mongo_block.get("db_name") or mongo_block.get("db") or mongo_block.get("database") or ""
    except Exception:
        mongo_db = ""
    name = str(st.secrets.get("MONGO_DB_NAME", "") or mongo_db or "").strip()
    if name:
        return name
    uri = _mongo_uri()
    try:
        if "/" in uri:
            tail = uri.split("/", 3)[-1]
            db = tail.split("?", 1)[0].strip()
            if db and not db.startswith("@") and db not in ("admin",):
                return db
    except Exception:
        pass
    return "ytcc_auth"

def _mongo_sessions_coll_name() -> str:
    try:
        mongo_block = st.secrets.get("mongo", {}) or {}
        coll = mongo_block.get("sessions_coll") or mongo_block.get("sessions_collection") or mongo_block.get("coll") or ""
    except Exception:
        coll = ""
    return str(st.secrets.get("MONGO_SESSIONS_COLL", "") or coll or "sessions").strip() or "sessions"

def _mongo_sessions_coll():
    try:
        cli = _mongo_client()
        if cli is None:
            return None
        db = cli[_mongo_db_name()]
        coll = db[_mongo_sessions_coll_name()]
        # Best-effort TTL index (optional). Store expiresAt as datetime.
        try:
            from pymongo import ASCENDING  # type: ignore
            coll.create_index([("expiresAt", ASCENDING)], expireAfterSeconds=0)
            coll.create_index([("uid", ASCENDING)])
        except Exception:
            pass
        return coll
    except Exception as e:
        print(f"⚠️ [mongo] coll init failed: {e}")
        return None

def _make_session_id() -> str:
    return base64.urlsafe_b64encode(os.urandom(24)).decode("utf-8").rstrip("=")

def _create_mongo_session(uid: str, ttl_hours: int = None) -> Optional[str]:
    try:
        coll = _mongo_sessions_coll()
        if coll is None:
            return None
        ttl = ttl_hours if ttl_hours is not None else int(st.secrets.get("AUTH_TOKEN_TTL_HOURS", 24*14) or (24*14))
        exp = int(time.time() + max(60, ttl * 3600))
        sid = _make_session_id()
        now = datetime.utcnow()
        coll.insert_one({
            "_id": sid,
            "uid": uid,
            "exp": exp,
            "expiresAt": datetime.utcfromtimestamp(exp),
            "revoked": False,
            "createdAt": now,
            "lastSeenAt": now,
        })
        return sid
    except Exception as e:
        print(f"⚠️ [mongo] create session failed: {e}")
        return None

def _verify_mongo_session(sid: str) -> Optional[dict]:
    try:
        if not sid or "." in sid:
            return None
        coll = _mongo_sessions_coll()
        if coll is None:
            return None
        doc = coll.find_one({"_id": sid, "revoked": {"$ne": True}})
        if not doc:
            return None
        exp = int(doc.get("exp") or 0)
        if exp < int(time.time()):
            return None
        uid = str(doc.get("uid") or "").strip()
        if not uid:
            return None
        try:
            coll.update_one({"_id": sid}, {"$set": {"lastSeenAt": datetime.utcnow()}})
        except Exception:
            pass
        return {"uid": uid, "exp": exp, "sid": sid}
    except Exception as e:
        print(f"⚠️ [mongo] verify session failed: {e}")
        return None

def _revoke_mongo_session(sid: str) -> None:
    try:
        if not sid or "." in sid:
            return
        coll = _mongo_sessions_coll()
        if coll is None:
            return
        coll.update_one({"_id": sid}, {"$set": {"revoked": True, "revokedAt": datetime.utcnow()}})
    except Exception as e:
        print(f"⚠️ [mongo] revoke session failed: {e}")

def _redirect_with_auth(auth_value: str):
    """Ensure the browser URL includes ?auth=... .

    1) Prefer Streamlit-native query param update + rerun (no iframe / no JS sandbox).
    2) Fallback to a tiny JS redirect that targets the *parent* window, with a visible
       manual link as a backup (so we never show a blank white screen).
    """
    auth_value = str(auth_value or "").strip()
    if not auth_value:
        return

    # 1) Native (most reliable on Streamlit Cloud)
    try:
        st.query_params["auth"] = auth_value
        if "logout" in st.query_params:
            del st.query_params["logout"]
        st.rerun()
    except Exception as e:
        print(f"⚠️ [_redirect_with_auth] st.query_params failed: {e}")

    # 2) JS fallback (must target parent window; component iframe's window.location won't change address bar)
    try:
        from urllib.parse import quote as _q
        safe_link = "?auth=" + _q(auth_value, safe="")
    except Exception:
        safe_link = "?auth=" + auth_value

    st.info("로그인 처리 중입니다… 자동 이동이 안 되면 아래 '계속'을 눌러주세요.")
    st.markdown(f"<div style='text-align:center; margin-top:0.5rem;'><a href='{safe_link}' style='font-weight:700;'>계속</a></div>", unsafe_allow_html=True)

    safe_val = json.dumps(auth_value)
    st_html(
        f"""
        <script>
          (function () {{
            try {{
              var target = (window.parent && window.parent !== window) ? window.parent : window;
              var url = new URL(target.location.href);
              url.searchParams.set("auth", {safe_val});
              url.searchParams.delete("logout");
              target.location.replace(url.toString());
            }} catch (e) {{
              try {{
                var target2 = (window.parent && window.parent !== window) ? window.parent : window;
                var base = target2.location.href.split("?")[0];
                target2.location.replace(base + "?auth=" + encodeURIComponent({safe_val}));
              }} catch (e2) {{}}
            }}
          }})();
        </script>
        """,
        height=0
    )
    st.stop()


# --- Browser localStorage helpers (team-tool login persistence) ---
_AUTH_LS_KEY = str(st.secrets.get("AUTH_LS_KEY", "") or "ytcc_auth_token").strip() or "ytcc_auth_token"

def _ls_get_item(key: str):
    if not _SJE_AVAILABLE:
        return None
    try:
        v = streamlit_js_eval(
            js_expressions=f"localStorage.getItem({json.dumps(key)})",
            key=f"ls_get::{key}",
            want_output=True
        )
        if v is None:
            return None
        if isinstance(v, str):
            return v
        return str(v)
    except Exception as e:
        print(f"⚠️ [localStorage] get failed: {e}")
        return None

def _ls_set_item(key: str, value: str):
    if not _SJE_AVAILABLE:
        return
    try:
        _k = f"ls_set::{key}::{int(time.time()*1000)}"
        streamlit_js_eval(
            js_expressions=f"localStorage.setItem({json.dumps(key)}, {json.dumps(str(value))});",
            key=_k,
            want_output=False
        )
    except Exception as e:
        print(f"⚠️ [localStorage] set failed: {e}")

def _ls_del_item(key: str):
    if not _SJE_AVAILABLE:
        return
    try:
        _k = f"ls_del::{key}::{int(time.time()*1000)}"
        streamlit_js_eval(
            js_expressions=f"localStorage.removeItem({json.dumps(key)});",
            key=_k,
            want_output=False
        )
    except Exception as e:
        print(f"⚠️ [localStorage] del failed: {e}")

def _get_persisted_token(qp: dict) -> str:
    # Priority: URL qp auth (if present) > session_state cached token > localStorage
    tok = ""
    try:
        if isinstance(qp, dict) and "auth" in qp:
            v = qp.get("auth")
            if isinstance(v, list):
                v = v[0] if v else ""
            tok = str(v or "").strip()
    except Exception:
        pass
    if not tok:
        tok = str(st.session_state.get("_auth_token") or "").strip()
    if not tok:
        tok = str(_ls_get_item(_AUTH_LS_KEY) or "").strip()
    return tok

def _logout_and_clear():
    """
    Robust logout for team-tool mode (MongoDB session + localStorage persistence)

    Goals:
      1) Revoke current MongoDB session immediately (best-effort)
      2) Delete browser localStorage token so refresh won't auto-login
      3) Clear Streamlit session auth state
      4) Navigate to a clean URL (no query params) without blank/white screen
    """
    # ---- 0) Identify current token (prefer in-memory, then URL, then localStorage) ----
    tok = str(st.session_state.get("_auth_token") or "").strip()
    if not tok:
        try:
            qp = _qp_get()
            v = qp.get("auth") if isinstance(qp, dict) else ""
            if isinstance(v, list):
                v = v[0] if v else ""
            tok = str(v or "").strip()
        except Exception:
            tok = ""
    if not tok:
        try:
            tok = str(_ls_get_item(_AUTH_LS_KEY) or "").strip()
        except Exception:
            tok = ""

    # ---- 1) Revoke mongo session (opaque token without '.') ----
    try:
        if tok and "." not in tok:
            _revoke_mongo_session(tok)
    except Exception as e:
        print(f"⚠️ [logout] revoke session failed: {e}")

    # ---- 2) Delete localStorage token (best-effort) ----
    try:
        _ls_del_item(_AUTH_LS_KEY)
    except Exception as e:
        print(f"⚠️ [logout] localStorage delete failed: {e}")

    # ---- 3) Clear auth-related session_state keys ----
    for k in [
        "auth_ok",
        "auth_user_id",
        "auth_role",
        "auth_display_name",
        "_auth_token",
    ]:
        try:
            st.session_state.pop(k, None)
        except Exception:
            pass

    # Keep other session data reset behavior (existing logic)
    _reset_chat_only(keep_auth=False)

    # ---- 4) Clear query params (best-effort) ----
    try:
        st.query_params.clear()
    except Exception:
        pass

    # ---- 5) User-visible logout feedback + hard clean URL navigation ----
    st.markdown("✅ 로그아웃 처리 중…")

    # JS fallback: remove localStorage + navigate to clean URL (parent if framed)
    # Using replace() avoids creating history entries.
    st_html(
        f"""
        <script>
          (function () {{
            try {{
              var w = window.parent || window;
              try {{ w.localStorage.removeItem({json.dumps(_AUTH_LS_KEY)}); }} catch(e) {{}}
              var clean = w.location.pathname + (w.location.hash || "");
              w.location.replace(clean);
            }} catch (e) {{
              // If navigation is blocked for any reason, do nothing; Streamlit rerun will show login screen.
            }}
          }})();
        </script>
        """,
        height=0
    )

    # Don't leave a blank white page; stop after rendering the message above.
    st.stop()


def require_auth():

    users = st.session_state.get("_auth_users_cache") or _load_auth_users_from_secrets()
    st.session_state["_auth_users_cache"] = users
    auth_enabled = bool(users)

    if not auth_enabled:
        return

    qp = _qp_get()
    if "logout" in qp:
        _logout_and_clear()

    # 1) 이미 인증된 경우: 토큰을 localStorage에 보관해서 새로고침 복구를 보장
    if is_authenticated():
        u = get_current_user() or {}
        if u and (u.get("active") is False):
            st.session_state.pop("auth_ok", None)
            st.session_state.pop("auth_user_id", None)
            st.session_state.pop("_auth_token", None)
        else:
            tok = _get_persisted_token(qp)
            if not tok:
                uid = st.session_state["auth_user_id"]
                tok = _create_mongo_session(uid) if _mongo_enabled() else None
                if not tok:
                    tok = _make_auth_token(uid)
            st.session_state["_auth_token"] = tok
            _ls_set_item(_AUTH_LS_KEY, tok)
            return

    # 2) 미인증인 경우: URL auth가 없더라도 localStorage 토큰으로 복구 시도
    token_str = _get_persisted_token(qp)
    payload = _verify_auth_token(token_str) if "." in token_str else _verify_mongo_session(token_str)
    if payload:
        uid = payload["uid"]
        rec = users.get(uid)
        if rec and (rec.get("active") is not False):
            st.session_state["auth_ok"] = True
            st.session_state["auth_user_id"] = uid
            st.session_state["auth_role"] = rec.get("role", "user")
            st.session_state["auth_display_name"] = rec.get("display_name", uid)
            st.session_state["client_instance_id"] = st.session_state.get("client_instance_id") or uuid4().hex[:10]
            st.session_state["_auth_token"] = token_str
            _ls_set_item(_AUTH_LS_KEY, token_str)
            return

    # 3) 로그인 UI
    c1, c2, c3 = st.columns([1.0, 1.5, 1.0])
    with c2:
        st.markdown("<div style='height:10vh;'></div>", unsafe_allow_html=True)
        st.markdown(
            """
            <div style="text-align:center;">
              <div class="ytcc-login-title">💬 유튜브 댓글분석 AI</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
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

            tok = _create_mongo_session(uid) if _mongo_enabled() else None
            if not tok:
                tok = _make_auth_token(uid)

            st.session_state["_auth_token"] = tok
            _ls_set_item(_AUTH_LS_KEY, tok)

            # URL은 건드리지 않음 (query param 없이도 localStorage로 새로고침 유지)
            st.rerun()

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
        max_retries = len(self.rot.keys)
        last_error = None

        for _ in range(max_retries + 1):
            try:
                return factory(self.service).execute()
            except HttpError as e:
                last_error = e
                status = getattr(getattr(e, 'resp', None), 'status', None)
                msg = (getattr(e, 'content', b'').decode('utf-8', 'ignore') or '').lower()
                
                if status in (403, 429) and any(t in msg for t in ["quota", "rate", "limit"]):
                    print(f"⚠️ [YouTube API] 키 만료/제한 감지. 다음 키로 교체 시도... (Current: {self.rot.idx})")
                    self.rot.rotate() 
                    self._build()    
                    continue        
                
                raise e
        
        raise last_error
# endregion


# region [GitHub & Session Management]
def _gh_headers(token: str):
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
    base = re.sub(r"[^0-9A-Za-z가-힣]", "", kw)
    base = base[:12] if base else "세션"
    return base

def _next_session_number(user_id: str, base: str) -> int:
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
    if not os.path.exists(csv_path):
        return "", 0, 0, {"error": "csv_not_found"}

    try:
        df_all = pd.read_csv(csv_path)
    except Exception:
        return "", 0, 0, {"error": "csv_read_failed"}

    if df_all.empty:
        return "", 0, 0, {"error": "csv_empty"}

    total_rows = len(df_all)

    unique_rows = None
    try:
        if dedup_key in df_all.columns:
            unique_rows = df_all[dedup_key].astype(str).str.strip().replace("", pd.NA).dropna().nunique()
    except Exception:
        unique_rows = None

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
    if not text:
        return ""
    
    text = re.sub(r"^```html", "", text, flags=re.MULTILINE | re.IGNORECASE)
    text = re.sub(r"^```", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\s+(?=<)", "", text, flags=re.MULTILINE)
    
    lines = text.splitlines()
    cleaned = []
    
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
def call_gemini_rotating(model_name, keys, system_instruction, user_payload,
                         timeout_s=240, max_tokens=8192) -> str:
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

    if cached_info and not large_context_text:
        cache_name = cached_info.get("name")
        creator_key = cached_info.get("key")
        
        genai.configure(api_key=creator_key)
        try:
            active_cache = caching.CachedContent.get(cache_name)
            with GeminiInflightSlot():
                active_cache.update(ttl=timedelta(minutes=CACHE_TTL_MINUTES))
            
            final_model = genai.GenerativeModel.from_cached_content(
                cached_content=active_cache,
                generation_config={"temperature": 0.2, "max_output_tokens": GEMINI_MAX_TOKENS}
            )
        except Exception as e:
            active_cache = None
            large_context_text = st.session_state.get("sample_text_full_context", "")
            if not large_context_text:
                return "⚠️ [오류] 세션이 만료되어 복구할 데이터가 없습니다. 새로고침 해주세요."

    if not active_cache and large_context_text:
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
                break
            except Exception as e:
                msg = str(e).lower()
                if "too short" in msg or "argument" in msg:
                    active_cache = None
                    break
                if "429" in msg or "quota" in msg:
                    rk.rotate()
                    continue
                raise e

    try:
        if final_model:
            with GeminiInflightSlot():
                resp = final_model.generate_content(user_query, safety_settings=safety_settings)
        else:
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
            
            if isinstance(content, str) and msg.get("role") == "assistant" and ("<div" in content or "<style" in content):
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
                full_html = f"<div class='yt-report'>{report_style}{content}</div>"
                st.markdown(full_html, unsafe_allow_html=True)
                
            else:
                st.markdown(content)
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
    
    # [수정] 자사 IP 모드 - 파이어베이스 연동 로직 적용
    if own_mode:
        # 1. 최신 데이터 동기화 및 로드 (메모리)
        all_pgc_data = get_all_pgc_data()
        
        if not all_pgc_data:
            # st.warning("자사 IP 데이터를 불러올 수 없습니다 (Firebase 연동 확인 필요)")
            pass
        else:
            # 2. 메모리 상에서 필터링
            for base_kw in (kw_main or []):
                matched_ids = filter_pgc_data_by_keyword(all_pgc_data, base_kw, start_dt, end_dt)
                pgc_ids.extend(matched_ids)
            
        pgc_ids = list(dict.fromkeys(pgc_ids))

    if only_these_videos and extra_video_ids:
        all_ids = extra_video_ids
    else:
        all_ids = []
        # UGC 검색
        for base_kw in (kw_main or ["유튜브"]):
            from urllib.parse import quote
            clean_kw = base_kw.replace(" ", "")
            search_kw = clean_kw if clean_kw.startswith("#") else f"#{clean_kw}"
            if search_kw:
                all_ids.extend(yt_search_videos(rt, search_kw, 100, "viewCount", kst_to_rfc3339_utc(start_dt), kst_to_rfc3339_utc(end_dt)))
        
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

    large_context_text = (
        f"{metrics_block}\n"
        f"[키워드]: {', '.join(kw_main)}\n"
        f"[기간(KST)]: {schema['start_iso']} ~ {schema['end_iso']}\n\n"
        f"[댓글 샘플]:\n{sample_text}\n"
    )
    user_query_part = f"[사용자 원본 질문]: {user_query}"

    if "current_cache" in st.session_state:
        del st.session_state["current_cache"]

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


def run_followup_turn(user_query: str):
    if not (schema := st.session_state.get("last_schema")):
        return "오류: 이전 분석 기록이 없습니다. 새 채팅을 시작해주세요."

    context = "\n".join(f"[이전 {'Q' if m['role'] == 'user' else 'A'}]: {m['content']}" for m in st.session_state["chat"][-10:])

    followup_instruction = (
        "🛑 [지시사항 변경] 🛑\n"
        "지금부터는 전체 요약가가 아니라, 사용자의 질문 하나하나를 파고드는 **'심층 분석가'**로서 행동해.\n"
        "첫 질문에 대한 응답처럼 규격화된 HTML로 주지 않아도 된다.\n"
        "이전의 요약 미션은 잊어. 오직 아래 [현재 질문]에만 집중해서 답해.\n\n"
        "=== 답변 전략 ===\n"
        "1. 질문의 의도(속성/대상)를 먼저 파악해라.(파악한 의도는 답변을 위한 내부 지침으로만 활용하고, 사용자에게 보여주지 않아도 된다)\n"
        "2. 네 기억 속에 있는 [댓글 샘플]에서 그와 관련된 구체적인 증거(댓글)를 찾아라.\n"
        "3. 증거 댓글은 눈에 잘 띄도록 반드시 `<div class='quote'>댓글 내용</div>` 태그로 감싸서 출력해라.\n"
        "4. 질문과 관련 없는 TMI(다른 배우, 다른 이슈 등)는 절대 말하지 마라.\n"
        "5. 만약 관련 내용이 데이터에 없으면 '데이터에서 확인되지 않는다'고 딱 잘라 말해라.\n"
    )

    user_payload = (
        f"{followup_instruction}\n\n"
        f"{context}\n\n"
        f"[현재 질문]: {user_query}\n"
        f"[기간(KST)]: {schema.get('start_iso', '?')} ~ {schema.get('end_iso', '?')}\n"
    )

    with st.spinner("💬 답변 생성 중... "):
        response_raw = call_gemini_smart_cache(GEMINI_MODEL, GEMINI_API_KEYS, "", user_payload, large_context_text=None)
        response = tidy_answer(response_raw)

    return response
# endregion


# region [Main Execution]
require_auth()

with st.sidebar:
    st.markdown('<div style="height: 20px;"></div>', unsafe_allow_html=True)

    if st.session_state.get("auth_user_id"):
        disp = st.session_state.get("auth_display_name", st.session_state.get("auth_user_id"))
        role = st.session_state.get("auth_role", "user")
        
        c_user, c_logout = st.columns([0.75, 0.25], gap="small")
        with c_user:
            st.markdown(f"""
            <div style="display:flex; align-items:baseline; padding-top:4px;">
                <span class="user-info-text">{disp}</span>
                <span class="user-role-text">({role})</span>
            </div>
            """, unsafe_allow_html=True)
            
        with c_logout:
            st.markdown(
                """
                <a href="?logout=true" target="_self" 
                   style="float:right; color:#6b7280; font-size:0.75rem; text-decoration:underline; 
                          font-weight:500; cursor:pointer; margin-top:4px;">
                   로그아웃
                </a>
                """, 
                unsafe_allow_html=True
            )
            
        st.markdown('<div style="border-bottom:1px solid #efefef; margin-bottom:12px; margin-top:2px;"></div>', unsafe_allow_html=True)

    if st.button("＋ 새 분석 시작", type="primary", use_container_width=True):
        _reset_chat_only(keep_auth=True)
        st.rerun()
    
    st.markdown('<div style="margin-bottom: 6px;"></div>', unsafe_allow_html=True)
    
    if st.session_state.chat:
        c1, c2 = st.columns(2, gap="small") 
        with c1:
            has_data = bool(st.session_state.last_csv)
            if st.button("세션 저장", use_container_width=True, disabled=not has_data):
                if has_data:
                    with st.spinner("저장..."):
                        success, result = save_current_session_to_github()
                    if success:
                        st.success("완료")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error(result)
        
        with c2:
            pdf_title = _session_title_for_pdf()
            render_pdf_capture_button("PDF 저장", pdf_title)

    st.markdown('<div class="session-list-container">', unsafe_allow_html=True)
    st.markdown('<div class="session-header">Recent History</div>', unsafe_allow_html=True)

    if not all([GITHUB_TOKEN, GITHUB_REPO]):
        st.caption("GitHub 미설정")
    else:
        try:
            user_id = st.session_state.get("auth_user_id") or "public"
            sessions = sorted(github_list_dir(GITHUB_REPO, GITHUB_BRANCH, f"sessions/{user_id}", GITHUB_TOKEN), reverse=True)
            
            if not sessions: 
                st.caption("기록 없음")
            else:
                editing_session = st.session_state.get("editing_session", None)
                for sess in sessions:
                    if sess == editing_session:
                        with st.container(border=True):
                            new_name = st.text_input("이름 변경", value=sess, key=f"new_name_{sess}", label_visibility="collapsed")
                            ec1, ec2 = st.columns(2)
                            if ec1.button("V", key=f"save_{sess}", use_container_width=True):
                                st.session_state.session_to_rename = (sess, new_name)
                                st.session_state.pop('editing_session', None)
                                st.rerun()
                            if ec2.button("X", key=f"cancel_{sess}", use_container_width=True):
                                st.session_state.pop('editing_session', None)
                                st.rerun()
                    else:
                        sc1, sc2 = st.columns([0.85, 0.15], gap="small")
                        with sc1:
                            st.markdown('<div class="sess-name">', unsafe_allow_html=True)
                            if st.button(f"▪ {sess}", key=f"sess_{sess}", use_container_width=True):
                                st.session_state.session_to_load = sess
                                st.rerun()
                            st.markdown('</div>', unsafe_allow_html=True)
                        with sc2:
                            st.markdown('<div class="more-menu">', unsafe_allow_html=True)
                            if hasattr(st, "popover"):
                                with st.popover(":", use_container_width=True):
                                    if st.button("수정", key=f"more_edit_{sess}", use_container_width=True):
                                        st.session_state.editing_session = sess
                                        st.rerun()
                                    if st.button("삭제", key=f"more_del_{sess}", type="primary", use_container_width=True):
                                        st.session_state.session_to_delete = sess
                                        st.rerun()
                            st.markdown('</div>', unsafe_allow_html=True)
        except Exception: 
            st.error("Error")
            
    st.markdown("""
        <div style="margin-top:auto; padding-top:1rem; font-size:0.9rem; color:#6b7280; text-align:center;">
            Media) Marketing Team - Data Insight Part<br>Powered by Gemini
        </div>
    """, unsafe_allow_html=True)


if not st.session_state.chat:
    st.markdown(
        """
<div style="display:flex; flex-direction:column; align-items:center; justify-content:center;
            text-align:center; padding-top:8vh;">
  <div class="ytcc-main-title">유튜브 댓글분석 AI</div>
  <p style="font-size:1.1rem; color:#6b7280; max-width:600px; margin-top:10px; margin-bottom: 2rem;">
    유튜브 여론이 궁금한 드라마에 대해 대화형식으로 물어보세요<br>
    유튜브 댓글 기반의 시청자 반응을 AI가 분석해줍니다.
  </p>
  
  <div style="background-color:#fff1f2; border:1px solid #ffe4e6; border-radius:12px; 
              padding:1rem 1.5rem; max-width:650px; text-align:left; margin-bottom:1rem; width:100%;">
    <h4 style="margin:0 0 0.5rem 0; font-size:0.95rem; font-weight:700; color:#9f1239;">⚠️ 사용 전 확인해주세요</h4>
    <ul style="margin:0; padding-left:1.2rem; font-size:0.9rem; color:#881337; line-height:1.6;">
        <li><strong>첫 질문 시</strong> 댓글 수집 및 AI 분석에 시간이 소요될 수 있습니다.</li>
        <li>한 세션에서는 <strong>하나의 주제</strong>만 진행해야 분석 정확도가 유지됩니다.</li>
        <li>정확한 분석을 위해 질문에 <strong>기간을 명시</strong>해주세요 (예: 최근 48시간).</li>
    </ul>
  </div>

  <div style="padding:1.5rem; border:1px solid #e5e7eb; border-radius:16px;
              background-color:#ffffff; max-width:650px; text-align:left; box-shadow:0 4px 6px -1px rgba(0,0,0,0.05); width:100%;">
    <h4 style="margin-bottom:1rem; font-size:1rem; font-weight:700; color:#374151;">💡 이렇게 질문해보세요</h4>
    <div style="display:flex; gap:8px; flex-wrap:wrap;">
        <span style="background:#f3f4f6; padding:6px 12px; border-radius:20px; font-size:0.85rem; color:#4b5563;">최근 24시간 태풍상사 반응 요약해줘</span>
        <span style="background:#f3f4f6; padding:6px 12px; border-radius:20px; font-size:0.85rem; color:#4b5563;">https://youtu.be/xxxx 분석해줘</span>
        <span style="background:#f3f4f6; padding:6px 12px; border-radius:20px; font-size:0.85rem; color:#4b5563;">12월 한달간 프로보노 반응 분석해줘</span>
        <span style="background:#f3f4f6; padding:6px 12px; border-radius:20px; font-size:0.85rem; color:#4b5563;">(후속대화)"정경호"연기력에 대한 반응은 어때?</span>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

    _, col_toggle, _ = st.columns([1.3, 1, 1.3])
    with col_toggle:
        st.write("") 
        st.toggle(
            "🏢 자사 IP 모드", 
            key="own_ip_mode",
        )
        cur_toggle = bool(st.session_state.get("own_ip_mode", False))
        prev_toggle = st.session_state.get("own_ip_toggle_prev", None)
        
        # [수정] 자사 IP 모드 토글 시 상태 확인
        if cur_toggle and (prev_toggle is None or prev_toggle is False):
            with st.spinner("데이터 동기화 중..."):
                all_data = get_all_pgc_data()
                if all_data:
                    st.success(f"데이터 동기화 완료 ({len(all_data):,}개 영상)")
                else:
                    st.warning("데이터가 없거나 로드에 실패했습니다.")
        st.session_state["own_ip_toggle_prev"] = cur_toggle

else:
    render_metadata_and_downloads()
    render_chat()
    scroll_to_bottom()


if prompt := st.chat_input("질문을 입력하거나 영상 URL을 붙여넣으세요..."):
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