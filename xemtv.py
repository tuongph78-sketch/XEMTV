#!/usr/bin/env python3
"""Lọc, đối chiếu EPG & chuẩn hóa tvg-id, ráp logo & EPG, dọn tên, đổi link AceStream và sắp xếp kênh VN."""

import gzip
import re
import sys
import threading
import time
import unicodedata
import urllib.parse
import xml.etree.ElementTree as ET
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Final, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

SOURCES: Final[list[str]] = [
    "https://raw.githubusercontent.com/tuongph78-sketch/LINK-IPTV/refs/heads/main/output.m3u",
]

# Cấu hình tải kênh từ bit.ly/aceball111, đổi acestream:// sang http:// và ghép vào file đầu ra
ENABLE_MERGE_ACEBALL_PLAYLIST: Final[bool] = True
ACEBALL_PLAYLIST_URL: Final[str] = "https://bit.ly/aceball111"
ACE_HTTP_STREAM_BASE: Final[str] = "http://192.168.1.30:6878/ace/getstream"
CONVERT_ACESTREAM_TO_HTTP: Final[bool] = True
DEDUP_MERGED_ACEBALL_BY_URL: Final[bool] = True
BITLY_ALLOW_REDIRECTS: Final[bool] = True
BITLY_FETCH_TIMEOUT: Final[float] = 20.0

# Cấu hình EPG header (url-tvg)
DEFAULT_EPG_URLS: Final[tuple[str, ...]] = (
    "https://lichphatsong.io.vn/epg.xml",
)
USE_DEFAULT_EPG_HEADER: Final[bool] = True
MERGE_SOURCE_EPG_URLS: Final[bool] = False

# Cấu hình đối chiếu EPG XML để tự động gán tvg-id
MATCH_TVG_ID_FROM_EPG: Final[bool] = True
EPG_MATCH_SOURCES: Final[tuple[str, ...]] = (
    "https://lichphatsong.io.vn/epg.xml",
)
OVERWRITE_TVG_ID_FROM_EPG: Final[bool] = False
EPG_MIN_SIMILARITY: Final[float] = 0.86

# Bảng logo theo tvg-id gốc (không hậu tố hd)
TVG_ID_LOGO_MAP: Final[dict[str, str]] = {
    "vtv1": "https://i.postimg.cc/F1nZ7403/VTV1.png",
    "vtv2": "https://i.postimg.cc/t15dyQX4/VTV2.png",
    "vtv3": "https://i.postimg.cc/7bc3qNyC/VTV3.png",
    "vtv4": "https://i.postimg.cc/8jGLm7cp/VTV4.png",
    "vtv5": "https://i.postimg.cc/xJ8m2Xxt/VTV5.png",
    "vtv5tnb": "https://i.postimg.cc/CnffRWV7/VTV5-TNB.png",
    "vtv5tn": "https://i.postimg.cc/PPYwTsyr/VTV5-TN.png",
    "vtv6": "https://i.postimg.cc/tYQsrvj2/VTV6.png",
    "vtv7": "https://i.postimg.cc/xXgq10zj/VTV7.png",
    "vtv8": "https://i.postimg.cc/y3F84BKC/VTV8.png",
    "vtv9": "https://i.postimg.cc/SnQN5wtB/VTV9.png",
    "vtv10": "https://i.postimg.cc/tnH9zfxF/VTV10.png",
    "ono2tv": "https://i.imgur.com/nqwVkmM.png",
    "onbibi": "https://i.imgur.com/YFmPo7l.png",
    "oncine": "https://i.imgur.com/6f9ngwY.png",
    "onechannel": "https://i.imgur.com/87JEXEQ.png",
    "ongolf": "https://i.imgur.com/BGF2I58.png",
    "oninfotv": "https://i.imgur.com/jp57P0v.png",
    "onkids": "https://i.imgur.com/Y2SzhHG.png",
    "onlife": "https://i.imgur.com/2xT5HFx.png",
    "onmovies": "https://i.imgur.com/765Y3jw.png",
    "onmusic": "https://i.imgur.com/45BXPY3.png",
    "onstyle": "https://i.imgur.com/DgacoQ1.png",
    "ontrendingtv": "https://i.imgur.com/hHYzxmS.png",
    "onvfamily": "https://i.imgur.com/R82M53x.png",
    "onviedramas": "https://i.imgur.com/n7QICK7.png",
    "onviegiaitri": "https://i.imgur.com/7GSKm46.png",
    "htv1": "https://i.imgur.com/KEMSBD3.png",
    "htv2": "https://i.imgur.com/2npoF3Q.png",
    "htv3": "https://i.imgur.com/TCoIsVF.png",
    "htv4": "https://i.imgur.com/BcCA0x2.png",
    "htv5": "https://i.imgur.com/EI0mJRf.png",
    "htv7": "https://i.imgur.com/6GSN524.png",
    "htv9": "https://i.imgur.com/6GSN524.png",
    "htvcgiadinh": "https://i.imgur.com/Al6xJme.png",
    "htvcphim": "https://i.imgur.com/P0wqvmu.png",
    "htvcphunu": "https://i.imgur.com/FxzpvHI.png",
    "htvthethao": "https://i.imgur.com/ikaiET3.png",
    "htvcthuanviet": "https://i.imgur.com/KtMeo6q.png",
    "sctv2": "https://i.imgur.com/iMDjKF9.png",
    "sctv6": "https://i.imgur.com/tdozyTU.png",
    "hanoi1": "https://i.postimg.cc/hvL15x02/Hanoi-TV1.png",
    "hanoi2": "https://i.postimg.cc/Nft4rJgS/Hanoi-TV2.png",
    "caobang": "https://i.imgur.com/RUJmK5D.png",
    "laocai": "https://i.imgur.com/Vd0P0rx.png",
    "dienbien": "https://i.imgur.com/hMDRDxN.png",
    "thainguyen": "https://i.imgur.com/P3pbab8.png",
    "langson": "https://i.imgur.com/yxz02UD.png",
    "phutho": "https://i.imgur.com/LXuRwJY.png",
    "bacninh": "https://i.postimg.cc/BjpqCL1S/BNTV.png",
    "haiphong": "https://i.imgur.com/iq9JLb9.png",
    "hungyen": "https://i.imgur.com/fakTCLE.png",
    "sonla": "https://i.postimg.cc/3JnLC20C/Snews.png",
    "ninhbinh": "https://i.imgur.com/qHQTwJ9.png",
    "nghean": "https://i.postimg.cc/34pk2LsV/Logo-natv-2026.png",
    "quangtri": "https://i.imgur.com/6qOelwt.png",
    "danang1": "https://i.postimg.cc/8jwBQ42q/dnrt1.png",
    "danang2": "https://i.postimg.cc/870Rs5Pm/dnrt2.png",
    "quangngai1": "https://i.postimg.cc/pdnCmw7y/QNg-TV1.png",
    "daklak": "https://i.imgur.com/7CQqMeh.png",
    "khanhhoa1": "https://i.postimg.cc/14vcbJhC/KH1.png",
    "khanhhoa": "https://i.postimg.cc/2qn7JYr6/KH.png",
    "lamdong1": "https://i.postimg.cc/f35Jkqv6/LD1.png",
    "lamdong2": "https://i.postimg.cc/n99zRFfP/LD2.png",
    "tayninh1": "https://i.imgur.com/HFm3hbC.png",
    "dongnai1": "https://i.postimg.cc/HJN6GzyV/DN1.png",
    "dongnai2": "https://i.postimg.cc/Mf75Q8r8/DN2.png",
    "dongthap1": "https://i.imgur.com/TqAz7ET.png",
    "angiang1": "https://i.imgur.com/mgp6RAU.png",
    "angiang2": "https://i.imgur.com/efrauLr.png",
    "angiang3": "https://i.imgur.com/gZUV4z8.png",
    "camau": "https://i.imgur.com/ZKV1ok2.png",
}

APPLY_LOGO_BY_TVG_ID: Final[bool] = True
OVERWRITE_EXISTING_LOGO: Final[bool] = False

ALLOWED_GROUPS: Final[tuple[str, ...]] = ("VTV", "VTVcab", "HTV", "SCTV", "Địa Phương")
GROUP_RANK: Final[dict[str, int]] = {name: idx for idx, name in enumerate(ALLOWED_GROUPS)}

LOCAL_NORTH_TO_SOUTH: Final[tuple[tuple[str, ...], ...]] = (
    ("hà nội", "hanoi", "hbtv"),
    ("hà giang",),
    ("cao bằng",),
    ("lào cai",),
    ("lai châu",),
    ("điện biên",),
    ("yên bái",),
    ("tuyên quang",),
    ("bắc kạn",),
    ("thái nguyên",),
    ("lạng sơn",),
    ("phú thọ",),
    ("vĩnh phúc",),
    ("bắc giang",),
    ("bắc ninh",),
    ("quảng ninh",),
    ("hải phòng",),
    ("hải dương",),
    ("hưng yên",),
    ("hòa bình",),
    ("sơn la",),
    ("thái bình",),
    ("hà nam",),
    ("nam định",),
    ("ninh bình",),
    ("thanh hóa",),
    ("nghệ an",),
    ("hà tĩnh",),
    ("quảng bình",),
    ("quảng trị",),
    ("thừa thiên", "huế", "trt"),
    ("đà nẵng", "danang", "drt"),
    ("quảng nam", "qrt"),
    ("quảng ngãi", "ptq"),
    ("kon tum", "krt"),
    ("bình định", "bđtv", "bdtv"),
    ("phú yên", "ptp"),
    ("gia lai", "thgl"),
    ("đắk lắk", "daklak", "drt daklak", "ptđ"),
    ("đắk nông",),
    ("khánh hòa", "ktv"),
    ("ninh thuận",),
    ("bình thuận", "bttv"),
    ("lâm đồng", "ltv"),
    ("bình phước", "bptv"),
    ("tây ninh", "ttv11"),
    ("bình dương", "btv"),
    ("đồng nai", "đntv", "dntv"),
    ("bà rịa", "vũng tàu", "brt"),
    ("tp. hcm", "tp hcm", "tphcm", "sài gòn", "saigon"),
    ("long an", "la34"),
    ("tiền giang", "thtg"),
    ("bến tre", "thbt"),
    ("trà vinh", "thtv"),
    ("vĩnh long", "thvl"),
    ("đồng tháp", "thdt"),
    ("an giang", "atv"),
    ("kiên giang", "kgtv"),
    ("cần thơ", "cantho", "thtpct"),
    ("hậu giang", "hgtv"),
    ("sóc trăng", "sttv"),
    ("bạc liêu", "trt bl", "bltv"),
    ("cà mau", "ctv"),
)

SPECIAL_TVG_ID_MAP: Final[dict[str, str]] = {
    "vtv5taynambo": "vtv5tnb",
    "vtv5taynguyen": "vtv5tn",
    "htvcfamily": "htvcgiadinh",
    "htvcmovies": "htvcphim",
    "htvcwomen": "htvcphunu",
    "htvsports": "htvthethao",
    "onviedramas": "onviedramas",
    "onviegiaitri": "onviegiaitri",
    "onkids": "onkids",
    "onmusic": "onmusic",
    "oncine": "oncine",
    "ngheantv": "nghean",
    "quangngaitv": "quangngai1",
    "gialaitv": "gialai",
    "daklaktv": "daklak",
    "tayninthtv": "tayninh1",
    "dongnaitv1": "dongnai1",
    "dongthaptv": "dongthap1",
    "canthotv": "cantho1",
    "canthotv3": "cantho3",
    "danangtv2": "danang2",
}

CLEAN_GEO_PREFIX: Final[bool] = True
NORMALIZE_TVG_ID: Final[bool] = True
APPEND_HD_TO_VTV_TVG_ID: Final[bool] = True
APPEND_HD_TO_SCTV_TVG_ID: Final[bool] = True
SPECIAL_HD_TVG_IDS: Final[frozenset[str]] = frozenset({"htv7", "htv9"})
SORT_OUTPUT: Final[bool] = True
NORMALIZE_GROUP_TITLE: Final[bool] = True
ONLY_M3U8: Final[bool] = False
ADD_VLC_UA_FOR_M3U8: Final[bool] = True
VLC_USER_AGENT: Final[str] = "Mozilla/5.0"
KEEP_GEO_BLOCKED: Final[bool] = True
TAG_GEO_BLOCKED: Final[bool] = False
USE_RANGE_HEADER: Final[bool] = False
OUTPUT_FILE: Final[str] = "live-channels.m3u"
FETCH_TIMEOUT: Final[int] = 15
WORKERS: Final[int] = 32
STREAM_CONNECT_TIMEOUT: Final[float] = 3.0
STREAM_READ_TIMEOUT: Final[float] = 5.0
STREAM_RETRIES: Final[int] = 1
M3U8_MAX_PROBE_BYTES: Final[int] = 16384

HEADERS: Final[dict[str, str]] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

GEO_KEYWORDS: Final[tuple[str, ...]] = (
    "geo-blocked",
    "geo blocked",
    "geoblocked",
    "region locked",
    "not available in your country",
    "not available in your region",
    "restricted in your country",
    "country restriction",
)

# Pre-compiled Regex Patterns
EPG_NOISE_WORDS_RE: Final[re.Pattern[str]] = re.compile(
    r"\b(?:fhd|uhd|4k|hd|sd|1080p|720p|480p|1080i|576i|hevc|h264|h265|geo(?:\s*blocked)?)\b",
    flags=re.IGNORECASE,
)
BRACKETS_RE: Final[re.Pattern[str]] = re.compile(r"\[[^\]]*\]|\([^\)]*\)")
NON_ALNUM_RE: Final[re.Pattern[str]] = re.compile(r"[^a-z0-9]+")
WHITESPACE_RE: Final[re.Pattern[str]] = re.compile(r"\s+")
DIGITS_RE: Final[re.Pattern[str]] = re.compile(r"\d+")
ANY_NUM_RE: Final[re.Pattern[str]] = re.compile(r"(\d+)")

ACE_SCHEME_RE: Final[re.Pattern[str]] = re.compile(
    r"(?:acestream://|ace://)([a-zA-Z0-9]+)",
    flags=re.IGNORECASE,
)
TVG_ID_GET_RE: Final[re.Pattern[str]] = re.compile(r'tvg-id="([^"]*)"', flags=re.IGNORECASE)
TVG_ID_SET_RE: Final[re.Pattern[str]] = re.compile(r'(tvg-id=")([^"]*)(")', flags=re.IGNORECASE)
TVG_LOGO_GET_RE: Final[re.Pattern[str]] = re.compile(r'tvg-logo="([^"]*)"', flags=re.IGNORECASE)
TVG_LOGO_SET_RE: Final[re.Pattern[str]] = re.compile(r'tvg-logo="[^"]*"', flags=re.IGNORECASE)
TVG_COUNTRY_RE: Final[re.Pattern[str]] = re.compile(r'tvg-country="([^"]*)"', flags=re.IGNORECASE)
GROUP_TITLE_RE: Final[re.Pattern[str]] = re.compile(r'group-title="[^"]*"', flags=re.IGNORECASE)
HEADER_EPG_RE: Final[re.Pattern[str]] = re.compile(r'(?:url-tvg|x-tvg-url)="([^"]*)"', flags=re.IGNORECASE)
GEO_PREFIX_RE: Final[re.Pattern[str]] = re.compile(r"^\s*\[GEO\??\]\s*", flags=re.IGNORECASE)
GEO_TAG_RE: Final[re.Pattern[str]] = re.compile(r"\bgeo[-_\s]?block(ed)?\b", flags=re.IGNORECASE)
TVG_ID_DOMAIN_SPLIT_RE: Final[re.Pattern[str]] = re.compile(r"(?:\.vn@|@)", flags=re.IGNORECASE)
TVG_ID_SEP_RE: Final[re.Pattern[str]] = re.compile(r"[\s\-_]+")
QUALITY_SUFFIX_RE: Final[re.Pattern[str]] = re.compile(r"(?:fhd|hd|sd)$")

VTVCAB_RE: Final[re.Pattern[str]] = re.compile(
    r"(?:vtv\s*cab|vtvcab)(?:\s*\d+|\s*on)?|\bon\s+(?:sports?|football|cine|kids|music|golf|news|vie)",
    flags=re.IGNORECASE,
)
VTV_RE: Final[re.Pattern[str]] = re.compile(
    r"\bvtv(?:\s*\d+|\s*(?:cần thơ|cantho|tây nam bộ|tây nguyên|5|8|9))\b|\bvtv\b",
    flags=re.IGNORECASE,
)
HTV_RE: Final[re.Pattern[str]] = re.compile(
    r"\bhtvc?(?:\s*\d+|\s*phim|\s*ca nhạc|\s*du lịch|\s*gia đình|\s*phụ nữ|\s*thể thao|\s*thuần việt)?\b",
    flags=re.IGNORECASE,
)
SCTV_RE: Final[re.Pattern[str]] = re.compile(
    r"\bsctv(?:\s*\d+|\s*phim|\s*thể thao)?\b",
    flags=re.IGNORECASE,
)

LOCAL_RANK_PATTERNS: Final[tuple[tuple[int, re.Pattern[str]], ...]] = tuple(
    (
        idx,
        re.compile(
            rf"(?:^|[\s\-_/|,.])(?:{'|'.join(re.escape(a) for a in aliases)})(?:$|[\s\-_/|,.])",
            flags=re.IGNORECASE,
        ),
    )
    for idx, aliases in enumerate(LOCAL_NORTH_TO_SOUTH)
)

ALL_LOCAL_NAMES: Final[str] = "|".join(
    re.escape(alias) for group in LOCAL_NORTH_TO_SOUTH for alias in group
)
LOCAL_CHANNEL_RE: Final[re.Pattern[str]] = re.compile(
    rf"(?:^|[\s\-_/|,.])(?:{ALL_LOCAL_NAMES}|địa phương)(?:$|[\s\-_/|,.])",
    flags=re.IGNORECASE,
)

FOREIGN_RE: Final[re.Pattern[str]] = re.compile(
    r"\b(cnn|bbc|hbo|espn|nhk|kbs|mbc|sbs|cctv|cgtn|tvb|arirang|aljazeera|al jazeera|"
    r"fox|discovery|national geographic|nat geo|cartoon network|disney|nickelodeon|axn|"
    r"cinemax|mtv|bloomberg|cnbc|france 24|dw tv|tv5monde|bein\s*sports|sky sports|trt\s*world)\b"
    r"|[а-яА-ЯёЁ]",
    flags=re.IGNORECASE,
)

GROUP_NUM_PATTERNS: Final[dict[str, re.Pattern[str]]] = {
    "VTV": re.compile(r"\bvtv\s*(\d+)", flags=re.IGNORECASE),
    "VTVcab": re.compile(r"\b(?:vtv\s*cab|vtvcab)\s*(\d+)", flags=re.IGNORECASE),
    "HTV": re.compile(r"\bhtvc?\s*(\d+)", flags=re.IGNORECASE),
    "SCTV": re.compile(r"\bsctv\s*(\d+)", flags=re.IGNORECASE),
    "Địa Phương": re.compile(r"(\d+)", flags=re.IGNORECASE),
}

_thread_local = threading.local()
_all_sessions: list[requests.Session] = []
_sessions_lock = threading.Lock()


@dataclass(slots=True)
class StreamEntry:
    extinf: str
    url: str
    group: str
    order: int
    extra_tags: list[str] = field(default_factory=list)


@dataclass(slots=True)
class EpgChannel:
    channel_id: str
    display_names: list[str]


def create_configured_session(retries: int = 1, pool_maxsize: int = 32) -> requests.Session:
    sess = requests.Session()
    sess.headers.update(HEADERS)
    retry_cfg = Retry(
        total=retries,
        connect=retries,
        read=0,
        backoff_factor=0.3,
        status_forcelist=(429, 502, 503, 504),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(pool_connections=pool_maxsize, pool_maxsize=pool_maxsize, max_retries=retry_cfg)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    return sess


def get_thread_session() -> requests.Session:
    sess = getattr(_thread_local, "session", None)
    if sess is None:
        sess = create_configured_session(retries=0, pool_maxsize=10)
        _thread_local.session = sess
        with _sessions_lock:
            _all_sessions.append(sess)
    return sess


def close_all_thread_sessions() -> None:
    with _sessions_lock:
        while _all_sessions:
            sess = _all_sessions.pop()
            try:
                sess.close()
            except Exception:
                pass


def resolve_output_m3u_path() -> str:
    target = sys.argv[1].strip() if len(sys.argv) > 1 and sys.argv[1].strip() else OUTPUT_FILE
    if not target.lower().endswith(".m3u"):
        target = f"{target}.m3u"
    return target


def is_m3u8(url: str) -> bool:
    return urllib.parse.urlsplit(url.strip()).path.lower().endswith(".m3u8")


def strip_accents(text: str) -> str:
    text = text.replace("đ", "d").replace("Đ", "D")
    normalized = unicodedata.normalize("NFD", text)
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


def normalize_epg_key(name: str) -> str:
    s = strip_accents(name).lower()
    s = BRACKETS_RE.sub(" ", s)
    s = EPG_NOISE_WORDS_RE.sub(" ", s)
    s = NON_ALNUM_RE.sub(" ", s).strip()
    return WHITESPACE_RE.sub(" ", s)


def compact_epg_key(name: str) -> str:
    return normalize_epg_key(name).replace(" ", "")


def extract_digits_signature(text: str) -> tuple[str, ...]:
    return tuple(DIGITS_RE.findall(text))


def load_epg_xml_bytes(url: str, session: requests.Session) -> Optional[bytes]:
    try:
        resp = session.get(url, timeout=(5.0, max(FETCH_TIMEOUT, 20)))
        resp.raise_for_status()
        raw = resp.content
        if url.lower().endswith(".gz") or raw[:2] == b"\x1f\x8b":
            return gzip.decompress(raw)
        return raw
    except Exception as exc:
        print(f"⚠ Không tải được EPG XML '{url}': {exc}", file=sys.stderr)
        return None


def build_epg_lookup(
    session: requests.Session,
) -> tuple[dict[str, str], dict[tuple[str, ...], list[tuple[str, str]]]]:
    sources = EPG_MATCH_SOURCES if EPG_MATCH_SOURCES else DEFAULT_EPG_URLS
    channels_by_id: dict[str, EpgChannel] = {}

    with ThreadPoolExecutor(max_workers=min(4, max(1, len(sources)))) as pool:
        future_to_src = {pool.submit(load_epg_xml_bytes, src, session): src for src in sources}
        results: dict[str, Optional[bytes]] = {}
        for fut in as_completed(future_to_src):
            src = future_to_src[fut]
            results[src] = fut.result()

    for src in sources:
        xml_bytes = results.get(src)
        if not xml_bytes:
            continue
        try:
            root = ET.fromstring(xml_bytes)
        except ET.ParseError as exc:
            print(f"⚠ Lỗi cú pháp XML ở '{src}': {exc}", file=sys.stderr)
            continue

        for ch_elem in root.findall(".//channel"):
            ch_id = (ch_elem.attrib.get("id") or "").strip()
            if not ch_id:
                continue
            names = [
                (dn.text or "").strip()
                for dn in ch_elem.findall("display-name")
                if (dn.text or "").strip()
            ]
            if not names:
                names = [ch_id]

            if ch_id not in channels_by_id:
                channels_by_id[ch_id] = EpgChannel(channel_id=ch_id, display_names=names)
            else:
                seen_names = set(channels_by_id[ch_id].display_names)
                for n in names:
                    if n not in seen_names:
                        channels_by_id[ch_id].display_names.append(n)
                        seen_names.add(n)

    exact_map: dict[str, str] = {}
    fuzzy_buckets: dict[tuple[str, ...], list[tuple[str, str]]] = {}

    for ch in channels_by_id.values():
        for raw_name in (ch.channel_id, *ch.display_names):
            norm = normalize_epg_key(raw_name)
            comp = norm.replace(" ", "")
            digits = extract_digits_signature(norm)
            if norm:
                fuzzy_buckets.setdefault(digits, []).append((norm, ch.channel_id))
            if comp and comp not in exact_map:
                exact_map[comp] = ch.channel_id

    if channels_by_id:
        print(f"📺 Đã nạp {len(channels_by_id)} kênh từ EPG XML để đối chiếu tvg-id")

    return exact_map, fuzzy_buckets


def find_epg_tvg_id(
    channel_name: str,
    current_tvg_id: str,
    exact_map: dict[str, str],
    fuzzy_buckets: dict[tuple[str, ...], list[tuple[str, str]]],
) -> Optional[str]:
    for candidate in (current_tvg_id, channel_name):
        if not candidate:
            continue
        comp = compact_epg_key(candidate)
        if comp and comp in exact_map:
            return exact_map[comp]

    norm_target = normalize_epg_key(channel_name)
    if not norm_target:
        return None

    target_digits = extract_digits_signature(norm_target)
    candidates = fuzzy_buckets.get(target_digits, [])
    if not candidates:
        return None

    best_score = 0.0
    best_id: Optional[str] = None
    target_len = len(norm_target)
    max_len_diff = max(4, target_len // 2)
    matcher = SequenceMatcher(None, norm_target, "")

    for cand_norm, cand_id in candidates:
        if abs(len(cand_norm) - target_len) > max_len_diff:
            continue
        matcher.set_seq2(cand_norm)
        if matcher.quick_ratio() < EPG_MIN_SIMILARITY:
            continue
        score = matcher.ratio()
        if score > best_score:
            best_score = score
            best_id = cand_id

    if best_id and best_score >= EPG_MIN_SIMILARITY:
        return best_id
    return None


def get_tvg_id(extinf: str) -> str:
    m = TVG_ID_GET_RE.search(extinf)
    return (m.group(1) or "").strip() if m else ""


def set_tvg_id(extinf: str, new_tvg_id: str) -> str:
    m = TVG_ID_SET_RE.search(extinf)
    if m:
        return f"{extinf[:m.start(2)]}{new_tvg_id}{extinf[m.end(2):]}"
    if "," in extinf:
        prefix, name = extinf.rsplit(",", 1)
        return f'{prefix} tvg-id="{new_tvg_id}",{name}'
    return f'{extinf} tvg-id="{new_tvg_id}"'


def extract_epg_urls(m3u_text: str) -> list[str]:
    urls: list[str] = []
    for line in m3u_text.splitlines():
        s = line.strip()
        if s.startswith("#EXTM3U"):
            for m in HEADER_EPG_RE.finditer(s):
                for part in m.group(1).split(","):
                    u = part.strip()
                    if u and u not in urls:
                        urls.append(u)
    return urls


def build_epg_header(source_epg_urls: list[str]) -> str:
    combined: list[str] = []
    if USE_DEFAULT_EPG_HEADER:
        for u in DEFAULT_EPG_URLS:
            if u not in combined:
                combined.append(u)
    if MERGE_SOURCE_EPG_URLS or not combined:
        for u in source_epg_urls:
            if u not in combined:
                combined.append(u)
    if not combined:
        return "#EXTM3U"
    return f'#EXTM3U url-tvg="{",".join(combined)}"'


def clean_display_name(raw_name: str) -> str:
    name = raw_name.strip()
    if CLEAN_GEO_PREFIX:
        name = GEO_PREFIX_RE.sub("", name).strip()
    return name


def extract_channel_name(extinf: str) -> str:
    raw = extinf.rsplit(",", 1)[-1].strip() if "," in extinf else extinf.strip()
    return clean_display_name(raw)


def apply_clean_display_name(extinf: str) -> str:
    if not CLEAN_GEO_PREFIX or "," not in extinf:
        return extinf
    prefix, raw_name = extinf.rsplit(",", 1)
    return f"{prefix},{clean_display_name(raw_name)}"


def normalize_tvg_id_value(raw_id: str, group: str = "") -> str:
    cleaned = raw_id.strip()
    if not cleaned:
        return ""

    base = TVG_ID_DOMAIN_SPLIT_RE.split(cleaned, maxsplit=1)[0]
    compact = TVG_ID_SEP_RE.sub("", base).lower()
    if compact in SPECIAL_TVG_ID_MAP:
        compact = SPECIAL_TVG_ID_MAP[compact]

    compact = QUALITY_SUFFIX_RE.sub("", compact)

    if not compact:
        return ""

    if APPEND_HD_TO_VTV_TVG_ID and (
        group == "VTV" or (compact.startswith("vtv") and not compact.startswith("vtvcab"))
    ):
        if not compact.endswith("hd"):
            compact = f"{compact}hd"

    if compact in SPECIAL_HD_TVG_IDS and not compact.endswith("hd"):
        compact = f"{compact}hd"

    if APPEND_HD_TO_SCTV_TVG_ID and (group == "SCTV" or compact.startswith("sctv")):
        if not compact.endswith("hd"):
            compact = f"{compact}hd"

    return compact


def apply_normalized_tvg_id(extinf: str, group: str = "") -> str:
    if not NORMALIZE_TVG_ID:
        return extinf

    m = TVG_ID_SET_RE.search(extinf)
    if not m:
        return extinf

    new_id = normalize_tvg_id_value(m.group(2), group=group)
    return f"{extinf[:m.start(2)]}{new_id}{extinf[m.end(2):]}"


def apply_epg_matched_tvg_id(
    extinf: str,
    exact_map: dict[str, str],
    fuzzy_buckets: dict[tuple[str, ...], list[tuple[str, str]]],
) -> tuple[str, bool]:
    if not MATCH_TVG_ID_FROM_EPG or not exact_map:
        return extinf, False

    current_id = get_tvg_id(extinf)
    if current_id and not OVERWRITE_TVG_ID_FROM_EPG:
        return extinf, False

    ch_name = extract_channel_name(extinf)
    matched_id = find_epg_tvg_id(ch_name, current_id, exact_map, fuzzy_buckets)
    if matched_id and matched_id != current_id:
        return set_tvg_id(extinf, matched_id), True
    return extinf, False


def apply_logo_by_tvg_id(extinf: str) -> str:
    if not APPLY_LOGO_BY_TVG_ID:
        return extinf

    tvg_id = get_tvg_id(extinf).lower()
    base_tvg_id = QUALITY_SUFFIX_RE.sub("", tvg_id)
    mapped_logo = TVG_ID_LOGO_MAP.get(tvg_id) or TVG_ID_LOGO_MAP.get(base_tvg_id)
    if not mapped_logo:
        fallback_key = QUALITY_SUFFIX_RE.sub(
            "", normalize_tvg_id_value(extract_channel_name(extinf))
        )
        mapped_logo = TVG_ID_LOGO_MAP.get(fallback_key)
    if not mapped_logo:
        return extinf

    existing_logo = TVG_LOGO_GET_RE.search(extinf)
    if existing_logo and existing_logo.group(1).strip() and not OVERWRITE_EXISTING_LOGO:
        return extinf

    if existing_logo:
        return TVG_LOGO_SET_RE.sub(f'tvg-logo="{mapped_logo}"', extinf, count=1)

    if "," in extinf:
        prefix, name = extinf.rsplit(",", 1)
        return f'{prefix} tvg-logo="{mapped_logo}",{name}'
    return f'{extinf} tvg-logo="{mapped_logo}"'


def local_geo_rank(channel_name: str, extinf: str) -> int:
    haystack = f" {channel_name.lower()} {extinf.lower()} "
    for idx, pattern in LOCAL_RANK_PATTERNS:
        if pattern.search(haystack):
            return idx
    return len(LOCAL_NORTH_TO_SOUTH)


def detect_allowed_group(extinf: str) -> Optional[str]:
    country_match = TVG_COUNTRY_RE.search(extinf)
    if country_match:
        code = country_match.group(1).strip().upper()
        if code and code not in {"VN", "VI", "VNM"}:
            return None

    channel_name = extract_channel_name(extinf)

    if FOREIGN_RE.search(channel_name) and not (
        VTVCAB_RE.search(channel_name)
        or VTV_RE.search(channel_name)
        or HTV_RE.search(channel_name)
        or SCTV_RE.search(channel_name)
    ):
        return None

    if VTVCAB_RE.search(channel_name) or VTVCAB_RE.search(extinf):
        return "VTVcab"
    if VTV_RE.search(channel_name) or VTV_RE.search(extinf):
        return "VTV"
    if HTV_RE.search(channel_name) or HTV_RE.search(extinf):
        return "HTV"
    if SCTV_RE.search(channel_name) or SCTV_RE.search(extinf):
        return "SCTV"
    if LOCAL_CHANNEL_RE.search(f" {channel_name} ") or LOCAL_CHANNEL_RE.search(f" {extinf} "):
        return "Địa Phương"

    return None


def set_group_title(extinf: str, group_name: str) -> str:
    if not NORMALIZE_GROUP_TITLE:
        return extinf
    if GROUP_TITLE_RE.search(extinf):
        return GROUP_TITLE_RE.sub(f'group-title="{group_name}"', extinf, count=1)
    if "," in extinf:
        prefix, name = extinf.rsplit(",", 1)
        return f'{prefix} group-title="{group_name}",{name}'
    return f'{extinf} group-title="{group_name}"'


def channel_sort_key(entry: StreamEntry) -> tuple[int, int, int, int, str, int]:
    group_idx = GROUP_RANK.get(entry.group, len(ALLOWED_GROUPS))
    name = extract_channel_name(entry.extinf)

    geo_idx = local_geo_rank(name, entry.extinf) if entry.group == "Địa Phương" else 0

    num: Optional[int] = None
    group_pat = GROUP_NUM_PATTERNS.get(entry.group)
    if group_pat:
        m = group_pat.search(name)
        if m:
            num = int(m.group(1))
    if num is None:
        m_any = ANY_NUM_RE.search(name)
        if m_any:
            num = int(m_any.group(1))

    has_num = 0 if num is not None else 1
    num_val = num if num is not None else 10**9
    return (group_idx, geo_idx, has_num, num_val, name.lower(), entry.order)


def sort_entries(entries: list[StreamEntry]) -> list[StreamEntry]:
    if not SORT_OUTPUT:
        return entries
    return sorted(entries, key=channel_sort_key)


def fetch_text(url: str, session: requests.Session) -> Optional[str]:
    try:
        resp = session.get(url, timeout=(5.0, FETCH_TIMEOUT))
        resp.raise_for_status()
        return resp.text
    except Exception as exc:
        print(f"⚠ Bỏ qua nguồn {url}: {exc}", file=sys.stderr)
        return None


def parse_entries(
    m3u_text: str,
    start_order: int,
    epg_exact_map: dict[str, str],
    epg_fuzzy_buckets: dict[tuple[str, ...], list[tuple[str, str]]],
) -> tuple[list[StreamEntry], int, int]:
    entries: list[StreamEntry] = []
    skipped_group = 0
    epg_matched_count = 0
    extinf: Optional[str] = None
    extra_tags: list[str] = []
    next_order = start_order

    for line in m3u_text.splitlines():
        s = line.strip()
        if not s:
            continue
        if s.startswith("#EXTINF"):
            extinf = s
            extra_tags = []
            continue
        if s.startswith("#"):
            if extinf and s.startswith(("#EXTVLCOPT", "#KODIPROP")):
                extra_tags.append(s)
            continue
        if not extinf:
            continue

        matched_group = detect_allowed_group(extinf)
        if matched_group not in ALLOWED_GROUPS:
            skipped_group += 1
        elif not ONLY_M3U8 or is_m3u8(s):
            formatted_extinf = set_group_title(extinf, matched_group)
            formatted_extinf, was_matched = apply_epg_matched_tvg_id(
                formatted_extinf, epg_exact_map, epg_fuzzy_buckets
            )
            formatted_extinf = apply_normalized_tvg_id(formatted_extinf, group=matched_group)
            if was_matched:
                epg_matched_count += 1
            formatted_extinf = apply_logo_by_tvg_id(formatted_extinf)
            formatted_extinf = apply_clean_display_name(formatted_extinf)

            final_extra_tags = list(extra_tags)
            if ADD_VLC_UA_FOR_M3U8 and is_m3u8(s):
                ua_line = f"#EXTVLCOPT:http-user-agent={VLC_USER_AGENT}"
                has_ua = any(
                    t.lower().startswith("#extvlcopt:http-user-agent=")
                    for t in final_extra_tags
                )
                if not has_ua:
                    final_extra_tags.append(ua_line)

            entries.append(
                StreamEntry(
                    extinf=formatted_extinf,
                    url=s,
                    group=matched_group,
                    order=next_order,
                    extra_tags=final_extra_tags,
                )
            )
            next_order += 1

        extinf = None
        extra_tags = []

    return entries, skipped_group, epg_matched_count


def looks_like_geo_block(status_code: int, body_text: str, extinf: str) -> bool:
    if status_code in (403, 451):
        return True
    lower_body = body_text.lower()
    if any(kw in lower_body for kw in GEO_KEYWORDS):
        return True
    if GEO_TAG_RE.search(extinf):
        return True
    return False


def _extract_custom_headers(extra_tags: list[str]) -> dict[str, str]:
    headers: dict[str, str] = {}
    for tag in extra_tags:
        lower = tag.lower()
        if lower.startswith("#extvlcopt:http-user-agent="):
            headers["User-Agent"] = tag.split("=", 1)[1].strip()
        elif lower.startswith("#extvlcopt:http-referrer="):
            headers["Referer"] = tag.split("=", 1)[1].strip()
    return headers


def _probe_single_attempt(entry: StreamEntry, clean_url: str) -> str:
    session = get_thread_session()
    req_headers: dict[str, str] = _extract_custom_headers(entry.extra_tags)
    if USE_RANGE_HEADER:
        req_headers["Range"] = "bytes=0-4096"

    resp: Optional[requests.Response] = None
    try:
        resp = session.get(
            clean_url,
            headers=req_headers if req_headers else None,
            timeout=(STREAM_CONNECT_TIMEOUT, STREAM_READ_TIMEOUT),
            stream=True,
            allow_redirects=True,
        )

        chunks: list[bytes] = []
        total_read = 0
        for part in resp.iter_content(chunk_size=4096):
            if not part:
                continue
            chunks.append(part)
            total_read += len(part)
            if total_read >= 4096:
                preview_so_far = b"".join(chunks).decode("utf-8", errors="ignore").lower()
                if any(k in preview_so_far for k in ("#extm3u", "#ext-x-", "#extinf", "<!doctype", "<html")):
                    break
            if total_read >= M3U8_MAX_PROBE_BYTES:
                break

        raw_preview = b"".join(chunks)
        text_preview = raw_preview.decode("utf-8", errors="ignore")
        lower_preview = text_preview.lstrip().lower()

        if KEEP_GEO_BLOCKED and looks_like_geo_block(resp.status_code, text_preview, entry.extinf):
            return "GEO"

        if not (200 <= resp.status_code < 300) or not raw_preview:
            return "DEAD"

        if lower_preview.startswith(("<!doctype html", "<html")):
            return "GEO" if (KEEP_GEO_BLOCKED and any(k in lower_preview for k in GEO_KEYWORDS)) else "DEAD"

        if (is_m3u8(clean_url) or is_m3u8(resp.url)) and not any(
            k in lower_preview for k in ("#extm3u", "#ext-x-", "#extinf")
        ):
            return "DEAD"

        return "LIVE"
    except Exception:
        return "ERR"
    finally:
        if resp is not None:
            resp.close()


def check_stream(entry: StreamEntry) -> str:
    clean = entry.url.strip()
    if not clean.startswith(("http://", "https://")):
        return "DEAD"

    for attempt in range(STREAM_RETRIES + 1):
        status = _probe_single_attempt(entry, clean)
        if status in ("LIVE", "GEO"):
            return status
        if status == "DEAD":
            return "DEAD"
        if attempt < STREAM_RETRIES:
            time.sleep(0.25)

    if KEEP_GEO_BLOCKED and GEO_TAG_RE.search(entry.extinf):
        return "GEO"
    return "DEAD"


def format_extinf(extinf: str, is_geo: bool) -> str:
    cleaned = apply_clean_display_name(extinf)
    if not (is_geo and TAG_GEO_BLOCKED):
        return cleaned
    if "[GEO?]" in cleaned:
        return cleaned
    if "," in cleaned:
        prefix, name = cleaned.rsplit(",", 1)
        return f"{prefix},[GEO?] {name.strip()}"
    return f"{cleaned} [GEO?]"


def filter_streams(entries: list[StreamEntry]) -> tuple[list[StreamEntry], int, int, int]:
    status_map: dict[int, str] = {}
    total = len(entries)
    done = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(check_stream, e): idx for idx, e in enumerate(entries)}
        for fut in as_completed(futures):
            idx = futures[fut]
            done += 1
            try:
                status_map[idx] = fut.result()
            except Exception:
                status_map[idx] = "DEAD"

            if done % 50 == 0 or done == total:
                live_cnt = sum(1 for v in status_map.values() if v == "LIVE")
                geo_cnt = sum(1 for v in status_map.values() if v == "GEO")
                print(f"Đã kiểm tra {done}/{total} | Live: {live_cnt} | Geo?: {geo_cnt}")

    kept: list[StreamEntry] = []
    live_total = 0
    geo_total = 0
    dead_total = 0

    for i, e in enumerate(entries):
        st = status_map.get(i, "DEAD")
        if st == "LIVE":
            kept.append(e)
            live_total += 1
        elif st == "GEO" and KEEP_GEO_BLOCKED:
            e.extinf = format_extinf(e.extinf, is_geo=True)
            kept.append(e)
            geo_total += 1
        else:
            dead_total += 1

    return kept, live_total, geo_total, dead_total


def fetch_bitly_playlist_text(url: str, session: requests.Session) -> Optional[str]:
    clean_url = url.strip()
    if not clean_url:
        return None

    bitly_headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "*/*",
        "Cache-Control": "no-cache",
    }

    try:
        resp = session.get(
            clean_url,
            headers=bitly_headers,
            timeout=(6.0, max(BITLY_FETCH_TIMEOUT, float(FETCH_TIMEOUT))),
            allow_redirects=BITLY_ALLOW_REDIRECTS,
        )
        resp.raise_for_status()

        if not resp.encoding:
            resp.encoding = resp.apparent_encoding or "utf-8"
        text = resp.text or ""
        stripped = text.lstrip()

        if not stripped:
            print(f"⚠ Link bit.ly '{clean_url}' trả về nội dung rỗng.", file=sys.stderr)
            return None

        lower_head = stripped[:512].lower()
        if "#extm3u" not in lower_head and "#extinf" not in lower_head and lower_head.startswith(("<!doctype html", "<html")):
            print(
                f"⚠ Link bit.ly '{clean_url}' (đích: {resp.url}) trả về trang HTML thay vì playlist M3U.",
                file=sys.stderr,
            )
            return None

        if resp.url and resp.url != clean_url:
            print(f"🔗 bit.ly '{clean_url}' đã chuyển hướng tới: {resp.url}")
        return text
    except Exception as exc:
        print(f"⚠ Lỗi khi tải nguồn bit.ly '{clean_url}': {exc}", file=sys.stderr)
        return None


def convert_acestream_links(m3u_text: str) -> tuple[str, int]:
    converted = 0

    def _repl(match: re.Match[str]) -> str:
        nonlocal converted
        stream_id = match.group(1)
        converted += 1
        return f"{ACE_HTTP_STREAM_BASE}?id={stream_id}"

    return ACE_SCHEME_RE.sub(_repl, m3u_text), converted


def build_aceball_merged_lines(
    session: requests.Session,
    existing_urls: set[str],
) -> tuple[list[str], int]:
    if not ENABLE_MERGE_ACEBALL_PLAYLIST:
        return [], 0

    raw_text = fetch_bitly_playlist_text(ACEBALL_PLAYLIST_URL, session)
    if not raw_text or not raw_text.strip():
        print(f"⚠ Không tải được danh sách kênh từ {ACEBALL_PLAYLIST_URL}", file=sys.stderr)
        return [], 0

    if CONVERT_ACESTREAM_TO_HTTP:
        playlist_text, converted_count = convert_acestream_links(raw_text)
        print(
            f"🔄 Đã đổi {converted_count} link acestream:// sang {ACE_HTTP_STREAM_BASE}?id=..."
        )
    else:
        playlist_text = raw_text

    merged_lines: list[str] = []
    added_count = 0
    skipped_dups = 0
    current_extinf: Optional[str] = None
    current_tags: list[str] = []

    for line in playlist_text.splitlines():
        s = line.strip()
        if not s:
            continue
        if s.startswith("#EXTM3U"):
            continue
        if s.startswith("#EXTINF"):
            current_extinf = s
            current_tags = []
            continue
        if s.startswith("#"):
            if current_extinf is not None:
                current_tags.append(s)
            continue

        if DEDUP_MERGED_ACEBALL_BY_URL and s in existing_urls:
            skipped_dups += 1
            current_extinf = None
            current_tags = []
            continue

        existing_urls.add(s)
        if current_extinf is not None:
            merged_lines.append(current_extinf)
            merged_lines.extend(current_tags)
        merged_lines.append(s)
        added_count += 1
        current_extinf = None
        current_tags = []

    print(
        f"➕ Đã tải {ACEBALL_PLAYLIST_URL} để ghép nguyên bản {added_count} kênh "
        f"(bỏ trùng: {skipped_dups})"
    )
    return merged_lines, added_count


def write_m3u(
    entries: list[StreamEntry],
    path: str,
    epg_header: str,
    merged_lines: Optional[list[str]] = None,
) -> None:
    lines: list[str] = [epg_header]
    for item in entries:
        lines.append(item.extinf)
        lines.extend(item.extra_tags)
        lines.append(item.url)
    if merged_lines:
        lines.extend(merged_lines)

    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(lines) + "\n")


def main() -> None:
    start_time = time.perf_counter()
    output_path = resolve_output_m3u_path()
    collected: list[StreamEntry] = []
    seen_urls: set[str] = set()
    source_epg_urls: list[str] = []
    total_skipped_group = 0
    total_epg_matched = 0
    aceball_lines: list[str] = []
    aceball_added = 0

    try:
        with create_configured_session(retries=2, pool_maxsize=16) as session:

            epg_exact_map, epg_fuzzy_buckets = (
                build_epg_lookup(session) if MATCH_TVG_ID_FROM_EPG else ({}, {})
            )

            with ThreadPoolExecutor(max_workers=min(6, max(1, len(SOURCES)))) as pool:
                future_to_src = {pool.submit(fetch_text, src, session): src for src in SOURCES}
                fetched_sources: dict[str, Optional[str]] = {}
                for fut in as_completed(future_to_src):
                    src = future_to_src[fut]
                    fetched_sources[src] = fut.result()

            for src in SOURCES:
                text = fetched_sources.get(src)
                if not text or "#EXTM3U" not in text:
                    continue
                for ep_url in extract_epg_urls(text):
                    if ep_url not in source_epg_urls:
                        source_epg_urls.append(ep_url)
                parsed, skipped_cnt, matched_cnt = parse_entries(
                    text,
                    start_order=len(collected),
                    epg_exact_map=epg_exact_map,
                    epg_fuzzy_buckets=epg_fuzzy_buckets,
                )
                total_skipped_group += skipped_cnt
                total_epg_matched += matched_cnt
                for item in parsed:
                    norm_url = item.url.strip()
                    if norm_url not in seen_urls:
                        seen_urls.add(norm_url)
                        item.url = norm_url
                        collected.append(item)

            if total_skipped_group:
                print(f"🗂 Đã loại {total_skipped_group} kênh ngoài nhóm VTV / VTVcab / HTV / SCTV / Địa Phương")
            if total_epg_matched:
                print(f"🔗 Đã đối chiếu EPG và gán/cập nhật tvg-id cho {total_epg_matched} kênh")

            if not collected:
                print("❌ Không có kênh nào thuộc nhóm được chọn để kiểm tra.", file=sys.stderr)
                sys.exit(1)

            print(f"Tổng kênh cần kiểm tra: {len(collected)}")
            kept_entries, live_cnt, geo_cnt, dead_cnt = filter_streams(collected)
            if not kept_entries:
                print("❌ Không có kênh nào hợp lệ để ghi ra file.", file=sys.stderr)
                sys.exit(1)

            kept_urls = {e.url.strip() for e in kept_entries}
            aceball_lines, aceball_added = build_aceball_merged_lines(session, kept_urls)
    finally:
        close_all_thread_sessions()

    sorted_entries = sort_entries(kept_entries)
    epg_header = build_epg_header(source_epg_urls)
    write_m3u(sorted_entries, output_path, epg_header, merged_lines=aceball_lines)

    elapsed = time.perf_counter() - start_time
    group_counts = Counter(item.group for item in sorted_entries)
    group_summary = ", ".join(f"{g}: {group_counts.get(g, 0)}" for g in ALLOWED_GROUPS)
    total_written = len(sorted_entries) + aceball_added
    print(
        f"✅ Xong trong {elapsed:.1f}s → {output_path} | Tổng ghi: {total_written} "
        f"(Chính: {len(sorted_entries)}, Ghép aceball111: {aceball_added}) "
        f"| Live: {live_cnt} | Giữ geo-blocked: {geo_cnt} | Loại (dead): {dead_cnt}"
    )
    print(f"📊 Theo nhóm: {group_summary}")


if __name__ == "__main__":
    main()
