# -*- coding: utf-8 -*-
"""
collector.py — 유튜브 트렌드 스캐너 수집기
GitHub Actions에서 3시간마다 실행됨. 로컬 실행도 가능: python collector.py

사이클:
1. channels.txt 의 핸들 → 채널ID 변환 (캐시: docs/data/channels.json)
2. 채널별 RSS 로 신규 영상 발견 → videos.json 에 ID가 없을 때만 등록
3. YouTube Data API 로 추적 대상(2주 이내) 영상 통계 일괄 갱신 (50개 묶음)
   - API 키 없으면 RSS 조회수로 대체 (기능 저하 모드)
4. snapshots/YYYY-MM-DD.jsonl 에 이력 1줄씩 적재
5. 전일 +24h / 최근 +24h 증가분 계산해 videos.json 에 저장
6. 30일 경과 영상 제거 — 조회수 100만+ 쇼츠만 archive_hits.csv 에 기록
"""
import html
import json
import os
import re
import time
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timedelta, timezone

# ── 설정 ──────────────────────────────────────────────
TRACK_DAYS = 14      # 추적(통계 갱신) 기간 — 30까지 확장 가능
RETAIN_DAYS = 30     # 보관 기간 (지나면 제거, 체크리스트 5)
ARCHIVE_MIN_VIEWS = 1_000_000   # 아카이브 기록 기준 (쇼츠만)
REQUEST_DELAY = 0.3  # RSS 요청 간 딜레이(초)
KST = timezone(timedelta(hours=9))

BASE = os.path.dirname(os.path.abspath(__file__))
CHANNELS_TXT = os.path.join(BASE, "channels.txt")
DATA_DIR = os.path.join(BASE, "docs", "data")
CHANNELS_JSON = os.path.join(DATA_DIR, "channels.json")
VIDEOS_JSON = os.path.join(DATA_DIR, "videos.json")
SNAP_DIR = os.path.join(DATA_DIR, "snapshots")
ARCHIVE_CSV = os.path.join(DATA_DIR, "archive_hits.csv")

API_KEY = os.environ.get("YT_API_KEY", "").strip()
GEMINI_KEY = os.environ.get("GEMINI_API_KEY", "").strip()
GEMINI_MODEL = "gemini-2.5-flash"
MAX_SUMMARIES_PER_RUN = 25   # 무료 일일 한도 보호 (25 × 8회/일 = 최대 200회)
SUMMARIES_JSON = os.path.join(DATA_DIR, "summaries.json")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9",
}


def fetch(url, timeout=30):
    req = urllib.request.Request(url, headers=HEADERS)
    return urllib.request.urlopen(req, timeout=timeout).read().decode("utf-8", "ignore")


def load_json(path, default):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return default


def save_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))


# ── 1. 채널 핸들 → 채널ID ─────────────────────────────
def resolve_channel_id(handle, cache):
    if handle.startswith("UC"):
        return handle
    if handle in cache and cache[handle].get("id"):
        return cache[handle]["id"]
    cid, name = None, handle
    if API_KEY:  # API 방식 (안정적)
        try:
            q = urllib.parse.quote(handle)
            data = json.loads(fetch(
                "https://www.googleapis.com/youtube/v3/channels"
                f"?part=snippet&forHandle={q}&key={API_KEY}"))
            items = data.get("items", [])
            if items:
                cid = items[0]["id"]
                name = items[0]["snippet"]["title"]
        except Exception as e:
            print(f"  [resolve-api 실패] {handle}: {e}")
    if not cid:  # 채널 페이지 스크레이핑 (폴백)
        try:
            html = fetch("https://www.youtube.com/" + urllib.parse.quote(handle))
            m = re.search(r'"externalId":"(UC[^"]+)"', html)
            if m:
                cid = m.group(1)
                t = re.search(r'"title":"([^"]+)"', html)
                if t:
                    name = t.group(1)
        except Exception as e:
            print(f"  [resolve-scrape 실패] {handle}: {e}")
    if cid:
        cache[handle] = {"id": cid, "name": name,
                         "registered": datetime.now(KST).isoformat(timespec="seconds")}
    return cid


# ── 2. RSS 신규 영상 발견 ─────────────────────────────
def rss_entries(channel_id):
    rss = fetch(f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}")
    ch_m = re.search(r"<title>([^<]+)</title>", rss)
    ch_name = html.unescape(ch_m.group(1)) if ch_m else channel_id
    out = []
    for e in re.findall(r"<entry>.*?</entry>", rss, re.S):
        try:
            vid = re.search(r"<yt:videoId>([^<]+)</yt:videoId>", e).group(1)
            title = html.unescape(re.search(r"<title>([^<]*)</title>", e).group(1))
            link = re.search(r'<link rel="alternate" href="([^"]+)"', e).group(1)
            pub = re.search(r"<published>([^<]+)</published>", e).group(1)
            v = re.search(r'views="(\d+)"', e)
            views = int(v.group(1)) if v else 0
            lk = re.search(r'count="(\d+)"', e)
            likes = int(lk.group(1)) if lk else 0
            out.append({"id": vid, "title": title, "url": link, "pub": pub,
                        "views": views, "likes": likes,
                        "shorts": "/shorts/" in link})
        except Exception:
            continue
    return ch_name, out


# ── 3. API 통계 일괄 갱신 ─────────────────────────────
def parse_duration(iso):  # PT2M13S → 초
    if not iso:
        return None
    m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", iso)
    if not m:
        return None
    h, mi, s = (int(x) if x else 0 for x in m.groups())
    return h * 3600 + mi * 60 + s


def api_stats(video_ids):
    stats = {}
    for i in range(0, len(video_ids), 50):
        batch = ",".join(video_ids[i:i + 50])
        try:
            data = json.loads(fetch(
                "https://www.googleapis.com/youtube/v3/videos"
                f"?part=statistics,contentDetails&id={batch}&key={API_KEY}"))
            for item in data.get("items", []):
                st = item.get("statistics", {})
                stats[item["id"]] = {
                    "v": int(st.get("viewCount", 0)),
                    "lk": int(st.get("likeCount", 0)) if "likeCount" in st else None,
                    "cm": int(st.get("commentCount", 0)) if "commentCount" in st else None,
                    "dur": parse_duration(item.get("contentDetails", {}).get("duration")),
                }
        except Exception as e:
            print(f"  [api_stats 실패] batch {i//50}: {e}")
        time.sleep(0.2)
    return stats


# ── 4~5. 스냅샷 & 증가분 ─────────────────────────────
def load_recent_snapshots(now):
    """최근 49시간을 덮는 날짜 파일들 로드 → {vid: [(ts, views), ...]}"""
    hist = {}
    for d in range(3):
        day = (now - timedelta(days=d)).astimezone(KST).strftime("%Y-%m-%d")
        p = os.path.join(SNAP_DIR, day + ".jsonl")
        if not os.path.exists(p):
            continue
        with open(p, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    r = json.loads(line)
                    hist.setdefault(r["id"], []).append(
                        (datetime.fromisoformat(r["t"]), r["v"]))
                except Exception:
                    continue
    for vid in hist:
        hist[vid].sort()
    return hist


def views_at(hist_list, target, tol_hours=2.0):
    """target 시각에 가장 가까운 스냅샷 조회수 (허용오차 내). 없으면 None"""
    best, best_gap = None, tol_hours * 3600
    for ts, v in hist_list:
        gap = abs((ts - target).total_seconds())
        if gap <= best_gap:
            best, best_gap = v, gap
    return best


# ── 제미나이 요약 ─────────────────────────────────────
def gemini_summarize(video_url):
    """유튜브 URL을 제미나이에 보내 한국어 2~3문장 요약. 실패=None, 일일한도='QUOTA'"""
    body = json.dumps({
        "contents": [{"parts": [
            {"file_data": {"file_uri": video_url}},
            {"text": "이 영상의 내용을 한국어 2~3문장으로 요약해줘. "
                     "누구에 대한 어떤 사건·이야기인지, 핵심 포인트가 무엇인지 중심으로. "
                     "서론이나 부연 없이 요약문만 출력해."}
        ]}]
    }).encode("utf-8")
    req = urllib.request.Request(
        f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}"
        f":generateContent?key={GEMINI_KEY}",
        data=body, headers={"Content-Type": "application/json"})
    try:
        resp = json.loads(urllib.request.urlopen(req, timeout=120).read().decode("utf-8"))
        return resp["candidates"][0]["content"]["parts"][0]["text"].strip()
    except urllib.error.HTTPError as e:
        if e.code == 429:
            return "QUOTA"
        print(f"  [gemini {e.code}] {video_url}")
        return None
    except Exception as e:
        print(f"  [gemini 실패] {video_url}: {e}")
        return None


# ── 메인 ──────────────────────────────────────────────
def main():
    now = datetime.now(KST)
    os.makedirs(SNAP_DIR, exist_ok=True)
    print(f"수집 시작 {now.isoformat(timespec='seconds')} / API: {'ON' if API_KEY else 'OFF(RSS 대체)'}")

    # 채널 명부
    handles = []
    with open(CHANNELS_TXT, "r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if ln and not ln.startswith("#"):
                handles.append(ln)
    ch_cache = load_json(CHANNELS_JSON, {})

    db = load_json(VIDEOS_JSON, {"updated": None, "videos": {}})
    videos = db["videos"]

    # RSS 순회: 신규 발견 + (API 없을 때) 조회수 대체
    new_count = 0
    for h in handles:
        cid = resolve_channel_id(h, ch_cache)
        if not cid:
            print(f"[SKIP] 채널ID 못 찾음: {h}")
            continue
        try:
            ch_name, entries = rss_entries(cid)
        except Exception as e:
            print(f"[RSS 실패] {h}: {e}")
            time.sleep(REQUEST_DELAY)
            continue
        ch_cache.setdefault(h, {}).setdefault("name", ch_name)
        for e in entries:
            pub_dt = datetime.fromisoformat(e["pub"])
            age_days = (now - pub_dt).total_seconds() / 86400
            if age_days > RETAIN_DAYS:
                continue
            if e["id"] not in videos:  # ★ 중복 방지: ID 있으면 등록 안 함
                videos[e["id"]] = {
                    "ch": ch_name, "t": e["title"], "url": e["url"],
                    "s": e["shorts"], "pub": e["pub"], "dur": None,
                    "v": e["views"], "lk": e["likes"], "cm": None,
                    "dp": None, "dn": None, "upd": now.isoformat(timespec="seconds"),
                }
                new_count += 1
            elif not API_KEY:  # API 없으면 RSS 값으로라도 갱신
                videos[e["id"]]["v"] = e["views"]
                videos[e["id"]]["lk"] = e["likes"]
                videos[e["id"]]["upd"] = now.isoformat(timespec="seconds")
        time.sleep(REQUEST_DELAY)
    save_json(CHANNELS_JSON, ch_cache)
    print(f"RSS 완료: 신규 {new_count}개 / 전체 추적 {len(videos)}개")

    # 추적 대상(2주 이내) API 갱신
    track_ids = [vid for vid, v in videos.items()
                 if (now - datetime.fromisoformat(v["pub"])).total_seconds() / 86400 <= TRACK_DAYS]
    if API_KEY and track_ids:
        stats = api_stats(track_ids)
        for vid, st in stats.items():
            videos[vid].update({"v": st["v"], "upd": now.isoformat(timespec="seconds")})
            if st["lk"] is not None:
                videos[vid]["lk"] = st["lk"]
            if st["cm"] is not None:
                videos[vid]["cm"] = st["cm"]
            if st["dur"] is not None:
                videos[vid]["dur"] = st["dur"]
        print(f"API 갱신: {len(stats)}개")

    # 증가분 계산 (스냅샷 이력 기반)
    hist = load_recent_snapshots(now)
    for vid in track_ids:
        h = hist.get(vid, [])
        cur = videos[vid]["v"]
        v24 = views_at(h, now - timedelta(hours=24))
        v48 = views_at(h, now - timedelta(hours=48))
        videos[vid]["dn"] = (cur - v24) if v24 is not None else None
        videos[vid]["dp"] = (v24 - v48) if (v24 is not None and v48 is not None) else None

    # 오늘 스냅샷 적재
    snap_path = os.path.join(SNAP_DIR, now.strftime("%Y-%m-%d") + ".jsonl")
    with open(snap_path, "a", encoding="utf-8") as f:
        for vid in track_ids:
            v = videos[vid]
            f.write(json.dumps({"id": vid, "t": now.isoformat(timespec="seconds"),
                                "v": v["v"], "lk": v.get("lk"), "cm": v.get("cm")},
                               ensure_ascii=False) + "\n")

    # 30일 청소 + 100만 쇼츠 아카이브
    removed, archived = 0, 0
    for vid in list(videos.keys()):
        age = (now - datetime.fromisoformat(videos[vid]["pub"])).total_seconds() / 86400
        if age > RETAIN_DAYS:
            v = videos.pop(vid)
            removed += 1
            if v["s"] and v["v"] >= ARCHIVE_MIN_VIEWS:
                new_file = not os.path.exists(ARCHIVE_CSV)
                with open(ARCHIVE_CSV, "a", encoding="utf-8") as f:
                    if new_file:
                        f.write("views,title,channel,url\n")
                    t = v["t"].replace('"', "'")
                    f.write(f'{v["v"]},"{t}","{v["ch"]}",{v["url"]}\n')
                archived += 1
    # 오래된 스냅샷 파일 삭제
    for fn in os.listdir(SNAP_DIR):
        try:
            d = datetime.strptime(fn[:10], "%Y-%m-%d").replace(tzinfo=KST)
            if (now - d).days > RETAIN_DAYS:
                os.remove(os.path.join(SNAP_DIR, fn))
        except Exception:
            continue

    # 제미나이 요약 — 쇼츠만, 영상당 1회, 최신 우선
    if GEMINI_KEY:
        summaries = load_json(SUMMARIES_JSON, {})
        targets = [(vid, v) for vid, v in videos.items() if v["s"] and vid not in summaries]
        targets.sort(key=lambda x: x[1]["pub"], reverse=True)
        done = 0
        for vid, v in targets[:MAX_SUMMARIES_PER_RUN]:
            s = gemini_summarize(v["url"])
            if s == "QUOTA":
                print("  제미나이 일일 한도 도달 — 다음 사이클에 이어서 생성")
                break
            if s:
                summaries[vid] = s
                done += 1
            time.sleep(7)  # 분당 요청 한도 보호
        for vid in list(summaries.keys()):  # 제거된 영상의 요약 청소
            if vid not in videos:
                del summaries[vid]
        save_json(SUMMARIES_JSON, summaries)
        print(f"요약 생성: {done}개 (누적 {len(summaries)})")

    db["updated"] = now.isoformat(timespec="seconds")
    save_json(VIDEOS_JSON, db)
    print(f"완료: 추적 {len(videos)}개 / 제거 {removed} / 아카이브 {archived}")


if __name__ == "__main__":
    main()
