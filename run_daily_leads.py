import os, re, csv, time, random, datetime
import requests
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

OUT_DIR = "out"
KEYWORD_POOL_FILE = "keyword_pool.txt"
DONE_IDS_FILE = "done_channel_ids.txt"

# 你要：每天固定输出 20
DAILY_TARGET = 20

TARGET_REGIONS = ["US", "GB", "DE"]

MIN_SUBS = 5000
MAX_SUBS = 200000

ACTIVE_DAYS = 120
LOOKBACK_DAYS = 180

# -------------------------------
# 配额控制（关键：只严格控 search.list 次数）
# search.list 通常 ~100 units/次；默认日配额常见 10,000 units
# -------------------------------
DAILY_UNIT_BUDGET = 9500
SEARCH_LIST_COST = 100
MAX_SEARCH_CALLS = DAILY_UNIT_BUDGET // SEARCH_LIST_COST
SEARCH_CALLS_SOFT_LIMIT = max(1, int(MAX_SEARCH_CALLS * 0.90))  # 约 85 次

# 每天从关键词池抽多少个（不会跑完整库）
DAILY_KEYWORD_COUNT = 10

# 逐步加量：先小范围，找不够再扩大（仍受 search_calls 限制）
START_PAGES_PER_KEYWORD = 1
MAX_PAGES_PER_KEYWORD = 6  # 允许更大一点以保证凑够20，但仍会被预算器限制

# 联系方式提取（邮箱优先，其次网站；允许都没有）
EMAIL_REGEX = r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"
URL_REGEX = r"(https?://[^\s\"\'<>]+)"

POSITIVE = [
    "review","tested","testing","comparison","vs","unboxing","hands on","buying guide",
    "tech","gadget","gear","edc","outdoor","camping","travel","packing","disney","theme park",
    "summer essentials","hot weather"
]
SPONSOR = [
    "sponsored","paid promotion","partner","thanks to","in collaboration with",
    "affiliate","amazon storefront","commission","use my code","discount code","promo code"
]
BRANDS = [
    "warmco",
    "anker","ugreen","belkin","spigen","baseus",
    "dreo","jisulife","torras","opolar","gaiatop","koonie","comlife",
    "coleman","yeti","patagonia","thenorthface","rei"
]
NEGATIVE = [
    "prank","pranks","funny","comedy","meme","memes","troll","skit","joke","parody",
    "reaction","reacts","compilation"
]

def yt():
    key = os.environ.get("YOUTUBE_API_KEY", "").strip()
    if not key:
        raise SystemExit("Missing env YOUTUBE_API_KEY")
    return build("youtube", "v3", developerKey=key, cache_discovery=False)

def load_list(path):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return [x.strip() for x in f if x.strip()]

def load_done():
    return set(load_list(DONE_IDS_FILE))

def mark_done(cid):
    with open(DONE_IDS_FILE, "a", encoding="utf-8") as f:
        f.write(cid + "\n")

def uniq(xs):
    seen = set()
    out = []
    for x in xs:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

def utc_iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def score_text(text):
    t = (text or "").lower()
    s = 0
    for w in POSITIVE:
        if w in t: s += 2
    for w in SPONSOR:
        if w in t: s += 3
    for w in BRANDS:
        if w in t: s += 2
    for w in NEGATIVE:
        if w in t: s -= 8
    return s

def pick_daily_keywords(pool, n, date_str):
    # 用日期做种子：每天抽到不同组合；同一天多次跑也一致
    r = random.Random(date_str)
    pool = pool[:]
    r.shuffle(pool)
    return pool[:min(n, len(pool))]

def is_quota_exceeded(e: HttpError) -> bool:
    try:
        content = e.content.decode("utf-8", errors="ignore") if hasattr(e, "content") else str(e)
    except Exception:
        content = str(e)
    return ("quotaExceeded" in content) or ("youtube.quota" in content) or ('"reason": "quotaExceeded"' in content)

def safe_execute(req, max_retries=5, base_sleep=1.0):
    for i in range(max_retries):
        try:
            return req.execute()
        except HttpError as e:
            if is_quota_exceeded(e):
                raise
            status = getattr(e, "resp", None).status if getattr(e, "resp", None) else None
            if status in (429, 500, 502, 503, 504):
                sleep = base_sleep * (2 ** i) + random.random()
                time.sleep(min(30, sleep))
                continue
            raise
        except Exception:
            sleep = base_sleep * (2 ** i) + random.random()
            time.sleep(min(10, sleep))
    raise RuntimeError("Request failed after retries")

def search_channels_from_videos(ytc, keyword, region, published_after, pages, quota_state):
    out = []
    token = None
    for _ in range(pages):
        if quota_state["search_calls"] >= SEARCH_CALLS_SOFT_LIMIT:
            break

        req = ytc.search().list(
            part="snippet",
            q=keyword,
            type="video",
            order="date",
            regionCode=region,
            relevanceLanguage="en",
            maxResults=50,
            pageToken=token,
            publishedAfter=published_after
        )

        quota_state["search_calls"] += 1
        resp = safe_execute(req)

        for it in resp.get("items", []):
            cid = ((it.get("snippet", {}) or {}).get("channelId") or "").strip()
            if cid:
                out.append(cid)

        token = resp.get("nextPageToken")
        if not token:
            break

    return uniq(out)

def fetch_handle_about(channel_id):
    headers = {"User-Agent": "Mozilla/5.0", "Accept-Language": "en-US,en;q=0.9"}
    handle = ""
    about_html = ""

    urls = [
        f"https://www.youtube.com/channel/{channel_id}/about",
        f"https://www.youtube.com/channel/{channel_id}",
    ]

    for url in urls:
        try:
            r = requests.get(url, headers=headers, timeout=25)
            if r.status_code != 200:
                continue
            html = r.text

            if not handle:
                m = re.search(r'"canonicalBaseUrl"\s*:\s*"(/@[^"]+)"', html)
                if m:
                    handle = m.group(1).replace("/", "")
                else:
                    m = re.search(r'"vanityChannelUrl"\s*:\s*"https?://www\.youtube\.com/(@[^"]+)"', html)
                    if m:
                        handle = m.group(1)

            if "about" in url:
                about_html = html

            if handle and about_html:
                break
        except Exception:
            pass
        time.sleep(0.2)

    return handle, about_html

def extract_emails(text):
    if not text:
        return []
    emails = sorted(set(re.findall(EMAIL_REGEX, text)))
    emails = [e for e in emails if not e.lower().endswith("@example.com")]
    return emails

def normalize_url(u: str) -> str:
    return (u or "").strip().rstrip(").,;\"'")

def is_bad_url(u: str) -> bool:
    u2 = u.lower()
    bad = [
        "youtube.com", "youtu.be",
        "google.com", "goo.gl",
        "facebook.com/sharer", "twitter.com/intent",
        "support.google.com"
    ]
    return any(b in u2 for b in bad)

def extract_websites(text):
    if not text:
        return []
    urls = re.findall(URL_REGEX, text)
    urls = [normalize_url(u) for u in urls]
    urls = [u for u in urls if u and not is_bad_url(u)]
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out[:5]

def chunk(xs, n):
    for i in range(0, len(xs), n):
        yield xs[i:i+n]

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    today = datetime.datetime.utcnow().strftime("%Y%m%d")
    out_csv = os.path.join(OUT_DIR, f"youtube_leads_{today}.csv")

    pool = load_list(KEYWORD_POOL_FILE)
    if not pool:
        raise SystemExit("keyword_pool.txt is empty")

    keywords = pick_daily_keywords(pool, DAILY_KEYWORD_COUNT, today)

    ytc = yt()
    done = load_done()

    published_after = utc_iso(datetime.datetime.utcnow() - datetime.timedelta(days=LOOKBACK_DAYS))

    quota_state = {"search_calls": 0}
    pages = START_PAGES_PER_KEYWORD

    # 先收集“候选池”，最后排序后输出前20
    candidates = {}  # cid -> row
    MAX_CANDIDATES_TO_PROCESS = 1200  # 防止无限膨胀

    while True:
        # 如果候选池足够大，就可以停止继续扩量（提高稳定凑够20的概率）
        # 这里取 120 是经验值：最终过滤/去重后一般能剩下20
        if len(candidates) >= 120:
            break
        if pages > MAX_PAGES_PER_KEYWORD:
            break
        if quota_state["search_calls"] >= SEARCH_CALLS_SOFT_LIMIT:
            break

        cids = []
        for region in TARGET_REGIONS:
            for kw in keywords:
                if quota_state["search_calls"] >= SEARCH_CALLS_SOFT_LIMIT:
                    break
                cids.extend(search_channels_from_videos(ytc, kw, region, published_after, pages, quota_state))
            if quota_state["search_calls"] >= SEARCH_CALLS_SOFT_LIMIT:
                break

        cids = [c for c in uniq(cids) if c not in done and c not in candidates]
        if not cids:
            pages += 1
            continue

        # 批量拉频道信息
        for batch in chunk(cids, 50):
            resp = safe_execute(
                ytc.channels().list(
                    part="snippet,statistics,contentDetails",
                    id=",".join(batch)
                )
            )

            for it in resp.get("items", []):
                if len(candidates) >= MAX_CANDIDATES_TO_PROCESS:
                    break

                cid = (it.get("id", "") or "").strip()
                if not cid or cid in done or cid in candidates:
                    continue

                snippet = it.get("snippet", {}) or {}
                stats = it.get("statistics", {}) or {}

                title = snippet.get("title", "") or ""
                country = snippet.get("country", "") or ""

                try:
                    subs = int(stats.get("subscriberCount", 0) or 0)
                except Exception:
                    subs = 0

                if subs < MIN_SUBS or subs > MAX_SUBS:
                    continue
                if country and country not in TARGET_REGIONS:
                    continue

                base_text = (title + " " + (snippet.get("description", "") or "")).lower()
                if any(w in base_text for w in NEGATIVE):
                    continue

                uploads = (it.get("contentDetails", {}) or {}).get("relatedPlaylists", {}).get("uploads")
                if not uploads:
                    continue

                try:
                    pl = safe_execute(
                        ytc.playlistItems().list(
                            part="snippet",
                            playlistId=uploads,
                            maxResults=1
                        )
                    )
                except HttpError as e:
                    if is_quota_exceeded(e):
                        break
                    continue

                items = pl.get("items", [])
                if not items:
                    continue
                last_pub = items[0]["snippet"].get("publishedAt", "")
                if not last_pub:
                    continue

                try:
                    last_dt = datetime.datetime.strptime(last_pub, "%Y-%m-%dT%H:%M:%SZ")
                except Exception:
                    continue
                if last_dt < datetime.datetime.utcnow() - datetime.timedelta(days=ACTIVE_DAYS):
                    continue

                # 抓 about（用于邮箱/网站/打分），但失败也不让频道报废
                handle, about_html = fetch_handle_about(cid)

                combined_text = (snippet.get("description", "") or "") + "\n" + (about_html or "")
                emails = extract_emails(combined_text)
                websites = extract_websites(combined_text)

                has_email = 1 if emails else 0
                has_website = 1 if websites else 0

                # 你允许没有邮箱/网站：contact 为空
                contact = "; ".join(emails) if emails else ("; ".join(websites) if websites else "")

                s = score_text(combined_text)

                candidates[cid] = {
                    "channel_id": cid,
                    "handle": handle,
                    "channel_name": title,
                    "country": country,
                    "subs": subs,
                    "last_upload_utc": last_pub,
                    "emails": "; ".join(emails),
                    "websites": "; ".join(websites),
                    "contact": contact,
                    "has_email": has_email,
                    "has_website": has_website,
                    "score": s,
                    "channel_url": f"https://www.youtube.com/channel/{cid}",
                    "handle_url": (f"https://www.youtube.com/{handle}" if handle else "")
                }

            if len(candidates) >= MAX_CANDIDATES_TO_PROCESS:
                break

        pages += 1

    # 最终排序：邮箱优先 > 网站 > score > subs
    rows_sorted = sorted(
        candidates.values(),
        key=lambda r: (r["has_email"], r["has_website"], r["score"], r["subs"]),
        reverse=True
    )

    rows = rows_sorted[:DAILY_TARGET]

    # 如果仍不足 20（极少数：候选太少/触发配额/网络失败），也照样输出已有的
    header = [
        "channel_id","handle","channel_name","country","subs","last_upload_utc",
        "emails","websites","contact","score","channel_url","handle_url"
    ]
    with open(out_csv, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in header})

    # 只对“最终输出的20个（或不足20的全部）”写入 done，确保不重复
    for r in rows:
        mark_done(r["channel_id"])

    print(
        f"Saved: {out_csv} rows={len(rows)}/{DAILY_TARGET} "
        f"keywords={len(keywords)} regions={len(TARGET_REGIONS)} "
        f"pages_used_up_to={min(pages-1, MAX_PAGES_PER_KEYWORD)} "
        f"search_calls={quota_state['search_calls']}/{SEARCH_CALLS_SOFT_LIMIT} "
        f"(search_budget~{SEARCH_CALLS_SOFT_LIMIT*SEARCH_LIST_COST} units) "
        f"candidates={len(candidates)}"
    )

    if len(rows) < DAILY_TARGET:
        print(
            "WARNING: Could not reach 20 within quota/filters/network. "
            "Consider: increase DAILY_KEYWORD_COUNT, MAX_PAGES_PER_KEYWORD, LOOKBACK_DAYS, "
            "or reduce filters; also ensure done_channel_ids.txt is persisted across runs."
        )

if __name__ == "__main__":
    try:
        main()
    except HttpError as e:
        if is_quota_exceeded(e):
            print("Quota exceeded. Exiting gracefully.")
            raise SystemExit(0)
        raise
