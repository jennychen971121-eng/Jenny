import os, re, csv, time, random, datetime
import requests
from googleapiclient.discovery import build

OUT_DIR = "out"
KEYWORD_POOL_FILE = "keyword_pool.txt"
DONE_IDS_FILE = "done_channel_ids.txt"

# 每天产出：至少10，最多30
MIN_DAILY = 10
MAX_DAILY = 30

# 目标国家：美国/英国/德国（regionCode 用于搜索；country 字段做尽力筛选）
TARGET_REGIONS = ["US", "GB", "DE"]

# 方案2：粉丝范围（更均衡）
MIN_SUBS = 5000
MAX_SUBS = 200000

# 活跃度 & 搜索窗口
ACTIVE_DAYS = 120
LOOKBACK_DAYS = 180

# 搜索规模（不够会自动加大 pages）
PAGES_PER_KEYWORD = 8
DAILY_KEYWORD_COUNT = 16

# 只输出有邮箱
EMAIL_REGEX = r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"

# 适配风扇产品的频道倾向信号（加分排序）
POSITIVE = [
    "review","tested","testing","comparison","vs","unboxing","hands on","buying guide",
    "tech","gadget","gear","edc","outdoor","camping","travel","packing","disney","theme park",
    "summer essentials","hot weather"
]

# 合作/赞助/变现信号（更像能接合作）
SPONSOR = [
    "sponsored","paid promotion","partner","thanks to","in collaboration with",
    "affiliate","amazon storefront","commission","use my code","discount code","promo code"
]

# 品牌信号（用于“合作过大牌/竞品更可信”的排序加分；你后续可随时补充）
BRANDS = [
    # 你们品牌
    "warmco",
    # 常见3C/配件
    "anker","ugreen","belkin","spigen","baseus",
    # 常见小风扇/风扇相关（可继续补）
    "dreo","jisulife","torras","opolar","gaiatop","koonie","comlife",
    # 户外/旅行品牌信号
    "coleman","yeti","patagonia","thenorthface","rei"
]

# 过滤“纯搞笑/整活”倾向（硬过滤）
NEGATIVE = [
    "prank","pranks","funny","comedy","meme","memes","troll","skit","joke","parody",
    "reaction","reacts","compilation"
]

def yt():
    key = os.environ.get("YOUTUBE_API_KEY","").strip()
    if not key:
        raise SystemExit("Missing env YOUTUBE_API_KEY")
    return build("youtube", "v3", developerKey=key)

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
    seen=set(); out=[]
    for x in xs:
        if x not in seen:
            seen.add(x); out.append(x)
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
    r = random.Random(date_str)
    pool = pool[:]
    r.shuffle(pool)
    return pool[:min(n, len(pool))]

def search_channels(ytc, keyword, region, published_after, pages):
    out=[]
    token=None
    for _ in range(pages):
        resp = ytc.search().list(
            part="snippet",
            q=keyword,
            type="video",
            order="date",
            regionCode=region,
            relevanceLanguage="en",
            maxResults=50,
            pageToken=token,
            publishedAfter=published_after
        ).execute()
        for it in resp.get("items", []):
            cid = (it.get("snippet", {}) or {}).get("channelId")
            if cid: out.append(cid)
        token = resp.get("nextPageToken")
        if not token: break
    return uniq(out)

def fetch_handle_about(channel_id):
    """
    你要的 @handle + About 页文本（用于邮箱/合作信号）
    注意：handle 可能被作者修改；永久定位/去重用 channel_id(UC...)
    """
    headers = {"User-Agent":"Mozilla/5.0","Accept-Language":"en-US,en;q=0.9"}
    handle = ""
    about_html = ""

    for url in [f"https://www.youtube.com/channel/{channel_id}/about",
                f"https://www.youtube.com/channel/{channel_id}"]:
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

    # 扩量策略：不够 MIN_DAILY 就逐步加大翻页
    pages = PAGES_PER_KEYWORD
    leads = {}  # cid -> row

    while True:
        cids = []
        for region in TARGET_REGIONS:
            for kw in keywords:
                cids.extend(search_channels(ytc, kw, region, published_after, pages))
        cids = [c for c in uniq(cids) if c not in done]

        for batch in chunk(cids, 50):
            resp = ytc.channels().list(
                part="snippet,statistics,contentDetails",
                id=",".join(batch)
            ).execute()

            for it in resp.get("items", []):
                cid = it.get("id","")
                if not cid or cid in done or cid in leads:
                    continue

                snippet = it.get("snippet", {}) or {}
                stats = it.get("statistics", {}) or {}

                title = snippet.get("title","")
                country = snippet.get("country","")  # 可能为空
                subs = int(stats.get("subscriberCount", 0))

                # 粉丝范围（方案2）
                if subs < MIN_SUBS or subs > MAX_SUBS:
                    continue

                # 国家尽力筛选：频道填了country且不在US/GB/DE则排除；没填country不强杀
                if country and country not in TARGET_REGIONS:
                    continue

                # 过滤搞笑倾向（频道名+简介命中负面词直接丢）
                base_text = (title + " " + (snippet.get("description","") or "")).lower()
                if any(w in base_text for w in NEGATIVE):
                    continue

                # 活跃度：看 uploads 最新一条
                uploads = it.get("contentDetails", {}).get("relatedPlaylists", {}).get("uploads")
                if not uploads:
                    continue
                pl = ytc.playlistItems().list(
                    part="snippet",
                    playlistId=uploads,
                    maxResults=1
                ).execute()
                items = pl.get("items", [])
                if not items:
                    continue
                last_pub = items[0]["snippet"]["publishedAt"]
                last_dt = datetime.datetime.strptime(last_pub, "%Y-%m-%dT%H:%M:%SZ")
                if last_dt < datetime.datetime.utcnow() - datetime.timedelta(days=ACTIVE_DAYS):
                    continue

                # 抓 handle + about 邮箱（硬条件：必须有邮箱）
                handle, about_html = fetch_handle_about(cid)
                emails = extract_emails((snippet.get("description","") or "") + "\n" + (about_html or ""))
                if not emails:
                    continue

                # 打分：频道简介 + about 页面（粗扫描）
                s = score_text((snippet.get("description","") or "") + "\n" + (about_html or ""))

                leads[cid] = {
                    "channel_id": cid,                 # 永久不变（去重靠它）
                    "handle": handle,                  # 你要的@xxx（可能被改名）
                    "channel_name": title,
                    "country": country,
                    "subs": subs,
                    "last_upload_utc": last_pub,
                    "email": "; ".join(emails),
                    "score": s,
                    "channel_url": f"https://www.youtube.com/channel/{cid}",
                    "handle_url": (f"https://www.youtube.com/{handle}" if handle else "")
                }

        if len(leads) >= MIN_DAILY or pages >= 20:
            break
        pages += 4  # 不够就加大翻页

    rows = sorted(leads.values(), key=lambda r: (r["score"], r["subs"]), reverse=True)[:MAX_DAILY]

    header = ["channel_id","handle","channel_name","country","subs","last_upload_utc","email","score","channel_url","handle_url"]
    with open(out_csv, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    # 只对“已输出”的写入 done，保证每天不重复
    for r in rows:
        mark_done(r["channel_id"])

    print(f"Saved: {out_csv} rows={len(rows)} keywords={len(keywords)} pages={pages}")

if __name__ == "__main__":
    main()
