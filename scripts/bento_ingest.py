#!/usr/bin/env python3
"""电影便当进货脚本 (bento_ingest.py)
三波扫描 TMDB discover: 老字号(高人数经典) / 隐藏菜单(人少分高) / 一口(短片),
再逐部拉详情(时长/国家/关键词), 派生菜系·菜色·分量·档位·熟成度, 落 data/bento.db。
幂等: INSERT OR REPLACE; 可重复跑, 每晚 cron 补新片。
"""
import asyncio, json, os, re, sqlite3, sys, time
from pathlib import Path
import httpx

ROOT = Path(os.environ.get('BENTO_ROOT', Path(__file__).resolve().parent.parent))
DB = ROOT / 'data' / 'bento.db'
LOG = Path('/tmp/bento_ingest.log')
KEY = re.search(r'^TMDB_API_KEY=(.+)$', (ROOT / '.env').read_text(), re.M).group(1).strip()
API = 'https://api.themoviedb.org/3'
RPS = 30  # 每秒请求上限, 留足余量

# ---------- 维度派生 ----------
CUISINE = {
    'CN': '中餐', 'TW': '中餐', 'SG': '中餐', 'HK': '港片', 'JP': '日料', 'KR': '韩餐',
    'US': '美餐', 'CA': '美餐', 'FR': '法餐', 'IT': '意餐', 'GB': '英餐', 'IE': '英餐',
    'TH': '泰餐', 'IN': '印度菜', 'IR': '伊朗菜', 'RU': '俄餐', 'DE': '德餐', 'AT': '德餐',
    'ES': '西班牙菜', 'MX': '拉美菜', 'AR': '拉美菜', 'BR': '拉美菜', 'CL': '拉美菜', 'CO': '拉美菜', 'CU': '拉美菜',
    'SE': '北欧菜', 'DK': '北欧菜', 'NO': '北欧菜', 'FI': '北欧菜', 'IS': '北欧菜',
}
DISH = {  # TMDB genre id -> 菜色
    10749: '马卡龙', 27: '河豚', 53: '生鱼片', 9648: '螃蟹', 35: '炸鸡', 18: '茶泡饭', 28: '铁板烧',
    878: '分子料理', 14: '棉花糖', 16: '熊饭团', 80: '卤煮', 10752: '压缩饼干', 99: '清汤',
    10751: '蛋包饭', 36: '老火汤', 10402: '汽水', 12: '篝火烤肉', 37: '牛排',
}
def portion(rt):
    if not rt: return ''
    return '一口' if rt < 40 else '小食' if rt < 80 else '正餐' if rt < 130 else '大份' if rt < 180 else '满汉全席'
def age(y):
    if not y: return ''
    return '刚出锅' if y >= 2024 else '温热' if y >= 2010 else '常温' if y >= 1990 else '陈酿' if y >= 1970 else '老坛'
def tier(votes, rating, y):
    if y and y >= 2023 and votes >= 500: return '当红'
    if votes >= 3000 and rating >= 7.0: return '老字号'
    if votes < 1000 and rating >= 7.2: return '隐藏菜单'
    return '家常'

# ---------- 扫描计划 ----------
COUNTRIES = ['CN', 'HK', 'TW', 'JP', 'KR', 'US', 'FR', 'IT', 'GB', 'TH', 'IN', 'IR', 'RU', 'DE', 'ES', 'MX', 'AR', 'BR', 'SE', 'DK', 'CA', 'AU']
DECADES = [(1900, 1969), (1970, 1989), (1990, 1999), (2000, 2009), (2010, 2016), (2017, 2021), (2022, 2026)]
def plans():
    # 波1 老字号: 全球, 人数>=1000, 分>=6, 按年代分片
    for a, b in DECADES:
        yield ('老字号', {'vote_count.gte': 1000, 'vote_average.gte': 6.0, 'primary_release_date.gte': f'{a}-01-01', 'primary_release_date.lte': f'{b}-12-31', 'sort_by': 'vote_count.desc'})
    # 波2 隐藏菜单: 按国家×年代, 人数 60-999, 分>=6.8
    for c in COUNTRIES:
        for a, b in DECADES:
            yield ('隐藏菜单', {'with_origin_country': c, 'vote_count.gte': 60, 'vote_count.lte': 999, 'vote_average.gte': 6.8, 'primary_release_date.gte': f'{a}-01-01', 'primary_release_date.lte': f'{b}-12-31', 'sort_by': 'vote_average.desc'})
    # 波3 一口: 短片, 人数>=25, 分>=6.5
    for a, b in DECADES:
        yield ('一口', {'with_runtime.lte': 40, 'vote_count.gte': 25, 'vote_average.gte': 6.5, 'primary_release_date.gte': f'{a}-01-01', 'primary_release_date.lte': f'{b}-12-31', 'sort_by': 'vote_average.desc'})

# ---------- 限速客户端 ----------
class Limiter:
    def __init__(self, rps): self.rps = rps; self.t = time.monotonic(); self.n = 0
    async def wait(self):
        self.n += 1
        target = self.t + self.n / self.rps
        d = target - time.monotonic()
        if d > 0: await asyncio.sleep(d)
        if self.n >= self.rps * 10: self.t = time.monotonic(); self.n = 0

lim = Limiter(RPS)
async def get(cli, path, **params):
    params['api_key'] = KEY; params.setdefault('language', 'zh-CN')
    for attempt in range(4):
        await lim.wait()
        try:
            r = await cli.get(f'{API}{path}', params=params, timeout=20)
            if r.status_code == 429:
                await asyncio.sleep(2 + attempt * 2); continue
            if r.status_code == 404: return None
            r.raise_for_status(); return r.json()
        except Exception as e:
            if attempt == 3: log(f'放弃 {path}: {e}'); return None
            await asyncio.sleep(1 + attempt)

def log(s):
    line = f'[{time.strftime("%H:%M:%S")}] {s}'
    print(line, flush=True); LOG.open('a').write(line + '\n')

# ---------- 入库 ----------
def init_db():
    DB.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(DB)
    db.executescript('''
    CREATE TABLE IF NOT EXISTS films (
      tmdb_id INTEGER PRIMARY KEY, title TEXT, title_orig TEXT, year INTEGER, release_date TEXT,
      runtime INTEGER, genre_ids TEXT, genres TEXT, countries TEXT, orig_lang TEXT,
      rating REAL, votes INTEGER, popularity REAL, poster TEXT, backdrop TEXT,
      overview TEXT, tagline TEXT, keywords TEXT,
      cuisine TEXT, dishes TEXT, portion TEXT, tier TEXT, age TEXT, flavors TEXT,
      waves TEXT, fetched_at TEXT);
    CREATE INDEX IF NOT EXISTS i_cuisine ON films(cuisine);
    CREATE INDEX IF NOT EXISTS i_portion ON films(portion);
    CREATE INDEX IF NOT EXISTS i_tier ON films(tier);
    CREATE INDEX IF NOT EXISTS i_rating ON films(rating);
    CREATE INDEX IF NOT EXISTS i_year ON films(year);
    ''')
    return db

def upsert(db, d, waves, genre_map):
    y = int(d['release_date'][:4]) if d.get('release_date') else None
    cs = [c['iso_3166_1'] for c in d.get('production_countries', [])] or list(d.get('origin_country') or [])
    cuisine = next((CUISINE[c] for c in cs if c in CUISINE), '其他')
    gids = [g['id'] for g in d.get('genres', [])]
    dishes = [DISH[g] for g in gids if g in DISH]
    kws = [k['name'] for k in (d.get('keywords') or {}).get('keywords', [])]
    db.execute('INSERT OR REPLACE INTO films VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (
        d['id'], d.get('title') or d.get('original_title'), d.get('original_title'), y, d.get('release_date'),
        d.get('runtime'), json.dumps(gids), json.dumps([genre_map.get(g, '') for g in gids], ensure_ascii=False),
        json.dumps(cs), d.get('original_language'), d.get('vote_average'), d.get('vote_count'), d.get('popularity'),
        d.get('poster_path'), d.get('backdrop_path'), d.get('overview'), d.get('tagline'), json.dumps(kws, ensure_ascii=False),
        cuisine, json.dumps(dishes, ensure_ascii=False), portion(d.get('runtime')), tier(d.get('vote_count') or 0, d.get('vote_average') or 0, y), age(y), '[]',
        json.dumps(sorted(waves), ensure_ascii=False), time.strftime('%Y-%m-%dT%H:%M:%S')))

async def main():
    LOG.write_text('')
    db = init_db()
    async with httpx.AsyncClient(http2=False) as cli:
        gl = await get(cli, '/genre/movie/list'); genre_map = {g['id']: g['name'] for g in (gl or {}).get('genres', [])}
        ids = {}  # tmdb_id -> set(waves)
        for wave, p in plans():
            page = 1; got = 0
            while page <= 500:
                d = await get(cli, '/discover/movie', page=page, include_adult='false', **p)
                if not d or not d.get('results'): break
                for m in d['results']:
                    ids.setdefault(m['id'], set()).add(wave); got += 1
                if page >= d.get('total_pages', 1): break
                page += 1
            log(f'扫描 {wave} {p.get("with_origin_country","ALL")} {p.get("primary_release_date.gte","")[:4]}-: {got} 部 | 累计去重 {len(ids)}')
        log(f'=== 扫描完成, 待拉详情 {len(ids)} 部 ===')
        have = {r[0] for r in db.execute('SELECT tmdb_id FROM films')}
        todo = [i for i in ids if i not in have]
        log(f'库中已有 {len(have)}, 本次需拉 {len(todo)}')
        sem = asyncio.Semaphore(8); done = 0
        async def one(i):
            nonlocal done
            async with sem:
                d = await get(cli, f'/movie/{i}', append_to_response='keywords')
                if d and (d.get('overview') or '').strip() == '':
                    en = await get(cli, f'/movie/{i}', language='en-US')
                    if en: d['overview'] = en.get('overview') or ''; d['tagline'] = d.get('tagline') or en.get('tagline')
                if d: upsert(db, d, ids[i], genre_map)
                done += 1
                if done % 200 == 0: db.commit(); log(f'详情 {done}/{len(todo)}')
        await asyncio.gather(*(one(i) for i in todo))
        db.commit()
    n = db.execute('SELECT COUNT(*) FROM films').fetchone()[0]
    log(f'=== 入库完成: films 共 {n} 部 ===')
    for row in db.execute('SELECT cuisine, COUNT(*) FROM films GROUP BY cuisine ORDER BY 2 DESC'): log(f'  {row[0]}: {row[1]}')

if __name__ == '__main__':
    asyncio.run(main())
