"""电影便当 (bento.py) —— 精神食粮
货架/自选/随机/详情/食单/小票/海报代理 + 主厨工具(bento)派发。
数据: data/bento.db (films 由 scripts/bento_ingest.py 进货; bento_wish/bento_log 在此建表)
"""
from __future__ import annotations
import json, os, re, sqlite3, time, random
from pathlib import Path
import httpx
from fastapi import APIRouter, Depends, Header, HTTPException, Query
from fastapi.responses import FileResponse, Response
from app import auth

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "data" / "bento.db"
POSTER_DIR = ROOT / "data" / "bento_posters"
router = APIRouter()


def _require_auth(authorization: str = Header(default="")) -> None:
    token = authorization.removeprefix("Bearer ").strip()
    if not token or not auth.verify_token(token):
        raise HTTPException(status_code=401, detail="unauthorized")


def _tmdb_key() -> str:
    m = re.search(r"^TMDB_API_KEY=(.+)$", (ROOT / ".env").read_text(), re.M)
    return m.group(1).strip() if m else ""


def _db() -> sqlite3.Connection:
    DB.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(DB, timeout=10)
    c.row_factory = sqlite3.Row
    c.executescript("""
    CREATE TABLE IF NOT EXISTS films (
      tmdb_id INTEGER PRIMARY KEY, title TEXT, title_orig TEXT, year INTEGER, release_date TEXT,
      runtime INTEGER, genre_ids TEXT, genres TEXT, countries TEXT, orig_lang TEXT,
      rating REAL, votes INTEGER, popularity REAL, poster TEXT, backdrop TEXT,
      overview TEXT, tagline TEXT, keywords TEXT,
      cuisine TEXT, dishes TEXT, portion TEXT, tier TEXT, age TEXT, flavors TEXT,
      waves TEXT, fetched_at TEXT);
    CREATE TABLE IF NOT EXISTS bento_wish (film_id INTEGER PRIMARY KEY, added_at TEXT);
    CREATE TABLE IF NOT EXISTS bento_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT, film_id INTEGER NOT NULL, watched_at TEXT,
      rating_me INTEGER, rating_k INTEGER, note_me TEXT, note_k TEXT,
      mood_in TEXT, mood_out TEXT, quote TEXT, frame TEXT, source TEXT, created_at TEXT);
    """)
    return c


# ---------- 维度派生(与进货脚本一致) ----------
CUISINE = {
    'CN': '中餐', 'TW': '中餐', 'SG': '中餐', 'HK': '港片', 'JP': '日料', 'KR': '韩餐',
    'US': '美餐', 'CA': '美餐', 'FR': '法餐', 'IT': '意餐', 'GB': '英餐', 'IE': '英餐',
    'TH': '泰餐', 'IN': '印度菜', 'IR': '伊朗菜', 'RU': '俄餐', 'DE': '德餐', 'AT': '德餐',
    'ES': '西班牙菜', 'MX': '拉美菜', 'AR': '拉美菜', 'BR': '拉美菜', 'CL': '拉美菜', 'CO': '拉美菜', 'CU': '拉美菜',
    'SE': '北欧菜', 'DK': '北欧菜', 'NO': '北欧菜', 'FI': '北欧菜', 'IS': '北欧菜',
}
DISH = {10749: '马卡龙', 27: '河豚', 53: '生鱼片', 9648: '螃蟹', 35: '炸鸡', 18: '茶泡饭', 28: '铁板烧',
        878: '分子料理', 14: '棉花糖', 16: '熊饭团', 80: '卤煮', 10752: '压缩饼干', 99: '清汤',
        10751: '蛋包饭', 36: '老火汤', 10402: '汽水', 12: '篝火烤肉', 37: '牛排'}
DISH_ZH = {'马卡龙': '爱情', '河豚': '恐怖', '生鱼片': '惊悚', '螃蟹': '悬疑', '炸鸡': '喜剧', '茶泡饭': '剧情',
           '铁板烧': '动作', '分子料理': '科幻', '棉花糖': '奇幻', '熊饭团': '动画', '卤煮': '犯罪',
           '压缩饼干': '战争', '清汤': '纪录', '蛋包饭': '家庭', '老火汤': '历史', '汽水': '音乐',
           '篝火烤肉': '冒险', '牛排': '西部'}
PORTIONS = ['一口', '小食', '正餐', '大份', '满汉全席']
TIERS = ['老字号', '当红', '隐藏菜单', '家常']
AGES = ['刚出锅', '温热', '常温', '陈酿', '老坛']


def _portion(rt):
    if not rt: return ''
    return '一口' if rt < 40 else '小食' if rt < 80 else '正餐' if rt < 130 else '大份' if rt < 180 else '满汉全席'
def _age(y):
    if not y: return ''
    return '刚出锅' if y >= 2024 else '温热' if y >= 2010 else '常温' if y >= 1990 else '陈酿' if y >= 1970 else '老坛'
def _tier(votes, rating, y):
    if y and y >= 2023 and votes >= 500: return '当红'
    if votes >= 3000 and rating >= 7.0: return '老字号'
    if votes < 1000 and rating >= 7.2: return '隐藏菜单'
    return '家常'


def _row(r: sqlite3.Row, wish: set | None = None, logged: dict | None = None) -> dict:
    d = dict(r)
    for k in ('genre_ids', 'genres', 'countries', 'keywords', 'dishes', 'flavors', 'waves'):
        try: d[k] = json.loads(d.get(k) or '[]')
        except Exception: d[k] = []
    d['poster_url'] = f"/api/bento/poster/w342{d['poster']}" if d.get('poster') else ''
    d['backdrop_url'] = f"/api/bento/poster/w780{d['backdrop']}" if d.get('backdrop') else ''
    if wish is not None: d['wished'] = d['tmdb_id'] in wish
    if logged is not None: d['watched'] = logged.get(d['tmdb_id'])
    return d


def _flags(c):
    wish = {r[0] for r in c.execute('SELECT film_id FROM bento_wish')}
    logged = {r[0]: r[1] for r in c.execute('SELECT film_id, MAX(watched_at) FROM bento_log GROUP BY film_id')}
    return wish, logged


def _where(q='', cuisine='', dish='', portion='', tier='', age='', min_rating=0.0, wished_only=False, watched_only=False):
    w, p = ['1=1'], []
    if q:
        w.append('(lower(title) LIKE ? OR lower(title_orig) LIKE ? OR keywords LIKE ?)')
        like = f'%{q.lower()}%'; p += [like, like, like]
    if cuisine: w.append('cuisine=?'); p.append(cuisine)
    if dish: w.append('dishes LIKE ?'); p.append(f'%"{dish}"%')
    if portion: w.append('portion=?'); p.append(portion)
    if tier: w.append('tier=?'); p.append(tier)
    if age: w.append('age=?'); p.append(age)
    if min_rating: w.append('rating>=?'); p.append(float(min_rating))
    if wished_only: w.append('tmdb_id IN (SELECT film_id FROM bento_wish)')
    if watched_only: w.append('tmdb_id IN (SELECT film_id FROM bento_log)')
    return ' AND '.join(w), p


LIGHT = 'tmdb_id,title,title_orig,year,runtime,genres,cuisine,dishes,portion,tier,age,rating,votes,poster,countries,overview,keywords,flavors,waves,genre_ids,backdrop,tagline,orig_lang,popularity,release_date,fetched_at'


@router.get('/api/bento/facets', dependencies=[Depends(_require_auth)])
def facets() -> dict:
    c = _db()
    out = {'total': c.execute('SELECT COUNT(*) FROM films').fetchone()[0]}
    for col in ('cuisine', 'portion', 'tier', 'age'):
        out[col] = {r[0]: r[1] for r in c.execute(f'SELECT {col}, COUNT(*) FROM films WHERE {col}!="" GROUP BY {col} ORDER BY 2 DESC')}
    dish = {}
    for (ds,) in c.execute('SELECT dishes FROM films'):
        for d in json.loads(ds or '[]'): dish[d] = dish.get(d, 0) + 1
    out['dish'] = dict(sorted(dish.items(), key=lambda x: -x[1]))
    out['dish_zh'] = DISH_ZH
    out['wish'] = c.execute('SELECT COUNT(*) FROM bento_wish').fetchone()[0]
    out['logged'] = c.execute('SELECT COUNT(DISTINCT film_id) FROM bento_log').fetchone()[0]
    return out


@router.get('/api/bento/search', dependencies=[Depends(_require_auth)])
async def search(q: str = '', cuisine: str = '', dish: str = '', portion: str = '', tier: str = '', age: str = '',
                 min_rating: float = 0, sort: str = 'hot', page: int = 1, size: int = 30, wished: int = 0, watched: int = 0) -> dict:
    c = _db()
    w, p = _where(q.strip(), cuisine, dish, portion, tier, age, min_rating, bool(wished), bool(watched))
    order = {'hot': 'votes DESC', 'rating': 'rating DESC, votes DESC', 'new': 'year DESC, votes DESC', 'old': 'year ASC', 'random': 'RANDOM()'}.get(sort, 'votes DESC')
    if watched: order = '(SELECT MAX(watched_at) FROM bento_log l WHERE l.film_id=films.tmdb_id) DESC'
    if q.strip():  # 片名精确/前缀命中优先, 再按人气
        ql = q.strip().lower().replace("'", "''"); order = f"(lower(title)='{ql}' OR lower(title_orig)='{ql}') DESC, (lower(title) LIKE '{ql}%' OR lower(title_orig) LIKE '{ql}%') DESC, " + order
    total = c.execute(f'SELECT COUNT(*) FROM films WHERE {w}', p).fetchone()[0]
    # 本地搜不到就现拉 TMDB 补货
    if q.strip() and total < 3 and page == 1:
        added = await tmdb_search_and_add(q.strip())
        if added:
            total = c.execute(f'SELECT COUNT(*) FROM films WHERE {w}', p).fetchone()[0]
    rows = c.execute(f'SELECT {LIGHT} FROM films WHERE {w} ORDER BY {order} LIMIT ? OFFSET ?', p + [size, (page - 1) * size]).fetchall()
    wish, logged = _flags(c)
    return {'total': total, 'page': page, 'items': [_row(r, wish, logged) for r in rows]}


@router.get('/api/bento/random', dependencies=[Depends(_require_auth)])
def random_pick(cuisine: str = '', dish: str = '', portion: str = '', tier: str = '', age: str = '', min_rating: float = 6.5, n: int = 1) -> dict:
    c = _db()
    w, p = _where('', cuisine, dish, portion, tier, age, min_rating)
    rows = c.execute(f'SELECT {LIGHT} FROM films WHERE {w} AND tmdb_id NOT IN (SELECT film_id FROM bento_log) ORDER BY RANDOM() LIMIT ?', p + [max(1, min(n, 6))]).fetchall()
    wish, logged = _flags(c)
    return {'items': [_row(r, wish, logged) for r in rows]}


@router.get('/api/bento/film/{fid}', dependencies=[Depends(_require_auth)])
async def film(fid: int) -> dict:
    c = _db()
    r = c.execute('SELECT * FROM films WHERE tmdb_id=?', (fid,)).fetchone()
    if not r:
        if await tmdb_fetch_and_upsert(fid):
            r = c.execute('SELECT * FROM films WHERE tmdb_id=?', (fid,)).fetchone()
    if not r: raise HTTPException(404, '这道菜厨房没有')
    wish, logged = _flags(c)
    d = _row(r, wish, logged)
    d['logs'] = [dict(x) for x in c.execute('SELECT * FROM bento_log WHERE film_id=? ORDER BY id DESC', (fid,))]
    return d


@router.post('/api/bento/wish', dependencies=[Depends(_require_auth)])
def wish(body: dict) -> dict:
    c = _db(); fid = int(body.get('film_id') or 0); on = bool(body.get('on', True))
    if on: c.execute('INSERT OR IGNORE INTO bento_wish VALUES (?,?)', (fid, time.strftime('%Y-%m-%dT%H:%M:%S')))
    else: c.execute('DELETE FROM bento_wish WHERE film_id=?', (fid,))
    c.commit(); return {'ok': True, 'wished': on}


@router.post('/api/bento/log', dependencies=[Depends(_require_auth)])
def log(body: dict) -> dict:
    c = _db(); fid = int(body.get('film_id') or 0)
    if not c.execute('SELECT 1 FROM films WHERE tmdb_id=?', (fid,)).fetchone(): raise HTTPException(404, '这道菜厨房没有')
    now = time.strftime('%Y-%m-%dT%H:%M:%S')
    cur = c.execute('INSERT INTO bento_log (film_id,watched_at,rating_me,rating_k,note_me,note_k,mood_in,mood_out,quote,frame,source,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)', (
        fid, body.get('watched_at') or now[:10], body.get('rating_me'), body.get('rating_k'), (body.get('note_me') or '')[:300], (body.get('note_k') or '')[:300],
        (body.get('mood_in') or '')[:20], (body.get('mood_out') or '')[:20], (body.get('quote') or '')[:200], body.get('frame'), body.get('source') or 'manual', now))
    c.execute('DELETE FROM bento_wish WHERE film_id=?', (fid,)); c.commit()
    return {'ok': True, 'id': cur.lastrowid}


@router.post('/api/bento/log/{lid}', dependencies=[Depends(_require_auth)])
def log_update(lid: int, body: dict) -> dict:
    c = _db(); cols = [k for k in ('rating_me', 'rating_k', 'note_me', 'note_k', 'mood_in', 'mood_out', 'quote', 'watched_at') if k in body]
    if cols:
        c.execute(f"UPDATE bento_log SET {', '.join(k + '=?' for k in cols)} WHERE id=?", [body[k] for k in cols] + [lid]); c.commit()
    return {'ok': True}


@router.get('/api/bento/log', dependencies=[Depends(_require_auth)])
def logs(limit: int = 60) -> dict:
    c = _db()
    rows = c.execute(f'SELECT l.*, f.title, f.title_orig, f.year, f.runtime, f.poster, f.backdrop, f.cuisine, f.dishes, f.portion, f.rating AS tmdb_rating FROM bento_log l JOIN films f ON f.tmdb_id=l.film_id ORDER BY l.watched_at DESC, l.id DESC LIMIT ?', (limit,)).fetchall()
    out = []
    for r in rows:
        d = dict(r); d['dishes'] = json.loads(d.get('dishes') or '[]')
        d['poster_url'] = f"/api/bento/poster/w342{d['poster']}" if d.get('poster') else ''
        d['backdrop_url'] = f"/api/bento/poster/w780{d['backdrop']}" if d.get('backdrop') else ''
        out.append(d)
    return {'items': out}


@router.get('/api/bento/stats', dependencies=[Depends(_require_auth)])
def stats() -> dict:
    c = _db()
    n = c.execute('SELECT COUNT(*) FROM bento_log').fetchone()[0]
    cu = {r[0]: r[1] for r in c.execute('SELECT f.cuisine, COUNT(*) FROM bento_log l JOIN films f ON f.tmdb_id=l.film_id GROUP BY f.cuisine ORDER BY 2 DESC')}
    dish = {}
    for (ds,) in c.execute('SELECT f.dishes FROM bento_log l JOIN films f ON f.tmdb_id=l.film_id'):
        for d in json.loads(ds or '[]'): dish[d] = dish.get(d, 0) + 1
    mins = c.execute('SELECT COALESCE(SUM(f.runtime),0) FROM bento_log l JOIN films f ON f.tmdb_id=l.film_id').fetchone()[0]
    return {'meals': n, 'cuisine': cu, 'dish': dict(sorted(dish.items(), key=lambda x: -x[1])), 'minutes': mins,
            'wish': c.execute('SELECT COUNT(*) FROM bento_wish').fetchone()[0]}


@router.get('/api/bento/poster/{size}/{path}')
async def poster(size: str, path: str):
    if size not in ('w185', 'w342', 'w500', 'w780', 'original') or not re.fullmatch(r'[A-Za-z0-9_\-]+\.(jpg|png|webp)', path):
        raise HTTPException(400, 'bad poster')
    POSTER_DIR.mkdir(parents=True, exist_ok=True)
    f = POSTER_DIR / f'{size}_{path}'
    if not f.exists():
        async with httpx.AsyncClient(timeout=20) as cli:
            r = await cli.get(f'https://image.tmdb.org/t/p/{size}/{path}')
            if r.status_code != 200: raise HTTPException(404, 'no poster')
            f.write_bytes(r.content)
    return FileResponse(f, headers={'Cache-Control': 'public, max-age=2592000, immutable'})


# ---------- TMDB 现拉补货 ----------
async def tmdb_fetch_and_upsert(fid: int) -> bool:
    key = _tmdb_key()
    if not key: return False
    async with httpx.AsyncClient(timeout=20) as cli:
        r = await cli.get(f'https://api.themoviedb.org/3/movie/{fid}', params={'api_key': key, 'language': 'zh-CN', 'append_to_response': 'keywords'})
        if r.status_code != 200: return False
        d = r.json()
        if not (d.get('overview') or '').strip():
            en = await cli.get(f'https://api.themoviedb.org/3/movie/{fid}', params={'api_key': key, 'language': 'en-US'})
            if en.status_code == 200: d['overview'] = en.json().get('overview') or ''
        gl = await cli.get('https://api.themoviedb.org/3/genre/movie/list', params={'api_key': key, 'language': 'zh-CN'})
        gmap = {g['id']: g['name'] for g in (gl.json().get('genres', []) if gl.status_code == 200 else [])}
    y = int(d['release_date'][:4]) if d.get('release_date') else None
    cs = [x['iso_3166_1'] for x in d.get('production_countries', [])] or list(d.get('origin_country') or [])
    gids = [g['id'] for g in d.get('genres', [])]
    c = _db()
    c.execute('INSERT OR REPLACE INTO films VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (
        d['id'], d.get('title') or d.get('original_title'), d.get('original_title'), y, d.get('release_date'), d.get('runtime'),
        json.dumps(gids), json.dumps([gmap.get(g, '') for g in gids], ensure_ascii=False), json.dumps(cs), d.get('original_language'),
        d.get('vote_average'), d.get('vote_count'), d.get('popularity'), d.get('poster_path'), d.get('backdrop_path'), d.get('overview'), d.get('tagline'),
        json.dumps([k['name'] for k in (d.get('keywords') or {}).get('keywords', [])], ensure_ascii=False),
        next((CUISINE[x] for x in cs if x in CUISINE), '其他'), json.dumps([DISH[g] for g in gids if g in DISH], ensure_ascii=False),
        _portion(d.get('runtime')), _tier(d.get('vote_count') or 0, d.get('vote_average') or 0, y), _age(y), '[]', '["现拉"]', time.strftime('%Y-%m-%dT%H:%M:%S')))
    c.commit(); return True


async def tmdb_search_and_add(q: str) -> int:
    key = _tmdb_key()
    if not key: return 0
    async with httpx.AsyncClient(timeout=20) as cli:
        r = await cli.get('https://api.themoviedb.org/3/search/movie', params={'api_key': key, 'language': 'zh-CN', 'query': q, 'include_adult': 'false'})
        if r.status_code != 200: return 0
        hits = [m for m in r.json().get('results', []) if (m.get('vote_average') or 0) >= 5.5][:5]
    n = 0
    for m in hits:
        if await tmdb_fetch_and_upsert(m['id']): n += 1
    return n


# ---------- 主厨工具 ----------
def _clean(s: str) -> str:
    return re.sub(r'[\[\]:|\n]', ' ', str(s or '')).strip()

def card(d: dict) -> str:
    meta = '·'.join(x for x in [d.get('cuisine') or '', '/'.join(d.get('dishes') or []), d.get('portion') or '', f"{d.get('rating') or 0:.1f}分"] if x)
    return f"[bento:{d['tmdb_id']}:{_clean(d.get('title'))}:{d.get('year') or ''}:{d.get('poster') or ''}|{_clean(meta)}]"

def _brief(d: dict) -> str:
    return f"《{d.get('title')}》({d.get('year')}) {d.get('cuisine')}·{'/'.join(d.get('dishes') or [])}·{d.get('portion')}·{d.get('rating') or 0:.1f}分·{d.get('tier')} — {(d.get('overview') or '')[:60]}"

async def tool_dispatch(args: dict) -> str:
    action = str(args.get('action') or 'search').strip()
    c = _db()
    if action in ('search', 'pick', 'random'):
        n = max(1, min(int(args.get('limit') or 3), 6))
        w, p = _where(str(args.get('q') or '').strip(), args.get('cuisine') or '', args.get('dish') or '', args.get('portion') or '', args.get('tier') or '', args.get('age') or '', float(args.get('min_rating') or 6.0))
        excl = ' AND tmdb_id NOT IN (SELECT film_id FROM bento_log)' if action != 'search' else ''
        order = 'RANDOM()' if action == 'random' else 'votes DESC'
        rows = c.execute(f'SELECT {LIGHT} FROM films WHERE {w}{excl} ORDER BY {order} LIMIT ?', p + [n if action != 'pick' else 12]).fetchall()
        if not rows and args.get('q'):
            if await tmdb_search_and_add(str(args.get('q'))):
                rows = c.execute(f'SELECT {LIGHT} FROM films WHERE {w} ORDER BY votes DESC LIMIT ?', p + [n]).fetchall()
        if not rows: return '厨房里没有符合条件的菜。可以放宽菜系/菜色/分量再试,或换个关键词。'
        items = [_row(r) for r in rows]
        if action == 'pick': items = random.sample(items, min(n, len(items)))
        lines = ['找到以下候选。回复Stella时把要推荐的那几条标记原样放进正文(每行一个,会渲染成便当卡),并用你自己的话说说为什么是它:']
        for d in items: lines.append(card(d)); lines.append('  ' + _brief(d))
        return '\n'.join(lines)
    if action == 'detail':
        r = c.execute('SELECT * FROM films WHERE tmdb_id=?', (int(args.get('film_id') or 0),)).fetchone()
        if not r: return '厨房里没有这道菜。'
        d = _row(r); return card(d) + '\n' + _brief(d) + '\n关键词: ' + ', '.join((d.get('keywords') or [])[:12]) + '\n简介: ' + (d.get('overview') or '')[:400]
    if action == 'wish':
        fid = int(args.get('film_id') or 0)
        if not c.execute('SELECT 1 FROM films WHERE tmdb_id=?', (fid,)).fetchone(): return '厨房里没有这道菜。'
        c.execute('INSERT OR IGNORE INTO bento_wish VALUES (?,?)', (fid, time.strftime('%Y-%m-%dT%H:%M:%S'))); c.commit()
        return f'已记进食单(film_id={fid})。'
    if action == 'log':
        fid = int(args.get('film_id') or 0)
        if not c.execute('SELECT 1 FROM films WHERE tmdb_id=?', (fid,)).fetchone(): return '厨房里没有这道菜。'
        now = time.strftime('%Y-%m-%dT%H:%M:%S')
        c.execute('INSERT INTO bento_log (film_id,watched_at,rating_me,rating_k,note_me,note_k,mood_in,mood_out,quote,frame,source,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)', (
            fid, args.get('watched_at') or now[:10], args.get('rating_me'), args.get('rating_k'), (args.get('note_me') or '')[:300], (args.get('note_k') or '')[:300],
            (args.get('mood_in') or '')[:20], (args.get('mood_out') or '')[:20], (args.get('quote') or '')[:200], None, 'chat', now))
        c.execute('DELETE FROM bento_wish WHERE film_id=?', (fid,)); c.commit()
        return f'小票已打印(film_id={fid})。Stella 可以在电影便当的小票页看到。'
    if action == 'wishlist':
        rows = c.execute(f'SELECT {LIGHT} FROM films WHERE tmdb_id IN (SELECT film_id FROM bento_wish) ORDER BY rowid DESC LIMIT 20').fetchall()
        return '食单里还没有菜。' if not rows else '食单(想看):\n' + '\n'.join(_brief(_row(r)) + f' [film_id={r["tmdb_id"]}]' for r in rows)
    if action == 'history':
        rows = c.execute('SELECT l.watched_at, l.rating_me, l.note_me, f.title, f.year, f.tmdb_id FROM bento_log l JOIN films f ON f.tmdb_id=l.film_id ORDER BY l.watched_at DESC LIMIT 15').fetchall()
        return '还没有吃过任何一份便当。' if not rows else '吃过的:\n' + '\n'.join(f"{r['watched_at']} 《{r['title']}》({r['year']}) 她打{r['rating_me'] or '-'}星 {('— ' + r['note_me']) if r['note_me'] else ''} [film_id={r['tmdb_id']}]" for r in rows)
    return f'未知动作 {action}。可用: search/pick/random/detail/wish/log/wishlist/history'


TOOL_DEF = {"type": "function", "function": {
    "name": "bento",
    "description": "电影便当(精神食粮)厨房。action=pick 按Stella的口味和你对她近况的了解挑1-3部(默认排除吃过的); search 按片名/关键词/维度找; random 盲盒; detail 看某部详情; wish 记进食单(想看); log 记吃过了并打印小票(可带评分1-5、心情、一句话); wishlist 看食单; history 看吃过的。维度取值—菜系cuisine: 中餐/港片/日料/韩餐/美餐/法餐/意餐/英餐/泰餐/印度菜/伊朗菜/俄餐/德餐/西班牙菜/拉美菜/北欧菜; 菜色dish: 马卡龙(爱情)/河豚(恐怖)/生鱼片(惊悚)/螃蟹(悬疑)/炸鸡(喜剧)/茶泡饭(剧情)/铁板烧(动作)/分子料理(科幻)/棉花糖(奇幻)/熊饭团(动画)/卤煮(犯罪)/压缩饼干(战争)/清汤(纪录)/蛋包饭(家庭)/老火汤(历史)/汽水(音乐)/篝火烤肉(冒险)/牛排(西部); 分量portion: 一口/小食/正餐/大份/满汉全席; 店铺tier: 老字号/当红/隐藏菜单/家常; 熟成度age: 刚出锅/温热/常温/陈酿/老坛。返回的[bento:...]标记原样放进正文会渲染成便当卡。",
    "parameters": {"type": "object", "properties": {
        "action": {"type": "string", "enum": ["pick", "search", "random", "detail", "wish", "log", "wishlist", "history"]},
        "q": {"type": "string", "description": "片名(中文或原名)或主题关键词。注意: 片库的主题关键词是 TMDB 的英文词(如 insomnia/revenge/road trip), 按主题找请用英文; 中文只对片名有效"},
        "cuisine": {"type": "string"}, "dish": {"type": "string"}, "portion": {"type": "string"}, "tier": {"type": "string"}, "age": {"type": "string"},
        "min_rating": {"type": "number", "description": "最低分,默认6"},
        "limit": {"type": "integer", "description": "返回几部,默认3,最多6"},
        "film_id": {"type": "integer", "description": "detail/wish/log 用的 tmdb_id"},
        "rating_me": {"type": "integer", "description": "log: Stella打的星(1-5)"}, "rating_k": {"type": "integer", "description": "log: 你打的星(1-5)"},
        "note_me": {"type": "string", "description": "log: 她的一句话"}, "note_k": {"type": "string", "description": "log: 你的一句话"},
        "mood_in": {"type": "string", "description": "log: 入场心情一个词"}, "mood_out": {"type": "string", "description": "log: 散场心情一个词"},
        "quote": {"type": "string", "description": "log: 名台词"}, "watched_at": {"type": "string", "description": "log: 观影日期 YYYY-MM-DD, 默认今天"}},
        "required": ["action"]}}}
