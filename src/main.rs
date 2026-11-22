use std::collections::HashMap;
use std::fs::{self, File};
use std::hash::{BuildHasherDefault, Hasher};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Instant;

use memchr::memchr;
use xxhash_rust::xxh3::{xxh3_128, xxh3_64};

mod parquet_out;

const GAP_MS: i64 = 1_800_000;
const WEEK_MS: i64 = 604_800_000;
const DAY_MS: i64 = 86_400_000;
const NB: usize = 256;
const CHUNK: usize = 8 << 20;

type B = &'static [u8];

// ---------- identity hasher for pre-hashed u64 keys ----------
#[derive(Default)]
struct Id(u64);
impl Hasher for Id {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, _: &[u8]) {
        unreachable!()
    }
    fn write_u64(&mut self, v: u64) {
        self.0 = v;
    }
}
type IdMap<V> = HashMap<u64, V, BuildHasherDefault<Id>>;

// ---------- mmap ----------
fn map_file(path: &Path) -> B {
    let f = File::open(path).unwrap_or_else(|e| panic!("open {:?}: {}", path, e));
    let len = f.metadata().unwrap().len() as usize;
    if len == 0 {
        return &[];
    }
    unsafe {
        let p = libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ, libc::MAP_PRIVATE, f.as_raw_fd(), 0);
        if p == libc::MAP_FAILED {
            panic!("mmap {:?} failed", path);
        }
        libc::madvise(p, len, libc::MADV_WILLNEED);
        std::slice::from_raw_parts(p as *const u8, len)
    }
}

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map_or(false, |x| x == "csv") && p.is_file())
            .collect(),
        Err(_) => vec![],
    };
    v.sort();
    v
}

fn pack(s: B) -> u64 {
    (s.as_ptr() as u64) | ((s.len() as u64) << 48)
}
pub fn unpack(r: u64) -> B {
    unsafe { std::slice::from_raw_parts((r & ((1u64 << 48) - 1)) as *const u8, (r >> 48) as usize) }
}

// ---------- chunking ----------
struct Chunk {
    data: B,
    start: usize,
    end: usize,
}

fn align(data: B, s: usize) -> usize {
    if s == 0 {
        return 0;
    }
    if s >= data.len() {
        return data.len();
    }
    if data[s - 1] == b'\n' {
        return s;
    }
    match memchr(b'\n', &data[s..]) {
        Some(i) => s + i + 1,
        None => data.len(),
    }
}

fn make_chunks(files: &[B]) -> Vec<Chunk> {
    let mut v = Vec::new();
    for &d in files {
        if d.is_empty() {
            continue;
        }
        // skip header line
        let first = match memchr(b'\n', d) {
            Some(i) => i + 1,
            None => continue,
        };
        let mut s = 0usize;
        while s < d.len() {
            let ne = (s + CHUNK).min(d.len());
            let e = if ne >= d.len() { d.len() } else { align(d, ne) };
            let st = if s == 0 { first } else { s };
            if st < e {
                v.push(Chunk { data: d, start: st, end: e });
            }
            s = e;
            if e == d.len() {
                break;
            }
        }
    }
    v
}

#[inline]
fn for_lines(c: &Chunk, mut f: impl FnMut(B)) {
    let d = c.data;
    let mut pos = c.start;
    while pos < c.end {
        let e = match memchr(b'\n', &d[pos..]) {
            Some(i) => pos + i,
            None => d.len(),
        };
        f(&d[pos..e]);
        pos = e + 1;
    }
}

// ---------- CSV fields ----------
#[inline]
fn unq(f: B) -> Option<B> {
    if f.is_empty() {
        return Some(f);
    }
    if f[0] == b'"' {
        if f.len() < 2 || f[f.len() - 1] != b'"' {
            return None;
        }
        let inner = &f[1..f.len() - 1];
        if memchr(b'"', inner).is_some() {
            None
        } else {
            Some(inner)
        }
    } else if memchr(b'"', f).is_some() {
        None
    } else {
        Some(f)
    }
}

#[inline]
fn split<const N: usize>(line: B) -> Option<[B; N]> {
    let mut out: [B; N] = [&line[0..0]; N];
    let mut n = 0;
    let mut s = 0;
    loop {
        match memchr(b',', &line[s..]) {
            Some(i) => {
                if n >= N - 1 {
                    return None;
                }
                out[n] = unq(&line[s..s + i])?;
                n += 1;
                s += i + 1;
            }
            None => {
                if n != N - 1 {
                    return None;
                }
                out[n] = unq(&line[s..])?;
                return Some(out);
            }
        }
    }
}

// ---------- timestamps ----------
#[inline]
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

#[inline]
fn dig2(s: &[u8], i: usize) -> Option<i64> {
    let a = s[i].wrapping_sub(b'0');
    let b = s[i + 1].wrapping_sub(b'0');
    if a > 9 || b > 9 {
        None
    } else {
        Some((a * 10 + b) as i64)
    }
}

fn parse_local(s: &[u8]) -> Option<i64> {
    // YYYY-MM-DDTHH:MM:SS
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let y = dig2(s, 0)? * 100 + dig2(s, 2)?;
    let mo = dig2(s, 5)?;
    let d = dig2(s, 8)?;
    let h = dig2(s, 11)?;
    let mi = dig2(s, 14)?;
    let se = dig2(s, 17)?;
    if !(1..=12).contains(&mo) || h > 23 || mi > 59 || se > 59 || d < 1 {
        return None;
    }
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let dim = match mo {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if leap {
                29
            } else {
                28
            }
        }
    };
    if d > dim {
        return None;
    }
    Some(((days_from_civil(y, mo, d) * 24 + h) * 60 + mi) * 60_000 + se * 1000)
}

fn parse_ts(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 0 {
        return None;
    }
    if s.iter().all(|c| c.is_ascii_digit()) {
        let mut v: i64 = 0;
        for &c in s {
            v = v.checked_mul(10)?.checked_add((c - b'0') as i64)?;
        }
        return Some(v);
    }
    if n == 20 && s[19] == b'Z' {
        return parse_local(s);
    }
    if n == 25 && (s[19] == b'+' || s[19] == b'-') && s[22] == b':' {
        let l = parse_local(s)?;
        let hh = dig2(s, 20)?;
        let mm = dig2(s, 23)?;
        if hh > 23 || mm > 59 {
            return None;
        }
        let off = (hh * 60 + mm) * 60_000;
        return Some(if s[19] == b'+' { l - off } else { l + off });
    }
    None
}

#[inline]
fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in s {
        if !c.is_ascii_digit() {
            return None;
        }
        v = (v * 10 + (c - b'0') as u64).min(1 << 40);
    }
    if (1..=10_000_000).contains(&v) {
        Some(v as u32)
    } else {
        None
    }
}

// ---------- records ----------
#[derive(Clone, Copy)]
#[repr(C)]
struct Ev {
    e1: u64,
    ingest: i64,
    ts: i64,
    uh: u64,
    uref: u64,
    e2: u32,
    price: u32,
    cat: u8,
    typ: u8, // 0 view, 1 cart, 2 purchase, 3 invalid copy
}

#[derive(Clone, Copy)]
#[repr(C)]
struct Pv {
    pkey: u64,
    ts: i64,
    uref: u64,
    price: u32,
    cat: u8,
    typ: u8,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct Ck {
    pkey: u64,
    ts: i64,
    idref: u64,
    camp: u16,
}

#[inline]
fn pbucket(pkey: u64) -> usize {
    (pkey.wrapping_mul(0x9E3779B97F4A7C15) >> 56) as usize
}

fn par<T: Send>(nt: usize, f: impl Fn(usize) -> T + Sync) -> Vec<T> {
    thread::scope(|s| {
        let hs: Vec<_> = (0..nt)
            .map(|w| {
                let f = &f;
                s.spawn(move || f(w))
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

// ---------- small tables ----------
fn read_small(path: &Path) -> Vec<Vec<B>> {
    let d = map_file(path);
    let mut rows = Vec::new();
    let mut first = true;
    let mut pos = 0;
    while pos < d.len() {
        let e = match memchr(b'\n', &d[pos..]) {
            Some(i) => pos + i,
            None => d.len(),
        };
        let line = &d[pos..e];
        pos = e + 1;
        if first {
            first = false;
            continue;
        }
        if let Some(f) = split::<2>(line) {
            rows.push(vec![f[0], f[1]]);
        }
    }
    rows
}

fn parse_manifest(path: &Path) -> (i64, i64) {
    let s = fs::read_to_string(path).unwrap();
    let get = |key: &str| -> i64 {
        let i = s.find(key).unwrap();
        let rest = &s[i + key.len()..];
        let q1 = rest.find('"').unwrap();
        let rest = &rest[q1 + 1..];
        let q2 = rest.find('"').unwrap();
        let rest = &rest[q2 + 1..];
        let q3 = rest.find('"').unwrap();
        parse_ts(rest[..q3].as_bytes()).unwrap()
    };
    (get("window_start"), get("window_end"))
}

// ---------- link table ----------
struct Links {
    map: IdMap<u32>, // id hash -> root
    names: Vec<u64>, // root -> name ref
}

fn find(p: &mut [u32], mut x: u32) -> u32 {
    while p[x as usize] != x {
        let g = p[p[x as usize] as usize];
        p[x as usize] = g;
        x = g;
    }
    x
}

fn build_links(files: &[B], nt: usize) -> Links {
    let chunks = make_chunks(files);
    let cur = AtomicUsize::new(0);
    let parts: Vec<Vec<(u64, B, u64, B)>> = par(nt, |_| {
        let mut v = Vec::new();
        loop {
            let i = cur.fetch_add(1, Ordering::Relaxed);
            if i >= chunks.len() {
                break;
            }
            for_lines(&chunks[i], |line| {
                if let Some([a, b]) = split::<2>(line) {
                    if !a.is_empty() && !b.is_empty() {
                        v.push((xxh3_64(a), a, xxh3_64(b), b));
                    }
                }
            });
        }
        v
    });
    let mut map: IdMap<u32> = IdMap::default();
    let mut refs: Vec<B> = Vec::new();
    let mut parent: Vec<u32> = Vec::new();
    let mut intern = |h: u64, s: B, map: &mut IdMap<u32>, refs: &mut Vec<B>, parent: &mut Vec<u32>| -> u32 {
        *map.entry(h).or_insert_with(|| {
            let i = refs.len() as u32;
            refs.push(s);
            parent.push(i);
            i
        })
    };
    for part in &parts {
        for &(ha, a, hb, b) in part {
            let ia = intern(ha, a, &mut map, &mut refs, &mut parent);
            let ib = intern(hb, b, &mut map, &mut refs, &mut parent);
            let ra = find(&mut parent, ia);
            let rb = find(&mut parent, ib);
            if ra != rb {
                let (lo, hi) = if ra < rb { (ra, rb) } else { (rb, ra) };
                parent[hi as usize] = lo;
            }
        }
    }
    let n = refs.len();
    let mut names: Vec<u64> = vec![0; n];
    let mut best: Vec<Option<B>> = vec![None; n];
    for i in 0..n {
        let r = find(&mut parent, i as u32) as usize;
        match best[r] {
            Some(b) if b <= refs[i] => {}
            _ => best[r] = Some(refs[i]),
        }
    }
    for r in 0..n {
        if let Some(b) = best[r] {
            names[r] = pack(b);
        }
    }
    for (_, v) in map.iter_mut() {
        *v = find(&mut parent, *v);
    }
    Links { map, names }
}

#[inline]
fn person_key(links: &Links, uh: u64) -> u64 {
    match links.map.get(&uh) {
        Some(&r) => r as u64,
        None => uh | (1 << 63),
    }
}

// ---------- main ----------
struct Ctx {
    ws: i64,
    we: i64,
    ncat: usize,
    ncamp: usize,
    ndays: usize,
    day0: i64,
}

struct UserRow {
    name: u64,
    day: i32,
    sessions: u32,
    events: u32,
    revenue: i64,
    topcat: u8,
}

struct WorkerOut {
    users: Vec<UserRow>,
    camp: Vec<[i64; 4]>, // sessions, purchasing, revenue, assisted
}

fn main() {
    let t0 = Instant::now();
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let outd = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into()));
    let nt: usize = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| thread::available_parallelism().map(|n| n.get()).unwrap_or(4));
    let tick = |what: &str| eprintln!("[{:7.3}s] {}", t0.elapsed().as_secs_f64(), what);

    let (ws, we) = parse_manifest(&data.join("manifest.json"));

    // products & campaigns
    let prows = read_small(&data.join("products.csv"));
    let mut cats: Vec<B> = prows.iter().map(|r| r[1]).collect();
    cats.push(b"unknown");
    cats.sort();
    cats.dedup();
    let ncat = cats.len();
    let unknown_rank = cats.iter().position(|c| *c == b"unknown").unwrap() as u8;
    let mut prod: IdMap<u8> = IdMap::default();
    for r in &prows {
        let rank = cats.iter().position(|c| *c == r[1]).unwrap() as u8;
        prod.insert(xxh3_64(r[0]), rank);
    }
    let crows = read_small(&data.join("campaigns.csv"));
    let mut camp: IdMap<u16> = IdMap::default();
    let mut camp_names: Vec<(B, B)> = Vec::new();
    for r in &crows {
        let h = xxh3_64(r[0]);
        if !camp.contains_key(&h) {
            camp.insert(h, camp_names.len() as u16);
            camp_names.push((r[0], r[1]));
        }
    }
    let ncamp = camp_names.len();
    let day0 = ws.div_euclid(DAY_MS);
    let ndays = (((we - 1).div_euclid(DAY_MS)) - day0 + 1) as usize;
    let ctx = Ctx { ws, we, ncat, ncamp, ndays, day0 };

    let map_dir = |name: &str| -> Vec<B> { list_csv(&data.join(name)).iter().map(|p| map_file(p)).collect() };
    let ev_files = map_dir("events");
    let ck_files = map_dir("clicks");
    let lk_files = map_dir("links");
    tick("mapped");

    let links = build_links(&lk_files, nt);
    tick("links");

    // ---- clicks ----
    let ck_chunks = make_chunks(&ck_files);
    let cur = AtomicUsize::new(0);
    let ck_pb: Vec<Vec<Vec<Ck>>> = par(nt, |_| {
        let mut pb: Vec<Vec<Ck>> = (0..NB).map(|_| Vec::new()).collect();
        loop {
            let i = cur.fetch_add(1, Ordering::Relaxed);
            if i >= ck_chunks.len() {
                break;
            }
            for_lines(&ck_chunks[i], |line| {
                let Some([id, uid, cid, ts]) = split::<4>(line) else { return };
                if id.is_empty() || uid.is_empty() {
                    return;
                }
                let Some(t) = parse_ts(ts) else { return };
                if t < ctx.ws - WEEK_MS || t >= ctx.we {
                    return;
                }
                let Some(&c) = camp.get(&xxh3_64(cid)) else { return };
                let pk = person_key(&links, xxh3_64(uid));
                pb[pbucket(pk)].push(Ck { pkey: pk, ts: t, idref: pack(id), camp: c });
            });
        }
        pb
    });
    tick("clicks parsed");

    // ---- events pass 1 ----
    let ev_chunks = make_chunks(&ev_files);
    let cur = AtomicUsize::new(0);
    let ev_b: Vec<Vec<Vec<Ev>>> = par(nt, |_| {
        let mut eb: Vec<Vec<Ev>> = (0..NB).map(|_| Vec::new()).collect();
        loop {
            let i = cur.fetch_add(1, Ordering::Relaxed);
            if i >= ev_chunks.len() {
                break;
            }
            for_lines(&ev_chunks[i], |line| {
                let Some([eid, uid, ts, ing, typ, pid, price]) = split::<7>(line) else { return };
                if eid.is_empty() {
                    return;
                }
                let h = xxh3_128(eid);
                let e1 = h as u64;
                let e2 = (h >> 64) as u32;
                let ing_ms = parse_ts(ing);
                let tcode: u8 = match typ {
                    b"view" => 0,
                    b"cart" => 1,
                    b"purchase" => 2,
                    _ => 3,
                };
                let mut ok = ing_ms.is_some() && tcode < 3 && !uid.is_empty() && !pid.is_empty();
                let mut tsv = 0i64;
                if ok {
                    match parse_ts(ts) {
                        Some(t) if t >= ctx.ws && t < ctx.we => tsv = t,
                        _ => ok = false,
                    }
                }
                let mut pr = 0u32;
                if ok && tcode == 2 {
                    match parse_price(price) {
                        Some(p) => pr = p,
                        None => ok = false,
                    }
                }
                if ok && ing_ms.unwrap() - tsv > DAY_MS {
                    ok = false;
                }
                let rec = if ok {
                    let cat = if tcode == 2 { *prod.get(&xxh3_64(pid)).unwrap_or(&unknown_rank) } else { 0 };
                    Ev { e1, e2, ingest: ing_ms.unwrap(), ts: tsv, uh: xxh3_64(uid), uref: pack(uid), price: pr, cat, typ: tcode }
                } else {
                    Ev { e1, e2, ingest: ing_ms.unwrap_or(i64::MAX), ts: 0, uh: 0, uref: 0, price: 0, cat: 0, typ: 3 }
                };
                eb[(e1 >> 56) as usize].push(rec);
            });
        }
        eb
    });
    tick("events parsed");

    // ---- dedup + route to person buckets ----
    let cur = AtomicUsize::new(0);
    let pv_pb: Vec<Vec<Vec<Pv>>> = par(nt, |_| {
        let mut pb: Vec<Vec<Pv>> = (0..NB).map(|_| Vec::new()).collect();
        let mut tmp: Vec<Ev> = Vec::new();
        loop {
            let b = cur.fetch_add(1, Ordering::Relaxed);
            if b >= NB {
                break;
            }
            tmp.clear();
            for w in &ev_b {
                tmp.extend_from_slice(&w[b]);
            }
            tmp.sort_unstable_by_key(|e| (e.e1, e.e2, e.ingest));
            let mut i = 0;
            while i < tmp.len() {
                let e = tmp[i];
                let mut j = i + 1;
                while j < tmp.len() && tmp[j].e1 == e.e1 && tmp[j].e2 == e.e2 {
                    j += 1;
                }
                i = j;
                if e.typ == 3 {
                    continue;
                }
                let pk = person_key(&links, e.uh);
                pb[pbucket(pk)].push(Pv { pkey: pk, ts: e.ts, uref: e.uref, price: e.price, cat: e.cat, typ: e.typ });
            }
        }
        pb
    });
    drop(ev_b);
    tick("deduped");

    // ---- per person ----
    let cur = AtomicUsize::new(0);
    let outs: Vec<WorkerOut> = par(nt, |_| {
        let mut wo = WorkerOut { users: Vec::new(), camp: vec![[0i64; 4]; (ctx.ncamp + 1) * ctx.ndays] };
        let mut evs: Vec<Pv> = Vec::new();
        let mut cks: Vec<Ck> = Vec::new();
        let mut cnt = vec![0u32; ctx.ncamp + 1];
        let mut pos = vec![0u32; ctx.ncamp + 1];
        let mut active: Vec<u16> = Vec::new();
        loop {
            let b = cur.fetch_add(1, Ordering::Relaxed);
            if b >= NB {
                break;
            }
            evs.clear();
            cks.clear();
            for w in &pv_pb {
                evs.extend_from_slice(&w[b]);
            }
            for w in &ck_pb {
                cks.extend_from_slice(&w[b]);
            }
            evs.sort_unstable_by_key(|e| (e.pkey, e.ts));
            cks.sort_unstable_by_key(|c| (c.pkey, c.ts));
            let mut ci = 0usize;
            let mut ei = 0usize;
            while ei < evs.len() {
                let pk = evs[ei].pkey;
                let mut ej = ei + 1;
                while ej < evs.len() && evs[ej].pkey == pk {
                    ej += 1;
                }
                while ci < cks.len() && cks[ci].pkey < pk {
                    ci += 1;
                }
                let cs = ci;
                let mut ce = cs;
                while ce < cks.len() && cks[ce].pkey == pk {
                    ce += 1;
                }
                ci = ce;
                let name = if pk >> 63 == 1 { evs[ei].uref } else { links.names[pk as usize] };
                person(&ctx, name, &evs[ei..ej], &cks[cs..ce], &mut wo, &mut cnt, &mut pos, &mut active);
                ei = ej;
            }
        }
        wo
    });
    tick("sessions built");

    parquet_out::write_all(&outd, &outs_users(&outs), &merge_campaign(&outs, &ctx, &camp_names, &cats), &cats, &camp_names, nt);
    tick("written");
    // skip freeing GBs of buffers at exit
    unsafe { libc::_exit(0) }
}

struct CampRow {
    camp: usize,
    day: i32,
    v: [i64; 4],
}

fn outs_users(outs: &[WorkerOut]) -> Vec<&[UserRow]> {
    outs.iter().map(|o| o.users.as_slice()).collect()
}

fn merge_campaign(outs: &[WorkerOut], ctx: &Ctx, _cn: &[(B, B)], _cats: &[B]) -> Vec<CampRow> {
    let mut rows = Vec::new();
    for c in 0..=ctx.ncamp {
        for d in 0..ctx.ndays {
            let mut v = [0i64; 4];
            for o in outs {
                let x = o.camp[c * ctx.ndays + d];
                for k in 0..4 {
                    v[k] += x[k];
                }
            }
            if v[0] > 0 || v[3] > 0 {
                rows.push(CampRow { camp: c, day: (ctx.day0 + d as i64) as i32, v });
            }
        }
    }
    rows
}

#[inline]
fn cadd(cnt: &mut [u32], pos: &mut [u32], active: &mut Vec<u16>, c: u16) {
    let cu = c as usize;
    if cnt[cu] == 0 {
        pos[cu] = active.len() as u32;
        active.push(c);
    }
    cnt[cu] += 1;
}

#[inline]
fn cdel(cnt: &mut [u32], pos: &mut [u32], active: &mut Vec<u16>, c: u16) {
    let cu = c as usize;
    cnt[cu] -= 1;
    if cnt[cu] == 0 {
        let p = pos[cu] as usize;
        let last = *active.last().unwrap();
        active.swap_remove(p);
        if last != c {
            pos[last as usize] = p as u32;
        }
    }
}

fn person(
    ctx: &Ctx,
    name: u64,
    evs: &[Pv],
    cks: &[Ck],
    wo: &mut WorkerOut,
    cnt: &mut [u32],
    pos: &mut [u32],
    active: &mut Vec<u16>,
) {
    let nd = ctx.ndays;
    let organic = ctx.ncamp;
    let mut lo = 0usize;
    let mut hi = 0usize;
    let mut prev_ts = i64::MIN;
    let mut in_sess = false;
    // session state
    let mut s_camp = 0usize;
    let mut s_day = 0usize;
    let mut s_rev = 0i64;
    let mut s_purch = false;
    // day accumulator
    let mut a_day: i32 = i32::MIN;
    let (mut a_sess, mut a_ev, mut a_rev) = (0u32, 0u32, 0i64);
    let mut a_cat = [0i64; 256];
    let ncat = ctx.ncat;

    macro_rules! finalize_session {
        () => {
            if in_sess {
                let r = &mut wo.camp[s_camp * nd + s_day];
                r[0] += 1;
                if s_purch {
                    r[1] += 1;
                }
                r[2] += s_rev;
            }
        };
    }
    macro_rules! flush_day {
        () => {
            if a_day != i32::MIN {
                let mut top = 255u8;
                if a_rev > 0 {
                    let mut best = 0i64;
                    for k in 0..ncat {
                        if a_cat[k] > best {
                            best = a_cat[k];
                            top = k as u8;
                        }
                    }
                }
                wo.users.push(UserRow { name, day: a_day, sessions: a_sess, events: a_ev, revenue: a_rev, topcat: top });
                a_sess = 0;
                a_ev = 0;
                a_rev = 0;
                for k in 0..ncat {
                    a_cat[k] = 0;
                }
            }
        };
    }

    for e in evs {
        if !in_sess || e.ts - prev_ts > GAP_MS {
            finalize_session!();
            in_sess = true;
            let start = e.ts;
            let day_abs = start.div_euclid(DAY_MS);
            s_day = (day_abs - ctx.day0) as usize;
            while hi < cks.len() && cks[hi].ts <= start {
                cadd(cnt, pos, active, cks[hi].camp);
                hi += 1;
            }
            while lo < hi && cks[lo].ts < start - WEEK_MS {
                cdel(cnt, pos, active, cks[lo].camp);
                lo += 1;
            }
            if hi > lo {
                let t = cks[hi - 1].ts;
                let mut best = hi - 1;
                let mut k = hi - 1;
                while k > lo && cks[k - 1].ts == t {
                    k -= 1;
                    if unpack(cks[k].idref) < unpack(cks[best].idref) {
                        best = k;
                    }
                }
                s_camp = cks[best].camp as usize;
                for &c in active.iter() {
                    wo.camp[c as usize * nd + s_day][3] += 1;
                }
            } else {
                s_camp = organic;
            }
            s_rev = 0;
            s_purch = false;
            let d32 = day_abs as i32;
            if d32 != a_day {
                flush_day!();
                a_day = d32;
            }
            a_sess += 1;
        }
        a_ev += 1;
        if e.typ == 2 {
            s_rev += e.price as i64;
            s_purch = true;
            a_rev += e.price as i64;
            a_cat[e.cat as usize] += e.price as i64;
        }
        prev_ts = e.ts;
    }
    finalize_session!();
    flush_day!();
    for i in lo..hi {
        cdel(cnt, pos, active, cks[i].camp);
    }
}
