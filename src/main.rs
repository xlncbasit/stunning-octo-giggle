use std::collections::HashMap;
use std::fs::{self, File};
use std::hash::{BuildHasherDefault, Hasher};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::thread;
use std::time::Instant;

use memchr::memchr;

mod arena;
mod parquet_out;
#[cfg(feature = "prof")]
mod prof;
mod scan;

use arena::{Arena, WcParts};

use scan::{scan, B};

const GAP_MS: i64 = 1_800_000;
const WEEK_MS: i64 = 604_800_000;
const DAY_MS: i64 = 86_400_000;
const CHUNK: usize = 512 << 10;
const P1_BITS: u32 = 10; // eid partitions (dedup)
const P2_BITS: u32 = 10; // person partitions (sessions)
const P1: usize = 1 << P1_BITS;
const P2: usize = 1 << P2_BITS;
const NONE: u32 = u32::MAX;
const PIPE: usize = 16; // link-table prefetch depth

// ---------- hashing ----------
const K0: u64 = 0xa0761d6478bd642f;
const K1: u64 = 0xe7037ed1a0b428db;
const K2: u64 = 0x8ebc6af09c88c6e3;
const K3: u64 = 0x589965cc75374cc3;

#[inline(always)]
fn mum(a: u64, b: u64) -> u64 {
    let r = (a as u128).wrapping_mul(b as u128);
    (r as u64) ^ ((r >> 64) as u64)
}
#[inline(always)]
fn ld8(s: &[u8], i: usize) -> u64 {
    u64::from_le_bytes(s[i..i + 8].try_into().unwrap())
}
#[inline(always)]
fn ld4(s: &[u8], i: usize) -> u64 {
    u32::from_le_bytes(s[i..i + 4].try_into().unwrap()) as u64
}

/// wyhash-style 128-bit hash, returned as two 64-bit lanes.
#[inline(always)]
fn hash2(s: &[u8]) -> (u64, u64) {
    let n = s.len();
    let mut seed = K0 ^ mum((n as u64) ^ K1, K2);
    let (a, b) = if n <= 16 {
        if n >= 8 {
            (ld8(s, 0), ld8(s, n - 8))
        } else if n >= 4 {
            ((ld4(s, 0) << 32) | ld4(s, n - 4), 0)
        } else if n > 0 {
            (((s[0] as u64) << 16) | ((s[n >> 1] as u64) << 8) | s[n - 1] as u64, 0)
        } else {
            (0, 0)
        }
    } else {
        let mut i = 0;
        while n - i > 16 {
            seed = mum(ld8(s, i) ^ K1, ld8(s, i + 8) ^ seed);
            i += 16;
        }
        (ld8(s, n - 16), ld8(s, n - 8))
    };
    let h1 = mum(a ^ K1, b ^ seed);
    let h2 = mum(a.rotate_left(29) ^ K3, b ^ seed ^ K2);
    (h1, h2)
}
#[inline(always)]
fn hash1(s: &[u8]) -> u64 {
    let h = hash2(s).0 >> 1;
    if h == 0 {
        1
    } else {
        h
    }
}

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

#[inline(always)]
fn pack(s: &[u8]) -> u64 {
    (s.as_ptr() as u64) | ((s.len() as u64) << 48)
}
#[inline(always)]
pub fn unpack(r: u64) -> B {
    unsafe { std::slice::from_raw_parts((r & ((1u64 << 48) - 1)) as *const u8, (r >> 48) as usize) }
}

// ---------- input files read with pread ----------
struct InFile {
    fd: i32,
    len: usize,
    data: B, // mapped; /data is tmpfs on the scorer, so this is copy-free
}

fn open_dir(dir: &Path) -> Vec<InFile> {
    list_csv(dir)
        .iter()
        .map(|p| {
            let data = map_file(p);
            let f = File::open(p).unwrap_or_else(|e| panic!("open {:?}: {}", p, e));
            let fd = f.as_raw_fd();
            std::mem::forget(f);
            InFile { fd, len: data.len(), data }
        })
        .collect()
}

#[derive(Clone, Copy)]
struct RChunk {
    file: u32,
    kind: u8, // 1 events, 2 clicks
    start: usize,
    end: usize,
}

fn make_rchunks(files: &[InFile], kind: u8, out: &mut Vec<RChunk>) {
    for (i, f) in files.iter().enumerate() {
        let mut s = 0;
        while s < f.len {
            let e = (s + CHUNK).min(f.len);
            out.push(RChunk { file: i as u32, kind, start: s, end: e });
            s = e;
        }
    }
}

fn pread_full(fd: i32, buf: &mut [u8], off: usize) {
    let mut done = 0;
    while done < buf.len() {
        let r = unsafe { libc::pread(fd, buf.as_mut_ptr().add(done) as *mut libc::c_void, buf.len() - done, (off + done) as libc::off_t) };
        assert!(r > 0, "pread failed");
        done += r as usize;
    }
}

/// The lines that start in [start, end) (the header line excluded), with the
/// file offset of their first byte.
fn chunk_lines(f: &InFile, start: usize, end: usize) -> (B, usize) {
    let d = f.data;
    let a = start.max(1) - 1;
    let first = match memchr(b'\n', &d[a..]) {
        Some(i) => a + i + 1,
        None => return (&d[0..0], 0),
    };
    if first >= end {
        return (&d[0..0], 0);
    }
    let stop = match memchr(b'\n', &d[end - 1..]) {
        Some(i) => end + i,
        None => d.len(),
    };
    (&d[first..stop], first)
}

// ---------- chunking ----------
#[derive(Clone, Copy)]
struct Chunk {
    data: B,
    start: usize,
    end: usize,
    kind: u8, // 0 links, 1 events, 2 clicks
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

fn make_chunks(files: &[B], kind: u8, out: &mut Vec<Chunk>) {
    for &d in files {
        let first = match memchr(b'\n', d) {
            Some(i) => i + 1,
            None => continue, // empty, or header only without newline
        };
        let mut s = first;
        while s < d.len() {
            let e = align(d, (s + CHUNK).min(d.len()));
            out.push(Chunk { data: d, start: s, end: e, kind });
            s = e;
        }
    }
}

// ---------- timestamps ----------
#[inline(always)]
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

#[inline(always)]
fn dig2(s: &[u8], i: usize) -> Option<i64> {
    let a = s[i].wrapping_sub(b'0');
    let b = s[i + 1].wrapping_sub(b'0');
    if a > 9 || b > 9 {
        None
    } else {
        Some((a * 10 + b) as i64)
    }
}

#[inline(always)]
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
    let dim = match mo {
        2 => {
            if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if d > dim {
        return None;
    }
    Some(((days_from_civil(y, mo, d) * 24 + h) * 60 + mi) * 60_000 + se * 1000)
}

#[inline(always)]
fn parse_ts(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 20 && s[19] == b'Z' {
        return parse_local(s);
    }
    if n == 25 && s[22] == b':' {
        if s[19] != b'+' && s[19] != b'-' {
            return None;
        }
        let l = parse_local(s)?;
        let hh = dig2(s, 20)?;
        let mm = dig2(s, 23)?;
        if hh > 23 || mm > 59 {
            return None;
        }
        let off = (hh * 60 + mm) * 60_000;
        return Some(if s[19] == b'+' { l - off } else { l + off });
    }
    if n == 0 {
        return None;
    }
    let mut v: i64 = 0;
    for &c in s {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v.checked_mul(10)?.checked_add(d as i64)?;
    }
    Some(v)
}

/// Per-thread cache of validated `YYYY-MM-DD` -> days since epoch: a window
/// only ever sees a few dozen dates, so the calendar work is done once each.
struct DateCache {
    k: [u64; 256],
    v: [i64; 256],
}

const BAD_DATE: i64 = i64::MIN;

impl DateCache {
    fn new() -> Self {
        DateCache { k: [0; 256], v: [0; 256] }
    }

    /// Days of the date in s[0..10] (s.len() >= 19), None if not a valid date.
    #[inline(always)]
    fn days(&mut self, s: &[u8]) -> Option<i64> {
        if s[4] != b'-' || s[7] != b'-' {
            return None;
        }
        let key = u32::from_le_bytes(s[0..4].try_into().unwrap()) as u64
            | (u16::from_le_bytes(s[5..7].try_into().unwrap()) as u64) << 32
            | (u16::from_le_bytes(s[8..10].try_into().unwrap()) as u64) << 48;
        let i = (key.wrapping_mul(0x9E3779B97F4A7C15) >> 56) as usize;
        if self.k[i] == key && key != 0 {
            let v = self.v[i];
            return if v == BAD_DATE { None } else { Some(v) };
        }
        let v = Self::compute(s).unwrap_or(BAD_DATE);
        self.k[i] = key;
        self.v[i] = v;
        if v == BAD_DATE {
            None
        } else {
            Some(v)
        }
    }

    fn compute(s: &[u8]) -> Option<i64> {
        let y = dig2(s, 0)? * 100 + dig2(s, 2)?;
        let mo = dig2(s, 5)?;
        let d = dig2(s, 8)?;
        if !(1..=12).contains(&mo) || d < 1 {
            return None;
        }
        let dim = match mo {
            2 => {
                if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 {
                    29
                } else {
                    28
                }
            }
            4 | 6 | 9 | 11 => 30,
            _ => 31,
        };
        if d > dim {
            return None;
        }
        Some(days_from_civil(y, mo, d))
    }
}

/// parse_local through the date cache: YYYY-MM-DDTHH:MM:SS
#[inline(always)]
fn parse_local_c(s: &[u8], dc: &mut DateCache) -> Option<i64> {
    if s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let days = dc.days(s)?;
    let h = dig2(s, 11)?;
    let mi = dig2(s, 14)?;
    let se = dig2(s, 17)?;
    if h > 23 || mi > 59 || se > 59 {
        return None;
    }
    Some(((days * 24 + h) * 60 + mi) * 60_000 + se * 1000)
}

/// parse_ts with the date cache and an overflow-free path for short F3 values.
#[inline(always)]
fn parse_ts_c(s: &[u8], dc: &mut DateCache) -> Option<i64> {
    let n = s.len();
    if n == 20 && s[19] == b'Z' {
        return parse_local_c(s, dc);
    }
    if n == 25 && s[22] == b':' {
        if s[19] != b'+' && s[19] != b'-' {
            return None;
        }
        let l = parse_local_c(s, dc)?;
        let hh = dig2(s, 20)?;
        let mm = dig2(s, 23)?;
        if hh > 23 || mm > 59 {
            return None;
        }
        let off = (hh * 60 + mm) * 60_000;
        return Some(if s[19] == b'+' { l - off } else { l + off });
    }
    if n == 0 {
        return None;
    }
    if n <= 8 {
        return digits8(s).map(|v| v as i64);
    }
    if n <= 16 {
        let hi = digits8(&s[..n - 8])?;
        let lo = digits8(&s[n - 8..])?;
        return Some((hi * 100_000_000 + lo) as i64);
    }
    parse_ts(s)
}

/// Value of 1..=8 ASCII digits (SWAR), None if any byte is not 0-9.
#[inline(always)]
fn digits8(s: &[u8]) -> Option<u64> {
    let mut b = [b'0'; 8];
    b[8 - s.len()..].copy_from_slice(s);
    let c = u64::from_le_bytes(b);
    const HI: u64 = 0xF0F0F0F0F0F0F0F0;
    const ZEROS: u64 = 0x3030303030303030;
    if c & HI != ZEROS || c.wrapping_add(0x0606060606060606) & HI != ZEROS {
        return None;
    }
    let v = c - ZEROS;
    let v = (v * 10 + (v >> 8)) & 0x00FF00FF00FF00FF;
    let v = (v * 100 + (v >> 16)) & 0x0000FFFF0000FFFF;
    Some((v * 10000 + (v >> 32)) & 0xFFFFFFFF)
}

const W_VIEW: u32 = u32::from_le_bytes(*b"view");
const W_CART: u32 = u32::from_le_bytes(*b"cart");
const W_PURCHASE: u64 = u64::from_le_bytes(*b"purchase");

/// 0 view, 1 cart, 2 purchase, 3 anything else (exact, case-sensitive)
#[inline(always)]
fn type_code(t: &[u8]) -> u32 {
    match t.len() {
        4 => match u32::from_le_bytes(t.try_into().unwrap()) {
            W_VIEW => 0,
            W_CART => 1,
            _ => 3,
        },
        8 if u64::from_le_bytes(t.try_into().unwrap()) == W_PURCHASE => 2,
        _ => 3,
    }
}

/// n for an id of the form c_<digits> without leading zeros (1..=7 digits)
#[inline(always)]
fn camp_num(id: &[u8]) -> Option<usize> {
    if id.len() < 3 || id.len() > 9 || id[0] != b'c' || id[1] != b'_' || (id[2] == b'0' && id.len() > 3) {
        return None;
    }
    digits8(&id[2..]).map(|v| v as usize)
}

#[inline(always)]
fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in s {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = (v * 10 + d as u64).min(1 << 40);
    }
    if (1..=10_000_000).contains(&v) {
        Some(v as u32)
    } else {
        None
    }
}

// ---------- records ----------
/// Dedup key for every well-formed event copy.
#[derive(Clone, Copy)]
#[repr(C)]
struct KeyRec {
    h: u64,
    ingest: u32, // seconds since epoch (ingest_ts is F1, whole seconds)
    rf: u32,     // thread << 28 | seq of the ok copy (ParseOut::locs), NONE otherwise
}

/// An ok event copy, routed to its person partition.
#[derive(Clone, Copy)]
#[repr(C)]
struct PRec {
    pkey: u64,
    ts: u32, // ms since base = window_start - 7d
    pp: u32, // price << 8 | killed << 7 | cat << 1 | purchase
}

const KILLED: u32 = 1 << 7;
const SEQ_BITS: u32 = 28;

/// a PRec's place: person partition and index within this thread's part of it
#[inline(always)]
fn rec_pos(p: usize, k: usize) -> u64 {
    ((p as u64) << 32) | k as u64
}

#[derive(Clone, Copy)]
#[repr(C)]
struct CRec {
    pkey: u64,
    pfx: u64, // first 8 bytes of click_id, big-endian
    loc: u64, // file << 56 | offset << 16 | len: where to re-read the full id on a tie
    ts: u32,
    camp: u32,
}

#[inline(always)]
fn p2_of(pkey: u64) -> usize {
    (pkey.wrapping_mul(0x9E3779B97F4A7C15) >> (64 - P2_BITS)) as usize
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

/// Zeroed, never-freed memory on huge pages (the process exits with _exit).
fn huge_zeroed<T>(n: usize) -> &'static [T] {
    let bytes = (n * std::mem::size_of::<T>()).max(4096);
    unsafe {
        let p = libc::mmap(
            std::ptr::null_mut(),
            bytes,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
            -1,
            0,
        );
        assert!(p != libc::MAP_FAILED, "mmap failed");
        libc::madvise(p, bytes, libc::MADV_HUGEPAGE);
        std::slice::from_raw_parts(p as *const T, n)
    }
}
fn zeroed_atomic64(n: usize) -> &'static [AtomicU64] {
    huge_zeroed(n)
}

/// First 8 bytes, zero padded, big-endian: when two prefixes differ they order
/// the ids byte-wise; equal prefixes need a full compare.
#[inline(always)]
fn prefix8(s: &[u8]) -> u64 {
    let mut b = [0u8; 8];
    let n = s.len().min(8);
    b[..n].copy_from_slice(&s[..n]);
    u64::from_be_bytes(b)
}

#[inline(always)]
fn prefetch<T>(p: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_prefetch(p as *const i8, std::arch::x86_64::_MM_HINT_T0);
    }
}

// ---------- small tables ----------
fn read_small(path: &Path) -> Vec<[B; 2]> {
    let d = map_file(path);
    let mut rows = Vec::new();
    if let Some(i) = memchr(b'\n', d) {
        scan::<2, _>(d, i + 1, d.len(), &mut |f: &[B; 2]| rows.push(*f));
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

// ---------- link table (lock-free) ----------
struct Links {
    keys: &'static [AtomicU64],   // id hash, 0 = empty
    refs: &'static [AtomicU64],   // packed bytes of the id
    parent: &'static [AtomicU32], // union-find, set when a slot is claimed; after build: root
    names: &'static [AtomicU64],  // root -> packed smallest id
    mask: usize,
    // lookup table over all ids: [2i] = id hash, [2i+1] = root | 1<<62 for a
    // linked id, or (for a singleton) its copied name | 1<<63, 0 while pending
    lut: &'static [AtomicU64],
    lmask: usize,
    singles: AtomicUsize,
    single_limit: usize,
}

const NAME_FLAG: u64 = 1 << 63;
const LINK_FLAG: u64 = 1 << 62;
const NAME_MAX: usize = (1 << 14) - 1;

impl Links {
    #[inline(always)]
    fn insert(&self, h: u64, r: u64) -> u32 {
        let mut i = h as usize & self.mask;
        loop {
            let k = self.keys[i].load(Relaxed);
            if k == h {
                return i as u32;
            }
            if k == 0 {
                match self.keys[i].compare_exchange(0, h, Relaxed, Relaxed) {
                    Ok(_) => {
                        self.refs[i].store(r, Relaxed);
                        self.parent[i].store(i as u32, Relaxed);
                        return i as u32;
                    }
                    Err(k2) if k2 == h => return i as u32,
                    Err(_) => {}
                }
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(always)]
    fn find(&self, mut x: u32) -> u32 {
        loop {
            let p = self.parent[x as usize].load(Relaxed);
            if p == x {
                return x;
            }
            let g = self.parent[p as usize].load(Relaxed);
            if g != p {
                self.parent[x as usize].store(g, Relaxed);
            }
            x = g;
        }
    }
    fn union(&self, a: u32, b: u32) {
        let (mut a, mut b) = (a, b);
        loop {
            a = self.find(a);
            b = self.find(b);
            if a == b {
                return;
            }
            let (lo, hi) = if a < b { (a, b) } else { (b, a) };
            if self.parent[hi as usize].compare_exchange(hi, lo, Relaxed, Relaxed).is_ok() {
                return;
            }
        }
    }
    #[inline(always)]
    fn prefetch(&self, h: u64) {
        prefetch(&self.lut[2 * (h as usize & self.lmask)]);
    }
    /// person key of an id hash: root | 1<<62 when linked, the hash | 1<<63 otherwise.
    /// The first sighting of a singleton copies its bytes into the table; returns
    /// false when that could not be done (table full or id too long).
    #[inline(always)]
    fn pkey(&self, h: u64, id: &[u8], arena: &mut Arena) -> (u64, bool) {
        let mut i = h as usize & self.lmask;
        loop {
            let k = self.lut[2 * i].load(Relaxed);
            if k == h {
                let v = self.lut[2 * i + 1].load(Relaxed);
                return if v & LINK_FLAG != 0 { (v, true) } else { (h | NAME_FLAG, true) };
            }
            if k == 0 {
                if id.len() > NAME_MAX || self.singles.load(Relaxed) >= self.single_limit {
                    return (h | NAME_FLAG, false);
                }
                if self.lut[2 * i].compare_exchange(0, h, Relaxed, Relaxed).is_ok() {
                    self.singles.fetch_add(1, Relaxed);
                    let p = arena.alloc(id.len());
                    unsafe { std::ptr::copy_nonoverlapping(id.as_ptr(), p, id.len()) };
                    self.lut[2 * i + 1].store(p as u64 | ((id.len() as u64) << 48) | NAME_FLAG, Relaxed);
                    return (h | NAME_FLAG, true);
                }
                continue;
            }
            i = (i + 1) & self.lmask;
        }
    }
    /// person key without inserting (clicks never name a person)
    #[inline(always)]
    fn pkey_only(&self, h: u64) -> u64 {
        let mut i = h as usize & self.lmask;
        loop {
            let k = self.lut[2 * i].load(Relaxed);
            if k == h {
                let v = self.lut[2 * i + 1].load(Relaxed);
                return if v & LINK_FLAG != 0 { v } else { h | NAME_FLAG };
            }
            if k == 0 {
                return h | NAME_FLAG;
            }
            i = (i + 1) & self.lmask;
        }
    }
    /// packed name of a singleton id hash, 0 if it was not stored
    fn single_name(&self, h: u64) -> u64 {
        let mut i = h as usize & self.lmask;
        loop {
            let k = self.lut[2 * i].load(Relaxed);
            if k == h {
                let v = self.lut[2 * i + 1].load(Relaxed);
                return if v & NAME_FLAG != 0 { v & !NAME_FLAG } else { 0 };
            }
            if k == 0 {
                return 0;
            }
            i = (i + 1) & self.lmask;
        }
    }
}

fn build_links(files: &[B], nt: usize, est_singles: usize) -> Links {
    let mut chunks = Vec::new();
    make_chunks(files, 0, &mut chunks);
    let lines: usize = files.iter().map(|d| memchr::memchr_iter(b'\n', d).count() + 1).sum();
    let cap = (2 * lines + 1).next_power_of_two().max(1024); // unique ids <= 2 * lines < cap
    let lk = Links {
        keys: zeroed_atomic64(cap),
        refs: zeroed_atomic64(cap),
        parent: huge_zeroed(cap),
        names: zeroed_atomic64(cap),
        mask: cap - 1,
        lut: &[],
        lmask: 0,
        singles: AtomicUsize::new(0),
        single_limit: 0,
    };
    let cur = AtomicUsize::new(0);
    let pairs: Vec<Vec<(u32, u32)>> = par(nt, |_| {
        let mut v = Vec::new();
        loop {
            let i = cur.fetch_add(1, Relaxed);
            if i >= chunks.len() {
                break;
            }
            let c = chunks[i];
            scan::<2, _>(c.data, c.start, c.end, &mut |f: &[B; 2]| {
                let (a, b) = (f[0], f[1]);
                if !a.is_empty() && !b.is_empty() {
                    let sa = lk.insert(hash1(a), pack(a));
                    let sb = lk.insert(hash1(b), pack(b));
                    if sa != sb {
                        v.push((sa, sb));
                    }
                }
            });
        }
        v
    });
    let cur = AtomicUsize::new(0);
    par(nt, |_| loop {
        let i = cur.fetch_add(1, Relaxed);
        if i >= pairs.len() {
            break;
        }
        for &(a, b) in &pairs[i] {
            lk.union(a, b);
        }
    });
    // compress every occupied slot to its root, then pick each root's smallest id
    let step = (cap + nt - 1) / nt;
    let unique: usize = par(nt, |w| {
        let mut n = 0;
        for s in w * step..((w + 1) * step).min(cap) {
            if lk.keys[s].load(Relaxed) != 0 {
                let r = lk.find(s as u32);
                lk.parent[s].store(r, Relaxed);
                n += 1;
            }
        }
        n
    })
    .iter()
    .sum();
    let lcap = ((unique + est_singles) * 2).next_power_of_two().max(16);
    let mut lk = lk;
    lk.lut = zeroed_atomic64(2 * lcap);
    lk.lmask = lcap - 1;
    lk.single_limit = lcap * 3 / 4 - unique;
    let lk = lk;
    par(nt, |w| {
        for s in w * step..((w + 1) * step).min(cap) {
            if lk.keys[s].load(Relaxed) == 0 {
                continue;
            }
            let r = lk.parent[s].load(Relaxed) as usize;
            let cand = lk.refs[s].load(Relaxed);
            let mut curv = lk.names[r].load(Relaxed);
            loop {
                if curv != 0 && unpack(curv) <= unpack(cand) {
                    break;
                }
                match lk.names[r].compare_exchange(curv, cand, Relaxed, Relaxed) {
                    Ok(_) => break,
                    Err(x) => curv = x,
                }
            }
            let k = lk.keys[s].load(Relaxed);
            let mut i = k as usize & lk.lmask;
            while lk.lut[2 * i].compare_exchange(0, k, Relaxed, Relaxed).is_err() {
                i = (i + 1) & lk.lmask;
            }
            lk.lut[2 * i + 1].store(r as u64 | LINK_FLAG, Relaxed);
        }
    });
    lk
}

// ---------- main ----------
pub struct Ctx {
    base: i64, // window_start - 7d
    ncat: usize,
    ncamp: usize,
    ndays: usize,
    day0: i64,
}

pub struct UserRow {
    pub name: u64,
    pub day: i32,
    pub sessions: u32,
    pub events: u32,
    pub revenue: i64,
    pub topcat: u8,
}

struct WorkerOut {
    users: Vec<UserRow>,
    camp: Vec<[i64; 4]>, // sessions, purchasing, revenue, assisted
}

struct ParseOut {
    arena: Arena,
    keys: WcParts<KeyRec>, // [P1]
    precs: WcParts<PRec>,  // [P2]
    crecs: WcParts<CRec>, // [P2]
    locs: Vec<u64>,          // seq -> rec_pos of each ok event
    snames: Vec<(u64, u64)>, // names the id table could not take: (rec_pos, packed user id), sorted
}
unsafe impl Send for ParseOut {}
unsafe impl Sync for ParseOut {}

fn main() {
    #[cfg(feature = "prof")]
    prof::start();
    let t0 = Instant::now();
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let outd = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into()));
    let nt: usize = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| thread::available_parallelism().map(|n| n.get()).unwrap_or(4))
        .clamp(1, 16);
    let verbose = std::env::var("SS_VERBOSE").is_ok();
    let tick = |what: &str| {
        if verbose {
            eprintln!("[{:7.3}s] {}", t0.elapsed().as_secs_f64(), what)
        }
    };

    let (ws, we) = parse_manifest(&data.join("manifest.json"));
    let base = ws - WEEK_MS;
    assert!(we - base < u32::MAX as i64, "window too long");

    // products & campaigns
    let prows = read_small(&data.join("products.csv"));
    let mut cats: Vec<B> = prows.iter().map(|r| r[1]).collect();
    cats.push(b"unknown");
    cats.sort();
    cats.dedup();
    assert!(cats.len() < 64, "too many categories");
    let ncat = cats.len();
    let unknown_rank = cats.iter().position(|c| *c == b"unknown").unwrap() as u32;
    let mut prod: IdMap<u32> = IdMap::default();
    for r in &prows {
        let rank = cats.binary_search(&r[1]).unwrap() as u32;
        prod.insert(hash1(r[0]), rank);
    }
    let crows = read_small(&data.join("campaigns.csv"));
    let mut camp: IdMap<u32> = IdMap::default();
    let mut camp_names: Vec<(B, B)> = Vec::new();
    for r in &crows {
        let h = hash1(r[0]);
        if !camp.contains_key(&h) {
            camp.insert(h, camp_names.len() as u32);
            camp_names.push((r[0], r[1]));
        }
    }
    let ncamp = camp_names.len();
    // campaign ids written as c_<n> (no leading zeros) resolve by n, checked byte for byte
    let mut camp_by_num: Vec<u32> = Vec::new();
    for (i, (id, _)) in camp_names.iter().enumerate() {
        if let Some(n) = camp_num(id) {
            if n < 1 << 20 {
                if camp_by_num.len() <= n {
                    camp_by_num.resize(n + 1, u32::MAX);
                }
                camp_by_num[n] = i as u32;
            }
        }
    }
    let camp_lookup = |cid: &[u8]| -> Option<u32> {
        if let Some(n) = camp_num(cid) {
            if let Some(&i) = camp_by_num.get(n) {
                if i != u32::MAX && camp_names[i as usize].0 == cid {
                    return Some(i);
                }
            }
        }
        camp.get(&hash1(cid)).copied()
    };
    let day0 = ws.div_euclid(DAY_MS);
    let ndays = (((we - 1).div_euclid(DAY_MS)) - day0 + 1) as usize;
    let ctx = Ctx { base, ncat, ncamp, ndays, day0 };

    let ev_files = open_dir(&data.join("events"));
    let ck_files = open_dir(&data.join("clicks"));
    assert!(ck_files.len() < 256, "too many click files");
    CLICK_FDS.set(ck_files.iter().map(|f| f.fd).collect()).unwrap();
    let lk_files: Vec<B> = list_csv(&data.join("links")).iter().map(|p| map_file(p)).collect();
    tick("opened");

    let ev_bytes: usize = ev_files.iter().map(|f| f.len).sum();
    let links = build_links(&lk_files, nt, ev_bytes / 85 / 24 + 1024);
    tick("links");

    // ---- one parse pass over events + clicks ----
    let mut chunks = Vec::new();
    make_rchunks(&ev_files, 1, &mut chunks);
    make_rchunks(&ck_files, 2, &mut chunks);
    let cur = AtomicUsize::new(0);
    let parsed: Vec<ParseOut> = par(nt, |tid| {
        let mut po = ParseOut {
            arena: Arena::new(),
            keys: WcParts::new(P1),
            precs: WcParts::new(P2),
            crecs: WcParts::new(P2),
            locs: Vec::with_capacity(ev_bytes / 70 / nt + 1024),
            snames: Vec::new(),
        };
        // route a resolved ok event and its dedup key (which records where the event
        // went); a singleton whose name the table could not take keeps a copy in snames
        let resolve = |po: &mut ParseOut, uh: u64, uref: u64, mut r: PRec, kp: usize, mut key: KeyRec| {
            let uid = unpack(uref);
            let (pk, named) = links.pkey(uh, uid, &mut po.arena);
            r.pkey = pk;
            let p = p2_of(pk);
            let pos = rec_pos(p, po.precs.next_index(p));
            let seq = po.locs.len();
            assert!(seq < (1 << SEQ_BITS) - 1, "too many events for one thread");
            key.rf = ((tid as u32) << SEQ_BITS) | seq as u32;
            po.locs.push(pos);
            if !named {
                let c = po.arena.alloc(uid.len());
                unsafe { std::ptr::copy_nonoverlapping(uid.as_ptr(), c, uid.len()) };
                po.snames.push((pos, c as u64 | ((uid.len() as u64) << 48)));
            }
            po.precs.push(&mut po.arena, p, r);
            po.keys.push(&mut po.arena, kp, key);
        };
        // prefetch pipelines: (uid hash, uid, record, key partition, key)
        let mut ep: [(u64, u64, PRec, usize, KeyRec); PIPE] = [(0, 0, PRec { pkey: 0, ts: 0, pp: 0 }, 0, KeyRec { h: 0, ingest: 0, rf: 0 }); PIPE];
        let mut cp: [(u64, CRec); PIPE] = [(0, CRec { pkey: 0, pfx: 0, loc: 0, ts: 0, camp: 0 }); PIPE];
        let (mut en, mut cn) = (0usize, 0usize);
        let mut dc = DateCache::new();
        loop {
            let i = cur.fetch_add(1, Relaxed);
            if i >= chunks.len() {
                break;
            }
            let c = chunks[i];
            let fl = if c.kind == 1 { &ev_files[c.file as usize] } else { &ck_files[c.file as usize] };
            let (data, off0) = chunk_lines(fl, c.start, c.end);
            if c.kind == 1 {
                scan::<7, _>(data, 0, data.len(), &mut |f: &[&[u8]; 7]| {
                    let [eid, uid, ts, ing, typ, pid, price] = *f;
                    if eid.is_empty() {
                        return;
                    }
                    let (h1, h2) = hash2(eid);
                    let ing_ms = parse_ts_c(ing, &mut dc);
                    let ingest = match ing_ms {
                        Some(m) => (m.div_euclid(1000)).clamp(0, u32::MAX as i64 - 1) as u32,
                        None => u32::MAX,
                    };
                    let tcode = type_code(typ);
                    let kp = (h2 >> (64 - P1_BITS)) as usize;
                    let key = KeyRec { h: h1 | 1, ingest, rf: NONE };
                    'ok: {
                        if tcode == 3 || uid.is_empty() || pid.is_empty() {
                            break 'ok;
                        }
                        let Some(im) = ing_ms else { break 'ok };
                        let Some(t) = parse_ts_c(ts, &mut dc) else { break 'ok };
                        if t < ws || t >= we || im - t > DAY_MS {
                            break 'ok;
                        }
                        let mut pp = 0u32;
                        if tcode == 2 {
                            let Some(p) = parse_price(price) else { break 'ok };
                            let cat = *prod.get(&hash1(pid)).unwrap_or(&unknown_rank);
                            pp = (p << 8) | (cat << 1) | 1;
                        }
                        let uh = hash1(uid);
                        links.prefetch(uh);
                        let slot = en % PIPE;
                        if en >= PIPE {
                            let (oh, ur, r, okp, okey) = ep[slot];
                            resolve(&mut po, oh, ur, r, okp, okey);
                        }
                        ep[slot] = (uh, pack(uid), PRec { pkey: 0, ts: (t - base) as u32, pp }, kp, key);
                        en += 1;
                        return;
                    }
                    po.keys.push(&mut po.arena, kp, key);
                });
            } else {
                scan::<4, _>(data, 0, data.len(), &mut |f: &[&[u8]; 4]| {
                    let [id, uid, cid, ts] = *f;
                    if id.is_empty() || uid.is_empty() {
                        return;
                    }
                    let Some(t) = parse_ts_c(ts, &mut dc) else { return };
                    if t < base || t >= we {
                        return;
                    }
                    let Some(cpn) = camp_lookup(cid) else { return };
                    let loc = (c.file as u64) << 56 | ((off0 + (id.as_ptr() as usize - data.as_ptr() as usize)) as u64) << 16 | id.len().min(0xFFFF) as u64;
                    let uh = hash1(uid);
                    links.prefetch(uh);
                    let slot = cn % PIPE;
                    if cn >= PIPE {
                        let (oh, mut r) = cp[slot];
                        r.pkey = links.pkey_only(oh);
                        po.crecs.push(&mut po.arena, p2_of(r.pkey), r);
                    }
                    cp[slot] = (uh, CRec { pkey: 0, pfx: prefix8(id), loc, ts: (t - base) as u32, camp: cpn });
                    cn += 1;
                });
            }
        }
        for k in en.saturating_sub(PIPE)..en {
            let (oh, ur, r, okp, okey) = ep[k % PIPE];
            resolve(&mut po, oh, ur, r, okp, okey);
        }
        po.snames.sort_unstable();
        for k in cn.saturating_sub(PIPE)..cn {
            let (oh, mut r) = cp[k % PIPE];
            r.pkey = links.pkey_only(oh);
            po.crecs.push(&mut po.arena, p2_of(r.pkey), r);
        }
        po.keys.finish(&mut po.arena);
        po.crecs.finish(&mut po.arena);
        po.precs.finish(&mut po.arena);
        po
    });
    tick("parsed");

    // ---- dedup: flag every non-winning ok copy in its PRec ----
    let cur = AtomicUsize::new(0);
    par(nt, |_| {
        let mut table: Vec<KeyRec> = Vec::new();
        let killit = |rf: u32| {
            if rf != NONE {
                let t = (rf >> SEQ_BITS) as usize;
                let pos = parsed[t].locs[(rf & ((1 << SEQ_BITS) - 1)) as usize];
                let r = parsed[t].precs.ptr_at((pos >> 32) as usize, pos as u32 as usize);
                unsafe { AtomicU32::from_ptr(std::ptr::addr_of_mut!((*r).pp)).fetch_or(KILLED, Relaxed) };
            }
        };
        loop {
            let p = cur.fetch_add(1, Relaxed);
            if p >= P1 {
                break;
            }
            let n: usize = parsed.iter().map(|po| po.keys.len(p)).sum();
            let cap = (n * 2).next_power_of_two().max(16);
            let mask = cap - 1;
            table.clear();
            table.resize(cap, KeyRec { h: 0, ingest: 0, rf: 0 });
            for po in &parsed {
                po.keys.for_each(p, |_, k| {
                    let mut i = k.h as usize & mask;
                    loop {
                        let e = &mut table[i];
                        if e.h == 0 {
                            *e = *k;
                            break;
                        }
                        if e.h == k.h {
                            if k.ingest < e.ingest {
                                killit(e.rf);
                                *e = *k;
                            } else {
                                killit(k.rf);
                            }
                            break;
                        }
                        i = (i + 1) & mask;
                    }
                });
            }
        }
    });
    tick("deduped");

    // ---- per person partition ----
    let mut order: Vec<usize> = (0..P2).collect();
    let psize = |p: usize| -> usize { parsed.iter().map(|po| po.precs.len(p)).sum() };
    order.sort_by_key(|&p| std::cmp::Reverse(psize(p)));
    let cur = AtomicUsize::new(0);
    let outs: Vec<WorkerOut> = par(nt, |_| {
        let mut wo = WorkerOut { users: Vec::new(), camp: vec![[0i64; 4]; (ctx.ncamp + 1) * ctx.ndays] };
        let mut st = PState::new(ctx.ncamp);
        loop {
            let i = cur.fetch_add(1, Relaxed);
            if i >= P2 {
                break;
            }
            st.run(&ctx, &links, &parsed, order[i], &mut wo);
        }
        wo
    });
    tick("sessions built");

    let users: Vec<&[UserRow]> = outs.iter().map(|o| o.users.as_slice()).collect();
    let camps = merge_campaign(&outs, &ctx);
    parquet_out::write_all(&outd, &users, &camps, &cats, &camp_names, nt);
    tick("written");
    #[cfg(feature = "prof")]
    prof::dump("/dev/shm/prof.txt");
    if verbose {
        if let Ok(sm) = fs::read_to_string("/proc/self/smaps_rollup") {
            for l in sm.lines().filter(|l| l.starts_with("Rss") || l.starts_with("AnonHuge") || l.starts_with("Anonymous")) {
                eprintln!("  {}", l);
            }
        }
    }
    // skip freeing GBs of buffers at exit
    unsafe { libc::_exit(0) }
}

pub struct CampRow {
    pub camp: usize,
    pub day: i32,
    pub v: [i64; 4],
}

fn merge_campaign(outs: &[WorkerOut], ctx: &Ctx) -> Vec<CampRow> {
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

// ---------- sessions per person partition ----------
#[derive(Clone, Copy)]
struct Ev {
    ts: u32,
    pp: u32,
}
#[derive(Clone, Copy)]
struct Ck {
    ts: u32,
    camp: u32,
    pfx: u64,
    loc: u64,
}

static CLICK_FDS: std::sync::OnceLock<Vec<i32>> = std::sync::OnceLock::new();

/// click_id of `a` < click_id of `b`, byte-wise; re-reads both ids from their
/// files only when the 8-byte prefixes tie
fn click_id_lt(a: &Ck, b: &Ck) -> bool {
    if a.pfx != b.pfx {
        return a.pfx < b.pfx;
    }
    let read = |loc: u64| -> Vec<u8> {
        let fd = CLICK_FDS.get().unwrap()[(loc >> 56) as usize];
        let mut v = vec![0u8; (loc & 0xFFFF) as usize];
        pread_full(fd, &mut v, ((loc >> 16) & ((1 << 40) - 1)) as usize);
        v
    };
    read(a.loc) < read(b.loc)
}

/// pkey -> local person id; starts small and doubles at half load.
struct PTab {
    e: Vec<(u64, u32)>,
    mask: usize,
    n: usize,
}

impl PTab {
    #[inline(always)]
    fn idx(pkey: u64, mask: usize) -> usize {
        (pkey.wrapping_mul(0x9E3779B97F4A7C15) >> 20) as usize & mask
    }
    fn reset(&mut self, cap: usize) {
        self.e.clear();
        self.e.resize(cap, (0, 0));
        self.mask = cap - 1;
        self.n = 0;
    }
    #[inline(always)]
    fn get(&self, pkey: u64) -> Option<u32> {
        let mut i = Self::idx(pkey, self.mask);
        loop {
            let (k, v) = self.e[i];
            if k == pkey {
                return Some(v);
            }
            if k == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
    /// Returns (pid, inserted)
    #[inline(always)]
    fn get_or_insert(&mut self, pkey: u64, next: u32) -> (u32, bool) {
        let mut i = Self::idx(pkey, self.mask);
        loop {
            let (k, v) = self.e[i];
            if k == pkey {
                return (v, false);
            }
            if k == 0 {
                self.e[i] = (pkey, next);
                self.n += 1;
                if self.n * 2 > self.e.len() {
                    self.grow();
                }
                return (next, true);
            }
            i = (i + 1) & self.mask;
        }
    }
    #[cold]
    fn grow(&mut self) {
        let old = std::mem::take(&mut self.e);
        let cap = old.len() * 2;
        self.e.resize(cap, (0, 0));
        self.mask = cap - 1;
        for (k, v) in old {
            if k != 0 {
                let mut i = Self::idx(k, self.mask);
                while self.e[i].0 != 0 {
                    i = (i + 1) & self.mask;
                }
                self.e[i] = (k, v);
            }
        }
    }
}

struct PState {
    tab: PTab,
    pkeys: Vec<u64>,
    pname: Vec<u64>,
    ecount: Vec<u32>,
    ccount: Vec<u32>,
    epid: Vec<u32>,
    evs: Vec<Ev>,
    eout: Vec<Ev>,
    cks: Vec<(u32, Ck)>,
    cout: Vec<Ck>,
    cnt: Vec<u32>,
    pos: Vec<u32>,
    active: Vec<u32>,
}

impl PState {
    fn new(ncamp: usize) -> Self {
        PState {
            tab: PTab { e: Vec::new(), mask: 0, n: 0 },
            pkeys: Vec::new(),
            pname: Vec::new(),
            ecount: Vec::new(),
            ccount: Vec::new(),
            epid: Vec::new(),
            evs: Vec::new(),
            eout: Vec::new(),
            cks: Vec::new(),
            cout: Vec::new(),
            cnt: vec![0; ncamp + 1],
            pos: vec![0; ncamp + 1],
            active: Vec::new(),
        }
    }

    fn run(&mut self, ctx: &Ctx, links: &Links, parsed: &[ParseOut], p: usize, wo: &mut WorkerOut) {
        let n: usize = parsed.iter().map(|po| po.precs.len(p)).sum();
        if n == 0 {
            return;
        }
        self.tab.reset((n / 8).next_power_of_two().max(256));
        self.pkeys.clear();
        self.pname.clear();
        self.ecount.clear();
        self.epid.clear();
        self.evs.clear();
        // group surviving events by person
        for (t, po) in parsed.iter().enumerate() {
            po.precs.for_each(p, |k, r| {
                if r.pp & KILLED != 0 {
                    return;
                }
                let pp = r.pp;
                let (pid, new) = self.tab.get_or_insert(r.pkey, self.pkeys.len() as u32);
                if new {
                    self.pkeys.push(r.pkey);
                    let name = if r.pkey >> 63 == 1 {
                        match links.single_name(r.pkey & !NAME_FLAG) {
                            0 => {
                                let sn = &parsed[t].snames;
                                let pos = rec_pos(p, k);
                                sn[sn.partition_point(|x| x.0 < pos)].1
                            }
                            n => n,
                        }
                    } else {
                        links.names[(r.pkey & 0xFFFF_FFFF) as usize].load(Relaxed)
                    };
                    self.pname.push(name);
                    self.ecount.push(0);
                }
                self.ecount[pid as usize] += 1;
                self.epid.push(pid);
                self.evs.push(Ev { ts: r.ts, pp });
            });
        }
        let np = self.pkeys.len();
        // counting sort by person
        let mut acc = 0u32;
        for c in self.ecount.iter_mut() {
            let x = *c;
            *c = acc;
            acc += x;
        }
        self.eout.clear();
        self.eout.resize(self.evs.len(), Ev { ts: 0, pp: 0 });
        let mut fill = self.ecount.clone();
        for (k, e) in self.evs.iter().enumerate() {
            let pid = self.epid[k] as usize;
            self.eout[fill[pid] as usize] = *e;
            fill[pid] += 1;
        }
        // clicks of persons that have events
        self.cks.clear();
        self.ccount.clear();
        self.ccount.resize(np, 0);
        for po in parsed {
            po.crecs.for_each(p, |_, c| {
                if let Some(pid) = self.tab.get(c.pkey) {
                    self.ccount[pid as usize] += 1;
                    self.cks.push((pid, Ck { ts: c.ts, camp: c.camp, pfx: c.pfx, loc: c.loc }));
                }
            });
        }
        let mut acc = 0u32;
        for c in self.ccount.iter_mut() {
            let x = *c;
            *c = acc;
            acc += x;
        }
        self.cout.clear();
        self.cout.resize(self.cks.len(), Ck { ts: 0, camp: 0, pfx: 0, loc: 0 });
        let mut cfill = self.ccount.clone();
        for &(pid, c) in &self.cks {
            self.cout[cfill[pid as usize] as usize] = c;
            cfill[pid as usize] += 1;
        }
        for pid in 0..np {
            let (es, ee) = (self.ecount[pid] as usize, fill[pid] as usize);
            let (cs, ce) = (self.ccount[pid] as usize, cfill[pid] as usize);
            let ev = &mut self.eout[es..ee];
            if ev.len() > 1 {
                ev.sort_unstable_by_key(|e| e.ts);
            }
            let ck = &mut self.cout[cs..ce];
            if ck.len() > 1 {
                ck.sort_unstable_by_key(|c| c.ts);
            }
            person(ctx, self.pname[pid], &self.eout[es..ee], &self.cout[cs..ce], wo, &mut self.cnt, &mut self.pos, &mut self.active);
        }
    }
}

#[inline]
fn cadd(cnt: &mut [u32], pos: &mut [u32], active: &mut Vec<u32>, c: u32) {
    let cu = c as usize;
    if cnt[cu] == 0 {
        pos[cu] = active.len() as u32;
        active.push(c);
    }
    cnt[cu] += 1;
}

#[inline]
fn cdel(cnt: &mut [u32], pos: &mut [u32], active: &mut Vec<u32>, c: u32) {
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

#[allow(clippy::too_many_arguments)]
fn person(ctx: &Ctx, name: u64, evs: &[Ev], cks: &[Ck], wo: &mut WorkerOut, cnt: &mut [u32], pos: &mut [u32], active: &mut Vec<u32>) {
    let nd = ctx.ndays;
    let organic = ctx.ncamp;
    let mut lo = 0usize;
    let mut hi = 0usize;
    let mut prev_ts = 0u32;
    let mut in_sess = false;
    let mut s_camp = 0usize;
    let mut s_day = 0usize;
    let mut s_rev = 0i64;
    let mut s_purch = false;
    let mut a_day: i32 = i32::MIN;
    let (mut a_sess, mut a_ev, mut a_rev) = (0u32, 0u32, 0i64);
    let mut a_cat = [0i64; 128];
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
        if !in_sess || (e.ts - prev_ts) as i64 > GAP_MS {
            finalize_session!();
            in_sess = true;
            let start = e.ts;
            let day_abs = (ctx.base + start as i64).div_euclid(DAY_MS);
            s_day = (day_abs - ctx.day0) as usize;
            while hi < cks.len() && cks[hi].ts <= start {
                cadd(cnt, pos, active, cks[hi].camp);
                hi += 1;
            }
            while lo < hi && (cks[lo].ts as i64) < start as i64 - WEEK_MS {
                cdel(cnt, pos, active, cks[lo].camp);
                lo += 1;
            }
            if hi > lo {
                let t = cks[hi - 1].ts;
                let mut best = hi - 1;
                let mut k = hi - 1;
                while k > lo && cks[k - 1].ts == t {
                    k -= 1;
                    if click_id_lt(&cks[k], &cks[best]) {
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
        if e.pp & 1 == 1 {
            let price = (e.pp >> 8) as i64;
            s_rev += price;
            s_purch = true;
            a_rev += price;
            a_cat[((e.pp >> 1) & 63) as usize] += price;
        }
        prev_ts = e.ts;
    }
    finalize_session!();
    flush_day!();
    for i in lo..hi {
        cdel(cnt, pos, active, cks[i].camp);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// parse_ts_c (date cache + SWAR digits) must agree with parse_ts everywhere.
    #[test]
    fn fast_ts_matches_reference() {
        let mut dc = DateCache::new();
        let fixed: &[&[u8]] = &[
            b"2026-10-03T23:58:10Z", b"2026-10-03T19:58:10-04:00", b"1791071890000", b"0001791071890000",
            b"1791071890000.0", b"1.79e12", b"-5", b"86400000", b"123456789012345678901", b"",
            b"2026-02-29T00:00:00Z", b"2028-02-29T00:00:00Z", b"2026-13-01T00:00:00Z", b"2026-10-03T24:00:00Z",
            b"2026-10-03T23:59:60Z", b"2026-10-03 23:58:10Z", b"2026-10-03t23:58:10Z", b"2026-10-03T23:58:10z",
            b"2026-10-03T23:58:10+24:00", b"2026-10-03T23:58:10+05:60", b"2026-10-03T23:58:10+0200",
            b"2026-10-03T23:58:10-00:00", b"2026-10-03T23:58:10+23:59", b"0", b"9", b"12345678", b"123456789",
            b"9999999999999999", b"99999999999999999", b"999999999999999999", b"9223372036854775807",
            b"9223372036854775808", b"\xd9\xa2\xd9\xa5", b"1\r", b"2026-10-03T23:58:10Z\r", b"/0", b":0",
        ];
        for s in fixed {
            assert_eq!(parse_ts_c(s, &mut dc), parse_ts(s), "{:?}", String::from_utf8_lossy(s));
        }
        // random strings over a digit-heavy alphabet, all lengths 0..=26
        let alphabet = b"0123456789-:TZ+/ .\xff";
        let mut x = 0x2545F4914F6CDD1Du64;
        for _ in 0..2_000_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let n = (x % 27) as usize;
            let mut v = Vec::with_capacity(n);
            let mut y = x;
            for i in 0..n {
                y = y.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407 + i as u64);
                let pick = (y >> 33) as usize;
                // mostly digits so long all-digit strings are common
                v.push(if pick % 4 != 0 { b'0' + (pick % 10) as u8 } else { alphabet[pick % alphabet.len()] });
            }
            assert_eq!(parse_ts_c(&v, &mut dc), parse_ts(&v), "{:?}", v);
        }
    }

    #[test]
    fn digits8_and_camp_num() {
        for k in 1..=8usize {
            for v in [0u64, 1, 7, 42, 99_999_999, 12_345_678, 10_000_000] {
                let s = format!("{:0width$}", v % 10u64.pow(k as u32), width = k);
                assert_eq!(digits8(s.as_bytes()), Some(v % 10u64.pow(k as u32)), "{}", s);
            }
        }
        assert_eq!(digits8(b"12a4"), None);
        assert_eq!(digits8(b"1/34"), None);
        assert_eq!(digits8(b"1:34"), None);
        assert_eq!(camp_num(b"c_123"), Some(123));
        assert_eq!(camp_num(b"c_0"), Some(0));
        assert_eq!(camp_num(b"c_01"), None);
        assert_eq!(camp_num(b"C_1"), None);
        assert_eq!(camp_num(b"c_"), None);
        assert_eq!(camp_num(b"c_1 "), None);
    }
}
