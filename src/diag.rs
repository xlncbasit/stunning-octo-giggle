//! Diagnostics for the scorer machine, printed to stderr. Only used on the
//! `diag` branch: it runs on the public dev set and then fails on purpose so
//! the scorer returns the logs.

use std::fs;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::time::Instant;

fn grep(path: &str, keys: &[&str]) {
    if let Ok(s) = fs::read_to_string(path) {
        let mut seen = std::collections::HashSet::new();
        for l in s.lines() {
            for k in keys {
                if l.starts_with(k) && seen.insert(l.to_string()) {
                    eprintln!("  {}: {}", path, l);
                }
            }
        }
    } else {
        eprintln!("  {}: unreadable", path);
    }
}

fn files(data: &Path) -> Vec<std::path::PathBuf> {
    let mut v = Vec::new();
    for d in ["events", "clicks", "links"] {
        if let Ok(rd) = fs::read_dir(data.join(d)) {
            for e in rd.flatten() {
                v.push(e.path());
            }
        }
    }
    v.sort();
    v
}

/// Fraction of each file's pages already in the page cache (mincore).
fn residency(paths: &[std::path::PathBuf]) {
    let (mut res, mut tot) = (0usize, 0usize);
    for p in paths {
        let f = fs::File::open(p).unwrap();
        let len = f.metadata().unwrap().len() as usize;
        if len == 0 {
            continue;
        }
        unsafe {
            let m = libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ, libc::MAP_PRIVATE, f.as_raw_fd(), 0);
            let pages = (len + 4095) / 4096;
            let mut v = vec![0u8; pages];
            if libc::mincore(m, len, v.as_mut_ptr()) == 0 {
                res += v.iter().filter(|&&b| b & 1 == 1).count();
            }
            tot += pages;
            libc::munmap(m, len);
        }
    }
    eprintln!("  page cache residency before reading: {} of {} pages ({:.1}%)", res, tot, 100.0 * res as f64 / tot.max(1) as f64);
}

fn read_all(paths: &[std::path::PathBuf], nt: usize, label: &str) {
    let t = Instant::now();
    let cur = std::sync::atomic::AtomicUsize::new(0);
    let total: usize = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nt)
            .map(|_| {
                let cur = &cur;
                s.spawn(move || {
                    let mut buf = vec![0u8; 1 << 20];
                    let mut n = 0usize;
                    loop {
                        let i = cur.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        if i >= paths.len() {
                            break;
                        }
                        let f = fs::File::open(&paths[i]).unwrap();
                        let mut off = 0;
                        loop {
                            let r = unsafe { libc::pread(f.as_raw_fd(), buf.as_mut_ptr() as *mut libc::c_void, buf.len(), off) };
                            if r <= 0 {
                                break;
                            }
                            n += r as usize;
                            off += r as i64;
                        }
                    }
                    n
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).sum()
    });
    let s = t.elapsed().as_secs_f64();
    eprintln!("  read {} with {} threads: {} MB in {:.3}s = {:.2} GB/s", label, nt, total >> 20, s, total as f64 / s / 1e9);
}

fn anon_touch(mb: usize, nt: usize) {
    let t = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..nt {
            s.spawn(move || unsafe {
                let per = (mb << 20) / nt;
                let p = libc::mmap(std::ptr::null_mut(), per, libc::PROT_READ | libc::PROT_WRITE, libc::MAP_PRIVATE | libc::MAP_ANONYMOUS, -1, 0) as *mut u8;
                libc::madvise(p as *mut libc::c_void, per, libc::MADV_HUGEPAGE);
                let mut k = 0;
                while k < per {
                    *p.add(k) = 1;
                    k += 4096;
                }
            });
        }
    });
    let s = t.elapsed().as_secs_f64();
    eprintln!("  fresh anon memory: {} MB touched by {} threads in {:.3}s = {:.2} GB/s", mb, nt, s, mb as f64 / 1024.0 / s);
}

pub fn run(data: &Path, nt: usize) {
    let t0 = Instant::now();
    eprintln!("=== DIAG (scorer environment) ===");
    let up = fs::read_to_string("/proc/uptime").unwrap_or_default();
    eprintln!("  /proc/uptime: {}", up.trim());
    if let Ok(st) = fs::read_to_string("/proc/self/stat") {
        let f: Vec<&str> = st.rsplit(')').next().unwrap_or("").split_whitespace().collect();
        eprintln!("  process starttime (clock ticks since boot): {}", f.get(19).unwrap_or(&"?"));
    }
    grep("/proc/cpuinfo", &["model name", "cpu MHz", "cache size", "siblings", "cpu cores"]);
    if let Ok(s) = fs::read_to_string("/proc/cpuinfo") {
        let fl = s.lines().find(|l| l.starts_with("flags")).unwrap_or("");
        let has = |x: &str| fl.split_whitespace().any(|w| w == x);
        eprintln!("  flags: avx2={} avx512f={} avx512bw={} bmi2={}", has("avx2"), has("avx512f"), has("avx512bw"), has("bmi2"));
    }
    eprintln!("  available_parallelism: {:?}", std::thread::available_parallelism().map(|n| n.get()));
    grep("/proc/meminfo", &["MemTotal", "MemFree", "MemAvailable", "Cached", "AnonHugePages"]);
    for p in ["/sys/fs/cgroup/cpu.max", "/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/cpuset.cpus.effective", "/sys/kernel/mm/transparent_hugepage/enabled", "/sys/kernel/mm/transparent_hugepage/defrag"] {
        eprintln!("  {}: {}", p, fs::read_to_string(p).unwrap_or_else(|_| "?".into()).trim());
    }
    if let Ok(m) = fs::read_to_string("/proc/mounts") {
        for l in m.lines().filter(|l| l.contains(" /data ") || l.contains(" /output ") || l.contains(" /scratch ") || l.contains(" / ")) {
            eprintln!("  mount: {}", l);
        }
    }
    eprintln!("  kernel: {}", fs::read_to_string("/proc/version").unwrap_or_default().trim());
    let paths = files(data);
    residency(&paths);
    read_all(&paths, 1, "first pass");
    residency(&paths);
    read_all(&paths, 1, "second pass");
    read_all(&paths, nt, "third pass");
    anon_touch(512, nt);
    anon_touch(512, 1);
    eprintln!("  diag took {:.3}s", t0.elapsed().as_secs_f64());
}
