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
    eprintln!("D0 cached {}/{} pages", res, tot);
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
    eprintln!("D0 read {} nt={} {}MB {:.4}s {:.2}GB/s", label, nt, total >> 20, s, total as f64 / s / 1e9);
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
    eprintln!("D0 anon {}MB nt={} {:.4}s {:.2}GB/s", mb, nt, s, mb as f64 / 1024.0 / s);
}

/// Wall time of a full run of this binary on `data` in a child process.
fn child_run(data: &Path) -> f64 {
    let base = std::env::var("SCRATCH_DIR").map(std::path::PathBuf::from).unwrap_or_else(|_| std::env::temp_dir());
    let out = base.join(format!("diag_out_{}", std::process::id()));
    let _ = fs::remove_dir_all(&out);
    let t = Instant::now();
    let st = std::process::Command::new("/proc/self/exe")
        .env("SS_NODIAG", "1")
        .env("DATA_DIR", data)
        .env("OUTPUT_DIR", &out)
        .stderr(std::process::Stdio::null())
        .status();
    let s = t.elapsed().as_secs_f64();
    let _ = fs::remove_dir_all(&out);
    if st.map_or(true, |x| !x.success()) {
        return -1.0;
    }
    s
}

fn mmap_touch_unmap(paths: &[std::path::PathBuf]) -> (f64, f64) {
    let t = Instant::now();
    let mut maps = Vec::new();
    let mut acc = 0usize;
    for p in paths {
        let f = fs::File::open(p).unwrap();
        let len = f.metadata().unwrap().len() as usize;
        if len == 0 {
            continue;
        }
        unsafe {
            let m = libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ, libc::MAP_PRIVATE, f.as_raw_fd(), 0) as *const u8;
            let mut k = 0;
            while k < len {
                acc += *m.add(k) as usize;
                k += 4096;
            }
            maps.push((m, len));
        }
    }
    let a = t.elapsed().as_secs_f64();
    let t = Instant::now();
    for (m, len) in maps {
        unsafe { libc::munmap(m as *mut libc::c_void, len) };
    }
    std::hint::black_box(acc);
    (a, t.elapsed().as_secs_f64())
}

fn calib() -> f64 {
    // single-thread integer work, comparable across machines
    let t = Instant::now();
    let mut x = 0x9E3779B97F4A7C15u64;
    for i in 0..200_000_000u64 {
        x = (x ^ i).wrapping_mul(0xD6E8FEB86659FD93).rotate_left(17);
    }
    std::hint::black_box(x);
    t.elapsed().as_secs_f64()
}

/// Runs this binary on `data` in a child process; returns (wall s, phase line).
fn child_phases(data: &Path) -> (f64, String) {
    let base = std::env::var("SCRATCH_DIR").map(std::path::PathBuf::from).unwrap_or_else(|_| std::env::temp_dir());
    let out = base.join(format!("diag_out_{}", std::process::id()));
    let _ = fs::remove_dir_all(&out);
    let t = Instant::now();
    let o = std::process::Command::new("/proc/self/exe")
        .env("SS_NODIAG", "1")
        .env("SS_VERBOSE", "1")
        .env("DATA_DIR", data)
        .env("OUTPUT_DIR", &out)
        .output();
    let s = t.elapsed().as_secs_f64();
    let _ = fs::remove_dir_all(&out);
    let ph = match o {
        Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stderr)
            .lines()
            .filter(|l| l.starts_with('['))
            .map(|l| {
                let v: Vec<&str> = l.trim_start_matches('[').split(']').collect();
                format!("{}={}", v.get(1).unwrap_or(&"").trim(), v[0].trim().trim_end_matches('s'))
            })
            .collect::<Vec<_>>()
            .join(" "),
        Ok(o) => format!("FAILED {:?}", o.status),
        Err(e) => format!("ERR {}", e),
    };
    (s, ph)
}

pub fn run(data: &Path, _nt: usize) {
    let t0 = Instant::now();
    let (w, ph) = child_phases(data);
    eprintln!("V dev: wall {:.3}s | {}", w, ph);
    let genp = std::env::var("SS_GEN").unwrap_or_else(|_| "/app/generate".into());
    let gen = Path::new(&genp);
    let scratch = std::env::var("SCRATCH_DIR").unwrap_or_else(|_| "/scratch".into());
    let g20 = Path::new(&scratch).join("g20");
    if gen.exists() {
        let t = Instant::now();
        let st = std::process::Command::new(gen)
            .args(["--seed", "2", "--events", "20000000", "--clicks", "5000000", "--output"])
            .arg(&g20)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
        eprintln!("V gen20 {:?} in {:.1}s", st.map(|s| s.success()), t.elapsed().as_secs_f64());
        for i in 0..3 {
            let (w, ph) = child_phases(&g20);
            eprintln!("V r{} 20M: wall {:.3}s | {}", i, w, ph);
        }
    } else {
        eprintln!("V no generator in image");
    }
    eprintln!("V diag total {:.1}s", t0.elapsed().as_secs_f64());
}
