//! Poor man's sampling profiler (local builds with --features prof only):
//! SIGPROF every 100 us of process CPU time records the interrupted RIP.

use std::sync::atomic::{AtomicUsize, Ordering};

const N: usize = 1 << 22;
static mut BUF: [u64; N] = [0; N];
static IDX: AtomicUsize = AtomicUsize::new(0);

extern "C" fn handler(_sig: i32, _info: *mut libc::siginfo_t, ctx: *mut libc::c_void) {
    unsafe {
        let uc = ctx as *mut libc::ucontext_t;
        let rip = (*uc).uc_mcontext.gregs[libc::REG_RIP as usize] as u64;
        let i = IDX.fetch_add(1, Ordering::Relaxed);
        if i < N {
            *std::ptr::addr_of_mut!(BUF[i]) = rip;
        }
    }
}

pub fn start() {
    unsafe {
        let mut sa: libc::sigaction = std::mem::zeroed();
        sa.sa_sigaction = handler as usize;
        sa.sa_flags = libc::SA_SIGINFO | libc::SA_RESTART;
        libc::sigaction(libc::SIGPROF, &sa, std::ptr::null_mut());
        let it = libc::itimerval {
            it_interval: libc::timeval { tv_sec: 0, tv_usec: 100 },
            it_value: libc::timeval { tv_sec: 0, tv_usec: 100 },
        };
        libc::setitimer(libc::ITIMER_PROF, &it, std::ptr::null_mut());
    }
}

pub fn dump(path: &str) {
    unsafe {
        let off = libc::itimerval { it_interval: libc::timeval { tv_sec: 0, tv_usec: 0 }, it_value: libc::timeval { tv_sec: 0, tv_usec: 0 } };
        libc::setitimer(libc::ITIMER_PROF, &off, std::ptr::null_mut());
    }
    let exe = std::fs::read_link("/proc/self/exe").unwrap();
    let maps = std::fs::read_to_string("/proc/self/maps").unwrap();
    let mut base = 0u64;
    for l in maps.lines() {
        if l.ends_with(exe.to_str().unwrap()) {
            let f: Vec<&str> = l.split_whitespace().collect();
            if u64::from_str_radix(f[2], 16).unwrap() == 0 {
                base = u64::from_str_radix(f[0].split('-').next().unwrap(), 16).unwrap();
                break;
            }
        }
    }
    let n = IDX.load(Ordering::Relaxed).min(N);
    let mut s = String::with_capacity(n * 12);
    for i in 0..n {
        let r = unsafe { *std::ptr::addr_of!(BUF[i]) };
        s.push_str(&format!("{:x}\n", r.wrapping_sub(base)));
    }
    std::fs::write(path, s).unwrap();
    eprintln!("prof: {} samples -> {}", n, path);
}
