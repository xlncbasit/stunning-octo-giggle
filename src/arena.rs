//! Per-thread bump arena on huge pages, and partitioned append buffers made of
//! fixed-size blocks carved from it: no Vec doubling, no copies, few page faults.

const REGION: usize = 256 << 20;
const BLK: usize = 4096;

pub struct Arena {
    cur: usize,
    end: usize,
}

impl Arena {
    pub fn new() -> Self {
        Arena { cur: 0, end: 0 }
    }

    #[inline(never)]
    fn refill(&mut self, bytes: usize) {
        let sz = REGION.max(bytes);
        unsafe {
            let p = libc::mmap(
                std::ptr::null_mut(),
                sz,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
                -1,
                0,
            );
            assert!(p != libc::MAP_FAILED, "arena mmap failed");
            libc::madvise(p, sz, libc::MADV_HUGEPAGE);
            self.cur = p as usize;
            self.end = self.cur + sz;
        }
    }

    /// A 64-byte aligned block (streaming stores need the alignment).
    #[inline(always)]
    pub fn alloc_block(&mut self, bytes: usize) -> *mut u8 {
        self.cur = (self.cur + 63) & !63;
        self.alloc(bytes)
    }

    #[inline(always)]
    pub fn alloc(&mut self, bytes: usize) -> *mut u8 {
        if self.cur + bytes > self.end {
            self.refill(bytes);
        }
        let r = self.cur;
        self.cur += bytes;
        r as *mut u8
    }
}

pub struct Parts<T> {
    cur: Vec<(*mut T, u32)>,
    full: Vec<Vec<*const T>>,
}

unsafe impl<T: Send> Send for Parts<T> {}
unsafe impl<T: Sync> Sync for Parts<T> {}

impl<T: Copy> Parts<T> {
    const PER: usize = BLK / std::mem::size_of::<T>();

    pub fn new(np: usize) -> Self {
        Parts { cur: vec![(std::ptr::null_mut(), Self::PER as u32); np], full: (0..np).map(|_| Vec::new()).collect() }
    }

    #[inline(always)]
    pub fn push(&mut self, arena: &mut Arena, p: usize, v: T) {
        let c = unsafe { self.cur.get_unchecked_mut(p) };
        if c.1 as usize == Self::PER {
            if !c.0.is_null() {
                self.full[p].push(c.0);
            }
            c.0 = arena.alloc_block(BLK) as *mut T;
            c.1 = 0;
        }
        unsafe { c.0.add(c.1 as usize).write(v) };
        c.1 += 1;
    }

    pub fn len(&self, p: usize) -> usize {
        let c = self.cur[p];
        self.full[p].len() * Self::PER + if c.0.is_null() { 0 } else { c.1 as usize }
    }

    #[inline(always)]
    pub fn for_each(&self, p: usize, mut f: impl FnMut(&T)) {
        for &b in &self.full[p] {
            for v in unsafe { std::slice::from_raw_parts(b, Self::PER) } {
                f(v);
            }
        }
        let c = self.cur[p];
        if !c.0.is_null() {
            for v in unsafe { std::slice::from_raw_parts(c.0 as *const T, c.1 as usize) } {
                f(v);
            }
        }
    }
}

#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct Line([u8; 64]);

/// Partitioned buffers for 16- or 32-byte records with software write-combining: each
/// partition stages one cache line, and full lines go out with non-temporal
/// stores, so scattering to many partitions costs little more than one stream.
pub struct WcParts<T> {
    stage: Vec<Line>,
    fill: Vec<u8>,
    cur: Vec<(*mut T, u32)>,
    full: Vec<Vec<*const T>>,
}

unsafe impl<T: Send> Send for WcParts<T> {}
unsafe impl<T: Sync> Sync for WcParts<T> {}

impl<T: Copy> WcParts<T> {
    const SZ: usize = std::mem::size_of::<T>();
    const PER: usize = BLK / Self::SZ;
    const LINE: usize = 64 / Self::SZ; // records per cache line

    pub fn new(np: usize) -> Self {
        assert!(Self::SZ == 16 || Self::SZ == 32);
        WcParts {
            stage: vec![Line([0; 64]); np],
            fill: vec![0; np],
            cur: vec![(std::ptr::null_mut(), Self::PER as u32); np],
            full: (0..np).map(|_| Vec::new()).collect(),
        }
    }

    #[inline(always)]
    fn room(&mut self, arena: &mut Arena, p: usize) -> *mut T {
        let c = unsafe { self.cur.get_unchecked_mut(p) };
        if c.1 as usize == Self::PER {
            if !c.0.is_null() {
                self.full[p].push(c.0);
            }
            c.0 = arena.alloc_block(BLK) as *mut T;
            c.1 = 0;
        }
        unsafe { c.0.add(c.1 as usize) }
    }

    #[inline(always)]
    pub fn push(&mut self, arena: &mut Arena, p: usize, v: T) {
        let f = unsafe { *self.fill.get_unchecked(p) } as usize;
        let line = unsafe { self.stage.get_unchecked_mut(p) };
        unsafe { (line.0.as_mut_ptr() as *mut T).add(f).write(v) };
        if f == Self::LINE - 1 {
            let dst = self.room(arena, p);
            unsafe { stream64(dst as *mut u8, self.stage.get_unchecked(p).0.as_ptr()) };
            self.cur[p].1 += Self::LINE as u32;
            self.fill[p] = 0;
        } else {
            self.fill[p] = (f + 1) as u8;
        }
    }

    /// Flush partial lines; must be called once before the buffers are read.
    pub fn finish(&mut self, arena: &mut Arena) {
        for p in 0..self.fill.len() {
            let f = self.fill[p] as usize;
            if f > 0 {
                let dst = self.room(arena, p);
                unsafe { std::ptr::copy_nonoverlapping(self.stage[p].0.as_ptr() as *const T, dst, f) };
                self.cur[p].1 += f as u32;
                self.fill[p] = 0;
            }
        }
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_sfence()
        };
    }

    pub fn len(&self, p: usize) -> usize {
        let c = self.cur[p];
        self.full[p].len() * Self::PER + if c.0.is_null() { 0 } else { c.1 as usize }
    }

    /// Index the next record pushed to partition `p` will get.
    #[inline(always)]
    pub fn next_index(&self, p: usize) -> usize {
        let c = unsafe { self.cur.get_unchecked(p) };
        self.full[p].len() * Self::PER + if c.0.is_null() { 0 } else { c.1 as usize } + unsafe { *self.fill.get_unchecked(p) } as usize
    }

    /// Address of record `k` of partition `p` (after `finish`).
    #[inline(always)]
    pub fn ptr_at(&self, p: usize, k: usize) -> *mut T {
        let (b, i) = (k / Self::PER, k % Self::PER);
        let base = if b < self.full[p].len() { self.full[p][b] as *mut T } else { self.cur[p].0 };
        unsafe { base.add(i) }
    }

    #[inline(always)]
    pub fn for_each(&self, p: usize, mut f: impl FnMut(usize, &T)) {
        let mut k = 0;
        for &b in &self.full[p] {
            for v in unsafe { std::slice::from_raw_parts(b, Self::PER) } {
                f(k, v);
                k += 1;
            }
        }
        let c = self.cur[p];
        if !c.0.is_null() {
            for v in unsafe { std::slice::from_raw_parts(c.0 as *const T, c.1 as usize) } {
                f(k, v);
                k += 1;
            }
        }
    }
}

#[inline(always)]
unsafe fn stream64(dst: *mut u8, src: *const u8) {
    #[cfg(target_feature = "avx2")]
    {
        use std::arch::x86_64::*;
        let a = _mm256_load_si256(src as *const __m256i);
        let b = _mm256_load_si256(src.add(32) as *const __m256i);
        _mm256_stream_si256(dst as *mut __m256i, a);
        _mm256_stream_si256(dst.add(32) as *mut __m256i, b);
    }
    #[cfg(not(target_feature = "avx2"))]
    std::ptr::copy_nonoverlapping(src, dst, 64);
}
