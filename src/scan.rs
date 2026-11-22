//! Line/field scanner for the rules.md CSV dialect.
//!
//! Works on 64-byte blocks: one SIMD pass gives bitmasks of '\n', ',' and '"'.
//! A field is valid when it holds no quote, or exactly two quotes that are its
//! first and last byte (they are stripped). Quote counts per field come from a
//! running popcount, so no byte is looked at twice.

pub type B = &'static [u8];

#[cfg(target_feature = "avx2")]
#[inline(always)]
unsafe fn masks_ptr(p: *const u8) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    let a = _mm256_loadu_si256(p as *const __m256i);
    let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
    let m = |c: u8| -> u64 {
        let v = _mm256_set1_epi8(c as i8);
        let lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, v)) as u32 as u64;
        let hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(b, v)) as u32 as u64;
        lo | (hi << 32)
    };
    (m(b'\n'), m(b','), m(b'"'))
}

#[cfg(not(target_feature = "avx2"))]
#[inline(always)]
unsafe fn masks_ptr(p: *const u8) -> (u64, u64, u64) {
    let (mut n, mut c, mut q) = (0u64, 0u64, 0u64);
    for i in 0..64 {
        let b = *p.add(i);
        n |= ((b == b'\n') as u64) << i;
        c |= ((b == b',') as u64) << i;
        q |= ((b == b'"') as u64) << i;
    }
    (n, c, q)
}

#[inline(always)]
fn masks(data: &[u8], base: usize) -> (u64, u64, u64) {
    if base + 64 <= data.len() {
        unsafe { masks_ptr(data.as_ptr().add(base)) }
    } else {
        let mut buf = [0u8; 64];
        let n = data.len() - base;
        buf[..n].copy_from_slice(&data[base..]);
        unsafe { masks_ptr(buf.as_ptr()) }
    }
}

/// Calls `f` with the N unquoted fields of every well-formed line that starts in
/// `[start, end)`. `end` must be a line start (or `data.len()`).
#[inline(always)]
pub fn scan<const N: usize, F: FnMut(&[B; N])>(data: B, start: usize, end: usize, f: &mut F) {
    let mut fs = [&data[0..0]; N];
    let mut nf = 0usize;
    let mut bad = false;
    let mut fstart = start;
    let mut q_fstart = 0u64; // quotes before fstart
    let mut cumq = 0u64; // quotes before current block
    let mut base = start;
    while base < end {
        let (mut nl, mut cm, mut qt) = masks(data, base);
        let lim = end - base;
        if lim < 64 {
            let v = (1u64 << lim) - 1;
            nl &= v;
            cm &= v;
            qt &= v;
        }
        let mut seps = nl | cm;
        while seps != 0 {
            let off = seps.trailing_zeros();
            let p = base + off as usize;
            let qb = cumq + (qt & ((1u64 << off) - 1)).count_ones() as u64;
            if nf < N {
                let q = qb - q_fstart;
                if q == 0 {
                    fs[nf] = unsafe { data.get_unchecked(fstart..p) };
                } else if q == 2 && p - fstart >= 2 && data[fstart] == b'"' && data[p - 1] == b'"' {
                    fs[nf] = unsafe { data.get_unchecked(fstart + 1..p - 1) };
                } else {
                    bad = true;
                }
            }
            nf += 1;
            if (nl >> off) & 1 == 1 {
                if !bad && nf == N {
                    f(&fs);
                }
                nf = 0;
                bad = false;
            }
            fstart = p + 1;
            q_fstart = qb;
            seps &= seps - 1;
        }
        cumq += qt.count_ones() as u64;
        base += 64;
    }
    // a last line with no trailing '\n'
    if end == data.len() && (fstart < end || nf > 0) {
        let p = end;
        let qb = cumq;
        if nf < N {
            let q = qb - q_fstart;
            if q == 0 {
                fs[nf] = &data[fstart..p];
            } else if q == 2 && p - fstart >= 2 && data[fstart] == b'"' && data[p - 1] == b'"' {
                fs[nf] = &data[fstart + 1..p - 1];
            } else {
                bad = true;
            }
        }
        nf += 1;
        if !bad && nf == N {
            f(&fs);
        }
    }
}
