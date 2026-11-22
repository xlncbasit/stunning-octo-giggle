//! A seeded random stream the generator owns, so a seed means the same bytes
//! on every machine and in every release.
//!
//! Every draw is integer arithmetic or IEEE add, multiply and divide, which give
//! the same bits on x86_64 and aarch64. Nothing here calls `ln`, `exp` or
//! `powf`: their results come from the platform's math library and can differ
//! in the last bit between macOS and Linux.

/// SplitMix64 (Steele, Lea and Flood), one independent stream per
/// `(seed, stream, index)`.
pub struct Random {
    state: u64,
}

impl Random {
    pub fn new(seed: u64, stream: Stream, index: u64) -> Self {
        let state = mix(mix(seed ^ 0x5D58_8B65_6C07_8965)
            ^ mix(stream as u64 + 1)
            ^ index.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        Self { state }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        mix(self.state)
    }

    /// Uniform in `0..bound` (Lemire's multiply-shift, without the rejection
    /// step: a bias of at most `bound / 2^64` does not matter here).
    pub fn below(&mut self, bound: u64) -> u64 {
        ((u128::from(self.next_u64()) * u128::from(bound)) >> 64) as u64
    }

    /// Uniform in `low..=high`.
    pub fn between(&mut self, low: i64, high: i64) -> i64 {
        low + self.below((high - low + 1) as u64) as i64
    }

    /// Uniform in `[0, 1)`.
    pub fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    pub fn chance(&mut self, probability: f64) -> bool {
        self.unit() < probability
    }

    /// Uniform in `[low, high)`.
    pub fn rate(&mut self, low: f64, high: f64) -> f64 {
        low + (high - low) * self.unit()
    }

    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len() as u64) as usize]
    }

    /// An index drawn by weight from a cumulative table that ends at 1.
    pub fn by_cumulative(&mut self, cumulative: &[f64]) -> usize {
        let draw = self.unit();
        cumulative
            .partition_point(|&bound| bound <= draw)
            .min(cumulative.len() - 1)
    }

    /// Roughly log-uniform in `[1, 2^buckets)`: a power-of-two bucket, then a
    /// uniform value inside it.
    pub fn spread(&mut self, buckets: u64) -> i64 {
        let low = 1i64 << self.below(buckets);
        low + self.below(low as u64) as i64
    }

    /// Fisher-Yates.
    pub fn shuffle<T>(&mut self, items: &mut [T]) {
        for last in (1..items.len()).rev() {
            items.swap(last, self.below(last as u64 + 1) as usize);
        }
    }
}

fn mix(value: u64) -> u64 {
    let mut value = value;
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

/// Which stream a draw comes from. Adding a stream never changes the others.
#[derive(Clone, Copy)]
pub enum Stream {
    Dataset,
    Products,
    Campaigns,
    UserPlan,
    User,
    ShuffleEvents,
    ShuffleClicks,
    Parts,
    ShuffleLinks,
}
