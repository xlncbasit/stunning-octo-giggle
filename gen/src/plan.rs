//! Everything decided once per seed, before any row is written: the window,
//! the dirt rates, the products and campaigns, and how many events, sessions
//! and clicks each user gets.

use crate::random::{Random, Stream};
use crate::text::{DAY, days_from_civil};

pub const PRODUCTS: usize = 50_000;
pub const CAMPAIGNS: usize = 2_000;
pub const CATEGORIES: [&str; 20] = [
    "apparel",
    "automotive",
    "baby",
    "beauty",
    "books",
    "electronics",
    "garden",
    "grocery",
    "home",
    "jewelry",
    "kitchen",
    "music",
    "office",
    "outdoors",
    "pets",
    "shoes",
    "sports",
    "tools",
    "toys",
    "video_games",
];
pub const CHANNELS: [&str; 5] = ["affiliate", "display", "email", "search", "social"];
pub const WINDOW_DAYS: i64 = 14;
/// Cents; the README's catalog prices run from 100 to this.
const MAXIMUM_PRODUCT_PRICE: i64 = 50_000;
const WHALE_USERS: usize = 5;
const EVENTS_PER_USER: u64 = 30;
/// Users who click ads at all. A click reaches every session in the 7 days
/// after it, so a clicking user's sessions are nearly all credited and this
/// share sets the share of credited sessions (about 60%, the README's "The generator").
const CLICKING_USER_SHARE: f64 = 0.65;
/// Sessions sit in two-hour slots, at most one per slot (user.rs says why).
pub const SLOTS: u32 = (WINDOW_DAYS as usize * SLOTS_PER_DAY) as u32;
/// Two-hour slots in a day.
pub const SLOTS_PER_DAY: usize = 12;
/// Fixed so a shard's rows, and so the bytes, never depend on the thread count.
const SHARDS: usize = 512;
/// The busiest ordinary people, and a share of the rest, use one ID after
/// another (cleared cookies), each linked only to the next: long chains.
const CHAIN_PEOPLE: usize = 3;
const CHAIN_SHARE: f64 = 0.000_05;
const CHAIN_ALIASES: (i64, i64) = (50, 400);
/// People who also used a shared device: its ID links them all into one
/// person, thousands of people at full size.
const KIOSK_SHARE: f64 = 0.004;
const KIOSKS: (i64, i64) = (3, 6);
/// How many files each folder gets, drawn per seed.
const EVENT_PARTS: (i64, i64) = (24, 160);
const CLICK_PARTS: (i64, i64) = (8, 48);
const LINK_PARTS: (i64, i64) = (4, 24);
/// Files left with only a header, per folder.
const EMPTY_PARTS: (i64, i64) = (1, 3);

/// Rates per row. The four the README's dirt table gives as ranges are drawn
/// per seed; the rest are single values, the same for every seed.
pub struct Rates {
    pub event_duplicates: f64,
    pub click_duplicates: f64,
    pub bad_timestamp: f64,
    pub late: f64,
    pub bad_user: f64,
    pub case_variant: f64,
    pub bad_type: f64,
    pub bad_price: f64,
    pub unknown_product: f64,
    pub unknown_campaign: f64,
    pub quoted_field: f64,
    pub malformed: f64,
}

#[derive(Clone, Copy, Default)]
pub struct UserPlan {
    pub events: u64,
    pub sessions: u32,
    pub clicks: u64,
    /// The person's IDs: their rows use any of them, and links join them.
    pub aliases: u32,
    /// The IDs are linked one after another, not each to an earlier one.
    pub chain: bool,
    /// The shared device this person also used, from 1; 0 for none.
    pub kiosk: u8,
    /// Global indexes of the user's first event and click, which make their IDs.
    pub first_event: u64,
    pub first_click: u64,
}

pub struct Dataset {
    pub seed: u64,
    pub window_start: i64,
    pub rates: Rates,
    pub user_key: [u64; 4],
    pub event_key: [u64; 4],
    pub click_key: [u64; 4],
    pub users: Vec<UserPlan>,
    pub shards: Vec<std::ops::Range<usize>>,
    /// Products by popularity rank, and the rank's cumulative weight.
    pub product_by_rank: Vec<u32>,
    pub product_rank_cumulative: Vec<f64>,
    pub product_category: Vec<u8>,
    pub product_price: Vec<i64>,
    pub campaign_by_rank: Vec<u32>,
    pub campaign_rank_cumulative: Vec<f64>,
    pub campaign_channel: Vec<u8>,
    pub whale_campaigns: Vec<u32>,
    /// Each folder's files by cumulative share of its rows: a few large, many
    /// small, some empty.
    pub event_parts: Vec<f64>,
    pub click_parts: Vec<f64>,
    pub link_parts: Vec<f64>,
}

impl Dataset {
    pub fn plan(seed: u64, events: u64, clicks: u64) -> Self {
        let mut random = Random::new(seed, Stream::Dataset, 0);
        let first_day = days_from_civil(2025, 1, 1);
        let last_day = days_from_civil(2028, 12, 31) - WINDOW_DAYS;
        let window_start = (first_day + random.below((last_day - first_day) as u64) as i64) * DAY;
        let rates = Rates {
            event_duplicates: random.rate(0.02, 0.04),
            click_duplicates: random.rate(0.01, 0.03),
            bad_timestamp: random.rate(0.003, 0.007),
            late: random.rate(0.005, 0.015),
            bad_user: 0.002,
            // "A few hundred" at the full 75M rows.
            case_variant: 0.000_005,
            bad_type: 0.001,
            bad_price: 0.05,
            unknown_product: 0.005,
            unknown_campaign: 0.005,
            quoted_field: 0.2,
            malformed: 0.000_5,
        };
        let mut next_key = || {
            [
                random.next_u64(),
                random.next_u64(),
                random.next_u64(),
                random.next_u64(),
            ]
        };
        let user_key = next_key();
        let event_key = next_key();
        let click_key = next_key();

        let user_count = (events / EVENTS_PER_USER).max(WHALE_USERS as u64 * 4) as usize;
        let whale_event_shares: Vec<f64> =
            (0..WHALE_USERS).map(|_| random.rate(0.01, 0.02)).collect();
        let whale_click_shares: Vec<f64> = (0..WHALE_USERS)
            .map(|_| random.rate(0.005, 0.015))
            .collect();
        let mut parts = Random::new(seed, Stream::Parts, 0);
        let kiosks = parts.between(KIOSKS.0, KIOSKS.1) as u32;
        let event_parts = part_shares(&mut parts, EVENT_PARTS);
        let click_parts = part_shares(&mut parts, CLICK_PARTS);
        let link_parts = part_shares(&mut parts, LINK_PARTS);
        let mut users = vec![UserPlan::default(); user_count];
        let mut click_weights = Vec::with_capacity(user_count);

        // Whales take the first indexes; scrambled IDs hide which they are.
        let event_counts = allocate(events, &whale_event_shares, user_count, |rank| {
            // Starting at place 10 keeps the busiest ordinary user well below
            // the whales. place^-0.75 comes from square roots, which are exact
            // (random.rs).
            let place = (rank + 10) as f64;
            1.0 / (place.sqrt() * place.sqrt().sqrt())
        });
        for (index, user) in users.iter_mut().enumerate() {
            user.events = event_counts[index];
            let mut plan_random = Random::new(seed, Stream::UserPlan, index as u64);
            let mean_session_events = plan_random.between(3, 8) as u64;
            let clicks_ads = index < WHALE_USERS || plan_random.chance(CLICKING_USER_SHARE);
            user.sessions = if index < WHALE_USERS {
                u64::from(SLOTS)
            } else {
                (user.events + mean_session_events / 2) / mean_session_events
            }
            .clamp(
                u64::from(user.events > 0),
                user.events.min(u64::from(SLOTS)),
            ) as u32;
            user.chain = (WHALE_USERS..WHALE_USERS + CHAIN_PEOPLE).contains(&index)
                || plan_random.chance(CHAIN_SHARE);
            user.aliases = if user.chain {
                plan_random.between(CHAIN_ALIASES.0, CHAIN_ALIASES.1)
            } else if index < WHALE_USERS {
                plan_random.between(3, 6)
            } else {
                match plan_random.below(100) {
                    0..45 => 1,
                    45..75 => 2,
                    75..88 => 3,
                    _ => plan_random.between(4, 8),
                }
            } as u32;
            if plan_random.chance(KIOSK_SHARE) {
                user.kiosk = 1 + plan_random.below(u64::from(kiosks)) as u8;
            }
            click_weights.push(if clicks_ads {
                f64::from(user.sessions)
            } else {
                0.0
            });
        }
        let click_counts = allocate(clicks, &whale_click_shares, user_count, |rank| {
            click_weights[WHALE_USERS + rank]
        });
        let (mut next_event, mut next_click) = (0, 0);
        for (user, clicks) in users.iter_mut().zip(click_counts) {
            user.clicks = clicks;
            user.first_event = next_event;
            user.first_click = next_click;
            next_event += user.events;
            next_click += user.clicks;
        }

        let shards = shards(&users);
        let mut products = Random::new(seed, Stream::Products, 0);
        let mut product_by_rank: Vec<u32> = (0..PRODUCTS as u32).collect();
        products.shuffle(&mut product_by_rank);
        let product_category = (0..PRODUCTS)
            .map(|_| products.below(CATEGORIES.len() as u64) as u8)
            .collect();
        // spread(9) is 1 to 511 with every doubling equally likely, so prices
        // start at 100 cents and cheap ones are the most common.
        let product_price = (0..PRODUCTS)
            .map(|_| {
                (products.spread(9) * 100 + products.between(0, 99)).min(MAXIMUM_PRODUCT_PRICE)
            })
            .collect();

        let mut campaigns = Random::new(seed, Stream::Campaigns, 0);
        let mut campaign_by_rank: Vec<u32> = (0..CAMPAIGNS as u32).collect();
        campaigns.shuffle(&mut campaign_by_rank);
        let campaign_channel = (0..CAMPAIGNS)
            .map(|_| campaigns.below(CHANNELS.len() as u64) as u8)
            .collect();
        let whale_campaign_count = campaigns.between(3, 5) as usize;
        let whale_campaigns = campaign_by_rank[CAMPAIGNS - whale_campaign_count..].to_vec();

        Self {
            seed,
            window_start,
            rates,
            user_key,
            event_key,
            click_key,
            users,
            shards,
            product_by_rank,
            product_rank_cumulative: cumulative(PRODUCTS, |rank| 1.0 / (rank + 1) as f64),
            product_category,
            product_price,
            campaign_by_rank,
            campaign_rank_cumulative: cumulative(CAMPAIGNS, |rank| 1.0 / (rank + 1) as f64),
            campaign_channel,
            whale_campaigns,
            event_parts,
            click_parts,
            link_parts,
        }
    }

    pub fn window_end(&self) -> i64 {
        self.window_start + WINDOW_DAYS * DAY
    }
}

/// Splits `total` exactly: each whale its share, the rest by weight over the
/// other users in order. Rounding the running total, rather than each share,
/// makes the counts add up to `total`.
fn allocate(
    total: u64,
    whale_shares: &[f64],
    users: usize,
    weight: impl Fn(usize) -> f64,
) -> Vec<u64> {
    let mut counts: Vec<u64> = whale_shares
        .iter()
        .map(|share| (share * total as f64) as u64)
        .collect();
    let rest = total - counts.iter().sum::<u64>();
    let weights = cumulative(users - whale_shares.len(), weight);
    let mut given = 0;
    for bound in weights {
        let through = ((bound * rest as f64) as u64).min(rest);
        counts.push(through - given);
        given = through;
    }
    *counts.last_mut().expect("there are users") += rest - given;
    counts
}

/// The running total of `weight` over `0..count`, scaled to end at exactly 1.
fn cumulative(count: usize, weight: impl Fn(usize) -> f64) -> Vec<f64> {
    let mut running = 0.0;
    let mut totals: Vec<f64> = (0..count)
        .map(|rank| {
            running += weight(rank);
            running
        })
        .collect();
    for total in &mut totals {
        *total /= running;
    }
    if let Some(last) = totals.last_mut() {
        *last = 1.0;
    }
    totals
}

/// A folder's files, as a cumulative share table for `by_cumulative`: Zipf
/// sizes in a shuffled order, so the largest holds about a fifth of the rows,
/// and a few files with no rows at all.
fn part_shares(random: &mut Random, count: (i64, i64)) -> Vec<f64> {
    let count = random.between(count.0, count.1) as usize;
    let mut ranks: Vec<usize> = (0..count).collect();
    random.shuffle(&mut ranks);
    let empty = random.between(EMPTY_PARTS.0, EMPTY_PARTS.1) as usize;
    cumulative(count, |part| {
        if ranks[part] >= count - empty {
            0.0
        } else {
            1.0 / (ranks[part] + 1) as f64
        }
    })
}

/// Contiguous user ranges with about the same number of rows each.
fn shards(users: &[UserPlan]) -> Vec<std::ops::Range<usize>> {
    let work = |user: &UserPlan| user.events + user.clicks;
    let target = (users.iter().map(work).sum::<u64>() / SHARDS as u64).max(1);
    let mut shards = Vec::with_capacity(SHARDS + 1);
    let (mut start, mut load) = (0, 0);
    for (index, user) in users.iter().enumerate() {
        load += work(user);
        if load >= target {
            shards.push(start..index + 1);
            start = index + 1;
            load = 0;
        }
    }
    if start < users.len() {
        shards.push(start..users.len());
    }
    shards
}
