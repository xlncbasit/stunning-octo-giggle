//! One user's rows: their sessions laid out in time, the events in them, the
//! clicks before them, and the dirt on every row.

use crate::plan::{
    CAMPAIGNS, Dataset, PRODUCTS, Rates, SLOTS, SLOTS_PER_DAY, UserPlan, WINDOW_DAYS,
};
use crate::random::{Random, Stream};
use crate::text::{
    DAY, HOUR, MINUTE, SECOND, bad_timestamp, scramble, valid_timestamp, write_id_suffix, write_utc,
};

/// Room for each person's alias numbers in the 40 bits `scramble` mixes.
const ALIAS_SLOTS: u64 = 512;
/// Kiosk IDs come from numbers above every person's aliases.
const KIOSK_BASE: u64 = 1 << 39;
/// A person's rows use their first ID this often, and any ID otherwise.
const FIRST_ALIAS_SHARE: f64 = 0.5;
/// A link repeated, either way round.
const REPEATED_LINK_SHARE: f64 = 0.08;
/// A link from an ID to itself.
const SELF_LINK_SHARE: f64 = 0.01;
/// A link to an old ID the person's rows never use.
const UNUSED_ALIAS_SHARE: f64 = 0.05;
/// An upper-cased side of a link: another ID, and the smallest in its person.
const LINK_CASE_VARIANT: f64 = 0.000_5;

/// Sessions start in their own two-hour slot, at most 25 minutes in, and last
/// at most an hour with a boundary gap. So consecutive sessions are always
/// more than 30 minutes apart and the sessions the generator means are the
/// sessions the rules find; only the boundary gaps split one on purpose.
const SLOT: i64 = 2 * HOUR;
const LATEST_START: i64 = 25 * MINUTE;
const LONGEST_SESSION: i64 = 30 * MINUTE - SECOND;
const SESSION_GAP: i64 = 30 * MINUTE;
/// A session crossing midnight starts in the 22:00 slot and keeps the next
/// slot free. Sessions otherwise end by 23:25, so these are the only ones.
const MIDNIGHT_SHARE: f64 = 0.025;
const BOUNDARY_GAP_SHARE: f64 = 0.001;
/// Sessions whose events carry milliseconds, which only F3 can write.
const MILLISECOND_START_SHARE: f64 = 0.03;
/// The share of a user's sessions that get clicks aimed at them; with the
/// clicks that reach later sessions too, about 60% of sessions are credited.
const TARGETED_SESSION_SHARE: f64 = 0.45;
const WHALE_CAMPAIGN_SHARE: f64 = 0.3;
/// The 22:00 slot, where a crossing session starts.
const LAST_SLOT_OF_DAY: usize = SLOTS_PER_DAY - 1;
/// Users with at most this many sessions may have some cross midnight. Each
/// crossing session takes two slots, and 80 × 2 stays under the 168 slots in
/// the window, so a free slot is always left to draw.
const MOST_SESSIONS_TO_CROSS: usize = 80;
const BAD_ORIGINAL_SHARE: f64 = 0.02;
/// Valid prices at the edges (`ODD_VALID_PRICES`): the maximum, the minimum, a leading zero.
const ODD_PRICE_SHARE: f64 = 0.001;
/// Unknown campaign IDs given an upper-case prefix, a case variant of a real one.
const UPPERCASE_CAMPAIGN_SHARE: f64 = 0.1;
/// Rows ingested 1 to 5 seconds before their `ts`, as a skewed clock would.
const EARLY_INGEST_SHARE: f64 = 0.005;
const SAME_MILLISECOND_SHARE: f64 = 0.01;
const CLICK_REACH: i64 = 7 * DAY;

/// Rows sorted into the files they go to, before the shuffle.
pub struct Parts {
    pub bytes: Vec<Vec<u8>>,
    pub starts: Vec<Vec<u32>>,
    /// Each file's cumulative share of the rows.
    shares: Vec<f64>,
}

impl Parts {
    pub fn new(shares: &[f64]) -> Self {
        Self {
            bytes: vec![Vec::new(); shares.len()],
            starts: vec![Vec::new(); shares.len()],
            shares: shares.to_vec(),
        }
    }

    fn push(&mut self, part: usize, line: &[u8]) {
        self.starts[part].push(self.bytes[part].len() as u32);
        self.bytes[part].extend_from_slice(line);
        self.bytes[part].push(b'\n');
    }

    fn pick(&self, random: &mut Random) -> usize {
        random.by_cumulative(&self.shares)
    }
}

pub struct Shard {
    pub events: Parts,
    pub clicks: Parts,
    pub links: Parts,
}

pub fn generate_shard(dataset: &Dataset, users: std::ops::Range<usize>) -> Shard {
    let mut shard = Shard {
        events: Parts::new(&dataset.event_parts),
        clicks: Parts::new(&dataset.click_parts),
        links: Parts::new(&dataset.link_parts),
    };
    for index in users {
        let plan = dataset.users[index];
        if plan.events > 0 {
            User::new(dataset, index, plan).generate(&mut shard);
        }
    }
    shard
}

struct User<'a> {
    dataset: &'a Dataset,
    plan: UserPlan,
    index: usize,
    random: Random,
    /// The person's IDs.
    ids: Vec<Vec<u8>>,
    row: Row,
}

/// The ID for a number: `u` and a keyed mixed-width suffix so no ID says whose it is.
fn user_id(dataset: &Dataset, number: u64) -> Vec<u8> {
    let mut id = b"u".to_vec();
    write_id_suffix(&mut id, number, &dataset.user_key);
    id
}

impl<'a> User<'a> {
    fn new(dataset: &'a Dataset, index: usize, plan: UserPlan) -> Self {
        let ids = (0..u64::from(plan.aliases.max(1)))
            .map(|alias| user_id(dataset, index as u64 * ALIAS_SLOTS + alias))
            .collect();
        Self {
            dataset,
            plan,
            index,
            random: Random::new(dataset.seed, Stream::User, index as u64),
            ids,
            row: Row::default(),
        }
    }

    /// Which of the person's IDs a row uses.
    fn alias(random: &mut Random, plan: &UserPlan, aliases: usize) -> usize {
        if aliases == 1 || (!plan.chain && random.chance(FIRST_ALIAS_SHARE)) {
            0
        } else {
            random.below(aliases as u64) as usize
        }
    }

    fn generate(&mut self, shard: &mut Shard) {
        let sessions = self.layout();
        let mut event_index = self.plan.first_event;
        for session in &sessions {
            for &ts in session {
                self.event(&mut shard.events, event_index, ts);
                event_index += 1;
            }
        }
        let starts: Vec<i64> = sessions.iter().map(|session| session[0]).collect();
        self.clicks(&mut shard.clicks, &starts);
        self.links(&mut shard.links);
    }

    /// The links joining the person's IDs: a chain for a chain person, and
    /// each ID to an earlier one otherwise, so they form one person; then
    /// repeats, self-links, unused old IDs, a shared device, and the dirt.
    fn links(&mut self, parts: &mut Parts) {
        let count = self.ids.len();
        let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let random = &mut self.random;
        if self.plan.chain {
            let mut order: Vec<usize> = (0..count).collect();
            random.shuffle(&mut order);
            for pair in order.windows(2) {
                pairs.push((self.ids[pair[0]].clone(), self.ids[pair[1]].clone()));
            }
        } else {
            for alias in 1..count {
                let earlier = random.below(alias as u64) as usize;
                pairs.push((self.ids[alias].clone(), self.ids[earlier].clone()));
            }
        }
        if random.chance(UNUSED_ALIAS_SHARE) {
            let unused = user_id(self.dataset, self.index as u64 * ALIAS_SLOTS + count as u64);
            pairs.push((random.pick(&self.ids).clone(), unused));
        }
        if self.plan.kiosk > 0 {
            let kiosk = user_id(self.dataset, KIOSK_BASE + u64::from(self.plan.kiosk));
            pairs.push((random.pick(&self.ids).clone(), kiosk));
        }
        if random.chance(SELF_LINK_SHARE) {
            let id = random.pick(&self.ids).clone();
            pairs.push((id.clone(), id));
        }
        let rates = &self.dataset.rates;
        let row = &mut self.row;
        for (a, b) in pairs {
            let copies = 1 + usize::from(random.chance(REPEATED_LINK_SHARE));
            for _ in 0..copies {
                let (first, second) = if random.chance(0.5) { (&a, &b) } else { (&b, &a) };
                row.clear();
                for id in [first, second] {
                    if random.chance(rates.bad_user) {
                        row.field(|_| {});
                    } else {
                        row.field(|out| out.extend_from_slice(id));
                        if random.chance(LINK_CASE_VARIANT) {
                            row.uppercase_last();
                        }
                    }
                }
                row.emit(random, rates, parts);
            }
        }
    }

    /// Each session's event times, in order.
    fn layout(&mut self) -> Vec<Vec<i64>> {
        let random = &mut self.random;
        let count = self.plan.sessions as usize;
        let mut used = [false; SLOTS as usize];
        let mut slots: Vec<(usize, bool)> = Vec::with_capacity(count);
        if count == SLOTS as usize {
            slots.extend((0..count).map(|slot| (slot, false)));
        } else {
            let may_cross = count <= MOST_SESSIONS_TO_CROSS;
            for _ in 0..count {
                if may_cross && random.chance(MIDNIGHT_SHARE) {
                    // Not the last day, so the slot after it is still in the window.
                    let day = random.below(WINDOW_DAYS as u64 - 1) as usize;
                    let slot = day * SLOTS_PER_DAY + LAST_SLOT_OF_DAY;
                    if !used[slot] && !used[slot + 1] {
                        used[slot] = true;
                        used[slot + 1] = true;
                        slots.push((slot, true));
                        continue;
                    }
                }
                loop {
                    let slot = random.below(u64::from(SLOTS)) as usize;
                    if !used[slot] {
                        used[slot] = true;
                        slots.push((slot, false));
                        break;
                    }
                }
            }
            slots.sort_unstable();
        }

        let mut sizes = vec![1u64; count];
        let weights: Vec<u64> = (0..count).map(|_| 1 + random.below(4)).collect();
        let weight_total: u64 = weights.iter().sum();
        let extra = self.plan.events - count as u64;
        let mut given = 0;
        let mut running = 0;
        for (size, weight) in sizes.iter_mut().zip(&weights) {
            running += weight;
            let through = extra * running / weight_total;
            *size += through - given;
            given = through;
        }

        slots
            .iter()
            .zip(sizes)
            .map(|(&(slot, crossing), size)| {
                let mut gaps: Vec<i64> = (1..size).map(|_| random.spread(10) * SECOND).collect();
                let natural: i64 = gaps.iter().sum();
                if natural > LONGEST_SESSION {
                    for gap in &mut gaps {
                        // Whole seconds, so dense users' timestamps can still
                        // be F1 or F2; many of their gaps become 0.
                        *gap = *gap * LONGEST_SESSION / natural / SECOND * SECOND;
                    }
                }
                if let Some(at) = gaps.iter().position(|_| random.chance(BOUNDARY_GAP_SHARE)) {
                    gaps[at] = SESSION_GAP + random.between(0, 1);
                }
                let duration: i64 = gaps.iter().sum();
                let slot_start = self.dataset.window_start + slot as i64 * SLOT;
                let start = if crossing && duration >= 2 * SECOND {
                    slot_start + SLOT - random.between(1, duration / SECOND) * SECOND
                } else {
                    slot_start + random.below((LATEST_START / SECOND) as u64) as i64 * SECOND
                } + if random.chance(MILLISECOND_START_SHARE) {
                    random.between(1, 999)
                } else {
                    0
                };
                let mut ts = start;
                let mut times = Vec::with_capacity(size as usize);
                times.push(ts);
                for gap in gaps {
                    ts += gap;
                    times.push(ts);
                }
                times
            })
            .collect()
    }

    fn event(&mut self, parts: &mut Parts, index: u64, ts: i64) {
        let dataset = self.dataset;
        let rates = &dataset.rates;
        let random = &mut self.random;
        let row = &mut self.row;
        row.clear();

        row.field(|out| {
            out.push(b'e');
            write_id_suffix(out, index, &dataset.event_key);
        });
        if random.chance(rates.case_variant) {
            row.uppercase_last();
        }
        let alias = Self::alias(random, &self.plan, self.ids.len());
        user_field(row, random, rates.bad_user, rates.case_variant, &self.ids[alias]);

        let timestamp_is_bad = random.chance(rates.bad_timestamp);
        row.field(|out| {
            if timestamp_is_bad {
                let outside = outside(random, dataset.window_start, dataset.window_end());
                bad_timestamp(out, random, ts, outside);
            } else {
                valid_timestamp(out, random, ts);
            }
        });
        let ingest_ts = ingest(random, rates.late, ts);
        row.field(|out| write_utc(out, ingest_ts));

        let kind = random.below(100);
        let purchase = kind < 3;
        row.field(|out| {
            if random.chance(rates.bad_type) {
                out.extend_from_slice(random.pick(&BAD_TYPES).as_bytes());
            } else {
                out.extend_from_slice(match kind {
                    0..3 => b"purchase".as_slice(),
                    3..13 => b"cart",
                    _ => b"view",
                });
            }
        });
        let product = dataset.product_by_rank
            [random.by_cumulative(&dataset.product_rank_cumulative)] as usize;
        row.field(|out| {
            if random.chance(rates.unknown_product) {
                match random.below(10) {
                    0 => {}
                    1 => {
                        out.push(b'P');
                        out.extend_from_slice((product + 1).to_string().as_bytes());
                    }
                    _ => out.extend_from_slice(
                        format!("p{}", random.between(PRODUCTS as i64 + 1, 99_999)).as_bytes(),
                    ),
                }
            } else {
                out.push(b'p');
                out.extend_from_slice((product + 1).to_string().as_bytes());
            }
        });
        let price = dataset.product_price[product];
        row.field(|out| {
            if purchase {
                if random.chance(rates.bad_price) {
                    out.extend_from_slice(random.pick(&BAD_PRICES).as_bytes());
                } else if random.chance(ODD_PRICE_SHARE) {
                    out.extend_from_slice(random.pick(&ODD_VALID_PRICES).as_bytes());
                } else {
                    out.extend_from_slice(price.to_string().as_bytes());
                }
            } else {
                match random.below(20) {
                    0..14 => {}
                    14..19 => out.extend_from_slice(price.to_string().as_bytes()),
                    _ => out.extend_from_slice(random.pick(&BAD_PRICES).as_bytes()),
                }
            }
        });

        let copies = if random.chance(rates.event_duplicates) {
            1 + random.below(3)
        } else {
            0
        };
        // "Events, in this order" step 2 in the README (fixture E5): only the earliest copy is bad, and a later
        // valid copy must not rescue the event.
        let original_present = if copies > 0 && !timestamp_is_bad && random.chance(BAD_ORIGINAL_SHARE) {
            let mut original = row.clone();
            original.replace(2, |out| {
                let outside = outside(random, dataset.window_start, dataset.window_end());
                bad_timestamp(out, random, ts, outside);
            });
            original.emit(random, rates, parts)
        } else {
            row.emit(random, rates, parts)
        };
        // Some upstream revisions change the attributed person. All later
        // copies use the same revised ID, including ties at the same ingest time.
        // Only revise a row whose earliest copy survived CSV corruption, so the
        // correct tables stay the same for every seed. Use a separate keyed draw
        // to keep the old generator's random stream and file choices intact.
        if original_present && copies > 0 && scramble(index, &dataset.click_key) % 64 == 0 {
            let other = (self.index + 1) % dataset.users.len();
            row.replace(1, |out| {
                out.extend_from_slice(&user_id(dataset, other as u64 * ALIAS_SLOTS));
            });
        }
        for _ in 0..copies {
            let later = ingest_ts + random.between(1, 600) * SECOND;
            row.replace(3, |out| write_utc(out, later));
            row.emit(random, rates, parts);
        }
    }

    fn clicks(&mut self, parts: &mut Parts, session_starts: &[i64]) {
        let earliest = self.dataset.window_start - CLICK_REACH;
        let mut targets: Vec<i64> = session_starts
            .iter()
            .copied()
            .filter(|_| self.random.chance(TARGETED_SESSION_SHARE))
            .collect();
        if targets.is_empty() {
            targets.push(*self.random.pick(session_starts));
        }
        let mut index = self.plan.first_click;
        let end = index + self.plan.clicks;
        while index < end {
            let random = &mut self.random;
            let start = *random.pick(&targets);
            let delta = match random.below(1000) {
                0..4 => CLICK_REACH,
                4..8 => CLICK_REACH + 1,
                8..12 => 0,
                12..16 => -1,
                16..750 => (random.spread(19) * SECOND).min(CLICK_REACH),
                _ => CLICK_REACH + random.between(1, 3 * DAY / SECOND) * SECOND,
            };
            let mut ts = start - delta;
            if ts < earliest {
                ts = start - random.below(((start - earliest) / SECOND) as u64 + 1) as i64 * SECOND;
            }
            let same_millisecond = end - index >= 2 && random.chance(SAME_MILLISECOND_SHARE);
            for _ in 0..1 + u64::from(same_millisecond) {
                self.click(parts, index, ts);
                index += 1;
            }
        }
    }

    fn click(&mut self, parts: &mut Parts, index: u64, ts: i64) {
        let dataset = self.dataset;
        let rates = &dataset.rates;
        let random = &mut self.random;
        let row = &mut self.row;
        row.clear();
        row.field(|out| {
            out.push(b'k');
            write_id_suffix(out, index, &dataset.click_key);
        });
        if random.chance(rates.case_variant) {
            row.uppercase_last();
        }
        let alias = Self::alias(random, &self.plan, self.ids.len());
        user_field(row, random, rates.bad_user, rates.case_variant, &self.ids[alias]);
        row.field(|out| {
            if random.chance(rates.unknown_campaign) {
                let id = if random.chance(UPPERCASE_CAMPAIGN_SHARE) {
                    // Existing catalog suffix in wrong case so case-insensitive matching is caught.
                    format!("C_{}", random.between(1, CAMPAIGNS as i64))
                } else {
                    format!("c_{}", random.between(CAMPAIGNS as i64 + 1, 9_999))
                };
                out.extend_from_slice(id.as_bytes());
                return;
            }
            let campaign = if random.chance(WHALE_CAMPAIGN_SHARE) {
                *random.pick(&dataset.whale_campaigns)
            } else {
                dataset.campaign_by_rank[random.by_cumulative(&dataset.campaign_rank_cumulative)]
            };
            out.extend_from_slice(format!("c_{}", campaign + 1).as_bytes());
        });
        row.field(|out| {
            if random.chance(rates.bad_timestamp) {
                let outside = outside(
                    random,
                    dataset.window_start - CLICK_REACH,
                    dataset.window_end(),
                );
                bad_timestamp(out, random, ts, outside);
            } else {
                valid_timestamp(out, random, ts);
            }
        });
        let copies = if random.chance(rates.click_duplicates) {
            1 + random.below(2)
        } else {
            0
        };
        for _ in 0..=copies {
            row.emit(random, rates, parts);
        }
    }
}

const BAD_TYPES: [&str; 8] = [
    "Purchase", "PURCHASE", "View", "cart ", "click", "refund", "", "views",
];
const BAD_PRICES: [&str; 9] = [
    "19.99",
    "-5",
    "+500",
    "0",
    "",
    "10000001",
    "250000000",
    "1e3",
    "00",
];
/// Valid, but easy to get wrong: the top of the range, and leading zeros.
const ODD_VALID_PRICES: [&str; 3] = ["10000000", "1", "0250"];

fn user_field(row: &mut Row, random: &mut Random, bad_user: f64, case_variant: f64, id: &[u8]) {
    if random.chance(bad_user) {
        row.field(|_| {});
    } else {
        row.field(|out| out.extend_from_slice(id));
        if random.chance(case_variant) {
            row.uppercase_last();
        }
    }
}

/// A time outside `[window_start, window_end)`, often right at its edge.
fn outside(random: &mut Random, window_start: i64, window_end: i64) -> i64 {
    match random.below(6) {
        0 => window_start - 1,
        1 => window_start - SECOND,
        2 => window_end,
        3 => window_start - random.between(1, 3 * DAY),
        _ => window_end + random.between(0, 3 * DAY),
    }
}

/// ingest_ts: whole seconds, usually minutes after ts. Late rows are 24 hours
/// to 5 days after, some exactly 24 h (kept) and 24 h + 1 s (dropped).
fn ingest(random: &mut Random, late: f64, ts: i64) -> i64 {
    let floor = ts - ts.rem_euclid(SECOND);
    let ceiling = if floor == ts { ts } else { floor + SECOND };
    if random.chance(late) {
        match random.below(10) {
            0 if floor == ts => ts + DAY,
            1 if floor == ts => ts + DAY + SECOND,
            _ => ceiling + random.between(DAY / SECOND + 1, 5 * DAY / SECOND) * SECOND,
        }
    } else if random.chance(EARLY_INGEST_SHARE) {
        floor - random.between(1, 5) * SECOND
    } else {
        ceiling + random.between(0, 600) * SECOND
    }
}

/// One row's fields, then its CSV line with random quotes and, rarely, a
/// malformation.
#[derive(Clone, Default)]
struct Row {
    bytes: Vec<u8>,
    ends: Vec<usize>,
    line: Vec<u8>,
}

impl Row {
    fn clear(&mut self) {
        self.bytes.clear();
        self.ends.clear();
    }

    fn field(&mut self, write: impl FnOnce(&mut Vec<u8>)) {
        write(&mut self.bytes);
        self.ends.push(self.bytes.len());
    }

    fn start(&self, field: usize) -> usize {
        if field == 0 { 0 } else { self.ends[field - 1] }
    }

    fn uppercase_last(&mut self) {
        let start = self.start(self.ends.len() - 1);
        self.bytes[start..].make_ascii_uppercase();
    }

    fn replace(&mut self, field: usize, write: impl FnOnce(&mut Vec<u8>)) {
        let (start, end) = (self.start(field), self.ends[field]);
        let mut value = Vec::new();
        write(&mut value);
        let tail = self.bytes.split_off(end);
        self.bytes.truncate(start);
        self.bytes.extend_from_slice(&value);
        self.bytes.extend_from_slice(&tail);
        let shift = value.len() as isize - (end - start) as isize;
        for later in &mut self.ends[field..] {
            *later = (*later as isize + shift) as usize;
        }
    }

    /// Whether the original, well-formed row was among the lines written.
    fn emit(&mut self, random: &mut Random, rates: &Rates, parts: &mut Parts) -> bool {
        let line = &mut self.line;
        line.clear();
        let mut start = 0;
        for (field, &end) in self.ends.iter().enumerate() {
            if field > 0 {
                line.push(b',');
            }
            let quote = random.chance(rates.quoted_field);
            if quote {
                line.push(b'"');
            }
            line.extend_from_slice(&self.bytes[start..end]);
            if quote {
                line.push(b'"');
            }
            start = end;
        }
        let mut original_present = true;
        if random.chance(rates.malformed) {
            let fault = random.below(6);
            // The final case adds a blank line and still writes the original row.
            original_present = fault == 5;
            match fault {
                0 => {
                    let last_comma = line.iter().rposition(|&byte| byte == b',').unwrap_or(0);
                    line.truncate(last_comma);
                }
                1 => line.extend_from_slice(b",x"),
                2 => line.insert(0, b'"'),
                3 => line.push(b'"'),
                // An empty event_id or click_id.
                4 => {
                    let first_comma = line.iter().position(|&byte| byte == b',').unwrap_or(0);
                    line.drain(..first_comma);
                }
                _ => {
                    let part = parts.pick(random);
                    parts.push(part, b"");
                }
            }
        }
        let part = parts.pick(random);
        parts.push(part, line);
        original_present
    }
}
