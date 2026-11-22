//! Minimal Parquet writer: one row group per file, one PLAIN, uncompressed v1
//! data page per column, thrift-compact footer. Just what the two tables need.

use std::fs;
use std::path::Path;

use super::{unpack, CampRow, UserRow, B};

// parquet enums
const T_INT32: i32 = 1;
const T_INT64: i32 = 2;
const T_BYTE_ARRAY: i32 = 6;
const REQUIRED: i32 = 0;
const OPTIONAL: i32 = 1;
const CT_UTF8: i32 = 0;
const CT_DATE: i32 = 6;
const ENC_PLAIN: i32 = 0;
const ENC_RLE: i32 = 3;

// thrift compact types
const TC_I32: u8 = 5;
const TC_I64: u8 = 6;
const TC_BINARY: u8 = 8;
const TC_LIST: u8 = 9;
const TC_STRUCT: u8 = 12;

fn varint(b: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        b.push((v as u8) | 0x80);
        v >>= 7;
    }
    b.push(v as u8);
}

struct Thrift {
    b: Vec<u8>,
    last: Vec<i16>,
}

impl Thrift {
    fn new() -> Self {
        Thrift { b: Vec::new(), last: vec![0] }
    }
    fn field(&mut self, id: i16, ty: u8) {
        let l = self.last.last_mut().unwrap();
        let d = id - *l;
        if d > 0 && d <= 15 {
            self.b.push(((d as u8) << 4) | ty);
        } else {
            self.b.push(ty);
            varint(&mut self.b, ((id << 1) ^ (id >> 15)) as u16 as u64);
        }
        *l = id;
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.field(id, TC_I32);
        varint(&mut self.b, ((v << 1) ^ (v >> 31)) as u32 as u64);
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.field(id, TC_I64);
        varint(&mut self.b, ((v << 1) ^ (v >> 63)) as u64);
    }
    fn bin(&mut self, id: i16, s: &[u8]) {
        self.field(id, TC_BINARY);
        varint(&mut self.b, s.len() as u64);
        self.b.extend_from_slice(s);
    }
    fn list(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, TC_LIST);
        if n < 15 {
            self.b.push(((n as u8) << 4) | elem);
        } else {
            self.b.push(0xF0 | elem);
            varint(&mut self.b, n as u64);
        }
    }
    fn begin(&mut self, id: i16) {
        self.field(id, TC_STRUCT);
        self.last.push(0);
    }
    /// a struct that is a list element (no field header)
    fn begin_elem(&mut self) {
        self.last.push(0);
    }
    fn end(&mut self) {
        self.b.push(0);
        self.last.pop();
    }
}

struct Col {
    name: &'static str,
    ty: i32,
    optional: bool,
    converted: Option<i32>,
    values: Vec<u8>,          // PLAIN-encoded non-null values
    defs: Option<Vec<bool>>,  // per row, for optional columns
}

impl Col {
    fn new(name: &'static str, ty: i32, optional: bool, converted: Option<i32>, cap: usize) -> Self {
        Col { name, ty, optional, converted, values: Vec::with_capacity(cap), defs: if optional { Some(Vec::new()) } else { None } }
    }
    fn i32(&mut self, v: i32) {
        self.values.extend_from_slice(&v.to_le_bytes());
    }
    fn i64(&mut self, v: i64) {
        self.values.extend_from_slice(&v.to_le_bytes());
    }
    fn bytes(&mut self, s: &[u8]) {
        self.values.extend_from_slice(&(s.len() as u32).to_le_bytes());
        self.values.extend_from_slice(s);
    }
    fn opt_bytes(&mut self, s: Option<&[u8]>) {
        let d = self.defs.as_mut().unwrap();
        d.push(s.is_some());
        if let Some(s) = s {
            self.bytes(s);
        }
    }
}

/// RLE/bit-packed hybrid, bit width 1, as bit-packed groups of 8.
fn encode_defs(defs: &[bool]) -> Vec<u8> {
    let groups = (defs.len() + 7) / 8;
    let mut b = Vec::with_capacity(groups + 10);
    varint(&mut b, ((groups as u64) << 1) | 1);
    for g in 0..groups {
        let mut byte = 0u8;
        for k in 0..8 {
            if defs.get(g * 8 + k).copied().unwrap_or(false) {
                byte |= 1 << k;
            }
        }
        b.push(byte);
    }
    b
}

fn write_file(path: &Path, cols: Vec<Col>, nrows: usize) {
    let total: usize = cols.iter().map(|c| c.values.len() + nrows / 8 + 64).sum();
    let mut out: Vec<u8> = Vec::with_capacity(total + 4096);
    out.extend_from_slice(b"PAR1");
    let mut metas: Vec<(i64, i64)> = Vec::new(); // (data_page_offset, total size)
    for c in &cols {
        let mut page = Vec::new();
        if let Some(d) = &c.defs {
            let enc = encode_defs(d);
            page.extend_from_slice(&(enc.len() as u32).to_le_bytes());
            page.extend_from_slice(&enc);
        }
        let page_len = page.len() + c.values.len();
        let mut h = Thrift::new();
        h.i32(1, 0); // DATA_PAGE
        h.i32(2, page_len as i32);
        h.i32(3, page_len as i32);
        h.begin(5);
        h.i32(1, nrows as i32);
        h.i32(2, ENC_PLAIN);
        h.i32(3, ENC_RLE);
        h.i32(4, ENC_RLE);
        h.end();
        h.b.push(0);
        let off = out.len() as i64;
        out.extend_from_slice(&h.b);
        out.extend_from_slice(&page);
        out.extend_from_slice(&c.values);
        metas.push((off, out.len() as i64 - off));
    }
    // footer
    let mut f = Thrift::new();
    f.i32(1, 1);
    f.list(2, TC_STRUCT, cols.len() + 1);
    f.begin_elem();
    f.bin(4, b"schema");
    f.i32(5, cols.len() as i32);
    f.end();
    for c in &cols {
        f.begin_elem();
        f.i32(1, c.ty);
        f.i32(3, if c.optional { OPTIONAL } else { REQUIRED });
        f.bin(4, c.name.as_bytes());
        if let Some(ct) = c.converted {
            f.i32(6, ct);
            f.begin(10); // LogicalType union
            f.begin(if ct == CT_UTF8 { 1 } else { 6 }); // STRING or DATE
            f.end();
            f.end();
        }
        f.end();
    }
    f.i64(3, nrows as i64);
    f.list(4, TC_STRUCT, 1);
    f.begin_elem();
    f.list(1, TC_STRUCT, cols.len());
    let mut total_size = 0i64;
    for (c, &(off, size)) in cols.iter().zip(&metas) {
        total_size += size;
        f.begin_elem();
        f.i64(2, off);
        f.begin(3);
        f.i32(1, c.ty);
        f.list(2, TC_I32, 2);
        varint(&mut f.b, (ENC_PLAIN << 1) as u64);
        varint(&mut f.b, (ENC_RLE << 1) as u64);
        f.list(3, TC_BINARY, 1);
        varint(&mut f.b, c.name.len() as u64);
        f.b.extend_from_slice(c.name.as_bytes());
        f.i32(4, 0); // UNCOMPRESSED
        f.i64(5, nrows as i64);
        f.i64(6, size);
        f.i64(7, size);
        f.i64(9, off);
        f.end();
        f.end();
    }
    f.i64(2, total_size);
    f.i64(3, nrows as i64);
    f.end();
    f.bin(6, b"shopping-sessions");
    f.b.push(0);
    out.extend_from_slice(&f.b);
    out.extend_from_slice(&(f.b.len() as u32).to_le_bytes());
    out.extend_from_slice(b"PAR1");
    fs::write(path, &out).unwrap();
}

fn write_users(path: &Path, rows: &[UserRow], cats: &[B]) {
    let n = rows.len();
    let mut name = Col::new("user_id", T_BYTE_ARRAY, false, Some(CT_UTF8), n * 16);
    let mut day = Col::new("day", T_INT32, false, Some(CT_DATE), n * 4);
    let mut ses = Col::new("sessions", T_INT64, false, None, n * 8);
    let mut evs = Col::new("events", T_INT64, false, None, n * 8);
    let mut rev = Col::new("revenue_cents", T_INT64, false, None, n * 8);
    let mut top = Col::new("top_category", T_BYTE_ARRAY, true, Some(CT_UTF8), n * 4);
    top.defs.as_mut().unwrap().reserve(n);
    for r in rows {
        name.bytes(unpack(r.name));
        day.i32(r.day);
        ses.i64(r.sessions as i64);
        evs.i64(r.events as i64);
        rev.i64(r.revenue);
        top.opt_bytes(if r.topcat == 255 { None } else { Some(cats[r.topcat as usize]) });
    }
    write_file(path, vec![name, day, ses, evs, rev, top], n);
}

fn write_camps(path: &Path, rows: &[CampRow], names: &[(B, B)]) {
    let n = rows.len();
    let mut id = Col::new("campaign_id", T_BYTE_ARRAY, false, Some(CT_UTF8), n * 12);
    let mut ch = Col::new("channel", T_BYTE_ARRAY, false, Some(CT_UTF8), n * 12);
    let mut day = Col::new("day", T_INT32, false, Some(CT_DATE), n * 4);
    let mut v: Vec<Col> = ["sessions", "purchasing_sessions", "revenue_cents", "assisted_sessions"]
        .iter()
        .map(|&nm| Col::new(nm, T_INT64, false, None, n * 8))
        .collect();
    for r in rows {
        let (a, b): (&[u8], &[u8]) = if r.camp < names.len() { names[r.camp] } else { (b"organic", b"organic") };
        id.bytes(a);
        ch.bytes(b);
        day.i32(r.day);
        for k in 0..4 {
            v[k].i64(r.v[k]);
        }
    }
    let mut cols = vec![id, ch, day];
    cols.extend(v);
    write_file(path, cols, n);
}

pub fn write_all(outd: &Path, users: &[&[UserRow]], camps: &[CampRow], cats: &[B], names: &[(B, B)], _nt: usize) {
    let ud = outd.join("user_daily");
    let cd = outd.join("campaign_daily");
    fs::create_dir_all(&ud).unwrap();
    fs::create_dir_all(&cd).unwrap();
    std::thread::scope(|s| {
        for (i, u) in users.iter().enumerate() {
            let ud = &ud;
            s.spawn(move || write_users(&ud.join(format!("part-{i}.parquet")), u, cats));
        }
        write_camps(&cd.join("part-0.parquet"), camps, names);
    });
}
