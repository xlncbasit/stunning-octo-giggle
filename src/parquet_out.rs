//! Minimal Parquet writer: one row group per file, one PLAIN, uncompressed v1
//! data page per column, thrift-compact footer. Just what the two tables need.

use std::fs;
use std::io::Write;
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

/// Buffered file output that tracks its position.
struct Sink {
    f: fs::File,
    buf: Vec<u8>,
    pos: usize,
}

impl Sink {
    fn new(path: &Path) -> Self {
        Sink { f: fs::File::create(path).unwrap(), buf: Vec::with_capacity(1 << 20), pos: 0 }
    }
    #[inline(always)]
    fn put(&mut self, b: &[u8]) {
        self.buf.extend_from_slice(b);
        self.pos += b.len();
        if self.buf.len() >= 1 << 20 {
            self.flush();
        }
    }
    fn flush(&mut self) {
        self.f.write_all(&self.buf).unwrap();
        self.buf.clear();
    }
}

/// A column described by its encoded page size and an emitter that streams
/// the page body (definition levels, if any, then PLAIN values).
struct Col<'a> {
    name: &'static str,
    ty: i32,
    optional: bool,
    converted: Option<i32>,
    size: usize,
    emit: Box<dyn Fn(&mut Sink) + 'a>,
}

fn bytes_col<'a>(name: &'static str, optional: bool, n: usize, get: impl Fn(usize) -> Option<&'a [u8]> + 'a) -> Col<'a> {
    let mut size = 0;
    for i in 0..n {
        if let Some(v) = get(i) {
            size += 4 + v.len();
        }
    }
    let groups = (n + 7) / 8;
    let mut hdr = Vec::new();
    varint(&mut hdr, ((groups as u64) << 1) | 1);
    if optional {
        size += 4 + hdr.len() + groups;
    }
    let emit = Box::new(move |w: &mut Sink| {
        if optional {
            w.put(&((hdr.len() + groups) as u32).to_le_bytes());
            w.put(&hdr);
            for g in 0..groups {
                let mut byte = 0u8;
                for k in 0..8 {
                    let i = g * 8 + k;
                    if i < n && get(i).is_some() {
                        byte |= 1 << k;
                    }
                }
                w.put(&[byte]);
            }
        }
        for i in 0..n {
            if let Some(v) = get(i) {
                w.put(&(v.len() as u32).to_le_bytes());
                w.put(v);
            }
        }
    });
    Col { name, ty: T_BYTE_ARRAY, optional, converted: Some(CT_UTF8), size, emit }
}

fn i32_col<'a>(name: &'static str, converted: Option<i32>, n: usize, get: impl Fn(usize) -> i32 + 'a) -> Col<'a> {
    let emit = Box::new(move |w: &mut Sink| {
        for i in 0..n {
            w.put(&get(i).to_le_bytes());
        }
    });
    Col { name, ty: T_INT32, optional: false, converted, size: 4 * n, emit }
}

fn i64_col<'a>(name: &'static str, n: usize, get: impl Fn(usize) -> i64 + 'a) -> Col<'a> {
    let emit = Box::new(move |w: &mut Sink| {
        for i in 0..n {
            w.put(&get(i).to_le_bytes());
        }
    });
    Col { name, ty: T_INT64, optional: false, converted: None, size: 8 * n, emit }
}

fn write_file(path: &Path, cols: Vec<Col>, nrows: usize) {
    let mut w = Sink::new(path);
    w.put(b"PAR1");
    let mut metas: Vec<(i64, i64)> = Vec::new(); // (data_page_offset, total size)
    for c in &cols {
        let mut h = Thrift::new();
        h.i32(1, 0); // DATA_PAGE
        h.i32(2, c.size as i32);
        h.i32(3, c.size as i32);
        h.begin(5);
        h.i32(1, nrows as i32);
        h.i32(2, ENC_PLAIN);
        h.i32(3, ENC_RLE);
        h.i32(4, ENC_RLE);
        h.end();
        h.b.push(0);
        let off = w.pos as i64;
        w.put(&h.b);
        let before = w.pos;
        (c.emit)(&mut w);
        assert_eq!(w.pos - before, c.size, "column {} size", c.name);
        metas.push((off, w.pos as i64 - off));
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
    w.put(&f.b);
    w.put(&(f.b.len() as u32).to_le_bytes());
    w.put(b"PAR1");
    w.flush();
}

fn write_users(path: &Path, rows: &[UserRow], cats: &[B]) {
    let n = rows.len();
    let cols = vec![
        bytes_col("user_id", false, n, move |i| Some(unpack(rows[i].name))),
        i32_col("day", Some(CT_DATE), n, move |i| rows[i].day),
        i64_col("sessions", n, move |i| rows[i].sessions as i64),
        i64_col("events", n, move |i| rows[i].events as i64),
        i64_col("revenue_cents", n, move |i| rows[i].revenue),
        bytes_col("top_category", true, n, move |i| {
            let t = rows[i].topcat;
            if t == 255 {
                None
            } else {
                Some(cats[t as usize])
            }
        }),
    ];
    write_file(path, cols, n);
}

fn write_camps(path: &Path, rows: &[CampRow], names: &[(B, B)]) {
    let n = rows.len();
    let nm = move |c: usize| -> (&[u8], &[u8]) {
        if c < names.len() {
            names[c]
        } else {
            (b"organic", b"organic")
        }
    };
    let cols = vec![
        bytes_col("campaign_id", false, n, move |i| Some(nm(rows[i].camp).0)),
        bytes_col("channel", false, n, move |i| Some(nm(rows[i].camp).1)),
        i32_col("day", Some(CT_DATE), n, move |i| rows[i].day),
        i64_col("sessions", n, move |i| rows[i].v[0]),
        i64_col("purchasing_sessions", n, move |i| rows[i].v[1]),
        i64_col("revenue_cents", n, move |i| rows[i].v[2]),
        i64_col("assisted_sessions", n, move |i| rows[i].v[3]),
    ];
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
