use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;

use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;

use super::{unpack, CampRow, UserRow, B};

const USER_SCHEMA: &str = "message user_daily {
  REQUIRED BINARY user_id (STRING);
  REQUIRED INT32 day (DATE);
  REQUIRED INT64 sessions;
  REQUIRED INT64 events;
  REQUIRED INT64 revenue_cents;
  OPTIONAL BINARY top_category (STRING);
}";

const CAMP_SCHEMA: &str = "message campaign_daily {
  REQUIRED BINARY campaign_id (STRING);
  REQUIRED BINARY channel (STRING);
  REQUIRED INT32 day (DATE);
  REQUIRED INT64 sessions;
  REQUIRED INT64 purchasing_sessions;
  REQUIRED INT64 revenue_cents;
  REQUIRED INT64 assisted_sessions;
}";

fn props() -> Arc<WriterProperties> {
    Arc::new(WriterProperties::builder().set_compression(Compression::UNCOMPRESSED).build())
}

fn ba(s: &[u8]) -> ByteArray {
    ByteArray::from(s.to_vec())
}

fn write_users(path: &Path, rows: &[&[UserRow]], cats: &[B]) {
    let schema = Arc::new(parse_message_type(USER_SCHEMA).unwrap());
    let file = File::create(path).unwrap();
    let mut w = SerializedFileWriter::new(file, schema, props()).unwrap();
    let all: Vec<&UserRow> = rows.iter().flat_map(|r| r.iter()).collect();
    for part in all.chunks(1 << 20) {
        let mut rg = w.next_row_group().unwrap();
        let names: Vec<ByteArray> = part.iter().map(|r| ba(unpack(r.name))).collect();
        let days: Vec<i32> = part.iter().map(|r| r.day).collect();
        let ses: Vec<i64> = part.iter().map(|r| r.sessions as i64).collect();
        let evs: Vec<i64> = part.iter().map(|r| r.events as i64).collect();
        let rev: Vec<i64> = part.iter().map(|r| r.revenue).collect();
        let mut defs: Vec<i16> = Vec::with_capacity(part.len());
        let mut tops: Vec<ByteArray> = Vec::new();
        for r in part {
            if r.topcat == 255 {
                defs.push(0);
            } else {
                defs.push(1);
                tops.push(ba(cats[r.topcat as usize]));
            }
        }
        let mut i = 0;
        while let Some(mut col) = rg.next_column().unwrap() {
            match i {
                0 => { col.typed::<ByteArrayType>().write_batch(&names, None, None).unwrap(); }
                1 => { col.typed::<Int32Type>().write_batch(&days, None, None).unwrap(); }
                2 => { col.typed::<Int64Type>().write_batch(&ses, None, None).unwrap(); }
                3 => { col.typed::<Int64Type>().write_batch(&evs, None, None).unwrap(); }
                4 => { col.typed::<Int64Type>().write_batch(&rev, None, None).unwrap(); }
                _ => { col.typed::<ByteArrayType>().write_batch(&tops, Some(&defs), None).unwrap(); }
            }
            col.close().unwrap();
            i += 1;
        }
        rg.close().unwrap();
    }
    w.close().unwrap();
}

fn write_camps(path: &Path, rows: &[CampRow], names: &[(B, B)]) {
    let schema = Arc::new(parse_message_type(CAMP_SCHEMA).unwrap());
    let file = File::create(path).unwrap();
    let mut w = SerializedFileWriter::new(file, schema, props()).unwrap();
    for part in rows.chunks(1 << 20) {
        let mut rg = w.next_row_group().unwrap();
        let nm = |c: usize| -> (&[u8], &[u8]) { if c < names.len() { names[c] } else { (b"organic", b"organic") } };
        let ids: Vec<ByteArray> = part.iter().map(|r| ba(nm(r.camp).0)).collect();
        let chs: Vec<ByteArray> = part.iter().map(|r| ba(nm(r.camp).1)).collect();
        let days: Vec<i32> = part.iter().map(|r| r.day).collect();
        let col_i64 = |k: usize| -> Vec<i64> { part.iter().map(|r| r.v[k]).collect() };
        let (s, p, rv, a) = (col_i64(0), col_i64(1), col_i64(2), col_i64(3));
        let mut i = 0;
        while let Some(mut col) = rg.next_column().unwrap() {
            match i {
                0 => { col.typed::<ByteArrayType>().write_batch(&ids, None, None).unwrap(); }
                1 => { col.typed::<ByteArrayType>().write_batch(&chs, None, None).unwrap(); }
                2 => { col.typed::<Int32Type>().write_batch(&days, None, None).unwrap(); }
                3 => { col.typed::<Int64Type>().write_batch(&s, None, None).unwrap(); }
                4 => { col.typed::<Int64Type>().write_batch(&p, None, None).unwrap(); }
                5 => { col.typed::<Int64Type>().write_batch(&rv, None, None).unwrap(); }
                _ => { col.typed::<Int64Type>().write_batch(&a, None, None).unwrap(); }
            }
            col.close().unwrap();
            i += 1;
        }
        rg.close().unwrap();
    }
    w.close().unwrap();
}

pub fn write_all(outd: &Path, users: &[&[UserRow]], camps: &[CampRow], cats: &[B], names: &[(B, B)], _nt: usize) {
    let ud = outd.join("user_daily");
    let cd = outd.join("campaign_daily");
    fs::create_dir_all(&ud).unwrap();
    fs::create_dir_all(&cd).unwrap();
    std::thread::scope(|s| {
        // one file per worker output, written in parallel
        let mut hs = Vec::new();
        for (i, u) in users.iter().enumerate() {
            let ud = &ud;
            hs.push(s.spawn(move || write_users(&ud.join(format!("part-{i}.parquet")), &[u], cats)));
        }
        write_camps(&cd.join("part-0.parquet"), camps, names);
        for h in hs {
            h.join().unwrap();
        }
    });
}
