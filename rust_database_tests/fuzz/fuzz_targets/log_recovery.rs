#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use rust_database::{DbStore, Record};

const PROBE: &[u8] = b"\0fuzz-probe\0";
const BATCH_BEGIN: u8 = 3;

#[derive(Debug, Arbitrary)]
struct Input {
    chunks: Vec<Chunk>,
}

#[derive(Debug, Arbitrary)]
enum Chunk {
    Raw(Vec<u8>),
    Frame(Vec<u8>),
    Rec {
        recid: u8,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Batch {
        recs: Vec<(u8, Vec<u8>, Vec<u8>)>,
        count_delta: i8,
        break_body_crc: bool,
        body_len_override: Option<u64>,
    },
}

fn crc32(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

fn frame(payload: &[u8], out: &mut Vec<u8>) {
    out.extend_from_slice(&crc32(payload).to_le_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(payload)
}

fn record_payload(recid: u8, key: Vec<u8>, value: Vec<u8>) -> Vec<u8> {
    Record { recid, key, value }.serialize().expect("serializing a Record cannot fail")
}

fn encode(input: &Input) -> Vec<u8> {
    let mut out = Vec::new();
    for chunk in &input.chunks {
        match chunk {
            Chunk::Raw(bytes) => out.extend_from_slice(bytes),
            Chunk::Frame(payload) => frame(payload, &mut out),
            Chunk::Rec { recid, key, value } => frame(&record_payload(*recid, key.clone(), value.clone()), &mut out),
            Chunk::Batch {
                recs,
                count_delta,
                break_body_crc,
                body_len_override,
            } => {
                let mut body = Vec::new();
                for (recid, key, value) in recs {
                    frame(&record_payload(*recid, key.clone(), value.clone()), &mut body);
                }
                let count = (recs.len() as u32).wrapping_add(*count_delta as i32 as u32);
                let mut body_crc = crc32(&body);
                if *break_body_crc {
                    body_crc ^= 1;
                }
                let mut meta = Vec::with_capacity(16);
                meta.extend_from_slice(&count.to_le_bytes());
                let body_len = body_len_override.unwrap_or(body.len() as u64);
                meta.extend_from_slice(&body_len.to_le_bytes());
                meta.extend_from_slice(&body_crc.to_le_bytes());

                frame(&record_payload(BATCH_BEGIN, Vec::new(), meta), &mut out);
                out.extend_from_slice(&body);
            }
        }
    }
    out
}

fuzz_target!(|input: Input| {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("0000000000.log"), encode(&input)).expect("write segment");

    let Ok(db) = DbStore::open(dir.path()) else {
        return;
    };

    let live: Vec<(Vec<u8>, Vec<u8>)> = db.scan_prefix(b"").map(|item| item.expect("every recovered key must be readable")).collect();
    for (key, value) in &live {
        assert_eq!(&db.get(key).expect("get of a live key"), value);
    }

    db.put(PROBE.to_vec(), b"alive".to_vec()).expect("a recovered database must accept writes");
    drop(db);

    let db = DbStore::open(dir.path()).expect("a database that was usable must reopen");
    assert_eq!(db.get(PROBE).expect("probe must survive restart"), b"alive");
    for (key, value) in live {
        if key != PROBE {
            assert_eq!(db.get(&key).expect("recovered key after restart"), value);
        }
    }
});