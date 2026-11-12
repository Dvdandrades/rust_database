#![no_main]

use libfuzzer_sys::fuzz_target;
use rust_database::Record;

fuzz_target!(|data: &[u8]| {
    let Ok(record) = Record::deserialize(data) else {
        return;
    };

    let bytes = record.serialize().expect("a record that was just deserialized must serialize");
    let again = Record::deserialize(&bytes).expect("re-serialized bytes must deserialize");
    assert_eq!(record, again);
});