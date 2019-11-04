use logformat::*;
use std::io::Cursor;

#[test]
fn can_read_entry() {
    let mut entry_buf = vec![0u8; 500];
    let mut data_buf = vec![0u8; 500];
    let entry = mem::Entry::Set {
        key: mem::Value::Integer(42),
        value: mem::Value::String("hey there".to_owned()),
    };

    {
        let entry_cursor = Cursor::new(&mut entry_buf);
        let data_cursor = Cursor::new(&mut data_buf);

        let mut writer = LogWriter::new(entry_cursor, data_cursor).expect("writer");
        writer.write_entry(&entry).expect("write entry");
        writer.flush().expect("flush");
    }

    {
        let entry_cursor = Cursor::new(&mut entry_buf);
        let data_cursor = Cursor::new(&mut data_buf);

        let mut reader = LogReader::new(entry_cursor, data_cursor).expect("reader");
        reader.seek(0).expect("seek");
        let should_be_our_entry = reader.read_entry().expect("lookup entry");

        assert_eq!(Some(entry), should_be_our_entry);
    }
}
