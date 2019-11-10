use logformat::page::{Page, PageBuffer, PageHeader, BUF_SIZE};
use logformat::*;
use std::io::Cursor;
use uuid::v1::Context;

#[test]
fn can_read_write_page() {
    let mut buffer = PageBuffer {
        buf: &mut [0; BUF_SIZE],
    };

    let node_id = &[0, 1, 2, 3, 4, 5];
    let context = Context::new(0);
    let header = PageHeader::new(node_id, &context, 0, 5000).unwrap();

    {
        let mut page = Page::new(header.clone());
        page.body.key_hash[0] = 0xAB;
        page.body.key_hash[1] = 0xCD;
        page.body.value_index[0] = 100;
        page.body.value_index[1] = 200;
        buffer.serialize(&page);
    }

    {
        let mut page = Page::default();
        buffer.deserialize(&mut page).unwrap();
        assert_eq!(0xAB, page.body.key_hash[0]);
        assert_eq!(0xCD, page.body.key_hash[1]);
        assert_eq!(100, page.body.value_index[0]);
        assert_eq!(200, page.body.value_index[1]);
        assert_eq!(header, page.header);
    }
}

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
