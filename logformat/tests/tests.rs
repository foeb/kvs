use logformat::page::{Page, PageBuffer, PageHeader, BUF_SIZE};
use uuid::v1::Context;

#[test]
fn can_read_write_page() {
    let mut buffer = PageBuffer {
        buf: [0; BUF_SIZE],
    };

    let node_id = &[0, 1, 2, 3, 4, 5];
    let context = Context::new(0);
    let header = PageHeader::new(node_id, &context, 0, 5000, 2).unwrap();

    {
        let mut page = Page::default();
        page.header = header.clone();
        page.body.key_hash[0] = 0xAB;
        page.body.key_hash[1] = 0xCD;
        page.body.value_index[0] = 100;
        page.body.value_index[1] = 200;
        buffer.serialize(&page);
    }

    {
        let mut page = Page::default();
        page.header = header.clone();
        buffer.deserialize(&mut page).unwrap();
        assert_eq!(0xAB, page.body.key_hash[0]);
        assert_eq!(0xCD, page.body.key_hash[1]);
        assert_eq!(100, page.body.value_index[0]);
        assert_eq!(200, page.body.value_index[1]);
        assert_eq!(header, page.header);
    }
}

