use std::io;
use super::storage::{StorageEngine, Page, PAGE_SIZE, HEADER_SIZE, PageHeader, PageType};

// catalog that stores the table names mapped to the root node for that table
pub struct Catalog;

impl Catalog {
    // create catalog page if file is empty
    pub fn init_if_missing(engine: &mut StorageEngine) -> io::Result<()> {

        if engine.file_len()? == 0 {
            let root_id = engine.allocate_page(PageType::Catalog)?;
            if root_id != 1 {
                return Err(io::Error::new(io::ErrorKind::Other, "expected first allocated page to be page 0"));
            }
        }
        Ok(())
    }

    // add an entry mapping table_name -> root page for that table
    pub fn add_table(engine: &mut StorageEngine, _table_name: &str, _root_pid: u32) -> io::Result<()> {
        let mut page_id = 0;
        let head_buf = &mut [0u8; HEADER_SIZE];
        engine.read_page_header(page_id, head_buf);
        let mut header = PageHeader::from_bytes(head_buf);

        let mut allocate_new = false;
        while header.free_space == 0 {
            let next = header.next_page;
            if next == 0 {
                allocate_new = true;
                break
            }
            engine.read_page_header(next, head_buf);
            header = PageHeader::from_bytes(head_buf);
        }

        if allocate_new {
            page_id = engine.allocate_page(PageType::Catalog)?;
            header.next_page = page_id;
        }

        // write entry to page
        let page_buf = &mut [0u8; PAGE_SIZE];
        engine.read_page(page_id, page_buf)?;
        let mut catalog_page = Page::from_bytes(page_buf);
        let record = format!("{}:{}\n", _table_name, _root_pid);
        catalog_page.write_record(&record);
        
        Ok(())
    }

    // find the root for a table
    pub fn lookup_root(engine: &mut StorageEngine, _table_name: &str) -> io::Result<Option<u32>> {
        let mut page_id = 0;
        let page_buf = &mut [0u8; PAGE_SIZE];
        engine.read_page(page_id, page_buf);
        let mut page = Page::from_bytes(page_buf);

        if let Some(root) = Self::get_root_for_table(&page, _table_name)? {
            return Ok(Some(root))
        }
        while page.header.next_page != 0 {
            page_id = page.header.next_page;
            engine.read_page(page_id, page_buf);
            page = Page::from_bytes(page_buf);

            if let Some(root) = Self::get_root_for_table(&page, _table_name)? {
                return Ok(Some(root))
            } 
        }

        Ok(None)
    }

    fn get_root_for_table(page: &Page, table_name: &str) -> io::Result<Option<u32>> {
        let text = std::str::from_utf8(&page.data).unwrap_or("");
        for line in text.split_terminator('\n') {
            if let Some((name, pid)) = line.split_once(':') {
                if name == table_name {
                    if let Ok(root) = pid.parse::<u32>() {
                        return Ok(Some(root))
                    }
                }
            }
        }
        Ok(None) 
    }

    pub fn list_tables(_eng: &mut StorageEngine) -> io::Result<Vec<(String, u32)>> {
        todo!()
    }
}