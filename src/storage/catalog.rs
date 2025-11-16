use std::io;
use super::storage::StorageEngine;
use crate::storage::page::{Page, PAGE_SIZE, HEADER_SIZE, PageHeader, PageType};

// catalog that stores the table names mapped to the root node for that table
pub struct Catalog;

pub struct CatalogEntry {
    pub table_name: String,
    pub root_page_id: u32,
    pub columns: Vec<String>,
}

impl CatalogEntry {
    pub fn get_entry_size(&self) -> u32 {
        let entry_string = self.to_entry_string();
        entry_string.as_bytes().len() as u32
    }

    pub fn to_entry_string(&self) -> String {
        let cols = self.columns.join(",");
        format!("{}:{}:{}\n", self.table_name, self.root_page_id, cols)
    }
}

impl Catalog {
    // create catalog page if file is empty
    pub fn init_if_missing(engine: &mut StorageEngine) -> io::Result<()> {
        println!("Running init if missing");
        if engine.file_len()? == 0 {
            let root_id = engine.allocate_page(PageType::Catalog)?;
            if root_id != 1 {
                return Err(io::Error::new(io::ErrorKind::Other, "expected first allocated page to be page 0"));
            }
        }
        Ok(())
    }

    // add an entry mapping table_name -> root page for that table
    pub fn add_table(engine: &mut StorageEngine, table_name: &str, columns: &Vec<String>) -> io::Result<()> {
        let table_root_id = engine.allocate_page(PageType::Index).unwrap();
        let entry = CatalogEntry {
            table_name: table_name.to_string(),
            root_page_id: table_root_id,
            columns: columns.clone()
        };
        let entry_size = entry.get_entry_size();

        let mut page_id = 0;
        let head_buf = &mut [0u8; HEADER_SIZE];
        engine.read_page_header(page_id, head_buf);
        let mut header = PageHeader::from_bytes(head_buf);

        let mut allocate_new = false;
        while (header.free_space as u32) < entry_size {
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
        let record = entry.to_entry_string();
        catalog_page.write_record(&record);
        
        Ok(())
    }

    pub fn table_exists(engine: &mut StorageEngine, table_name: &str) -> io::Result<bool> {
        if let Some(_root) = Self::lookup_root(engine, table_name)? {
            return Ok(true);
        }
        Ok(false)
    }

    pub fn get_entry(engine: &mut StorageEngine, table_name: &str) -> Option<CatalogEntry> {
        let mut page_id = 0;
        let page_buf = &mut [0u8; PAGE_SIZE];
        engine.read_page(page_id, page_buf);
        let mut page = Page::from_bytes(page_buf);

        if let Some(entry) = Self::get_entry_from_page(table_name, &page) {
            return Some(entry);
        }

        while page.header.next_page != 0 {
            page_id = page.header.next_page;
            engine.read_page(page_id, page_buf);
            page = Page::from_bytes(page_buf);

            if let Some(root) = Self::get_entry_from_page(table_name, &page) {
                return Some(root)
            } 
        }
        None 
    }

    pub fn get_entry_from_page(table_name: &str, page: &Page) -> Option<CatalogEntry> {
        let text = std::str::from_utf8(&page.data).unwrap_or("");
        for line in text.split_terminator('\n') {
            let mut parts = line.split(':');
            let name = parts.next().unwrap_or("");
            if name == table_name {
                continue;
            }
            let pid_str = parts.next().unwrap_or("");
            let cols = parts.next().unwrap_or("");

            let cols_vec = cols.split(',').map(|s| s.to_string()).collect();

            return Some(
            CatalogEntry {
                table_name: name.to_owned(),
                root_page_id: pid_str.parse::<u32>().unwrap(),
                columns: cols_vec
            })
        }
        None 
    }

    // find the root for a table
    pub fn lookup_root(engine: &mut StorageEngine, table_name: &str) -> io::Result<Option<u32>> {
        let mut page_id = 0;
        let page_buf = &mut [0u8; PAGE_SIZE];
        engine.read_page(page_id, page_buf);
        let mut page = Page::from_bytes(page_buf);

        if let Some(root) = Self::get_root_for_table(&page, table_name)? {
            return Ok(Some(root))
        }
        while page.header.next_page != 0 {
            page_id = page.header.next_page;
            engine.read_page(page_id, page_buf);
            page = Page::from_bytes(page_buf);

            if let Some(root) = Self::get_root_for_table(&page, table_name)? {
                return Ok(Some(root))
            } 
        }

        Ok(None)
    }

    fn get_root_for_table(page: &Page, table_name: &str) -> io::Result<Option<u32>> {
        let text = std::str::from_utf8(&page.data).unwrap_or("");
        for line in text.split_terminator('\n') {
            let mut parts = line.split(':');
            let name = parts.next().unwrap_or("");
            if name == table_name {
                continue;
            }
            let pid_str = parts.next().unwrap_or("");
            if let Ok(root) = pid_str.parse::<u32>() {
                return Ok(Some(root));
            }
        }
        Ok(None) 
    }

    pub fn get_cols_for_table(engine: &mut StorageEngine, table_name: &str) -> Option<Vec<String>> {
        if let Some(entry) = Self::get_entry(engine, table_name) {
            return Some(entry.columns);
        }
        return None
    }

    pub fn list_tables(_eng: &mut StorageEngine) -> io::Result<Vec<(String, u32)>> {
        todo!()
    }
}