// page based storage system

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::env;
use crate::storage::heap::HeapPage;

pub const DB_SUBPATH: &str = "data/tony.db";

pub fn default_db_path() -> std::io::Result<PathBuf> {
    let exe_dir = env::current_exe()?
        .parent()
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "executable has no parent dir"))?
        .to_path_buf();
    Ok(exe_dir.join(DB_SUBPATH))
}

pub const PAGE_SIZE: usize = 4096;
pub const HEADER_SIZE: usize = 9; // 1 byte page type + 4 bytes next page + 2 bytes record count + 2 bytes free space

// a page can be a header, contain data, be an index for tree search, be free space, or be a catalog for storing tables
#[derive(Debug, Clone, Copy)]
pub enum PageType {
    Heap = 0,
    Index = 1,
    Free = 2,
    Catalog = 3,
}

impl From<u8> for PageType {
    fn from(value: u8) -> Self {
        match value {
            0 => PageType::Heap,
            1 => PageType::Index,
            2 => PageType::Free,
            3 => PageType::Catalog,
            _ => panic!("Unknown page type"),
        }
    }
}

// stores metadata abuot the page
#[derive(Debug)]
pub struct PageHeader {
    pub page_type: PageType,
    pub next_page: u32,
    pub record_count: u16,
    pub free_space: u16,
}

impl PageHeader {
    pub fn new(page_type: PageType) -> Self {
        Self {
            page_type,
            next_page: 0,
            record_count: 0,
            free_space: (PAGE_SIZE - HEADER_SIZE) as u16,
        }
    }

    pub fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0] = self.page_type as u8;
        buf[1..5].copy_from_slice(&self.next_page.to_le_bytes());
        buf[5..7].copy_from_slice(&self.record_count.to_le_bytes());
        buf[7..9].copy_from_slice(&self.free_space.to_le_bytes());
        buf
    }

    pub fn from_bytes(buf: &[u8]) -> Self {
        assert!(buf.len() == HEADER_SIZE, "wrong buffer size for header. should be {}", HEADER_SIZE);
        let page_type = PageType::from(buf[0]);

        let mut next_page_bytes = [0u8; 4];
        next_page_bytes.copy_from_slice(&buf[1..5]);
        let next_page = u32::from_le_bytes(next_page_bytes);

        let mut record_count_bytes = [0u8; 2];
        record_count_bytes.copy_from_slice(&buf[5..7]);
        let record_count = u16::from_le_bytes(record_count_bytes);

        let mut free_space_bytes = [0u8; 2];
        free_space_bytes.copy_from_slice(&buf[7..9]);
        let free_space = u16::from_le_bytes(free_space_bytes);

        Self {
            page_type,
            next_page,
            record_count,
            free_space,
        }
    }
}

pub struct Page {
    pub header: PageHeader,
    pub data: Vec<u8>,
}

impl Page {
    pub fn new(page_type: PageType) -> Self {
        Self {
            header: PageHeader::new(page_type),
            // preallocates vec to max data size
            data: Vec::with_capacity(PAGE_SIZE - HEADER_SIZE),
        }
    }

    pub fn from_bytes(buf: &[u8; PAGE_SIZE]) -> Self {
        let header = PageHeader::from_bytes(&buf[..HEADER_SIZE]);

        let data_area = &buf[HEADER_SIZE..];
        // can cast to usize as u16 < usize
        let used_len = data_area.len() - (header.free_space as usize);
        let data = data_area[..used_len].to_vec();

        Self { header, data }
    }

    pub fn write_record(&mut self, record: &str) {
        let bytes = record.as_bytes();
        let len = bytes.len() as u32;
        let mut rec = Vec::new();
        // add the len then the record
        rec.extend_from_slice(&len.to_le_bytes());
        rec.extend_from_slice(bytes);

        // extend the data with the new record
        self.data.extend_from_slice(&rec);
        self.header.record_count += 1;
        self.header.free_space -= rec.len() as u16;
    }

    pub fn to_bytes(&self) -> [u8; PAGE_SIZE] {
        let mut buf = [0u8; PAGE_SIZE];
        buf[..HEADER_SIZE].copy_from_slice(&self.header.to_bytes());

        buf[HEADER_SIZE..HEADER_SIZE + self.data.len()].copy_from_slice(&self.data);

        buf
    }
}

pub struct StorageEngine {
    file: File,
}

// manages pages in a single file
impl StorageEngine {
    pub fn open() -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(default_db_path().unwrap())?;

        Ok(Self { file })
    }

    pub fn file_len(&self) -> std::io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn page_offset(page_num: u32) -> u64 {
        (page_num as u64) * (PAGE_SIZE as u64)
    }

    pub fn read_page(&mut self, page_num: u32, buf: &mut [u8; PAGE_SIZE]) -> std::io::Result<[u8; PAGE_SIZE]> {
        // ? propagates any error up, otherwise it will unwrap the io::Result Ok value and continue
        self.file.seek(SeekFrom::Start(Self::page_offset(page_num)))?;
        self.file.read_exact(buf)?;
        Ok(*buf)
    }

    pub fn read_page_header(&mut self, page_num: u32, buf: &mut [u8; HEADER_SIZE]) -> std::io::Result<[u8; HEADER_SIZE]> {
        self.file.seek(SeekFrom::Start(Self::page_offset(page_num)))?;
        self.file.read_exact(buf)?;
        Ok(*buf)
    }

    pub fn write_page(&mut self, page_num: u32, buf: & [u8; PAGE_SIZE]) -> std::io::Result<()> {
        self.file.seek(SeekFrom::Start(Self::page_offset(page_num)))?;
        self.file.write_all(buf)?;
        self.file.flush()?;
        Ok(())
    }

    pub fn find_or_allocate_heap_page(&mut self, _need_len: usize) -> std::io::Result<u32> {
        todo!()
    }

    pub fn allocate_page(&mut self, page_type: PageType) -> std::io::Result<u32> {
        let page_num = (self.file.metadata()?.len() / PAGE_SIZE as u64) as u32;
        let page = Page::new(page_type);
        self.write_page(page_num, &mut page.to_bytes())?;
        return Ok(page_num);
    }

    pub fn allocate_heap_page(&mut self) -> std::io::Result<u32> {
        let pid = self.allocate_page(PageType::Heap)?;
        HeapPage::init_new(self, pid)?;
        Ok(pid)
    }

    pub fn close(self) -> std::io::Result<()> {
        drop(self);
        return Ok(());
    }

    // to create a table create a root page & point it at a heap page head
    // the heap head is where we traverse from to find space
    // when record inserted, we get a record id and stick it in the BTree
    pub fn create_table(&mut self) -> std::io::Result<u32> {
        let root_id = self.allocate_page(PageType::Index)?;
        let heap_head = self.allocate_heap_page()?;
        let mut root_buf = [0u8; PAGE_SIZE];
        let root_buf = self.read_page(root_id, &mut root_buf)?;

        let mut root_page: Page = Page::from_bytes(&root_buf);
        root_page.header.next_page = heap_head;
        self.write_page(root_id, &root_page.to_bytes());
        Ok(root_id)
    }

    
}