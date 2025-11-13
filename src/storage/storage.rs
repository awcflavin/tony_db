// page based storage system

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::env;
use crate::storage::page::{self, HEADER_SIZE, HEAP_HEADER_SIZE, HeapPage, HeapPageHeader, PAGE_SIZE, Page, PageHeader, PageType, SLOT_ENTRY_SIZE, SlotEntry};

pub const DB_SUBPATH: &str = "data/tony.db";

pub fn default_db_path() -> std::io::Result<PathBuf> {
    let exe_dir = env::current_exe()?
        .parent()
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "executable has no parent dir"))?
        .to_path_buf();
    Ok(exe_dir.join(DB_SUBPATH))
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

    pub fn find_or_allocate_heap_page(&mut self, head_page: u32, need_len: usize) -> std::io::Result<u32> {
       let need = need_len + SLOT_ENTRY_SIZE;

       let mut current = head_page;
       loop {
            let mut header_buf = [0u8; HEAP_HEADER_SIZE];
            self.file.seek(SeekFrom::Start(Self::page_offset(current)))?;
            self.file.read_exact(&mut header_buf)?;

            let mut heap_hdr = HeapPageHeader::from_bytes(&mut header_buf);
            if heap_hdr.free_space() >= need {
                return Ok(current);
            }

            if heap_hdr.common.next_page != 0 {
                current = heap_hdr.common.next_page;
                continue;
            }

            let new_page = self.allocate_page(PageType::Heap)?;
            heap_hdr.common.next_page = new_page;

            // need to overwrite the heap_hdr after updating its next_page
            self.file.seek(SeekFrom::Start(Self::page_offset(current)))?;
            self.file.write_all(&heap_hdr.to_bytes())?;
            self.file.flush()?;

            return Ok(new_page);
       }
    }

    pub fn allocate_page(&mut self, page_type: PageType) -> std::io::Result<u32> {
        let page_num = (self.file.metadata()?.len() / PAGE_SIZE as u64) as u32;
        match page_type {
            PageType::Heap => {
                let heap_page = HeapPage::new();
                let mut buf = [0u8; PAGE_SIZE];
                buf[..HEADER_SIZE].copy_from_slice(&heap_page.header.to_bytes());
                self.write_page(page_num, &buf)?;
                return Ok(page_num);
            },
            _ => {
                let page = Page::new(page_type);
                self.write_page(page_num, &mut page.to_bytes())?;
                return Ok(page_num);
            }
        }
    }

    pub fn close(self) -> std::io::Result<()> {
        drop(self);
        return Ok(());
    }
    
}