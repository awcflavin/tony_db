use std::io;
use super::storage::{StorageEngine, PAGE_SIZE, HEADER_SIZE, PageHeader, PageType};


pub struct SlotEntry {
    pub id: u16,
    pub offset: u16, // how far to the start of this entry
    pub len: u16, // how far start to end
}

const SLOT_ENTRY_SIZE: usize = 6; // 3 x u16s

pub struct HeapPage {
    pub header: PageHeader,
    pub slots: Vec<SlotEntry>,
    pub data: Vec<u8>,
}

impl HeapPage {
    pub fn init_new(engine: &mut StorageEngine, page_id: u32) -> io::Result<()> {
        todo!()
    }

    pub fn insert(engine: &mut StorageEngine, page_id: u32, record: &[u8]) -> io::Result<()> {
        todo!()
    }

    pub fn read(engine: &mut StorageEngine, page_id: u32, slot: u16) -> io::Result<()> {
        todo!()
    }

    pub fn delete(engine: &mut StorageEngine, page_id: u32, slot: u16) -> io::Result<()> {
        todo!()
    }
}