use std::io;

pub const PAGE_SIZE: usize = 4096;
pub const HEADER_SIZE: usize = 9; // 1 byte page type + 4 bytes next page + 2 bytes record count + 2 bytes free space
pub const COMMON_HEADER_SIZE: usize = 5; // 1 byte type + 4 bytes next_page
pub const HEAP_HEADER_SIZE: usize = COMMON_HEADER_SIZE + 2 + 2; // + slot_count + free_start

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

pub struct CommonHeader {
    pub page_type: PageType,
    pub next_page: u32,
}

impl CommonHeader {
    pub fn to_bytes(&self) -> [u8; COMMON_HEADER_SIZE] {
        let mut b = [0u8; COMMON_HEADER_SIZE];
        b[0] = self.page_type as u8;
        b[1..5].copy_from_slice(&self.next_page.to_le_bytes());
        b
    }
    pub fn from_bytes(buf: &[u8]) -> Self {
        let mut np = [0u8;4]; np.copy_from_slice(&buf[1..5]);
        Self { page_type: PageType::from(buf[0]), next_page: u32::from_le_bytes(np) }
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

pub struct SlotEntry {
    pub id: u16,
    pub offset: u16, // how far to the start of this entry
    pub len: u16, // how far start to end
}

pub const SLOT_ENTRY_SIZE: usize = 6; // 3 x u16s

pub struct HeapPageHeader {
    pub common: CommonHeader,
    pub slot_count: u16,
    pub free_start: u16, // offset where next record bytes will be written (start of free space from top)
}

impl HeapPageHeader {
    pub fn new() -> Self {
        Self {
            common: CommonHeader { page_type: PageType::Heap, next_page: 0 },
            slot_count: 0,
            free_start: HEAP_HEADER_SIZE as u16, // data grows upward from end of header
        }
    }
    pub fn to_bytes(&self) -> [u8; HEAP_HEADER_SIZE] {
        let mut b = [0u8; HEAP_HEADER_SIZE];
        b[..COMMON_HEADER_SIZE].copy_from_slice(&self.common.to_bytes());
        b[COMMON_HEADER_SIZE..COMMON_HEADER_SIZE+2].copy_from_slice(&self.slot_count.to_le_bytes());
        b[COMMON_HEADER_SIZE+2..COMMON_HEADER_SIZE+4].copy_from_slice(&self.free_start.to_le_bytes());
        b
    }
    pub fn from_bytes(buf: &[u8]) -> Self {
        let common = CommonHeader::from_bytes(&buf[..COMMON_HEADER_SIZE]);
        let mut scb=[0u8;2]; scb.copy_from_slice(&buf[COMMON_HEADER_SIZE..COMMON_HEADER_SIZE+2]);
        let mut fsb=[0u8;2]; fsb.copy_from_slice(&buf[COMMON_HEADER_SIZE+2..COMMON_HEADER_SIZE+4]);
        Self { common, slot_count: u16::from_le_bytes(scb), free_start: u16::from_le_bytes(fsb) }
    }
    pub fn free_space(&self) -> usize {
        // slot directory grows downward from end of page
        let slot_dir_bytes = self.slot_count as usize * SLOT_ENTRY_SIZE;
        let slot_dir_start = PAGE_SIZE - slot_dir_bytes;
        if slot_dir_start < self.free_start as usize { 0 } else { slot_dir_start - self.free_start as usize }
    }
}

pub struct HeapPage {
    pub header: HeapPageHeader,
    pub slots: Vec<SlotEntry>,
    pub data: Vec<u8>,
}

impl HeapPage {
    pub fn new() -> Self {
        Self {
            header: HeapPageHeader::new(),
            // preallocates vec to max data size
            slots: Vec::new(),
            data: Vec::new(),
        }
    }

    pub fn from_bytes(buf: &[u8; PAGE_SIZE]) -> Self {
        let header = HeapPageHeader::from_bytes(&buf[..HEAP_HEADER_SIZE]);

        let mut slots = Vec::with_capacity(header.slot_count as usize);
        // slots from end of the page
        for i in 0..header.slot_count as usize {
            let start = PAGE_SIZE - ((i+1) * SLOT_ENTRY_SIZE);
            let mut idb = [0u8;2]; idb.copy_from_slice(&buf[start..start+2]);
            let mut offb = [0u8;2]; offb.copy_from_slice(&buf[start+2..start+4]);
            let mut lenb = [0u8;2]; lenb.copy_from_slice(&buf[start+4..start+6]);
            slots.push(SlotEntry {
                id: u16::from_le_bytes(idb),
                offset: u16::from_le_bytes(offb),
                len: u16::from_le_bytes(lenb),
            });
        }

        // entries
        let data_end = header.free_start as usize;
        let records = buf[HEAP_HEADER_SIZE..data_end].to_vec();

        Self {
            header,
            slots,
            data: records
        }
    }

    pub fn to_bytes(&self) -> [u8; PAGE_SIZE] {
        let mut buf = [0u8; PAGE_SIZE];
        buf[..HEAP_HEADER_SIZE].copy_from_slice(&self.header.to_bytes());

        let entries_end = HEAP_HEADER_SIZE + self.data.len();
        buf[HEAP_HEADER_SIZE..entries_end].copy_from_slice(&self.data);

        // from bottom for slots
        for (i, s) in self.slots.iter().enumerate() {
            let slot_start = PAGE_SIZE - ((i+1) * SLOT_ENTRY_SIZE);
            buf[slot_start..slot_start+2].copy_from_slice(&s.id.to_le_bytes());
            buf[slot_start+2..slot_start+4].copy_from_slice(&s.offset.to_le_bytes());
            buf[slot_start+4..slot_start+6].copy_from_slice(&s.len.to_le_bytes());
        }
        buf
    }

    pub fn write_record(&mut self, record: &str) -> io::Result<u16> {
        let bytes = record.as_bytes();
        let len = bytes.len() as u32;
        let mut rec = Vec::with_capacity(4+bytes.len());
        // add the len then the record
        rec.extend_from_slice(&len.to_le_bytes());
        rec.extend_from_slice(bytes);

        let offset = (HEAP_HEADER_SIZE + self.data.len()) as u16;
        self.data.extend_from_slice(&rec); // record into the data area
        self.header.free_start = offset + rec.len() as u16;

        let id = self.slots.len() as u16;
        self.slots.push(SlotEntry {
            id,
            offset: offset,
            len: rec.len() as u16,
        });
        self.header.slot_count = self.slots.len() as u16;

        Ok(id)
    }
}