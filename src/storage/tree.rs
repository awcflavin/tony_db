use crate::storage::storage::StorageEngine;
use crate::storage::page::{PageType, PAGE_SIZE, HEADER_SIZE};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordId {
    pub page_id: u32,
    pub slot: u32,
}

// these nodes are either leaf nodes or internal nodes
// they store children that are gt and lt its keys
struct Node {
    page_id: u32,
    is_leaf: bool,
    keys: Vec<String>,
    rids: Vec<RecordId>, // tie tree leaf nodes to records
    children: Vec<u32>,
    next_leaf: u32, // for scans
}

const NODE_HDR_SIZE: usize = 1 + 2 + 4; // is_leaf + key_count + next_leaf
const MAX_KEYS: usize = 4;

impl Node {

    fn new_leaf(page_id: u32) -> Self {
        Node {
            page_id,
            keys: Vec::new(),
            rids: Vec::new(),
            children: Vec::new(),
            is_leaf: true,
            next_leaf: 0,
        }
    }

    fn new_internal(page_id: u32) -> Self {
        Node {
            page_id,
            keys: Vec::new(),
            rids: Vec::new(),
            children: Vec::new(),
            is_leaf: false,
            next_leaf: 0,
        }
    }

    // get a node from storage
    fn load(engine: &mut StorageEngine, page_id: u32) -> std::io::Result<Self> {
        let mut buf = [0u8; PAGE_SIZE];
        engine.read_page(page_id, &mut buf)?;
        let content = &buf[HEADER_SIZE..];
        let is_leaf = content[0] == 1;
        let key_count = u16::from_le_bytes([content[1], content[2]]) as usize; // 2 bytes for u16
        let next_leaf = u32::from_le_bytes([content[3], content[4], content[5], content[6]]); // 4 bytes for u32
        let mut offset = NODE_HDR_SIZE;

        let mut keys = Vec::with_capacity(key_count);
        for _ in 0..key_count {
            let key_len = u16::from_le_bytes([content[offset], content[offset + 1]]) as usize;
            offset += 2;
            let key = String::from_utf8(content[offset..offset + key_len].to_vec()).unwrap();
            keys.push(key);
            offset += key_len;
        }

        let mut rids = Vec::new();
        let mut children = Vec::new();
        if !is_leaf {
            for _ in 0..key_count+1 { // +1 cos internal nodes have n+1 children for n keys
                let id = u32::from_le_bytes([
                    content[offset],
                    content[offset + 1],
                    content[offset + 2],
                    content[offset + 3],
                ]);
                offset += 4;
                children.push(id);
            }
        } else {
            // one record per key
            for _ in 0..key_count {
                let page_id = u32::from_le_bytes([
                    content[offset],
                    content[offset + 1],
                    content[offset + 2],
                    content[offset + 3],
                ]);
                offset += 4;
                let slot = u32::from_le_bytes([
                    content[offset],
                    content[offset + 1],
                    content[offset + 2],
                    content[offset + 3],
                ]);
                offset += 4;
                rids.push(RecordId { page_id, slot } );
            }
        }

        Ok(Self {
            page_id,
            is_leaf,
            keys,
            rids,
            children,
            next_leaf,
        })
    }

    // persist this in the storage
    fn persist(&self, engine: &mut StorageEngine) -> std::io::Result<()> {
        let mut buf = [0u8; PAGE_SIZE];
        let content = &mut buf[HEADER_SIZE..];
        content[0] = if self.is_leaf { 1 } else { 0 };
        let key_count = self.keys.len() as u16;
        content[1..3].copy_from_slice(&key_count.to_le_bytes()); // 1..3 = 2 bytes for u16
        content[3..7].copy_from_slice(&self.next_leaf.to_le_bytes()); // 4 bytes for u32
        let mut offset = NODE_HDR_SIZE;

        for key in &self.keys {
            let key_bytes = key.as_bytes();
            let key_len = key_bytes.len() as u16;
            content[offset..offset + 2].copy_from_slice(&key_len.to_le_bytes()); // +2 cos u16
            offset += 2;
            content[offset..offset + key_bytes.len()].copy_from_slice(key_bytes);
            offset += key_bytes.len();
        }

        if !self.is_leaf {
            for &child in &self.children {
                content[offset..offset + 4].copy_from_slice(&child.to_le_bytes()); // +4 cos u32
                offset += 4;
            }
        } else {
            for rid in &self.rids {
                content[offset..offset + 4].copy_from_slice(&rid.page_id.to_le_bytes());
                offset += 4;
                content[offset..offset + 4].copy_from_slice(&rid.slot.to_le_bytes());
                offset += 4;
            }
        }

        engine.write_page(self.page_id, &buf);
        Ok(())
    }

        
}


struct BTree {
    storage: StorageEngine,
    root: u32,
    column: String,
    // add table here at some stage so can index multiple columns
}

impl BTree {
    fn new(mut storage: StorageEngine, column: String) -> std::io::Result<Self> {
        let root_page = storage.allocate_page(PageType::Index)?;
        let root_node = Node::new_leaf(root_page);
        root_node.persist(&mut storage)?;
        Ok(BTree {storage, root: root_page, column })
    }

    fn insert(&mut self, key: String, rid: RecordId) -> std::io::Result<()> {
        let root = Node::load(&mut self.storage, self.root).unwrap();
        
        if root.keys.len() == MAX_KEYS {
            let new_root_page = self.storage.allocate_page(PageType::Index).unwrap();
            let mut new_root = Node::new_internal(new_root_page);
            new_root.children.push(root.page_id);
            self.split_child(&mut new_root, 0);
            new_root.persist(&mut self.storage).unwrap();
            self.root = new_root_page;
        }

        return self.insert_non_full(root.page_id, key, rid);
    }

    fn insert_non_full(&mut self, page_id: u32, key: String, rid: RecordId) -> std::io::Result<()> {
        let mut node = Node::load(&mut self.storage, page_id)?;
        if node.is_leaf {
            let pos = node.keys.binary_search(&key).unwrap_or_else(|e| e);
            node.keys.insert(pos, key);
            node.rids.insert(pos, rid);
            node.persist(&mut self.storage)?;
            return Ok(());
        }

        let mut next_index = match node.keys.binary_search(&key) {
            Ok(idx) => idx + 1,
            Err(idx) => idx,
        };

        let mut child = Node::load(&mut self.storage, node.children[next_index])?;
        if child.keys.len() == MAX_KEYS {
            self.split_child(&mut node, next_index)?;
            if key > node.keys[next_index] {
                next_index += 1;
            }
        }

        let next_child = node.children[next_index];
        return self.insert_non_full(next_child, key, rid)

    }

    fn split_child(&mut self, parent: &mut Node, index: usize) -> std::io::Result<()> {
        //get left
        let mut left = Node::load(&mut self.storage, parent.children[index])?;
        // create a new right node
        let right_id = self.storage.allocate_page(PageType::Index)?;
        let mut right = if left.is_leaf {
            Node::new_leaf(right_id)
        } else {
            Node::new_internal(right_id)
        };
        // move half the keys to right
        // no children if leaf
        if right.is_leaf {
            let mid = (left.keys.len() +1) /2;
            right.keys = left.keys.split_off(mid);
            right.next_leaf = left.next_leaf;
            left.next_leaf = right.page_id;

            parent.keys.insert(index, right.keys[0].clone()); // first key of right goes into parent
            parent.children.insert(index + 1, right.page_id);
        } else {
            let mid = (left.keys.len()+1) /2;
            right.keys = left.keys.split_off(mid);

            right.children = left.children.split_off(mid + 1); // split off leaves the remainder in left
            let mid_key = right.keys.remove(0); // remove mid key from right to go to parent
            parent.keys.insert(index, mid_key);
            parent.children.insert(index + 1, right.page_id);
        }

        // persist all
        left.persist(&mut self.storage)?;
        right.persist(&mut self.storage)?;
        parent.persist(&mut self.storage)?;
        
        Ok(())
    }

    pub fn get(&mut self, key: &String) -> std::io::Result<Option<RecordId>> {
        let mut pid = self.root;
        loop {
            let node = Node::load(&mut self.storage, pid)?;
            let pos = node.keys.binary_search(key);

            if node.is_leaf {
                match pos {
                    Ok(idx) => return Ok(Some(node.rids[idx])),
                    Err(_) => return Ok(None),
                }
            } else {
                pid = match pos {
                    Ok(idx) => node.children[idx + 1],
                    // err(idx) means idx is the first position where key could be inserted
                    // example:
                    // keys: [10, 20, 30]
                    // children: [c0, c1, c2, c3]
                    // key = 25 -> err(2) -> go to c2
                    // c2 are elements >20 and <30
                    Err(idx) => node.children[idx],
                };
            }
        }
    }

}


