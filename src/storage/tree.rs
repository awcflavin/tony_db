use crate::storage::memory::Storage;

// these nodes BOTH store values (keys) and have children
// they store children that are gt and lt its keys but its keys
// are themselves not stored in children,they are stored in this node
struct Node {
    keys: Vec<String>,
    children: Vec<Box<Node>>,
    is_leaf: bool,
}

impl Node {
    fn new(is_leaf: bool) -> Self {
        Node {
            keys: Vec::new(),
            children: Vec::new(),
            is_leaf,
        }
    }        
}

const MAX_KEYS: usize = 4; // Example order for B-tree

struct BTree {
    storage: Storage,
    root: Option<Box<Node>>,
    height: usize,
    column: String, // The column this B-tree indexes
}

impl BTree {
    fn new(storage: Storage, column: String) -> Self {
        BTree {
            storage,
            root: None,
            height: 0,
            column,
        }
    }

    fn insert(&mut self, key: String) {
        if let Some(mut root) = self.root.take() {
            if root.keys.len() == MAX_KEYS {
                let mut new_root = Box::new(Node::new(false));
                new_root.children.push(root);
                self.split_child(&mut new_root, 0);
                self.insert_non_full(&mut new_root, key);
                self.root = Some(new_root);
                self.height += 1;
            } else {
                self.insert_non_full(&mut root, key);
                self.root = Some(root);
            }
        } else {
            let mut root = Box::new(Node::new(true));
            root.keys.push(key);
            self.root = Some(root);
            self.height = 1;
        }
    }

    // when inserting a new node: create new node, add old node as child of new node,
    // then call this function to split the old node and update the new node
    // note this doesnt check if parent is full
    fn split_child(&mut self, parent: &mut Box<Node>, index: usize) {
        let child = &mut parent.children[index];
        let mid_index = MAX_KEYS / 2;

        let median = child.keys[mid_index].clone();

        let mut right = Box::new(Node::new(child.is_leaf));
        right.keys = child.keys.split_off(mid_index + 1);
        child.keys.pop(); // remove median from left cos in parent now

        if !child.is_leaf {
            right.children = child.children.split_off(mid_index + 1);
        }

        parent.keys.insert(index, median);
        parent.children.insert(index + 1, right);
    }

    // inserts a key into a non full node
    fn insert_non_full(&mut self, node: &mut Box<Node>, key: String) {
        // if leaf just stikc it in
        if node.is_leaf {
            let pos = node.keys.binary_search(&key).unwrap_or_else(|e| e);
            node.keys.insert(pos, key);
        // else find the child to recurse on
        } else {
            let mut index = node.keys.len();
            for (i, k) in node.keys.iter().enumerate() {
                if key < *k {
                    index = i;
                    break;
                }
            }
            if node.children[index].keys.len() == MAX_KEYS {
                self.split_child(node, index);
                if key > node.keys[index] {
                    index += 1;
                }
            }
            self.insert_non_full(&mut node.children[index], key);
        }
    }

    fn search(&self, key: &str) -> Option<&Node> {
        fn search_node<'a>(node: &'a Node, key: &str) -> Option<&'a Node> {
            let mut i = 0;
            while i < node.keys.len() && key > &node.keys[i] {
                i += 1;
            }

            if i < node.keys.len() && key == &node.keys[i] {
                return Some(node);
            }

            if node.is_leaf {
                return None;
            }

            return search_node(&node.children[i], key);
        }

        if let Some(root) = &self.root {
            search_node(root, key)
        } else {
            None
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        vec![]
    }

    fn from_bytes(data: &[u8], storage: Storage, column: String) -> Self {
        BTree {
            storage,
            root: None,
            height: 0,
            column,
        }
    }
}