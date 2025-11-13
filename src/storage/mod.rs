mod storage;
mod tree;
mod catalog;
mod page;
// use std::sync::{Arc, RwLock};
// use once_cell::sync::Lazy;

// type Database = HashMap<String, Vec<Vec<String>>>;
// static DB: Lazy<RwLock<Database>> = Lazy::new(|| {
//     println!("Initializing database storage...");
//     RwLock::new(HashMap::new())
// });

// pub fn insert_row(table: &str, row: Vec<String>) -> Result<(), String> {
//     let mut db = DB.write().map_err(|_| "Failed to acquire write lock".to_string())?;
    
//     // Get or create the table and insert the row
//     let table_data = db.entry(table.to_string()).or_insert_with(Vec::new);
//     table_data.push(row);
    
//     Ok(())
// }

// pub fn get_rows(table: &str) -> Result<Vec<Vec<String>>, String> {
//     let db = DB.read().map_err(|_| "Failed to acquire read lock".to_string())?;
    
//     match db.get(table) {
//         Some(table_data) => Ok(table_data.clone()),
//         None => Err(format!("Table '{}' not found", table))
//     }
// }

// pub fn delete_rows(table: &str, predicate: impl Fn(&[String]) -> bool) -> Result<usize, String> {
//     let mut db = DB.write().map_err(|_| "Failed to acquire write lock".to_string())?;
    
//     if let Some(table_data) = db.get_mut(table) {
//         let original_len = table_data.len();
//         table_data.retain(|row| !predicate(row));
//         let deleted = original_len - table_data.len();
//         Ok(deleted)
//     } else {
//         Err(format!("Table '{}' not found", table))
//     }
// }