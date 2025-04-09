use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use once_cell::sync::Lazy;

type Database = HashMap<String, Vec<<String>>;
static DB: Lazy<RwLock::new(Database)>> = Lazy::new(|| {
    println!("Initializing database storage...");
    RwLock::new(HashMap::new())
});

pub fn insert_row(row: Vec<String>) -> Result<(), String> {
    let mut db = db.write();
    
    db.insert()
    Ok(())
}

pub fn get_rows(table: &str) -> Result<(Vec<String>, Vec<Vec<String>>), String> {
    let db = DB.read().map_err(|_| "Failed to acquire read lock".to_string())?;
    
    match db.get(table) {
        Some(table_data) => Ok((table_data.0.clone(), table_data.1.clone())),
        None => Err(format!("Table '{}' not found", table))
    }
}

pub fn delete_rows(table: &str, predicate: impl Fn(&[String]) -> bool) -> Result<usize, String> {
    let mut db = DB.write().map_err(|_| "Failed to acquire write lock".to_string())?;
    
    if let Some(table_data) = db.get_mut(table) {
        let original_len = table_data.1.len();
        table_data.1.retain(|row| !predicate(row));
        let deleted = original_len - table_data.1.len();
        Ok(deleted)
    } else {
        Err(format!("Table '{}' not found", table))
    }
}