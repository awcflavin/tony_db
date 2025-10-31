use crate::parser;
use crate::parser::ast::{Query, SelectQuery, InsertQuery, DeleteQuery, Expression};
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug, Clone)]
pub enum QueryResult {
    Message(String),
    Rows(Vec<Vec<String>>),
}

// for now
lazy_static! {
    static ref STORAGE: Mutex<HashMap<String, Vec<Vec<String>>>> = Mutex::new(HashMap::new());
}

const TABLE_COLUMNS: &[&str] = &["column1", "column2"];

pub fn execute_query(query: &str) -> String {
    let parsed_query = match parser::parse_query(&query) {
        Ok(query) => query,
        Err(e) => return format!("Parse error: {}", e),
    };

    match parsed_query {
        parser::Query::Select(select_query) => {
            match execute_select(select_query) {
                Ok(QueryResult::Rows(rows)) => {
                    // Format rows as a string
                    if rows.is_empty() {
                        "No rows found".to_string()
                    } else {
                        rows.iter()
                            .map(|row| row.join(" | "))
                            .collect::<Vec<_>>()
                            .join("\n")
                    }
                }
                Ok(QueryResult::Message(msg)) => msg,
                Err(e) => format!("Execution error: {}", e),
            }
        },
        parser::Query::Insert(insert_query) => {
            match execute_insert(insert_query) {
                Ok(QueryResult::Message(msg)) => msg,
                Err(e) => format!("Execution error: {}", e),
                _ => format!("Execution error: unexpected result from insert"),
            }
        },
        // parser::Query::Update(update_query) => {
        //     execute_update(update_query)
        // },
        // parser::Query::Delete(delete_query) => {
        //     execute_delete(delete_query)
        // }
        _ => "Unsupported query type".to_string(),
    }
}

fn execute_select(query: SelectQuery) -> Result<QueryResult, String> {
    let table_name = "default"; // single table
    let storage = STORAGE.lock().unwrap();
    let table = storage.get(table_name).cloned().unwrap_or_default();

    let filtered_rows: Vec<Vec<String>> = table
        .into_iter()
        .filter(|row| {
            if let Some(expr) = &query.where_clause {
                eval_where(expr, row)
            } else {
                true
            }
        })
        .collect();

    Ok(QueryResult::Rows(filtered_rows))
}

fn execute_insert(query: InsertQuery) -> Result<QueryResult, String> {
    let table_name = "default";

    // validate column count
    if query.values.len() != TABLE_COLUMNS.len() {
        return Err(format!(
            "Column count mismatch: expected {} values for columns {:?}, got {}",
            TABLE_COLUMNS.len(), TABLE_COLUMNS, query.values.len()
        ));
    }

    let mut storage = STORAGE.lock().map_err(|e| format!("Storage lock poisoned: {}", e))?;
    let table = storage.entry(table_name.to_string()).or_insert_with(Vec::new);

    // clone values into a row
    let row: Vec<String> = query.values.into_iter().collect();
    table.push(row);

    Ok(QueryResult::Message("Inserted 1 row".to_string()))
}

// fn execute_delete(query: DeleteQuery) -> &'static str {
//     "delete"
// }

// fn execute_update(query: UpdateQuery) -> &'static str {
//     "update"
// }

fn eval_where(expr: &Expression, row: &[String]) -> bool {
    use crate::parser::ast::Expression::*;
    
    match expr {
        BinaryOp { left, operator, right} => {
            let left_val = match &**left {
                Column(name) => {
                    if let Some(idx) = TABLE_COLUMNS.iter().position(|&c| c==name) {
                        row.get(idx).cloned().unwrap_or_default()
                    } else {
                        "".to_string()
                    }
                }
                Value(val) => val.clone(),
                _ => "".to_string(),
            };

            let right_val = match &**right {
                Value(val) => val.clone(),
                _ => "".to_string(),
            };

            match operator.as_str() {
                "=" => left_val == right_val,
                ">" => left_val > right_val,
                "<" => left_val < right_val,
                _ => false,
            }
        }
        _ => true,
    }
}