use crate::parser;
use crate::parser::ast::{Query, SelectQuery, InsertQuery, DeleteQuery, CreateQuery, Expression};
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug, Clone)]
pub enum QueryResult {
    Message(String),
    Rows(Vec<Vec<String>>),
}

pub struct Table {
    name: String,
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
}

impl Table {
    fn new(name: String, columns: Vec<String>) -> Self {
        Table {
            name,
            columns,
            rows: Vec::new(),
        }
    }

    fn insert(&mut self, row: Vec<String>) -> Result<(), String> {
        if row.len() != self.columns.len() {
            return Err(format!(
                "Column count mismatch: expected {} values, got {}",
                self.columns.len(),
                row.len()
            ));
        }
        self.rows.push(row);
        Ok(())
    }

}

pub struct Executor {
    // hashmap until storage is wired up
    storage: Mutex<HashMap<String, Table>>,
}

impl Executor {
    pub fn new() -> Self {
        Executor {
            storage: Mutex::new(HashMap::new()),
        }
    }

    pub fn execute_query(&self, query: &str) -> String {
        let parsed_query = match parser::parse_query(&query) {
            Ok(query) => query,
            Err(e) => return format!("Parse error: {}", e),
        };

        match parsed_query {
            parser::Query::Select(select_query) => {
                match self.execute_select(select_query) {
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
                match self.execute_insert(insert_query) {
                    Ok(QueryResult::Message(msg)) => msg,
                    Err(e) => format!("Execution error: {}", e),
                    _ => format!("Execution error: unexpected result from insert"),
                }
            },
            parser::Query::Create(create_query) => {
                match self.execute_create(create_query) {
                    Ok(QueryResult::Message(msg)) => msg,
                    Err(e) => format!("Execution error: {}", e),
                    _ => format!("Execution error: unexpected result from create"),
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

    fn execute_select(&self, query: SelectQuery) -> Result<QueryResult, String> {
        let storage = self.storage.lock().map_err(|e| format!("Storage lock poisoned: {}", e))?;
        let table_opt = storage.get(&query.table_name);
        if table_opt.is_none() {
            return Err(format!("Table '{}' not found", query.table_name));
        }
        let table = table_opt.unwrap();
        let rows = &table.rows;

        let filtered_rows: Vec<Vec<String>> = rows
            .iter()
            .filter(|row| {
                if let Some(expr) = &query.where_clause {
                    self.eval_where(expr, &table.columns, row)
                } else {
                    true
                }
            })
            .cloned()// have an iterator over &T but I need an iterator over T. use cloned for this
            .collect();

        Ok(QueryResult::Rows(filtered_rows))
    }

    fn execute_insert(&self, query: InsertQuery) -> Result<QueryResult, String> {
        let mut storage = self.storage.lock().map_err(|e| format!("Storage lock poisoned: {}", e))?;
        let table = storage.get_mut(&query.table_name);
        if table.is_none() {
            return Err(format!("Table '{}' not found", query.table_name));
        }
        let table = table.unwrap();

        // validate column count
        if query.values.len() != table.columns.len() {
            return Err(format!(
                "Column count mismatch: expected {} values for columns {:?}, got {}",
                table.columns.len(), table.columns, query.values.len()
            ));
        }

        // clone values into a row
        let row: Vec<String> = query.values.into_iter().collect();
        table.insert(row)?;

        Ok(QueryResult::Message("Inserted 1 row".to_string()))
    }

    // fn execute_delete(query: DeleteQuery) -> &'static str {
    //     "delete"
    // }

    // fn execute_update(query: UpdateQuery) -> &'static str {
    //     "update"
    // }

    fn execute_create(&self, query: CreateQuery) -> Result<QueryResult, String> {
        let table_name = query.table_name;

        let mut storage = self.storage.lock().map_err(|e| format!("Storage lock poisoned: {}", e))?;
        if storage.contains_key(&table_name) {
            return Err(format!("Table '{}' already exists", table_name));
        }

        let new_table = Table {
            name: table_name.clone(),
            columns: query.columns,
            rows: Vec::new(),
        };

        storage.insert(table_name.clone(), new_table);

        Ok(QueryResult::Message(format!("Table '{}' created", table_name)))
    }

    fn eval_where(&self, expr: &Expression, columns: &[String], row: &[String]) -> bool {
        use crate::parser::ast::Expression::*;
        
        match expr {
            BinaryOp { left, operator, right} => {
                let left_val = match &**left {
                    Column(name) => {
                        if let Some(idx) = columns.iter().position(|c| c==name) {
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
}