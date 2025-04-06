use crate::parser;
use crate::parser::ast::{SelectQuery, InsertQuery, UpdateQuery, DeleteQuery};

pub fn execute_query(query: &str) -> &str {
    let parsed_query = parser::parse_query(&query).unwrap();

    match parsed_query {
        parser::Query::Select(select_query) => {
            execute_select(select_query)
        },
        parser::Query::Insert(insert_query) => {
            execute_insert(insert_query)
        },
        parser::Query::Update(update_query) => {
            execute_update(update_query)
        },
        parser::Query::Delete(delete_query) => {
            execute_delete(delete_query)
        }
    }
}

fn execute_select(query: SelectQuery) -> &'static str {
    "select"
}

fn execute_insert(query: InsertQuery) -> &'static str {
    "insert"
}

fn execute_delete(query: DeleteQuery) -> &'static str {
    "delete"
}

fn execute_update(query: UpdateQuery) -> &'static str {
    "update"
}