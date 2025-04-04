mod parser;

fn execute_query(&query: String) {
    let parsed_query = parser.parse_query(query).unwrap();
}