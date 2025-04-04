mod lexer;
mod ast;

use lexer::Lexer;
use ast::Query;

/// Parses a SQL query string into a general `Query` structure.
///
/// # Arguments
/// - `input`: The SQL query string to parse.
///
/// # Returns
/// - `Result<Query, String>`: A `Query` structure on success, or an error message on failure.
pub fn parse_query(input: &str) -> Result<Query, String> {
    let mut lexer = Lexer::new(input.to_string());
    let mut tokens = Vec::new();

    // Tokenize the input query
    while let Some(token) = lexer.next_token() {
        tokens.push(token);
    }

    // Delegate the parsing of tokens to the AST module
    ast::parse_tokens(tokens)
}