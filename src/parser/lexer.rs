#[derive(Debug, PartialEq)]
pub enum Token {
    Select,
    Insert,
    // Update,
    Delete,
    Create,
    Where,
    Values,
    Set,
    Identifier(String),
    StringLiteral(String),
    Operator(String),
    Comma,
    Semicolon,
    ParenOpen,
    ParenClose,
}

pub struct Lexer {
    input: String,
    position: usize,
}

impl Lexer {
    pub fn new(input: String) -> Self {
        Self { input, position: 0 }
    }

    pub fn next_token(&mut self) -> Option<Token> {
        // Loop so we can skip unknown characters (for example stray backslashes
        // introduced by shell escaping) instead of returning None which would
        // terminate tokenization early.
        loop {
            self.skip_whitespace();
            if self.position >= self.input.len() {
                return None;
            }

            let current_char = self.input.as_bytes()[self.position] as char;

            match current_char {
                ',' => {
                    self.position += 1;
                    return Some(Token::Comma);
                }
                ';' => {
                    self.position += 1;
                    return Some(Token::Semicolon);
                }
                '(' => {
                    self.position += 1;
                    return Some(Token::ParenOpen);
                }
                ')' => {
                    self.position += 1;
                    return Some(Token::ParenClose);
                }
                '=' | '<' | '>' => {
                    self.position += 1;
                    return Some(Token::Operator(current_char.to_string()));
                }
                '"' | '\'' => return self.parse_string_literal(current_char),
                _ if current_char.is_alphabetic() => return self.parse_identifier_or_keyword(),
                // Unknown / unhandled characters (backslashes, stray escapes, etc.)
                // will be skipped and tokenization continues.
                _ => {
                    self.position += 1;
                    continue;
                }
            }
        }
    }

    fn skip_whitespace(&mut self) {
        while self.position < self.input.len()
            && self.input.as_bytes()[self.position].is_ascii_whitespace()
        {
            self.position += 1;
        }
    }

    fn parse_string_literal(&mut self, quote: char) -> Option<Token> {
        self.position += 1; // skip first quote
        let start = self.position;

        while self.position < self.input.len()
            && self.input.as_bytes()[self.position] as char != quote
        {
            self.position += 1;
        }

        if self.position >= self.input.len() {
            return None; // string wasnt terminated
        }

        let literal = &self.input[start..self.position];
        self.position += 1; // skip end quote
        Some(Token::StringLiteral(literal.to_string()))
    }

    fn parse_identifier_or_keyword(&mut self) -> Option<Token> {
        let start = self.position;

        while self.position < self.input.len()
            && self.input.as_bytes()[self.position].is_ascii_alphanumeric()
        {
            self.position += 1;
        }

        let identifier = &self.input[start..self.position];
        match identifier.to_uppercase().as_str() {
            "SELECT" => Some(Token::Select),
            "INSERT" => Some(Token::Insert),
            "DELETE" => Some(Token::Delete),
            "CREATE" => Some(Token::Create),
            "WHERE" => Some(Token::Where),
            "VALUES" => Some(Token::Values),
            "SET" => Some(Token::Set),
            _ => Some(Token::Identifier(identifier.to_string())),
        }
    }
}