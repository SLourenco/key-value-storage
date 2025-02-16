use std::collections::HashMap;
use std::io::Read;
use std::io::{BufRead, BufReader};
use std::net::TcpStream;

pub fn read_headers(reader: &mut BufReader<&TcpStream>) -> HashMap<String, String> {
    let mut headers = HashMap::new();
    for line in reader.by_ref().lines() {
        let line = line.unwrap();
        if line.is_empty() {
            break; // End of headers
        }
        if let Some((key, value)) = line.split_once(": ") {
            headers.insert(key.to_lowercase(), value.to_string());
        }
    }
    headers
}
