// #![feature(test)]
// extern crate test;
use crate::distributed::node::Follower;
mod distributed;
mod http;
mod storage;

use crate::distributed::rpc::{AppendEntriesRequest, VoteRequest};
use crate::distributed::{new_distributed_storage, DistributedStorage};
use crate::http::read_headers;
use crate::storage::KV;
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Error, ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::str::FromStr;
use std::{env, str};

const DEFAULT_PORT: &str = "4000";
const HOST: &str = "127.0.0.1";
const DEFAULT_DATA_DIR: &str = "data-dir";

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut port = DEFAULT_PORT;
    let mut data_dir = DEFAULT_DATA_DIR;
    let mut distributed = true;

    for i in 0..args.len() {
        if args[i] == "port" && i + 1 < args.len() {
            port = &args[i + 1];
        }
        if args[i] == "data-dir" && i + 1 < args.len() {
            data_dir = &args[i + 1];
        }

        if args[i] == "distributed" && i + 1 < args.len() {
            distributed = (&args[i + 1]).parse().unwrap();
        }
    }

    let endpoint = format!("{}:{}", HOST, port);
    let listener =
        TcpListener::bind(endpoint).expect(format!("Failed to bind to port {}", port).as_str());
    println!("HTTP server running on {}...", port);

    let distributed_storage =
        new_distributed_storage(HOST, port.parse().unwrap(), data_dir, distributed);
    if let Err(e) = distributed_storage {
        println!("Failed to initialize distributed storage: {}", e);
        return;
    }

    let mut distributed_storage = distributed_storage.unwrap();
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                handle_client(stream, &mut distributed_storage);
            }
            Err(e) => eprintln!("Connection failed: {}", e),
        }
    }
}

fn handle_client(mut stream: TcpStream, distributed_storage: &mut DistributedStorage) {
    let mut reader = BufReader::new(&stream);
    let mut request_line = String::new();

    // Read the request line (e.g., "GET /?name=Alice HTTP/1.1")
    if reader.read_line(&mut request_line).is_err() {
        return;
    }

    let request_parts: Vec<&str> = request_line.trim().split_whitespace().collect();
    if request_parts.len() < 3 {
        return;
    }

    let method = request_parts[0]; // HTTP method
    let path = request_parts[1]; // URL path (may include query params)
    let (route, query_params) = parse_path(path);

    let response = match (method, route) {
        ("GET", "/") => get(query_params, distributed_storage),
        ("POST", "/append-entries") => {
            let result = read_append_entries_request(reader);
            let s = match result {
                Err(e) => format_response(format!("Failed to read response: {}", e.to_string())),
                Ok((_, v)) => {
                    let r = distributed_storage.node.append_entries(v);
                    match r {
                        Err(e) => format_response(format!("Failed to append entries: {}", e)),
                        Ok((term, ok)) => format_response(format!("{},{}", term, ok)),
                    }
                }
            };
            s
        }
        ("POST", "/request-vote") => {
            let result = read_vote_request(reader);
            let s = match result {
                Err(e) => format_response(format!("Failed to read response: {}", e.to_string())),
                Ok((_, v)) => {
                    let r = distributed_storage.node.vote(v);
                    match r {
                        Err(e) => format_response(format!("Failed to request vote: {}", e)),
                        Ok((term, ok)) => format_response(format!("{},{}", term, ok)),
                    }
                }
            };
            s
        }
        ("POST", "/") => {
            let (_, body) = read_kv_request(reader);
            put(body, distributed_storage)
        }
        ("DELETE", "/") => delete(query_params, distributed_storage),
        _ => default_response(),
    };

    stream.write_all(response.as_bytes()).unwrap();
}

// Parses a typical URL path (e.g.: /path?arg1=val1&arg2=val2)
fn parse_path(path: &str) -> (&str, HashMap<String, String>) {
    let mut parts = path.splitn(2, '?');
    let route = parts.next().unwrap_or("/");
    let mut query_params = HashMap::new();

    if let Some(query) = parts.next() {
        for pair in query.split('&') {
            if let Some((key, value)) = pair.split_once('=') {
                query_params.insert(key.to_string(), value.to_string());
            }
        }
    }

    (route, query_params)
}

fn read_kv_request(mut reader: BufReader<&TcpStream>) -> (HashMap<String, String>, Vec<KV>) {
    let headers = read_headers(&mut reader);
    let content_length = headers
        .get("content-length")
        .unwrap_or(&"0".to_string())
        .parse()
        .unwrap_or(0);

    let mut body_map = Vec::new();
    if content_length > 0 {
        let mut buffer = vec![0; content_length];
        if reader.read_exact(&mut buffer).is_ok() {
            let content = String::from_utf8_lossy(&buffer);
            for line in content.lines() {
                let mut parts = line.split(',');
                if let (Some(key), Some(value)) = (parts.next(), parts.next()) {
                    body_map.push(KV {
                        key: key.parse().unwrap(),
                        value: value.to_string(),
                    });
                }
            }
        }
    }

    (headers, body_map)
}

fn read_append_entries_request(
    mut reader: BufReader<&TcpStream>,
) -> Result<(HashMap<String, String>, AppendEntriesRequest), Error> {
    let headers = read_headers(&mut reader);
    let content_length = headers
        .get("content-length")
        .unwrap_or(&"0".to_string())
        .parse()
        .unwrap_or(0);

    if content_length <= 0 {
        return Err(Error::new(ErrorKind::InvalidInput, "Content is empty"));
    }

    let mut buffer = vec![0; content_length];
    let mut append_entries_request: AppendEntriesRequest = Default::default();
    if reader.read_exact(&mut buffer).is_ok() {
        let content = String::from_utf8_lossy(&buffer);
        for line in content.lines() {
            append_entries_request = AppendEntriesRequest::from_str(line)?;
        }
    }

    Ok((headers, append_entries_request))
}

fn read_vote_request(
    mut reader: BufReader<&TcpStream>,
) -> Result<(HashMap<String, String>, VoteRequest), Error> {
    let headers = read_headers(&mut reader);
    let content_length = headers
        .get("content-length")
        .unwrap_or(&"0".to_string())
        .parse()
        .unwrap_or(0);

    if content_length <= 0 {
        return Err(Error::new(ErrorKind::InvalidInput, "Content is empty"));
    }

    let mut buffer = vec![0; content_length];
    let mut vote_request: VoteRequest = Default::default();
    if reader.read_exact(&mut buffer).is_ok() {
        let content = String::from_utf8_lossy(&buffer);
        for line in content.lines() {
            vote_request = VoteRequest::from_str(line)?;
        }
    }

    Ok((headers, vote_request))
}

// Basic HTTP response
fn format_response(body: String) -> String {
    format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
        body.len(),
        body
    )
}

fn default_response() -> String {
    let get_request_instructions = "curl --location 'http://localhost:4000?key=1'";
    let get_range_req_instructions =
        "curl --location 'http://localhost:4000?start_key=1&end_key=10'";
    let put_request_instructions = "curl --location 'http://localhost:4000/' --header 'Content-Type: text/plain' --data 'key:1,value:2000'";
    let bulk_put_req_instructions = "curl --location 'http://localhost:4000' --header 'Content-Type: text/plain' --data 'key:1,value:2000\nkey:2,value:5000\nkey:5,value:4000\nkey:11,value:502'";
    let delete_request_instructions =
        "curl --location --request DELETE 'http://localhost:4000?key=1'";
    format_response(format!(
        "Usage:\nREAD: {}\nREAD KEY RANGE: {}\nPUT: {}\nBATCH PUT: {}\nDELETE: {}\n",
        get_request_instructions,
        get_range_req_instructions,
        put_request_instructions,
        bulk_put_req_instructions,
        delete_request_instructions
    ))
}

fn get(query_params: HashMap<String, String>, storage: &DistributedStorage) -> String {
    let key = query_params.get("key").cloned();
    if let Some(key) = key {
        let result = storage.get(key.parse().unwrap());
        return match result {
            Err(result) => {
                format_response(format!("Failed to read response: {}", result.to_string()))
            }
            Ok(result) => format_response(format!("Value: {}", result)),
        };
    }

    let start_key = query_params.get("start_key").cloned();
    let end_key = query_params.get("end_key").cloned();
    if start_key.is_some() && end_key.is_some() {
        let result = storage.range(
            start_key.unwrap().parse().unwrap(),
            end_key.unwrap().parse().unwrap(),
        );
        return match result {
            Err(result) => format_response(format!("Failed to read range: {}", result.to_string())),
            Ok(result) => format_response(format!("Value: {:?}", result)),
        };
    }
    default_response()
}

fn put(body: Vec<KV>, storage: &mut DistributedStorage) -> String {
    println!("Received: {:?}", body);
    if body.len() == 0 {
        return default_response();
    }

    if body.len() == 1 {
        let f = body.first().cloned().unwrap();
        let result = storage.put(f.key, f.value);
        return match result {
            Err(result) => {
                format_response(format!("Failed to put key. Err {}", result.to_string()))
            }
            Ok(()) => format_response("Key saved".to_string()),
        };
    }

    let result = storage.batch_put(body);
    match result {
        Err(result) => format_response(format!(
            "Failed to batch put keys. Err: {}",
            result.to_string()
        )),
        Ok(()) => format_response("Keys saved".to_string()),
    }
}

fn delete(query_params: HashMap<String, String>, storage: &mut DistributedStorage) -> String {
    let key = query_params.get("key").cloned();
    if let Some(key) = key {
        let result = storage.delete(key.parse().unwrap());
        return match result {
            Err(result) => format_response(format!("Failed to delete: {}", result.to_string())),
            Ok(()) => format_response("Key deleted".to_string()),
        };
    }
    default_response()
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use test::Bencher;
//
//     #[bench]
//     fn bench_batch_put(b: &mut Bencher) {
//         let storage = new_bit_cask("test-data");
//         assert!(storage.is_ok());
//         let mut storage = storage.unwrap();
//         b.iter(|| storage.batch_put(vec![KV{ key: 1, value: "123".to_string() }]));
//     }
// }
