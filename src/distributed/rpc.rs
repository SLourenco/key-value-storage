use crate::distributed::node::LogEntry;
use crate::http::read_headers;
use std::collections::HashMap;
use std::io::{BufReader, Error, ErrorKind, Read, Write};
use std::net::TcpStream;

#[derive(Default)]
pub struct AppendEntriesRequest {
    pub node: u64,
    pub term: u64,
    pub leader_id: u64,
    pub prev_log_idx: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub lead_commit: u64,
}

#[derive(Default)]
pub struct VoteRequest {
    pub node: u64,
    pub term: u64,
    pub candidate_id: u64,
    pub last_log_idx: u64,
    pub last_log_term: u64,
}

#[derive(Default)]
pub struct NodeResponse {
    pub term: u64,
    pub accepted: bool,
}

#[derive(Clone, Default)]
pub(crate) struct HTTPNode {
    host: String,
    nodes: HashMap<u64, u16>,
}

pub(crate) fn new_rpc(host: &str, ports: HashMap<u64, u16>) -> Result<HTTPNode, Error> {
    let n = HTTPNode {
        host: host.to_string(),
        nodes: ports,
    };
    Ok(n)
}

impl HTTPNode {
    pub(crate) fn append_entries(&self, req: AppendEntriesRequest) -> Result<NodeResponse, Error> {
        let node_port = self.nodes.get(&req.node);
        if node_port.is_none() {
            return Err(Error::new(
                ErrorKind::NotFound,
                format!("Node {} not found", req.node),
            ));
        }
        let node_port = node_port.unwrap();
        let mut stream = TcpStream::connect((self.host.as_str(), node_port.clone()))?;

        let mut body = format!(
            "{},{},{},{},{},",
            req.node, req.term, req.leader_id, req.prev_log_idx, req.prev_log_term
        );
        for e in req.entries.clone() {
            body = format!("{}+{}", body, e.format())
        }
        body = format!("{},{}", body, req.lead_commit);
        // Send the HTTP POST request
        println!("Sending append entries to {} with data {}", req.node, body);
        let request = format!(
            "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            "/append-entries", self.host, body.len(), body
        );

        stream.write_all(request.as_bytes())?;
        let response = read_response(BufReader::new(&stream))?;
        println!(
            "Received append entries response for node {}(term {}): {}",
            req.node, response.term, response.accepted
        );
        Ok(response)
    }

    pub(crate) fn request_vote(&self, req: VoteRequest) -> Result<NodeResponse, Error> {
        let node_port = self.nodes.get(&req.node);
        if node_port.is_none() {
            return Err(Error::new(
                ErrorKind::NotFound,
                format!("Node {} not found", req.node),
            ));
        }
        let node_port = node_port.unwrap();
        let mut stream = TcpStream::connect((self.host.as_str(), node_port.clone()))?;

        let body = format!(
            "{},{},{},{},{}",
            req.node, req.term, req.candidate_id, req.last_log_idx, req.last_log_term
        );
        let request = format!(
            "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            "/request-vote", self.host, body.len(), body
        );

        stream.write_all(request.as_bytes())?;
        let response = read_response(BufReader::new(&stream))?;
        println!(
            "Received vote response for node {}(term {}): {}",
            req.node, response.term, response.accepted
        );
        Ok(response)
    }
}

fn read_response(mut reader: BufReader<&TcpStream>) -> Result<NodeResponse, Error> {
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
    let mut res: NodeResponse = Default::default();
    if reader.read_exact(&mut buffer).is_ok() {
        let content = String::from_utf8_lossy(&buffer);
        for line in content.lines() {
            let mut parts = line.split(',');
            if let (Some(term), Some(ok)) = (parts.next(), parts.next()) {
                res.term = term.parse().unwrap();
                res.accepted = ok.parse().unwrap();
            }
        }
    }

    Ok(res)
}
