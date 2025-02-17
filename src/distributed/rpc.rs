use crate::distributed::entry::LogEntry;
use crate::http::read_headers;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Formatter;
use std::io::{BufReader, Error, ErrorKind, Read, Write};
use std::net::TcpStream;
use std::str::FromStr;

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

impl fmt::Display for AppendEntriesRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut body = format!(
            "{},{},{},{},{},",
            self.node, self.term, self.leader_id, self.prev_log_idx, self.prev_log_term
        );
        for e in self.entries.clone() {
            body = format!("{}+{}", body, e.to_string());
        }
        body = format!("{},{}", body, self.lead_commit);

        write!(f, "{}", body)
    }
}

impl FromStr for AppendEntriesRequest {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // e.g.: 6000,2,5000,0,0,,0
        let mut parts = s.split(',');
        if let (
            Some(node),
            Some(term),
            Some(leader_id),
            Some(prev_log_idx),
            Some(prev_log_term),
            Some(entries),
            Some(lead_commit),
        ) = (
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
        ) {
            let mut parsed_entries = Vec::new();
            if entries.len() > 0 {
                let entries = entries.split('+');
                for e in entries {
                    if e.len() <= 0 {
                        continue;
                    }
                    parsed_entries.push(LogEntry::from_str(e)?);
                }
            }

            return Ok(AppendEntriesRequest {
                node: node.parse().unwrap(),
                term: term.parse().unwrap(),
                leader_id: leader_id.parse().unwrap(),
                prev_log_idx: prev_log_idx.parse().unwrap(),
                prev_log_term: prev_log_term.parse().unwrap(),
                entries: parsed_entries,
                lead_commit: lead_commit.parse().unwrap(),
            });
        }
        Err(Error::new(
            ErrorKind::InvalidInput,
            format!("Could not parse AppendEntriesRequest: {}", s),
        ))
    }
}

#[derive(Default)]
pub struct VoteRequest {
    pub node: u64,
    pub term: u64,
    pub candidate_id: u64,
    pub last_log_idx: u64,
    pub last_log_term: u64,
}

impl fmt::Display for VoteRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let body = format!(
            "{},{},{},{},{}",
            self.node, self.term, self.candidate_id, self.last_log_idx, self.last_log_term
        );
        write!(f, "{}", body)
    }
}

impl FromStr for VoteRequest {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split(',');
        if let (
            Some(node),
            Some(term),
            Some(candidate_id),
            Some(last_log_idx),
            Some(last_log_term),
        ) = (
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
        ) {
            return Ok(VoteRequest {
                node: node.parse().unwrap(),
                term: term.parse().unwrap(),
                candidate_id: candidate_id.parse().unwrap(),
                last_log_idx: last_log_idx.parse().unwrap(),
                last_log_term: last_log_term.parse().unwrap(),
            });
        }
        Err(Error::new(
            ErrorKind::InvalidInput,
            format!("Could not parse VoteRequest: {}", s),
        ))
    }
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

        let body = req.to_string();
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

        let body = req.to_string();
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
