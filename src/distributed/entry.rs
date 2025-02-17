use crate::storage::KV;
use std::fmt;
use std::fmt::Formatter;
use std::io::{Error, ErrorKind};
use std::str::FromStr;

#[derive(Clone, Default)]
pub struct LogEntry {
    pub term: u64,
    pub entry: String,
    pub entry_idx: u64,
}

impl fmt::Display for LogEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}|{}|{}", self.term, self.entry, self.entry_idx)
    }
}

impl FromStr for LogEntry {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut e = s.split("|");
        if let (Some(term), Some(entry), Some(e_idx)) = (e.next(), e.next(), e.next()) {
            return Ok(LogEntry {
                term: term.parse::<u64>().unwrap_or(0),
                entry: entry.to_string(),
                entry_idx: e_idx.parse::<u64>().unwrap_or(0),
            });
        }
        Err(Error::new(
            ErrorKind::InvalidInput,
            format!("Invalid log entry {}", s),
        ))
    }
}

impl LogEntry {
    pub fn format_command(&self, cmd: &str, values: Vec<KV>) -> String {
        if cmd == "DELETE" {
            return format!("{}:{}", cmd, values.first().unwrap().key);
        }

        let mut entries = String::new();
        for v in values.clone() {
            entries.push_str(&format!("{}.{};", v.key, v.value));
        }
        if values.len() > 0 {
            entries.pop();
        }
        let encoded = format!("{}:{}", cmd, entries);
        encoded
    }

    pub fn parse_command(&self, cmd: &str) -> Result<(String, Vec<KV>), Error> {
        let mut c = cmd.split(":");
        let mut f_values = Vec::new();
        if let (Some(command), Some(values)) = (c.next(), c.next()) {
            if command == "DELETE" {
                f_values.push(KV {
                    key: values.parse().unwrap(),
                    value: Default::default(),
                });
                return Ok((command.to_string(), f_values));
            }

            let values_iter = values.split(";");
            for v in values_iter {
                let mut kv = v.split(".");
                if let (Some(key), Some(val)) = (kv.next(), kv.next()) {
                    f_values.push(KV {
                        key: key.parse().unwrap(),
                        value: val.to_string(),
                    })
                } else {
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        format!("Invalid value {}", command),
                    ));
                }
            }
            return Ok((command.to_string(), f_values));
        }
        Err(Error::new(
            ErrorKind::InvalidInput,
            format!("Invalid command {}", cmd),
        ))
    }
}
