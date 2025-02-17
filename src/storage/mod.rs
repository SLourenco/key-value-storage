mod benchmark;
pub mod bit_cask;
mod data_files;

use std::io::Error;

#[derive(Debug, Clone)]
pub struct KV {
    pub key: usize,
    pub value: String,
}

pub trait KVStorage {
    fn get(&self, key: usize) -> Result<String, Error>;
    fn put(&mut self, key: usize, value: String) -> Result<(), Error>;
    fn delete(&mut self, key: usize) -> Result<(), Error>;
    fn range(&self, start: usize, end: usize) -> Result<Vec<KV>, Error>;
    fn batch_put(&mut self, kvs: Vec<KV>) -> Result<(), Error>;
}
