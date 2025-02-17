use crate::storage::data_files::{create_new_active_file, create_new_file, delete_file, save};
use crate::storage::{KVStorage, KV};
use std::cmp::min;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{Error, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{mpsc, Arc, Mutex};
use std::thread::available_parallelism;
use std::time::Duration;
use std::{fs, thread};

const HINT_FILE_NAME: &str = "hint-file";

#[derive(Clone)]
struct Key {
    filename: String,
    timestamp: u64,
    name: usize,
    offset: u64,
    length: usize,
}

#[derive(Clone, Default)]
pub struct BitCask {
    pub(crate) data_dir: String,
    active_dir: String,
    key_dir: Arc<Mutex<BTreeMap<usize, Key>>>,
}

pub fn new_bit_cask(data_dir: &str) -> Result<BitCask, Error> {
    let mut bc = BitCask {
        data_dir: data_dir.to_string(),
        active_dir: Default::default(),
        key_dir: Arc::new(Mutex::new(Default::default())),
    };

    bc.init()?;

    Ok(bc)
}

impl KVStorage for BitCask {
    fn get(&self, key: usize) -> Result<String, Error> {
        let kd = self.key_dir.lock().unwrap();
        let k = kd.get(&key);
        match k {
            Some(k) => {
                let result = read_from_file(
                    k.filename.clone(),
                    vec![Key {
                        filename: "".to_string(),
                        name: k.name,
                        offset: k.offset,
                        length: k.length,
                        timestamp: k.timestamp,
                    }],
                )?;
                let (_, v) = result.first().unwrap();
                Ok(v.to_string())
            }
            None => Ok("".to_string()),
        }
    }

    fn put(&mut self, key: usize, value: String) -> Result<(), Error> {
        let result = save(&self.data_dir, &self.active_dir, vec![(key, value)]);
        match result {
            Ok((r, active_dir)) => {
                self.active_dir = active_dir;
                let (dir, offset, length, ts) = r.first().unwrap();
                let mut kd = self.key_dir.lock().unwrap();
                kd.insert(
                    key,
                    Key {
                        filename: dir.to_string(),
                        timestamp: *ts,
                        name: key,
                        offset: *offset,
                        length: *length,
                    },
                );
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    fn delete(&mut self, key: usize) -> Result<(), Error> {
        let mut kd = self.key_dir.lock().unwrap();
        kd.remove(&key);
        Ok(())
    }

    fn range(&self, start: usize, end: usize) -> Result<Vec<KV>, Error> {
        // Group keys by filename
        let mut grouped_keys: HashMap<String, Vec<Key>> = HashMap::new();
        let kd = self.key_dir.lock().unwrap();
        for (_, value) in kd.range(start..=end) {
            grouped_keys
                .entry(value.filename.clone())
                .or_insert_with(Vec::new)
                .push(Key {
                    filename: value.filename.clone(),
                    name: value.name,
                    offset: value.offset,
                    length: value.length,
                    timestamp: value.timestamp,
                });
        }

        let default_parallelism_approx =
            min(available_parallelism()?.get() - 1, grouped_keys.len());
        let (gk_tx, gk_rx) = mpsc::channel();
        let rx = Arc::new(Mutex::new(gk_rx));
        let (r_tx, r_rx) = mpsc::channel();
        let mut handles: Vec<thread::JoinHandle<Result<(), Error>>> = vec![];

        for _ in 0..default_parallelism_approx {
            let tx = r_tx.clone();
            let rx = Arc::clone(&rx);
            let handle = thread::spawn(move || {
                while let Ok((filename, keys)) = rx.lock().unwrap().recv() {
                    let result = read_from_file(filename, keys)?;
                    result.iter().for_each(|(k, v)| {
                        tx.send(KV {
                            key: *k,
                            value: v.to_string(),
                        })
                        .unwrap();
                    });
                }
                return Ok(());
            });
            handles.push(handle);
        }

        for g in grouped_keys {
            gk_tx.send(g).unwrap();
        }
        drop(gk_tx);
        drop(r_tx);

        for handle in handles {
            handle.join().unwrap()?;
        }

        let mut results = vec![];
        for kv in r_rx {
            results.push(kv);
        }

        Ok(results)
    }

    fn batch_put(&mut self, kvs: Vec<KV>) -> Result<(), Error> {
        let data_vec: Vec<(usize, String)> =
            kvs.iter().map(|kv| (kv.key, kv.value.clone())).collect();
        let (results, active_dir) = save(&self.data_dir, &self.active_dir, data_vec)?;
        self.active_dir = active_dir;

        let mut kd = self.key_dir.lock().unwrap();
        for (kv, (dir, offset, length, ts)) in kvs.into_iter().zip(results) {
            kd.insert(
                kv.key,
                Key {
                    filename: dir,
                    timestamp: ts,
                    name: kv.key,
                    offset,
                    length,
                },
            );
        }

        Ok(())
    }
}

impl BitCask {
    fn init(&mut self) -> Result<(), Error> {
        let path = Path::new(&self.data_dir);
        fs::create_dir_all(path)?;

        println!("Creating new active data file...");
        self.active_dir = create_new_active_file(&self.data_dir)?;

        println!("Building key dir from existing data...");
        let keys = compute_key_dir(&self.data_dir, &self.active_dir)?;
        self.key_dir = Arc::new(Mutex::new(keys));

        let data_dir = self.data_dir.clone();
        let active_dir = self.active_dir.clone();
        let key_dir = Arc::clone(&self.key_dir);
        let compact = move |data_dir: String,
                            active_dir: String,
                            key_dir: Arc<Mutex<BTreeMap<usize, Key>>>| {
            let data_dir = data_dir.clone();
            let active_dir = active_dir.clone();
            // killed off when main program finishes
            loop {
                let key_dir = Arc::clone(&key_dir);
                thread::sleep(Duration::from_secs(60));
                println!("compaction starting...");
                // copy key_dir to avoid locking other processes
                let cloned_key_dir = {
                    let key_dir_guard = key_dir.lock().unwrap();
                    key_dir_guard.clone()
                };
                let r = compact_files(&*data_dir, cloned_key_dir);
                if r.is_err() {
                    println!("Error compacting: {:?}", r.err().unwrap());
                    return;
                }
                let new_key_dir = r.unwrap();
                println!("new compacted key_dir created!. Creating hint file...");
                let chr = create_hint_file(&*data_dir, new_key_dir.clone());
                if chr.is_err() {
                    println!("Error creating hint file: {:?}", chr.err().unwrap());
                    return;
                }
                println!("hint file created! Updating keys in memory...");
                {
                    let mut key_dir_guard = key_dir.lock().unwrap();
                    for (k, v) in new_key_dir {
                        key_dir_guard.insert(k, v);
                    }
                }
                println!("Key dir updated! Deleting old files...");
                let dfr = delete_old_files(&*data_dir, &*active_dir, key_dir);
                if dfr.is_err() {
                    println!("Error deleting old files: {:?}", dfr.err().unwrap());
                    return;
                }
                println!("compaction done. Sleeping for 10 sec");
            }
        };
        thread::spawn(move || compact(data_dir, active_dir, key_dir));

        println!("key dir created. Ready!");
        Ok(())
    }
}

fn read_from_file(filename: String, keys: Vec<Key>) -> Result<Vec<(usize, String)>, Error> {
    let mut file = File::open(filename)?;
    let mut results = Vec::new();

    for info in keys {
        file.seek(SeekFrom::Start(info.offset + 8))?;

        let mut length_buf = [0u8; 8];
        if file.read_exact(&mut length_buf).is_err() {
            break;
        }
        let v_length = usize::from_be_bytes(length_buf);

        // let mut key_buf = [0u8; 8];
        // file.read_exact(&mut key_buf)?;
        // let key = usize::from_be_bytes(key_buf);
        // do not read key here
        file.seek(SeekFrom::Current(8))?;

        let mut value_buf = vec![0u8; v_length];
        file.read_exact(&mut value_buf)?;
        let result =
            String::from_utf8(value_buf).map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
        results.push((info.name, result));
    }

    Ok(results)
}

fn read_keys_and_offsets(filename: String) -> Result<Vec<(u64, usize, u64, u64)>, Error> {
    let mut file = File::open(filename).map_err(|e| Error::new(e.kind(), e.to_string()))?;
    let mut results = Vec::new();
    let mut offset = 0;
    loop {
        let mut ts_buf = [0u8; 8];
        if file.read_exact(&mut ts_buf).is_err() {
            break;
        }
        let ts = u64::from_be_bytes(ts_buf);

        let mut length_buf = [0u8; 8];
        if file.read_exact(&mut length_buf).is_err() {
            break;
        }
        let v_length = u64::from_be_bytes(length_buf);

        let mut key_buf = [0u8; 8];
        if file.read_exact(&mut key_buf).is_err() {
            break;
        }
        let key = usize::from_be_bytes(key_buf);
        results.push((ts, key, offset, v_length));

        if file.seek(SeekFrom::Current(v_length as i64)).is_err() {
            break;
        };
        offset += 8 + 8 + 8 + v_length; // Move to the next key
    }

    Ok(results)
}

fn compute_key_dir(data_dir: &str, active_file: &str) -> Result<BTreeMap<usize, Key>, Error> {
    let r = read_hint_file(data_dir);
    if r.is_ok() {
        return r;
    }

    println!("No hint file present. Build key dir from data files...");
    let mut new_dir: BTreeMap<usize, Key> = BTreeMap::new();

    for entry in fs::read_dir(data_dir)? {
        let path = entry?.path();
        let Some(file) = path.file_name() else {
            return Err(Error::new(ErrorKind::InvalidData, "Path is not a file"));
        };
        let Some(filename) = file.to_str() else {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "File does not have a name",
            ));
        };
        let full_filename = format!("{}/{}", data_dir, filename);
        if full_filename.ends_with(HINT_FILE_NAME) || full_filename == active_file {
            continue;
        }
        // ignoring corrupted files might be preferable to failing the entire merge
        let k = read_keys_and_offsets(full_filename.to_string())?;
        for (ts, key, offset, v_len) in k {
            if new_dir.contains_key(&key) {
                let existing = new_dir.get(&key);
                if existing.unwrap().timestamp >= ts {
                    continue;
                }
            }
            new_dir.insert(
                key,
                Key {
                    filename: full_filename.to_string(),
                    timestamp: ts,
                    name: key,
                    offset: offset,
                    length: v_len as usize,
                },
            );
        }
    }
    Ok(new_dir)
}

fn compact_files(
    data_dir: &str,
    key_dir: BTreeMap<usize, Key>,
) -> Result<BTreeMap<usize, Key>, Error> {
    let mut active_dir = create_new_active_file(&data_dir)?;
    let mut new_dir: BTreeMap<usize, Key> = BTreeMap::new();

    for (k, v) in key_dir {
        // Could probably write multiple keys, to avoid opening the file multiple times
        let result = read_from_file(v.filename.clone(), vec![v])?;
        let (new_key, filename) = save(data_dir, &*active_dir, result)?;
        active_dir = filename;
        let (dir, offset, length, ts) = new_key.first().unwrap();
        new_dir.insert(
            k,
            Key {
                filename: dir.clone(),
                timestamp: *ts,
                name: k,
                offset: *offset,
                length: *length,
            },
        );
    }

    Ok(new_dir)
}

fn create_hint_file(data_dir: &str, key_dir: BTreeMap<usize, Key>) -> Result<(), Error> {
    let filename = format!("{}/{}", data_dir, HINT_FILE_NAME);
    create_new_file(&filename)?;

    let mut file = OpenOptions::new().write(true).append(true).open(filename)?;

    for (_, v) in key_dir {
        file.write_all(&v.timestamp.to_be_bytes())?;
        file.write_all(&v.length.to_be_bytes())?;
        file.write_all(&v.name.to_be_bytes())?;
        file.write_all(&v.filename.len().to_be_bytes())?;
        file.write_all(v.filename.as_bytes())?;
        file.write_all(&v.offset.to_be_bytes())?;
    }

    Ok(())
}

fn read_hint_file(data_dir: &str) -> Result<BTreeMap<usize, Key>, Error> {
    let filename = format!("{}/{}", data_dir, HINT_FILE_NAME);
    let mut file = OpenOptions::new().read(true).open(filename)?;

    println!("Hint file found. Loading key dir from it...");
    let mut new_dir: BTreeMap<usize, Key> = BTreeMap::new();
    loop {
        let mut ts_buf = [0u8; 8];
        if file.read_exact(&mut ts_buf).is_err() {
            break;
        }
        let ts = u64::from_be_bytes(ts_buf);

        let mut length_buf = [0u8; 8];
        if file.read_exact(&mut length_buf).is_err() {
            break;
        }
        let v_length = u64::from_be_bytes(length_buf);

        let mut key_buf = [0u8; 8];
        if file.read_exact(&mut key_buf).is_err() {
            break;
        }
        let key = usize::from_be_bytes(key_buf);

        let mut filename_len_buf = [0u8; 8];
        if file.read_exact(&mut filename_len_buf).is_err() {
            break;
        }
        let filename_len = usize::from_be_bytes(filename_len_buf);

        let mut filename_buf = vec![0u8; filename_len];
        if file.read_exact(&mut filename_buf).is_err() {
            break;
        }
        let filename =
            String::from_utf8(filename_buf).map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        let mut offset_buf = [0u8; 8];
        if file.read_exact(&mut offset_buf).is_err() {
            break;
        }
        let offset = usize::from_be_bytes(offset_buf);

        new_dir.insert(
            key,
            Key {
                filename,
                timestamp: ts,
                name: key,
                offset: offset as u64,
                length: v_length as usize,
            },
        );
    }

    Ok(new_dir)
}

fn delete_old_files(
    data_dir: &str,
    active_dir: &str,
    key_dir: Arc<Mutex<BTreeMap<usize, Key>>>,
) -> Result<(), Error> {
    let mut used_files: HashSet<String> = HashSet::new();

    // ensure no new active files are created while we read the list of files
    let l = key_dir.lock();
    let files = fs::read_dir(data_dir)?;
    for v in l.unwrap().values() {
        used_files.insert(v.filename.to_string());
    }

    let mut count = 0;
    for entry in files {
        let path = entry?.path();
        let Some(file) = path.file_name() else {
            return Err(Error::new(ErrorKind::InvalidData, "Path is not a file"));
        };
        let Some(filename) = file.to_str() else {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "File does not have a name",
            ));
        };
        let full_filename = format!("{}/{}", data_dir, filename);
        if full_filename.ends_with(HINT_FILE_NAME)
            || full_filename == active_dir
            || used_files.contains(&full_filename.to_string())
        {
            continue;
        }
        delete_file(&*full_filename)?;
        count += 1;
    }
    println!("Compacted {} files", count);
    Ok(())
}
