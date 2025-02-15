use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{Error, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

const FILE_MAX_OFFSET: u64 = 10_000_000;

pub(crate) fn save(
    data_dir: &str,
    active_dir: &str,
    data_vec: Vec<(usize, String)>,
) -> Result<(Vec<(String, u64, usize, u64)>, String), Error> {
    let mut file = OpenOptions::new()
        .write(true)
        .append(true)
        .open(active_dir)?;

    let mut results = Vec::new();
    let mut offset = file.seek(SeekFrom::End(0))?;
    let mut current_active_dir = active_dir.to_string();

    for (key, value) in data_vec {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        file.write_all(&ts.to_be_bytes())?;
        let v_length = value.len();
        file.write_all(&v_length.to_be_bytes())?;
        file.write_all(&key.to_be_bytes())?;
        file.write_all(value.as_bytes())?;
        results.push((current_active_dir.to_string(), offset, v_length, ts));
        offset += 8 + 8 + 8 + v_length as u64;

        if offset > FILE_MAX_OFFSET {
            file.flush()?;
            current_active_dir = create_new_active_file(data_dir)?;
            file = OpenOptions::new()
                .write(true)
                .append(true)
                .open(current_active_dir.to_string())?;
            offset = 0;
        }
    }

    Ok((results, current_active_dir))
}

pub(crate) fn create_new_active_file(data_dir: &str) -> Result<String, Error> {
    let filename = format!("{}/data-file{}", data_dir, get_random());
    File::create(filename.clone())?;
    Ok(filename.to_string())
}

pub(crate) fn create_new_file(full_filename: &str) -> Result<(), Error> {
    let path = Path::new(full_filename);
    if path.exists() {
        fs::remove_file(path)?;
    }
    File::create(full_filename)?;
    Ok(())
}

fn get_random() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

pub(crate) fn delete_file(full_filename: &str) -> Result<(), Error> {
    fs::remove_file(full_filename)?;
    Ok(())
}
