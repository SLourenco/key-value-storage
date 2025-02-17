use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{Error, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::Path;

pub(crate) fn get_log_filename(id: u64) -> String {
    format!("log/file-{}", id)
}
pub(crate) fn create_new_file(full_filename: &str) -> Result<bool, Error> {
    let path = Path::new(full_filename);
    fs::create_dir_all(path.parent().unwrap())?;
    if !path.exists() {
        File::create(full_filename)?;
        return Ok(true);
    }
    Ok(false)
}

pub(crate) fn append_to_file(full_filename: &str, content: Vec<(u64, &str)>) -> Result<(), Error> {
    let mut file = OpenOptions::new()
        .write(true)
        .append(true)
        .open(full_filename)?;

    file.seek(SeekFrom::End(0))?;
    for (term, entry) in content {
        file.write_all(&term.to_be_bytes())?;
        file.write_all(&entry.len().to_be_bytes())?;
        file.write_all(entry.as_bytes())?;
    }

    Ok(())
}

pub(crate) fn read_log_file(full_filename: &str) -> Result<Vec<(u64, String)>, Error> {
    let mut file = OpenOptions::new().read(true).open(full_filename)?;

    let mut logs = Vec::new();
    loop {
        let mut term_buf = [0u8; 8];
        if file.read_exact(&mut term_buf).is_err() {
            break;
        }
        let term = u64::from_be_bytes(term_buf);

        let mut length_buf = [0u8; 8];
        if file.read_exact(&mut length_buf).is_err() {
            break;
        }
        let v_length = u64::from_be_bytes(length_buf);

        let mut value_buf = vec![0u8; v_length as usize];
        file.read_exact(&mut value_buf)?;
        let command =
            String::from_utf8(value_buf).map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        logs.push((term, command));
    }

    Ok(logs)
}
