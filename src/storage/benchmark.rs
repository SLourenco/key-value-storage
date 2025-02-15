#[cfg(test)]
mod tests {
    use crate::storage::bit_cask::new_bit_cask;
    use crate::storage::KV;
    use std::thread;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    #[test]
    fn insert_retrieve_test() {
        let storage = new_bit_cask("test-data");
        assert!(storage.is_ok());
        let mut storage = storage.unwrap();
        let put_result = storage.put(123, "my-value".to_string());
        assert!(put_result.is_ok());
        let get_result = storage.get(123);
        assert!(get_result.is_ok());
        assert_eq!("my-value", get_result.unwrap());
    }

    #[test]
    fn bulk_insert_retrieve_test() {
        let storage = new_bit_cask("test-data");
        assert!(storage.is_ok());
        let mut storage = storage.unwrap();
        let put_result = storage.batch_put(vec![
            KV {
                key: 456,
                value: "456 my value".to_string(),
            },
            KV {
                key: 789,
                value: "789 my value".to_string(),
            },
            KV {
                key: 1111,
                value: "1111 my value".to_string(),
            },
        ]);
        assert!(put_result.is_ok());
        let get_result = storage.range(400, 900);
        assert!(get_result.is_ok());
        let r = get_result.unwrap();
        assert_eq!(2, r.len());
        let f = r.first().unwrap();
        assert_eq!(456, f.key);
        assert_eq!("456 my value", f.value);
        let s = r.last().unwrap();
        assert_eq!(789, s.key);
        assert_eq!("789 my value", s.value);
    }

    #[test]
    fn timing_bulk_insert() {
        // 1_000_000_000 exceeds memory available
        // 1_000_000 takes about 4.60 seconds
        // benchmark shows around 3500 ns/iteration
        let record_count = 1_000_000;
        let mut records = Vec::with_capacity(record_count);
        for i in 1..record_count {
            let v = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
                .to_string();
            records.push(KV { key: i, value: v })
        }

        use std::time::Instant;
        let now = Instant::now();
        let storage = new_bit_cask("test-data");
        assert!(storage.is_ok());
        let mut storage = storage.unwrap();
        let put_result = storage.batch_put(records);

        let elapsed = now.elapsed();
        println!(
            "Inserted {} records. Elapsed: {:.2?}",
            record_count, elapsed
        );
        assert!(put_result.is_ok());
    }

    #[test]
    fn timing_single_insert() {
        // 1_000_000 takes about 6 seconds
        // 10_000_000 takes about 8.76s
        // benchmark shows around 3500 ns/iteration
        let record_count = 1_000_000;
        use std::time::Instant;
        let now = Instant::now();
        let storage = new_bit_cask("test-data");
        assert!(storage.is_ok());
        let mut storage = storage.unwrap();

        for i in 1..record_count {
            let _ = storage.put(i, "my-value".to_string());
        }

        let elapsed = now.elapsed();
        println!(
            "Inserted {} records. Elapsed: {:.2?}",
            record_count, elapsed
        );
    }

    #[test]
    fn insert_50_gb() {
        // some random json struct
        let value = r#"{"_id":"67af531e45a1886ffae3a157","index":0,"guid":"9df745eb-d74d-4edb-975f-36effe080bb5","isActive":true,"balance":"$1,548.07","picture":"http://placehold.it/32x32","age":25,"eyeColor":"blue","name":"Mccarthy Garcia","gender":"male","company":"KYAGORO","email":"mccarthygarcia@kyagoro.com","phone":"+1 (868) 539-3028","address":"122 Kansas Place, Venice, Maryland, 499","about":"Deserunt aliquip elit minim labore nostrud deserunt fugiat ipsum ea occaecat voluptate ipsum eu. Amet labore fugiat ad cupidatat occaecat eu elit laboris aute. Deserunt ullamco officia consectetur consequat adipisicing amet eiusmod ex amet. Quis sit culpa eiusmod id qui pariatur ea culpa sunt deserunt. Sit eu fugiat nulla esse ea nisi mollit laborum in.\r\n","registered":"2016-12-06T06:18:13 -00:00","latitude":39.945306,"longitude":27.872656,"tags":["minim","aliqua","occaecat","eiusmod","anim","nisi","veniam"],"friends":[{"id":0,"name":"Gibson Bowman"},{"id":1,"name":"Davidson Forbes"},{"id":2,"name":"Meadows Doyle"}],"greeting":"Hello, Mccarthy Garcia! You have 6 unread messages.","favoriteFruit":"apple"}"#;
        // took about 11 minutes and inserted around 52GB of data
        // Max memory used was 7 GB
        let record_count = 50_000_000;
        let batch_size = 1_000_000;
        let storage = new_bit_cask("test-data");
        assert!(storage.is_ok());
        let mut storage = storage.unwrap();

        let mut total = 0;
        for j in 0..(record_count / batch_size) {
            for i in total..(batch_size * (j + 1)) {
                let _ = storage.put(i, value.to_string());
            }
            total += batch_size;
            // Better mimic behaviour of getting multiple requests over a period of time
            thread::sleep(Duration::from_millis(3000));
        }
    }
}
