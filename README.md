# Key Value Storage

Implements the Bitcask algorithm, with a BTreeMap structure for keys, allowing range lookup.

## Exercise Requirements

### Low latency per item read or written

Reads involve a lookup in an ordered map structure in memory to file the file name and offset and reading from it.

Range reads will make use of the parallelism available in the machine and read from multiple files. Based on the ideas
behind [Wisckey](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf),
it expects a better performance in the now more common SDDs.

Writes involve writing to that ordered map structure and appending to the currently active write file.

Bulk writes work pretty much the same as regular writes, with appending to the same file. A file rollover is in place,
to keep files in a manageable size.

### High throughput, especially when writing an incoming stream of random items

As writes are all ot the same file, it should handle an incoming stream relatively well. Sorting of keys is made in
memory.
Tests made without compiler optimization showed 1M PUT method calls to take around 9 s (specs: AMD Ryzen™ 7 PRO 7840U,
32 GB RAM)

### Ability to handle datasets much larger than RAM w/o degradation

Since keys are stored in memory, this is the limiting factor for this algorithm.
While it can handle data much larger than RAM (tests were made with 52 GB of data),
it will eventually hit a cap when the memory cannot hold the keys anymore.

To address this, one can compromise in storing the key in dedicated file, similar to what is discussed
in [Wisckey](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf).
This will have the caveat of reducing performance. Some middle ground could be explored, of having a key file with a
cache in memory.

### Cash friendliness, both in terms of fast recovery and not losing data

All data is written into files, so losses will not happen if the operation completes successfully.
On startup, data files are read and in memory structure is rebuilt, resuming normal operation.
A hint file, built on a background job that compacts data files, is used for a faster startup, if present.

### Predictable behavior under heavy access load or large volume

If operating under the limits of RAM for the keys, individual reads are a single file lookup and the size of files is
capped.
So it will be consistent when operating with heavy access or large volume of data.

The http code was not the focus of the exercise, so there is significant room for improvement.
For production ready code, alternatives like gRPC should be considered to reduce network bottlenecks.

For large volume of data, aside from the files being capped, the compaction background job deletes old data files,
creating new ones with the current set of data used. This reduces wasted disk space.

## Requirements

* Rust `1.84.1` installed. [Documentation](https://www.rust-lang.org/tools/install)

## Running

To start a single server, do:

```bash
cargo run
```

A server will start at port 4000.

You can then do:

```bash
curl localhost:4000
```

and you will get a response with the available commands:

```
Usage:
READ: curl --location 'http://localhost:4000?key=1'
READ KEY RANGE: curl --location 'http://localhost:4000?start_key=1&end_key=10'
PUT: curl --location 'http://localhost:4000/' --header 'Content-Type: text/plain' --data 'key:1,value:2000'
BATCH PUT: curl --location 'http://localhost:4000' --header 'Content-Type: text/plain' --data 'key:1,value:2000
key:2,value:5000
key:5,value:4000
key:11,value:502'
DELETE: curl --location --request DELETE 'http://localhost:4000?key=1
```

### Arguments available

You can pass arguments to the command to specify some configurations:

- port: port where the server starts
- data-dir: directory where the data files are stored

Example:

```bash
cargo run port 5000 data-dir other-data-dir
```

## Tests

There are a few tests configured for different properties of the storage engine.
They involve inserting around 50GB of data, testing bulk put with 1M+ records and other basic tests.

It is preferable to run each test individually, instead of the whole suite.

You can run a single test by doing:

```bash
cargo test test-name
```

Example:

```bash
cargo test insert_retrieve_test
```

## Benchmark

If you want to run the benchmarks you will need to enable rust nightly

```bash
rustup default nightly
```

Then you will have to uncomment the following code (this is so the regular code runs in the stable version):

```rust
#![feature(test)]
extern crate test;
mod storage;
// (...)

#[cfg(test)]
mod tests {
    use super::*;
    use test::Bencher;

    #[bench]
    fn bench_batch_put(b: &mut Bencher) {
        let storage = new_bit_cask("test-data");
        assert!(storage.is_ok());
        let mut storage = storage.unwrap();
        b.iter(|| storage.batch_put(vec![KV { key: 1, value: "123".to_string() }]));
    }
}
```

After uncommenting, you can do:

```bash
cargo +nightly bench
```

and you will get something similar to:

```
bench
   Compiling key-value-storage v0.1.0 (/home/sergio-lourenco/projects/open-source/key-value-storage)
    Finished `bench` profile [optimized] target(s) in 0.42s
     Running unittests src/main.rs (target/release/deps/key_value_storage-223efe45b5e4e188)

running 6 tests
test storage::benchmark::tests::bulk_insert_retrieve_test ... ignored
test storage::benchmark::tests::insert_50_gb ... ignored
test storage::benchmark::tests::insert_retrieve_test ... ignored
test storage::benchmark::tests::timing_bulk_insert ... ignored
test storage::benchmark::tests::timing_single_insert ... ignored
test tests::bench_batch_put ... bench:       3,641.75 ns/iter (+/- 559.90)

test result: ok. 0 passed; 0 failed; 5 ignored; 1 measured; 0 filtered out; finished in 0.99s
```

You can re-enable the stable version by doing:

```bash
rustup default stable
```

You will have to comment the code for the benchmarks to be able to run on stable again