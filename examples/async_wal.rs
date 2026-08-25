use bytes::{BufMut, Bytes, BytesMut};

use dbprimkit::Error::RingBufferFull;
use dbprimkit::io::{AsyncIoBackend, TokioFileIoBackend};
use dbprimkit::wal::LogEntry;
use dbprimkit::wal::r#async::TokioFileWalInstance;
use futures::StreamExt;
use rand::RngExt;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

#[tokio::main]
async fn main() {
    let dir = AsRef::<Path>::as_ref("/dev/shm").to_path_buf();
    let name = "test_wal";
    let lsn_limit = 1000000u64;
    let adv = true;
    let mut rng = rand::rng();
    let mut bufs = Vec::<Bytes>::new();
    let long_thr_cnt = 2usize;
    let normal_thr_cnt = 14usize;
    for idx in 0..(long_thr_cnt + normal_thr_cnt) {
        let len = if idx < normal_thr_cnt {
            if idx == 5 {
                0
            } else {
                rng.random_range(0..4096)
            }
        } else {
            rng.random_range(256 * 1024..1024 * 1024)
        };
        let mut bm = BytesMut::with_capacity(len);
        let byte = rng.random::<u8>();
        bm.put_bytes(byte, len);
        bufs.push(bm.freeze());
    }

    let inst = TokioFileWalInstance::new(name, dir.as_path());
    let writer = Arc::new(inst.open_wal_writer().await.unwrap());

    let wstart_time = Instant::now();
    let mut handles = vec![];
    for idx in 0..bufs.len() {
        let buf = bufs[idx].clone();
        let wal = writer.clone();
        handles.push(tokio::spawn(async move {
            let mut logs = Vec::new();
            let mut lsn;
            let mut adv_lsn = 0u64;
            loop {
                let res = wal.append(buf.clone());
                lsn = match res {
                    Err(RingBufferFull) => {
                        tokio::time::sleep(Duration::from_micros(1)).await;
                        continue;
                    }
                    Err(_) => res.unwrap(),
                    Ok(lsn) => lsn,
                };

                logs.push(LogEntry {
                    lsn,
                    payload: buf.clone(),
                });
                if buf.len() >= 256 * 1024 {
                    tokio::time::sleep(Duration::from_micros(1)).await;
                }
                if idx == 0 && adv {
                    if lsn % 1000 == 0 {
                        let max_lsn = wal.get_max_lsn();
                        if max_lsn > 1000 {
                            adv_lsn = max_lsn - 1000;
                            wal.advance_lsn(adv_lsn).unwrap();
                        }
                    }
                }

                if lsn >= lsn_limit {
                    break;
                }
            }
            return (logs, adv_lsn);
        }));
    }

    let mut max_wlsn = 0u64;
    let mut log_map = BTreeMap::<u64, Bytes>::new();
    let mut max_adv_lsn = 0u64;
    let mut total_wbytes = 0u64;
    for handle in handles {
        let res = handle.await.unwrap();
        if res.1 > 0 {
            max_adv_lsn = res.1;
        }
        for log in res.0 {
            if log_map.contains_key(&log.lsn) {
                panic!("duplicated LSN found");
            }
            if max_wlsn < log.lsn {
                max_wlsn = log.lsn;
            }
            total_wbytes += log.payload.len() as u64;
            log_map.insert(log.lsn, log.payload);
        }
    }
    let mut flashed_lsn = writer.get_max_lsn();
    let mut last_flashed_lsn = 0u64;
    let mut printed = false;
    while max_wlsn > flashed_lsn {
        if flashed_lsn != last_flashed_lsn {
            printed = false;
            last_flashed_lsn = flashed_lsn;
        } else {
            if !printed {
                eprintln!("No change in flashed_lsn, lsn={}", flashed_lsn);
                printed = true;
            }
        }
        tokio::time::sleep(Duration::from_micros(1)).await;
        flashed_lsn = writer.get_max_lsn();
    }
    println!(
        "info: max_wlsn={}, total_wbytes={}, wduration={:?}",
        max_wlsn,
        total_wbytes,
        wstart_time.elapsed()
    );
    let inst = Arc::try_unwrap(writer).unwrap().stop();
    let rstart_time = Instant::now();
    let reader = inst.open_wal_reader().await.unwrap();
    let min_rlsn = reader.get_min_lsn();
    assert!(min_rlsn <= max_adv_lsn + 1);
    let mut stream = reader.get_log_stream(min_rlsn).await.unwrap();
    let mut max_rlsn = 0u64;
    let mut total_rbytes = 0u64;
    while let Some(log) = stream.next().await {
        let rlog = log.unwrap();
        if rlog.lsn <= max_rlsn {
            panic!(
                "rLSN not in increase order, rlsn={}, max_rlsn={}",
                rlog.lsn, max_rlsn
            );
        }
        max_rlsn = rlog.lsn;
        total_rbytes += rlog.payload.len() as u64;
        let wpayload = log_map
            .remove(&rlog.lsn)
            .expect(&format!("Unexpected LSN in rlog, lsn={}", rlog.lsn));
        assert_eq!(
            rlog.payload,
            wpayload,
            "lsn={}, llen={}, rlen={}",
            rlog.lsn,
            rlog.payload.len(),
            wpayload.len(),
        );
    }
    assert_eq!(max_wlsn, max_rlsn);
    println!(
        "info: max_rlsn={}, total_rbytes={}, rduration={:?}",
        max_rlsn,
        total_rbytes,
        rstart_time.elapsed()
    );
    let names = TokioFileIoBackend::list_files(&dir, Some(|n: &str| n.starts_with(name)))
        .await
        .unwrap();
    for n in names {
        let p = &dir.join(&n);
        TokioFileIoBackend::remove_file(p).await.unwrap();
    }
}
