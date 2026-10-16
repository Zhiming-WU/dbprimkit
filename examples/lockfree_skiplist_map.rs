use dbprimkit::skiplist::map::lockfree::SkipListMap;
use rand::Rng;
use rand::rngs::SmallRng;

fn main() {
    let order = 12usize;
    let table = SkipListMap::<u64, u64>::new(order, None);
    let mut rng: SmallRng = rand::make_rng();

    let max = 2usize.pow(order as u32);
    let mut vec = Vec::<u64>::with_capacity(max);
    while vec.len() < max {
        vec.push(rng.next_u64());
    }

    let thr_num = 4usize;
    let op_cnt = 2u64.pow((order + 2) as u32);
    let mask = max - 1;
    let vec_ref = &vec;
    let table_ref = &table;

    std::thread::scope(|s| {
        for _ in 0..thr_num {
            s.spawn(move || {
                let mut rng: SmallRng = rand::make_rng();
                for op_idx in 0..op_cnt {
                    let rnd = rng.next_u32();
                    let mut idx = rnd as usize & mask;
                    let op = rnd & 0x7;
                    match op {
                        x if x >= 4 => {
                            // inserting
                            if x == 4 {
                                idx = op_idx as usize & mask;
                            }
                            let key = vec_ref[idx];
                            let _ = table_ref.insert(key, idx as u64);
                        }
                        0 => {
                            // get. key may not in map
                            let key = vec_ref[idx];
                            if let Ok(Some(entry)) = table_ref.get(&key) {
                                let v = entry.value();
                                let idx_u64 = idx as u64;
                                assert!(*v == idx_u64 || *v == idx_u64 >> 1)
                            }
                        }
                        1 => {
                            // get_mut. key may not in map
                            let key = vec_ref[idx];
                            if let Ok(Some(mut entry)) = table_ref.get_mut(&key) {
                                let v = entry.value_mut().unwrap();
                                let idx_u64 = idx as u64;
                                assert!(*v == idx_u64 || *v == idx_u64 >> 1);
                                *v = idx_u64 >> 1;
                            }
                        }
                        2 => {
                            // remove. key may not in map
                            let key = vec_ref[idx];
                            let idx_u64 = idx as u64;
                            if let Ok(Some(res)) = table_ref.remove(&key) {
                                assert!(res.0 == key);
                                assert!(res.1 == idx_u64 || res.1 == idx_u64 >> 1)
                            }
                        }
                        3 => {
                            // remove_wo_kv. key may not in map
                            let key = vec_ref[idx];
                            table_ref.remove_wo_kv(&key);
                        }
                        _ => (),
                    }
                }
            });
        }
    });
}
