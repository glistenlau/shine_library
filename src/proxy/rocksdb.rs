use std::{
    borrow::BorrowMut,
    path::PathBuf,
    str::FromStr,
    sync::{Arc, Mutex, MutexGuard, RwLock},
};

use anyhow::{anyhow, Result};

use lazy_static::lazy_static;
use log::info;
use rocksdb::{
    ColumnFamily, ColumnFamilyDescriptor, Direction, IteratorMode, MergeOperands, Options,
    WriteBatch, DB,
};

pub struct RocksdbProxy {
    conn: Option<Arc<Mutex<DB>>>,
    path: String,
    merge_op: Option<RocksMergeOp>,
}

fn json_array_merge(
    new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut result: Vec<u8> = Vec::with_capacity(operands.len());
    result.push(b'[');
    let mut process_op = |op: &[u8]| {
        if result.len() > 1 {
            result.push(b',');
        }
        for (i, e) in op.iter().enumerate() {
            if i == 0 && e.eq(&b'[') {
                continue;
            }
            if i == op.len() - 1 && e.eq(&b']') {
                continue;
            }
            result.push(*e)
        }
    };

    if let Some(op) = existing_val {
        process_op(op);
    }

    for op in operands {
        process_op(op);
    }

    result.push(b']');

    Some(result)
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RocksMergeOp {
    JsonArray,
}

impl RocksdbProxy {
    fn new() -> Self {
        RocksdbProxy {
            conn: None,
            path: String::new(),
            merge_op: None,
        }
    }

    fn new_conn(path: &str, merge_op: Option<RocksMergeOp>) -> Result<DB> {
        info!(
            "Creating rocksdb connection with path={}, merge_op={:?}",
            path, &merge_op
        );
        let mut opts = Self::get_options(merge_op);

        Self::set_merge_operator(&mut opts, merge_op);

        match DB::list_cf(&opts, path) {
            Ok(cf_list) => {
                let cf_descriptors: Vec<ColumnFamilyDescriptor> = cf_list
                    .iter()
                    .map(|cf_str| ColumnFamilyDescriptor::new(cf_str, opts.clone()))
                    .collect();
                match DB::open_cf_descriptors(&opts, &path, cf_descriptors) {
                    Ok(db) => return Ok(db),
                    Err(e) => return Err(anyhow!("failed to connect rocksdb {}", e)),
                };
            }
            Err(e) => {
                log::info!("list cf error: {}", e);
                match DB::open(&opts, &path) {
                    Ok(db) => return Ok(db),
                    Err(e) => return Err(anyhow!("failed to connect rocksdb {}", e)),
                }
            }
        }
    }

    fn set_merge_operator(options: &mut Options, merge_op: Option<RocksMergeOp>) {
        if merge_op.is_none() {
            return;
        }

        match merge_op.unwrap() {
            RocksMergeOp::JsonArray => {
                info!("setting JsonArray merge operator");
                options.set_merge_operator_associative("json-array-merge", json_array_merge)
            }
        }
    }

    pub fn get_conn(
        &mut self,
        path: &str,
        merge_op: Option<RocksMergeOp>,
    ) -> Result<Arc<Mutex<DB>>> {
        if self.conn.is_none() || self.path.ne(path) || self.merge_op.ne(&merge_op) {
            log::info!("No existing rocksdb connection, try to create a new one.");
            let conn = Self::new_conn(path, Some(RocksMergeOp::JsonArray))?;
            self.conn = Some(Arc::new(Mutex::new(conn)));
            self.path = String::from(path);
            self.merge_op = merge_op;
        }

        Ok(Arc::clone(&self.conn.as_ref().unwrap()))
    }

    pub fn get<K>(cf: Option<&str>, key: K, db: &mut DB) -> Result<Option<String>>
    where
        K: AsRef<[u8]>,
    {
        let res_opt = if let Some(cf_str) = cf {
            Self::create_cf_if_not(cf_str, db)?;
            let cf_handle = db.cf_handle(cf_str).unwrap();
            db.get_cf(cf_handle, key)
        } else {
            db.get(key)
        };
        match res_opt {
            Ok(Some(str_vals)) => Ok(Some(String::from_utf8(str_vals)?)),
            Ok(None) => Ok(None),
            Err(e) => Err(anyhow!("RocksDB get error: {}", e)),
        }
    }

    pub fn batch_get<T, I>(cf: Option<&str>, keys: I, db: &DB) -> Result<Vec<Option<String>>>
    where
        T: AsRef<[u8]>,
        I: IntoIterator<Item = T>,
    {
        let mut res_bytes = match cf.and_then(|cf_str| db.cf_handle(cf_str)) {
            Some(cf_handle) => {
                let multi_keys: Vec<(&ColumnFamily, T)> =
                    keys.into_iter().map(|key| (cf_handle, key)).collect();
                db.multi_get_cf(multi_keys)
            }
            None => db.multi_get(keys),
        };

        Ok(res_bytes
            .drain(..)
            .map(|res| match res {
                Ok(res_op) => res_op,
                Err(_) => None,
            })
            .map(|res_op| {
                res_op.and_then(|res_byte| match String::from_utf8(res_byte) {
                    Ok(s) => Some(s),
                    Err(_) => None,
                })
            })
            .collect())
    }

    pub fn iterator<K>(
        cf: Option<&str>,
        iter_mode: IteratorMode,
        to: Option<K>,
        direction: Direction,
        db: &mut DB,
    ) -> Result<Vec<(String, String)>>
    where
        K: AsRef<[u8]>,
    {
        let mut iterator = if let Some(cf_str) = cf {
            Self::create_cf_if_not(cf_str, db)?;
            let cf_handle = db.cf_handle(cf_str).unwrap();
            db.iterator_cf(cf_handle, iter_mode)
        } else {
            db.iterator(iter_mode)
        };

        let mut result: Vec<(String, String)> = Vec::with_capacity(iterator.size_hint().0);
        while let Some((key, val)) = iterator.next() {
            if let Some(ref to_val) = to {
                let should_break = match direction {
                    Direction::Forward => to_val.as_ref().le(&key),
                    Direction::Reverse => to_val.as_ref().ge(&key),
                };
                if should_break {
                    break;
                }
            }
            result.push((
                String::from_utf8(key.to_vec()).unwrap(),
                String::from_utf8(val.to_vec()).unwrap(),
            ));
        }

        Ok(result)
    }

    pub fn query_range_forward<K>(
        cf: Option<&str>,
        from: Option<K>,
        to: Option<K>,
        db: &mut DB,
    ) -> Result<Vec<(String, String)>>
    where
        K: AsRef<[u8]>,
    {
        let from_key;
        let iter_mode = match from {
            Some(fk) => {
                from_key = fk;
                IteratorMode::From(from_key.as_ref().clone(), Direction::Forward)
            }
            None => IteratorMode::Start,
        };

        Self::iterator(cf, iter_mode, to, Direction::Forward, db)
    }

    pub fn query_range_reserve<K>(
        cf: Option<&str>,
        from: Option<K>,
        to: Option<K>,
        db: &mut DB,
    ) -> Result<Vec<(String, String)>>
    where
        K: AsRef<[u8]>,
    {
        let from_key;
        let iter_mode = match from {
            Some(fk) => {
                from_key = fk;
                IteratorMode::From(from_key.as_ref().clone(), Direction::Reverse)
            }
            None => IteratorMode::End,
        };

        Self::iterator(cf, iter_mode, to, Direction::Reverse, db)
    }

    fn convert_range_result_to_array_result(
        mut result: Vec<(String, String)>,
    ) -> Vec<(String, Result<Vec<String>>)> {
        let parsed_result: Vec<(String, Result<Vec<String>>)> = result
            .drain(..)
            .map(|(k, v)| {
                let parsed_val: Result<Vec<String>> =
                    serde_json::from_str::<Vec<String>>(&v).map_err(|e| e.into());
                (k, parsed_val)
            })
            .collect();
        parsed_result
    }

    pub fn query_range_array_forward<K>(
        cf: Option<&str>,
        from: Option<K>,
        to: Option<K>,
        db: &mut DB,
    ) -> Result<Vec<(String, Result<Vec<String>>)>>
    where
        K: AsRef<[u8]>,
    {
        let result = Self::query_range_forward(cf, from, to, db)?;
        let array_result = Self::convert_range_result_to_array_result(result);
        Ok(array_result)
    }

    pub fn query_range_array_reserve<K>(
        cf: Option<&str>,
        from: Option<K>,
        to: Option<K>,
        db: &mut DB,
    ) -> Result<Vec<(String, Result<Vec<String>>)>>
    where
        K: AsRef<[u8]>,
    {
        let result = Self::query_range_reserve(cf, from, to, db)?;
        let array_result = Self::convert_range_result_to_array_result(result);
        Ok(array_result)
    }

    pub fn append_to_array<V>(cf: Option<&str>, key: V, val: &str, db: &mut DB) -> Result<()>
    where
        V: AsRef<[u8]>,
    {
        {
            let current_merge_op = CURRENT_MERGE_OP.lock().unwrap();
            if current_merge_op.ne(&Some(RocksMergeOp::JsonArray)) {
                return Err(anyhow!(
                    "The merge op is not JsonArray, cannot append to array."
                ));
            }
        }

        let val_str = serde_json::to_string(val).unwrap();

        let result = if let Some(cf_str) = cf {
            Self::create_cf_if_not(cf_str, db)?;
            let cf_handle = db.cf_handle(cf_str).unwrap();
            db.merge_cf(cf_handle, key, val_str)
        } else {
            db.merge(key, val_str)
        };

        result.map_err(|e| e.into())
    }

    pub fn delete<K>(cf: Option<&str>, key: K, db: &mut DB) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        let result = if let Some(cf_str) = cf {
            Self::create_cf_if_not(cf_str, db)?;
            let cf_handle = db.cf_handle(cf_str).unwrap();
            db.delete_cf(cf_handle, key)
        } else {
            db.delete(key)
        };

        result.map_err(|e| e.into())
    }

    pub fn batch_delete<K, I>(cf: Option<&str>, key: I, db: &mut DB) -> Result<()>
    where
        K: AsRef<[u8]>,
        I: IntoIterator<Item = K>,
    {
        let mut write_batch = WriteBatch::default();
        if let Some(cf_str) = cf {
            Self::create_cf_if_not(cf_str, db)?;
            let cf_handle = db.cf_handle(cf_str).unwrap();
            key.into_iter()
                .for_each(|key_str| write_batch.delete_cf(cf_handle, key_str));
        } else {
            key.into_iter()
                .for_each(|key_str| write_batch.delete(key_str));
        }

        db.write(write_batch)?;
        Ok(())
    }

    pub fn delete_range<K>(cf: Option<&str>, from: K, to: K, db: &mut DB) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        let mut write_batch = WriteBatch::default();
        if let Some(cf_str) = cf {
            Self::create_cf_if_not(cf_str, db)?;
            let cf_handle = db.cf_handle(cf_str).unwrap();
            write_batch.delete_range_cf(cf_handle, from, to);
        } else {
            write_batch.delete_range(from, to);
        };

        db.write(write_batch)?;
        Ok(())
    }

    pub fn batch_write<K, I>(cf: Option<&str>, keys: I, values: I, db: &mut DB) -> Result<()>
    where
        K: AsRef<[u8]>,
        I: IntoIterator<Item = K>,
    {
        let mut write_batch = WriteBatch::default();
        let mut keys_iter = keys.into_iter();
        let mut vals_iter = values.into_iter();

        if let Some(cf_str) = cf {
            Self::create_cf_if_not(cf_str, db)?;

            let cf_handle = db.cf_handle(cf_str).unwrap();
            while let (Some(k), Some(v)) = (keys_iter.next(), vals_iter.next()) {
                write_batch.put_cf(cf_handle, k, v);
            }
        } else {
            while let (Some(k), Some(v)) = (keys_iter.next(), vals_iter.next()) {
                write_batch.put(k, v);
            }
        }
        if keys_iter.next().is_some() || keys_iter.next().is_some() {
            return Err(anyhow!("The sizes of keys and values are not equal."));
        }
        db.write(write_batch)?;
        Ok(())
    }

    fn get_options(merge_op: Option<RocksMergeOp>) -> Options {
        let mut opts = Options::default();
        opts.create_if_missing(true);

        Self::set_merge_operator(&mut opts, merge_op);
        opts
    }

    fn create_cf_if_not(cf: &str, db: &mut DB) -> Result<()> {
        if let Some(_) = db.cf_handle(cf) {
            return Ok(());
        }

        let current_merge_op = CURRENT_MERGE_OP.lock().unwrap();

        let opts = Self::get_options(*current_merge_op);

        db.create_cf(cf, &opts)?;
        Ok(())
    }
}

lazy_static! {
    static ref CURRENT_PATH: Mutex<Option<String>> = Mutex::new(None);
    static ref CURRENT_MERGE_OP: Mutex<Option<RocksMergeOp>> = Mutex::new(None);
    static ref CONN: Mutex<DB> = Mutex::new(
        RocksdbProxy::new_conn(
            &CURRENT_PATH.lock().unwrap().as_ref().unwrap(),
            *CURRENT_MERGE_OP.lock().unwrap()
        )
        .unwrap()
    );
    static ref DATA_STORE: Arc<Mutex<RocksdbProxy>> = Arc::new(Mutex::new(RocksdbProxy::new()));
}

pub fn get_conn_mutex(path: &str, merge_op: Option<RocksMergeOp>) -> Arc<Mutex<DB>> {
    let data_store = DATA_STORE.clone();
    let mut data_store_lock = data_store.lock().unwrap();
    data_store_lock.get_conn(path, merge_op).unwrap().clone()
}

pub fn get_conn(path: &str, merge_op: Option<RocksMergeOp>) -> MutexGuard<'static, DB> {
    let mut is_path_changed = false;
    let mut is_merge_op_changed = false;
    let mut initialized = true;
    {
        let mut current_path = CURRENT_PATH.lock().unwrap();
        let mut current_merge_op = CURRENT_MERGE_OP.lock().unwrap();
        if current_path.is_some() {
            is_path_changed = current_path.as_ref().unwrap().ne(path);
        } else {
            initialized = false;
        }

        is_merge_op_changed = current_merge_op.ne(&merge_op);

        if is_path_changed || !initialized {
            *current_path = Some(path.to_string());
        }

        if is_merge_op_changed {
            *current_merge_op = merge_op;
        }
    }

    let mut conn_lock = match CONN.lock() {
        Ok(mutex_gard) => mutex_gard,
        Err(e) => e.into_inner(),
    };

    if initialized && (is_path_changed || is_merge_op_changed) {
        info!(
            "making a new rocksdb connection with path={} merge_op={:?}",
            path, &merge_op
        );
        *conn_lock = RocksdbProxy::new_conn(path, merge_op).unwrap();
    }

    conn_lock
}

#[cfg(test)]
mod tests {

    use rocksdb::{merge_operator::MergeFn, MergeOperands, Options, WriteOptions};
    use serde_json::{json, Value};

    use super::*;

    static TEST_CF: &str = "test_cf";

    fn get_test_conn() -> MutexGuard<'static, DB> {
        get_conn("./data/_test", Some(RocksMergeOp::JsonArray))
    }

    fn run_rocksdb_test<F>(test_fn: F)
    where
        F: FnOnce(&mut DB),
    {
        let mut conn = get_test_conn();
        let _cf_handle = match conn.cf_handle(TEST_CF) {
            Some(ch) => ch,
            None => {
                println!("test_cf not exists, create it...");
                conn.create_cf(TEST_CF, &Options::default()).unwrap();
                conn.cf_handle(TEST_CF).unwrap()
            }
        };

        test_fn(&mut conn);

        conn.drop_cf(TEST_CF).unwrap();
    }

    #[test]
    fn test_cf() {
        let mut conn_lock = get_test_conn();
        let cf_handle = match conn_lock.cf_handle(TEST_CF) {
            Some(ch) => ch,
            None => {
                println!("test_cf not exists, create it...");
                conn_lock.create_cf(TEST_CF, &Options::default()).unwrap();
                conn_lock.cf_handle(TEST_CF).unwrap()
            }
        };
        let mut write_batch = WriteBatch::default();
        write_batch.put_cf(cf_handle, "test_key", "test_val");
        conn_lock.write(write_batch).unwrap();
        write_batch = WriteBatch::default();
        write_batch.put_cf(cf_handle, "test_key", "test_val");
        write_batch.put_cf(cf_handle, "test_key2", "test_val2");
        conn_lock.write(write_batch).unwrap();
        assert_eq!(
            conn_lock.get_cf(cf_handle, "test_key").unwrap().unwrap(),
            b"test_val",
            "test_key should be write successfully."
        );
        assert_eq!(
            conn_lock.get_cf(cf_handle, "test_key2").unwrap().unwrap(),
            b"test_val2",
            "test_key2 should be write successfully."
        );
        conn_lock.drop_cf(TEST_CF).unwrap();
    }

    fn merge(a: &mut Value, b: &Value) {
        match (a, b) {
            (&mut Value::Object(ref mut a), &Value::Object(ref b)) => {
                for (k, v) in b {
                    merge(a.entry(k.clone()).or_insert(Value::Null), v);
                }
            }
            (a, b) => {
                *a = b.clone();
            }
        }
    }

    fn obj_merge(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut result: Vec<u8> = Vec::with_capacity(operands.len());
        let mut existing_value: Value = existing_val
            .map(|v| serde_json::from_slice(v).unwrap())
            .unwrap_or(json!("{}"));
        for op in operands.iter() {
            for e in op {
                result.push(*e)
            }
        }
        let new_value: Value = serde_json::from_slice(&result).unwrap();
        merge(&mut existing_value, &new_value);
        Some(existing_value.to_string().into_bytes())
    }

    #[test]
    fn test_merge() {
        let mut opts = Options::default();
        opts.set_merge_operator_associative("object_merger", obj_merge);
        opts.create_if_missing(true);

        let db = get_test_conn();
        db.put(b"test", b"{\"name\": \"John Doe\"}");
        db.merge(b"test", b"{\"gender\": \"male\"}");
        let r = db.get(b"test").unwrap();
        println!("{}", String::from_utf8(r.unwrap()).unwrap());
    }

    #[test]
    fn test_merge_array() -> Result<()> {
        let mut opts = Options::default();

        let db = get_test_conn();

        db.merge(
            b"test1",
            serde_json::to_string("Hello, world!").unwrap().as_bytes(),
        );

        let r = db.get(b"test1").unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&r.unwrap()).unwrap();
        match parsed {
            Value::Array(arr) => {
                assert_eq!(1, arr.len())
            }
            _ => return Err(anyhow!("failed.")),
        }
        Ok(())
    }
}
