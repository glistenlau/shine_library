use std::{panic, sync::Arc};

use rocksdb::DB;
use shine_library::proxy::rocksdb::{get_conn, RocksdbProxy, RocksMergeOp};

const TEST_ROCKSDB_PATH: &str = "./data/_test";
const TEST_CF: &str = "TEST_CF";

fn run_rocksdb_test<F>(test_fn: F, merge_op: Option<RocksMergeOp>)
where
    F: FnOnce(&mut DB) -> () + panic::UnwindSafe,
{
    let result = panic::catch_unwind(|| {
        let mut conn = get_conn(TEST_ROCKSDB_PATH, merge_op);
        test_fn(&mut conn)
    });

    let mut conn = get_conn(TEST_ROCKSDB_PATH, merge_op);
    conn.drop_cf(TEST_CF);

    assert!(result.is_ok())
}

#[test]
fn test_json_array_merge() {
    let test_fn = |db: &mut DB| {
        let result = RocksdbProxy::append_to_array(Some(TEST_CF), b"test", "[val1]", db);
        assert!(result.is_ok(), "append val1 error: {}", result.unwrap_err());

        let result = RocksdbProxy::append_to_array(Some(TEST_CF), b"test", "[val2]", db);
        assert!(result.is_ok(), "append val2 error: {}", result.unwrap_err());

        let result = RocksdbProxy::get(Some(TEST_CF), b"test", db);
        assert!(result.is_ok(), "get value error: {}", result.unwrap_err());

        let arr_str_opt = result.unwrap();
        assert!(arr_str_opt.is_some());

        let arr_str = arr_str_opt.unwrap();
        assert_eq!("[\"[val1]\",\"[val2]\"]", arr_str);

        let parse_result = serde_json::from_str::<Vec<String>>(&arr_str);
        assert!(
            parse_result.is_ok(),
            "parse results error: {}",
            parse_result.unwrap_err()
        );
        let arr = parse_result.unwrap();
        assert_eq!(2, arr.len());
        assert_eq!("[val1]", arr[0]);
        assert_eq!("[val2]", arr[1]);
    };

    run_rocksdb_test(test_fn, Some(RocksMergeOp::JsonArray));
}

#[test]
fn test_query_range_forward() {
    let test_fn = |db: &mut DB| {
        let result = RocksdbProxy::batch_write(
            Some(TEST_CF),
            vec!["2022-03-12", "2022-03-15", "2022-03-18"],
            vec!["val1", "val2", "val3"],
            db,
        );

        assert!(result.is_ok(), "set batch error: {}", result.unwrap_err());

        let result = RocksdbProxy::query_range_forward(
            Some(TEST_CF),
            Some(b"2022-03-11"),
            Some(b"2022-03-15"),
            db,
        );
        assert!(result.is_ok(), "query range error: {}", result.unwrap_err());

        let arr = result.unwrap();
        assert_eq!(1, arr.len(), "query range result should have 1 element.");

        assert_eq!(
            &(String::from("2022-03-12"), String::from("[\"[val1]\"]")),
            arr.get(0).unwrap(),
            "query range result should be the first value."
        );

        let result = RocksdbProxy::query_range_forward(
            Some(TEST_CF),
            Some(b"2022-03-11"),
            Some(b"2022-03-16"),
            db,
        );
        assert!(result.is_ok(), "query range error: {}", result.unwrap_err());

        let arr = result.unwrap();
        assert_eq!(2, arr.len(), "query range result should have 1 element.");

        assert_eq!(
            &(String::from("2022-03-12"), String::from("[\"[val1]\"]")),
            arr.get(0).unwrap(),
            "query range result should be the first value."
        );

        assert_eq!(
            &(String::from("2022-03-15"), String::from("[\"[val2]\"]")),
            arr.get(1).unwrap(),
            "query range result should be the first value."
        );

        let result =
            RocksdbProxy::query_range_forward(Some(TEST_CF), Some(b"2022-03-15"), None, db);
        assert!(result.is_ok(), "query range error: {}", result.unwrap_err());

        let arr = result.unwrap();
        assert_eq!(2, arr.len(), "query range result should have 1 element.");

        assert_eq!(
            &(String::from("2022-03-15"), String::from("[\"[val2]\"]")),
            arr.get(0).unwrap(),
            "query range result should be the first value."
        );

        assert_eq!(
            &(String::from("2022-03-18"), String::from("[\"[val3]\"]")),
            arr.get(1).unwrap(),
            "query range result should be the first value."
        );
    };

    run_rocksdb_test(test_fn, Some(RocksMergeOp::JsonArray));
}
