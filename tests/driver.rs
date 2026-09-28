//! End-to-end tests using the official MongoDB Rust driver against a
//! single-node Mango server.

use futures_util::TryStreamExt;
use mango::app::{self, Config};
use mongodb::bson::{Bson, Document, doc, oid::ObjectId};
use mongodb::options::{FindOneAndUpdateOptions, FindOptions, IndexOptions, ReturnDocument, UpdateOptions};
use mongodb::{Client, IndexModel};
use std::time::Duration;

struct TestServer {
    _dir: tempfile::TempDir,
    uri: String,
}

async fn start() -> TestServer {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = Config::single_node(dir.path().to_path_buf(), "127.0.0.1:0");
    cfg.heartbeat = Duration::from_millis(20);
    cfg.election_timeout = Duration::from_millis(100);
    let running = app::start(cfg).await.unwrap();
    // wait for the single node to elect itself
    let mut st = running.raft.status.clone();
    tokio::time::timeout(Duration::from_secs(5), st.wait_for(|s| s.role == mango::raft::Role::Leader)).await.unwrap().unwrap();
    TestServer { _dir: dir, uri: format!("mongodb://{}/?directConnection=true", running.addr) }
}

async fn client(s: &TestServer) -> Client {
    Client::with_uri_str(&s.uri).await.unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn crud_with_official_driver() {
    let s = start().await;
    let c = client(&s).await;
    let db = c.database("shop");
    let coll = db.collection::<Document>("items");

    let r = coll.insert_one(doc! {"name": "apple", "price": 3, "tags": ["fruit", "red"]}).await.unwrap();
    assert!(matches!(r.inserted_id, Bson::ObjectId(_)));
    let r = coll
        .insert_many(vec![
            doc! {"_id": 1, "name": "pear", "price": 5, "tags": ["fruit"]},
            doc! {"_id": 2, "name": "kale", "price": 7, "tags": ["veg", "green"]},
            doc! {"_id": 3, "name": "lime", "price": 2, "tags": ["fruit", "green"]},
        ])
        .await
        .unwrap();
    assert_eq!(r.inserted_ids.len(), 3);

    assert_eq!(coll.count_documents(doc! {}).await.unwrap(), 4);
    assert_eq!(coll.count_documents(doc! {"tags": "fruit"}).await.unwrap(), 3);
    assert_eq!(coll.estimated_document_count().await.unwrap(), 4);

    let found: Vec<Document> = coll
        .find(doc! {"price": {"$gte": 3}})
        .with_options(FindOptions::builder().sort(doc! {"price": -1}).projection(doc! {"_id": 0, "name": 1}).build())
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(found, vec![doc! {"name": "kale"}, doc! {"name": "pear"}, doc! {"name": "apple"}]);

    let one = coll.find_one(doc! {"_id": 2}).await.unwrap().unwrap();
    assert_eq!(one.get_str("name").unwrap(), "kale");

    let r = coll.update_many(doc! {"tags": "green"}, doc! {"$inc": {"price": 1}, "$set": {"sale": true}}).await.unwrap();
    assert_eq!((r.matched_count, r.modified_count), (2, 2));
    let r = coll
        .update_one(doc! {"_id": 99}, doc! {"$set": {"name": "new"}})
        .with_options(UpdateOptions::builder().upsert(true).build())
        .await
        .unwrap();
    assert_eq!(r.upserted_id, Some(Bson::Int32(99)));

    let updated = coll
        .find_one_and_update(doc! {"_id": 3}, doc! {"$push": {"tags": "sour"}})
        .with_options(FindOneAndUpdateOptions::builder().return_document(ReturnDocument::After).build())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(updated.get_array("tags").unwrap().len(), 3);

    let r = coll.delete_many(doc! {"price": {"$lt": 4}}).await.unwrap();
    assert_eq!(r.deleted_count, 2); // apple(3), lime(3)
    let r = coll.replace_one(doc! {"_id": 99}, doc! {"name": "replaced"}).await.unwrap();
    assert_eq!(r.modified_count, 1);
    assert_eq!(coll.find_one(doc! {"_id": 99}).await.unwrap().unwrap(), doc! {"_id": 99, "name": "replaced"});

    let mut names = coll.distinct("name", doc! {}).await.unwrap();
    names.sort_by_key(|b| b.to_string());
    assert_eq!(names.len(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn indexes_and_errors() {
    let s = start().await;
    let c = client(&s).await;
    let coll = c.database("app").collection::<Document>("users");
    coll.create_index(IndexModel::builder().keys(doc! {"email": 1}).options(IndexOptions::builder().unique(true).build()).build())
        .await
        .unwrap();
    coll.insert_one(doc! {"email": "a@x.io"}).await.unwrap();
    let err = coll.insert_one(doc! {"email": "a@x.io"}).await.unwrap_err();
    assert!(err.to_string().contains("E11000"), "{err}");
    let names = coll.list_index_names().await.unwrap();
    assert_eq!(names, vec!["_id_".to_string(), "email_1".to_string()]);
    coll.drop_index("email_1").await.unwrap();
    coll.insert_one(doc! {"email": "a@x.io"}).await.unwrap();

    // unordered bulk insert reports every failure
    let err = coll.insert_many(vec![doc! {"_id": 1}, doc! {"_id": 1}, doc! {"_id": 2}]).ordered(false).await.unwrap_err();
    assert!(err.to_string().contains("E11000"));
    assert_eq!(coll.count_documents(doc! {"_id": {"$in": [1, 2]}}).await.unwrap(), 2);

    // bad operators come back as errors, not crashes
    let err = coll.find_one(doc! {"a": {"$bogus": 1}}).await.unwrap_err();
    assert!(err.to_string().contains("unknown operator"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn cursors_and_aggregation() {
    let s = start().await;
    let c = client(&s).await;
    let coll = c.database("stats").collection::<Document>("events");
    let docs: Vec<Document> = (0..1000).map(|i| doc! {"_id": i, "kind": format!("k{}", i % 7), "v": i}).collect();
    coll.insert_many(docs).await.unwrap();

    // Forces several getMore round trips.
    let all: Vec<Document> = coll.find(doc! {}).batch_size(64).await.unwrap().try_collect().await.unwrap();
    assert_eq!(all.len(), 1000);
    let ids: Vec<i32> = all.iter().map(|d| d.get_i32("_id").unwrap()).collect();
    assert_eq!(ids, (0..1000).collect::<Vec<_>>());

    let limited: Vec<Document> =
        coll.find(doc! {"v": {"$gte": 500}}).sort(doc! {"v": 1}).skip(10).limit(5).await.unwrap().try_collect().await.unwrap();
    assert_eq!(limited.iter().map(|d| d.get_i32("v").unwrap()).collect::<Vec<_>>(), vec![510, 511, 512, 513, 514]);

    let out: Vec<Document> = coll
        .aggregate(vec![
            doc! {"$match": {"v": {"$lt": 70}}},
            doc! {"$group": {"_id": "$kind", "n": {"$sum": 1}, "total": {"$sum": "$v"}}},
            doc! {"$sort": {"_id": 1}},
            doc! {"$limit": 2},
        ])
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(out, vec![doc! {"_id": "k0", "n": 10, "total": 315}, doc! {"_id": "k1", "n": 10, "total": 325}]);
}

#[tokio::test(flavor = "multi_thread")]
async fn catalog_commands() {
    let s = start().await;
    let c = client(&s).await;
    let db = c.database("cat");
    db.create_collection("a").await.unwrap();
    assert!(db.create_collection("a").await.is_err());
    db.collection::<Document>("b").insert_one(doc! {"x": 1}).await.unwrap();
    let mut names = db.list_collection_names().await.unwrap();
    names.sort();
    assert_eq!(names, vec!["a", "b"]);
    assert!(c.list_database_names().await.unwrap().contains(&"cat".to_string()));

    c.database("admin").run_command(doc! {"renameCollection": "cat.b", "to": "cat.c"}).await.unwrap();
    let mut names = db.list_collection_names().await.unwrap();
    names.sort();
    assert_eq!(names, vec!["a", "c"]);
    assert_eq!(db.collection::<Document>("c").count_documents(doc! {}).await.unwrap(), 1);

    db.collection::<Document>("a").drop().await.unwrap();
    db.drop().await.unwrap();
    assert!(!c.list_database_names().await.unwrap().contains(&"cat".to_string()));

    let st = c.database("admin").run_command(doc! {"mangoStatus": 1}).await.unwrap();
    assert_eq!(st.get_str("role").unwrap(), "leader");
    assert_eq!(st.get_str("ripeness").unwrap(), "ripe");
    let _ = ObjectId::new();
}

#[tokio::test(flavor = "multi_thread")]
async fn transactions_are_rejected_clearly() {
    let s = start().await;
    let c = client(&s).await;
    let coll = c.database("t").collection::<Document>("x");
    coll.insert_one(doc! {"a": 1}).await.unwrap();
    let mut session = c.start_session().await.unwrap();
    session.start_transaction().await.unwrap();
    let err = coll.insert_one(doc! {"a": 2}).session(&mut session).await.unwrap_err();
    assert!(err.to_string().contains("transactions"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn scram_sha256_auth() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = Config::single_node(dir.path().to_path_buf(), "127.0.0.1:0");
    cfg.heartbeat = Duration::from_millis(20);
    cfg.election_timeout = Duration::from_millis(100);
    cfg.auth = true;
    cfg.root_user = Some(("root".into(), "hunter2-but-longer".into()));
    let running = app::start(cfg).await.unwrap();
    let addr = running.addr;

    // Unauthenticated clients are refused.
    let anon = Client::with_uri_str(format!("mongodb://{addr}/?directConnection=true")).await.unwrap();
    let err = anon.database("x").collection::<Document>("y").insert_one(doc! {"a": 1}).await.unwrap_err();
    assert!(err.to_string().contains("requires authentication"), "{err}");

    // Wait for the root user to be bootstrapped, then log in.
    let root_uri = format!("mongodb://root:hunter2-but-longer@{addr}/?directConnection=true&authSource=admin");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let root = loop {
        let c = Client::with_uri_str(&root_uri).await.unwrap();
        if c.database("x").collection::<Document>("y").insert_one(doc! {"a": 1}).await.is_ok() {
            break c;
        }
        assert!(std::time::Instant::now() < deadline, "root login never succeeded");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    // A read-only user can read but not write.
    root.database("app").run_command(doc! {"createUser": "reader", "pwd": "readerpass", "roles": ["read"]}).await.unwrap();
    let reader = Client::with_uri_str(format!("mongodb://reader:readerpass@{addr}/?directConnection=true&authSource=app")).await.unwrap();
    assert_eq!(reader.database("x").collection::<Document>("y").count_documents(doc! {}).await.unwrap(), 1);
    let err = reader.database("x").collection::<Document>("y").insert_one(doc! {"a": 2}).await.unwrap_err();
    assert!(err.to_string().contains("not authorized"), "{err}");

    // Wrong password fails.
    let bad = Client::with_uri_str(format!("mongodb://root:wrong@{addr}/?directConnection=true&authSource=admin")).await.unwrap();
    let err = bad.database("x").collection::<Document>("y").count_documents(doc! {}).await.unwrap_err();
    assert!(err.to_string().to_lowercase().contains("auth"), "{err}");
}
