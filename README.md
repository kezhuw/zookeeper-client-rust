# ZooKeeper client in async rust
[![crates.io](https://img.shields.io/crates/v/zookeeper-client)](https://crates.io/crates/zookeeper-client)
[![docs.rs](https://img.shields.io/docsrs/zookeeper-client)](https://docs.rs/zookeeper-client)
[![github-ci](https://github.com/kezhuw/zookeeper-client-rust/actions/workflows/ci.yml/badge.svg?event=push)](https://github.com/kezhuw/zookeeper-client-rust/actions)
[![mit-license](https://img.shields.io/github/license/kezhuw/zookeeper-client-rust)](LICENSE)
[![codecov](https://codecov.io/gh/kezhuw/zookeeper-client-rust/branch/master/graph/badge.svg?token=C98TXPU5ZZ)](https://codecov.io/gh/kezhuw/zookeeper-client-rust)

ZooKeeper client writes in async rust.

## Features
* No callbacks.
* No catch-all watcher.
* `StateWatcher` tracks session state updates.
* `OneshotWatcher` tracks oneshot ZooKeeper node event.
* `PersistentWatcher` tracks persistent and recursive persistent ZooKeeper node events.
* No event type `XyzWatchRemoved` as there is no way to receive such event after watchers dropped.
* Cloneable `Client` and `Client::chroot` enables session sharing cross multiple different rooted clients.

## Examples

### Basics
```rust
use zookeeper_client as zk;

let path = "/abc";
let data = "path_data".as_bytes().to_vec();
let child_path = "/abc/efg";
let child_data = "child_path_data".as_bytes().to_vec();
let create_options = zk::CreateMode::Persistent.with_acls(zk::Acls::anyone_all());

let cluster = "localhost:2181";
let client = zk::Client::connect(cluster).await.unwrap();
let (_, stat_watcher) = client.check_and_watch_stat(path).await.unwrap();

let (stat, _) = client.create(path, &data, &create_options).await.unwrap();
assert_eq!((data.clone(), stat), client.get_data(path).await.unwrap());

let event = stat_watcher.changed().await;
assert_eq!(event.event_type, zk::EventType::NodeCreated);
assert_eq!(event.path, path);

let path_client = client.clone().chroot(path).unwrap();
assert_eq!((data, stat), path_client.get_data("/").await.unwrap());

let (_, _, child_watcher) = client.get_and_watch_children(path).await.unwrap();

let (child_stat, _) = client.create(child_path, &child_data, &create_options).await.unwrap();

let child_event = child_watcher.changed().await;
assert_eq!(child_event.event_type, zk::EventType::NodeChildrenChanged);
assert_eq!(child_event.path, path);

let relative_child_path = child_path.strip_prefix(path).unwrap();
assert_eq!((child_data.clone(), child_stat), path_client.get_data(relative_child_path).await.unwrap());

let (_, _, event_watcher) = client.get_and_watch_data("/").await.unwrap();
drop(client);
drop(path_client);

let session_event = event_watcher.changed().await;
assert_eq!(session_event.event_type, zk::EventType::Session);
assert_eq!(session_event.session_state, zk::SessionState::Closed);
```

### Recipes
```rust
use zookeeper_client as zk;

let cluster = "localhost:2181";
let client = zk::Client::connect(cluster).await.unwrap();

let prefix = zk::LockPrefix::new_curator("/app/locks", "latch-").unwrap();
let options = zk::LockOptions::new(zk::Acls::anyone_all())
    .with_ancestor_options(zk::CreateMode::Persistent.with_acls(zk::Acls::anyone_all()))
    .unwrap();
let latch = client.lock(prefix, b"", options).await.unwrap();
latch.create("/app/data", b"data", &zk::CreateMode::Ephemeral.with_acls(zk::Acls::anyone_all())).await.unwrap();
```

For more examples, see [zookeeper.rs](tests/zookeeper.rs).

## License
The MIT License (MIT). See [LICENSE](LICENSE) for the full license text.

## References
* [ZooKeeper Documentation](https://zookeeper.apache.org/doc/current/index.html)
