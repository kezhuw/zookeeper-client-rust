use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;
use std::{fs, future};

use assert_matches::assert_matches;
use assertor::*;
use maplit::hashmap;
use pretty_assertions::assert_eq;
use rand::distributions::Standard;
use rand::Rng;
use rcgen::{Certificate, CertificateParams};
#[allow(unused_imports)]
use tempfile::{tempdir, TempDir};
use test_case::test_case;
use testcontainers::clients::Cli as DockerCli;
use testcontainers::core::{Healthcheck, RunnableImage, WaitFor};
use testcontainers::images::generic::GenericImage;
use tokio::select;
use zookeeper_client as zk;

static PERSISTENT_OPEN: &zk::CreateOptions<'static> = &zk::CreateMode::Persistent.with_acls(zk::Acls::anyone_all());
static CONTAINER_OPEN: &zk::CreateOptions<'static> = &zk::CreateMode::Container.with_acls(zk::Acls::anyone_all());

fn random_data() -> Vec<u8> {
    let rng = rand::thread_rng();
    rng.sample_iter(Standard).take(32).collect()
}

fn zookeeper_image_with_version_and_properties<'a>(version: &'a str, mut properties: Vec<&'a str>) -> GenericImage {
    properties.insert(0, "-Dzookeeper.DigestAuthenticationProvider.superDigest=super:D/InIHSb7yEEbrWz8b9l71RjZJU=");
    properties.insert(0, "-Dzookeeper.enableEagerACLCheck=true");
    let jvm_properties = properties.join(" ");
    let healthcheck = Healthcheck::default()
        .with_cmd(["./bin/zkServer.sh", "status"].iter())
        .with_interval(Duration::from_secs(2))
        .with_retries(60);
    GenericImage::new("zookeeper", version)
        .with_env_var("SERVER_JVMFLAGS", jvm_properties)
        .with_healthcheck(healthcheck)
        .with_wait_for(WaitFor::Healthcheck)
}

fn zookeeper_image_with_properties<'a>(properties: Vec<&'a str>) -> GenericImage {
    zookeeper_image_with_version_and_properties("3.9.0", properties)
}

fn zookeeper_image_with_port_and_volumes<'a>(
    port: u16,
    volumes: HashMap<&'a str, &'a Path>,
) -> RunnableImage<GenericImage> {
    let mut image: RunnableImage<_> = zookeeper_image().with_exposed_port(port).into();
    for (dest, source) in volumes {
        image = image.with_volume((source.to_str().unwrap(), dest));
    }
    image
}

fn zookeeper_image() -> GenericImage {
    zookeeper_image_with_properties(Vec::default())
}

fn zookeeper34_image() -> GenericImage {
    zookeeper_image_with_version_and_properties("3.4", Vec::default())
}

async fn example() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);

    let cluster = format!("127.0.0.1:{}", zk_port);

    let path = "/abc";
    let data = "path_data".as_bytes().to_vec();
    let child_path = "/abc/efg";
    let child_data = "child_path_data".as_bytes().to_vec();

    let client = zk::Client::connect(&cluster).await.unwrap();
    let (_, stat_watcher) = client.check_and_watch_stat(path).await.unwrap();

    let (stat, _) = client.create(path, &data, PERSISTENT_OPEN).await.unwrap();
    assert_eq!((data.clone(), stat), client.get_data(path).await.unwrap());

    let event = stat_watcher.changed().await;
    assert_eq!(event.event_type, zk::EventType::NodeCreated);
    assert_eq!(event.path, path);

    let mut multi_reader = client.new_multi_reader();
    multi_reader.add_get_data(path).unwrap();
    multi_reader.add_get_children("/").unwrap();
    let mut results = multi_reader.commit().await.unwrap();
    assert_eq!(results.len(), 2);
    assert_matches!(results.remove(0), zk::MultiReadResult::Data { data: got_data, stat: got_stat } => {
        assert_eq!(got_data, data);
        assert_eq!(got_stat, stat);
    });
    assert_matches!(results.remove(0), zk::MultiReadResult::Children { children } => {
        assert_that!(children).contains("abc".to_string());
    });

    let path_client = client.clone().chroot(path).unwrap();
    assert_eq!((data.clone(), stat), path_client.get_data("/").await.unwrap());

    let (_, _, child_watcher) = client.get_and_watch_children(path).await.unwrap();

    let (child_stat, _) = client.create(child_path, &child_data, PERSISTENT_OPEN).await.unwrap();

    let child_event = child_watcher.changed().await;
    assert_eq!(child_event.event_type, zk::EventType::NodeChildrenChanged);
    assert_eq!(child_event.path, path);

    let relative_child_path = child_path.strip_prefix(path).unwrap();
    assert_eq!((child_data.clone(), child_stat), path_client.get_data(relative_child_path).await.unwrap());

    multi_reader.add_get_children(path).unwrap();
    multi_reader.add_get_data(child_path).unwrap();
    let mut results = multi_reader.commit().await.unwrap();
    assert_eq!(results.len(), 2);
    assert_matches!(results.remove(0), zk::MultiReadResult::Children { children } => {
        assert_eq!(children, vec!["efg"]);
    });
    assert_matches!(results.remove(0), zk::MultiReadResult::Data { data, stat } => {
        assert_eq!(data, child_data);
        assert_eq!(stat, child_stat);
    });

    let (_, _, event_watcher) = client.get_and_watch_data("/").await.unwrap();
    drop(client);
    drop(path_client);

    let session_event = event_watcher.changed().await;
    assert_eq!(session_event.event_type, zk::EventType::Session);
    assert_eq!(session_event.session_state, zk::SessionState::Closed);
}

#[test_log::test(tokio::test)]
async fn test_example() {
    tokio::spawn(async move { example().await }).await.unwrap()
}

async fn connect(cluster: &str, chroot: &str) -> zk::Client {
    let client = zk::Client::connect(cluster).await.unwrap();
    if chroot.len() <= 1 {
        return client;
    }
    let mut i = 1;
    while i <= chroot.len() {
        let j = match chroot[i..].find('/') {
            Some(j) => j + i,
            None => chroot.len(),
        };
        let path = &chroot[..j];
        match client.create(path, Default::default(), PERSISTENT_OPEN).await {
            Ok(_) | Err(zk::Error::NodeExists) => {},
            Err(err) => panic!("{err}"),
        }
        i = j + 1;
    }
    client.chroot(chroot).unwrap()
}

#[test_log::test(tokio::test)]
async fn test_connect_nohosts() {
    assert_that!(zk::Client::connect("127.0.0.1:100,127.0.0.1:101").await.unwrap_err()).is_equal_to(zk::Error::NoHosts);
}

#[test_log::test(tokio::test)]
async fn test_connect_session_expired() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);

    let cluster = format!("127.0.0.1:{}", zk_port);
    let client = zk::Client::builder().detach().connect(&cluster).await.unwrap();
    let timeout = client.session_timeout();
    let (id, password) = client.into_session();

    tokio::time::sleep(timeout * 2).await;

    assert_that!(zk::Client::builder().with_session(id, password).connect(&cluster).await.unwrap_err())
        .is_equal_to(zk::Error::SessionExpired);
}

#[test_case("/"; "no_chroot")]
#[test_case("/x"; "chroot_x")]
#[test_case("/x/y"; "chroot_x_y")]
#[test_log::test(tokio::test)]
async fn test_multi(chroot: &str) {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);

    let cluster = format!("127.0.0.1:{}", zk_port);
    let client = connect(&cluster, chroot).await;

    let mut writer = client.new_multi_writer();
    assert_that!(writer.commit().await.unwrap()).is_empty();

    writer.add_set_data("/a", &random_data(), None).unwrap();
    writer.abort();
    assert_that!(writer.commit().await.unwrap()).is_empty();

    writer.add_create("/a", "/a.0".as_bytes(), PERSISTENT_OPEN).unwrap();
    let mut results = writer.commit().await.unwrap();
    assert_matches!(results.remove(0), zk::MultiWriteResult::Create { path, stat } => {
        assert_eq!(path, "/a");
        assert_that!(stat.is_invalid()).is_false();
    });
    assert_that!(results).is_empty();
    assert_that!(writer.commit().await.unwrap()).is_empty();

    let (data, a_stat) = client.get_data("/a").await.unwrap();
    assert_eq!(data, "/a.0".as_bytes());

    writer.add_check_version("/a", a_stat.version + 1).unwrap();
    writer.add_set_data("/a", &random_data(), None).unwrap();
    writer.add_create("/a", &random_data(), PERSISTENT_OPEN).unwrap();
    assert_eq!(writer.commit().await.unwrap_err(), zk::MultiWriteError::OperationFailed {
        index: 0,
        source: zk::Error::BadVersion
    });

    writer.add_set_data("/a", &random_data(), None).unwrap();
    writer.add_check_version("/a", a_stat.version + 1).unwrap();
    writer.add_create("/a", &random_data(), PERSISTENT_OPEN).unwrap();
    assert_eq!(writer.commit().await.unwrap_err(), zk::MultiWriteError::OperationFailed {
        index: 2,
        source: zk::Error::NodeExists
    });

    writer.add_set_data("/a", "/a.1".as_bytes(), None).unwrap();
    writer.add_check_version("/a", a_stat.version + 1).unwrap();
    writer.add_set_data("/a", "/a.2".as_bytes(), None).unwrap();
    writer.add_check_version("/a", a_stat.version + 2).unwrap();
    writer.add_create("/a/b", "/a/b.0".as_bytes(), PERSISTENT_OPEN).unwrap();
    let mut results = writer.commit().await.unwrap();
    assert_matches!(results.remove(0), zk::MultiWriteResult::SetData { stat } => {
        assert_that!(stat.czxid).is_equal_to(a_stat.czxid);
        assert_that!(stat.mzxid).is_greater_than(a_stat.mzxid);
        assert_that!(stat.version).is_equal_to(a_stat.version + 1);
    });
    assert_matches!(results.remove(0), zk::MultiWriteResult::Check);
    assert_matches!(results.remove(0), zk::MultiWriteResult::SetData { stat } => {
        assert_that!(stat.czxid).is_equal_to(a_stat.czxid);
        assert_that!(stat.mzxid).is_greater_than(a_stat.mzxid);
        assert_that!(stat.version).is_equal_to(a_stat.version + 2);
    });
    assert_matches!(results.remove(0), zk::MultiWriteResult::Check);
    assert_matches!(results.remove(0), zk::MultiWriteResult::Create { path, stat } => {
        assert_eq!(path, "/a/b");
        assert_that!(stat.is_invalid()).is_false();
    });
    assert_that!(results).is_empty();

    let (_, a_stat) = client.get_data("/a").await.unwrap();
    let (_, b_stat) = client.get_data("/a/b").await.unwrap();

    let mut reader = client.new_multi_reader();
    assert_that!(reader.commit().await.unwrap()).is_empty();

    reader.add_get_data("/a").unwrap();
    reader.abort();
    assert_that!(reader.commit().await.unwrap()).is_empty();

    reader.add_get_data("/a").unwrap();
    reader.add_get_data("/a/b").unwrap();
    reader.add_get_data("/a/c").unwrap();
    reader.add_get_children("/a").unwrap();
    reader.add_get_children("/a/b").unwrap();
    reader.add_get_children("/a/c").unwrap();
    let mut results = reader.commit().await.unwrap();
    assert_that!(reader.commit().await.unwrap()).is_empty();
    assert_matches!(results.remove(0), zk::MultiReadResult::Data { data, stat } => {
        assert_eq!(data, "/a.2".as_bytes());
        assert_eq!(stat, a_stat);
    });
    assert_matches!(results.remove(0), zk::MultiReadResult::Data { data, stat } => {
        assert_eq!(data, "/a/b.0".as_bytes());
        assert_eq!(stat, b_stat);
    });
    assert_matches!(results.remove(0), zk::MultiReadResult::Error { err: zk::Error::NoNode });
    assert_matches!(results.remove(0), zk::MultiReadResult::Children { children } => {
        assert_eq!(children, vec!["b".to_string()]);
    });
    assert_matches!(results.remove(0), zk::MultiReadResult::Children { children } => {
        assert_that!(children).is_empty();
    });
    assert_matches!(results.remove(0), zk::MultiReadResult::Error { err: zk::Error::NoNode });
    assert_that!(results).is_empty();
}

#[test_log::test(tokio::test)]
async fn test_multi_async_order() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);

    let cluster = format!("127.0.0.1:{}", zk_port);
    let client = zk::Client::connect(&cluster).await.unwrap();

    client.create("/a", "a0".as_bytes(), PERSISTENT_OPEN).await.unwrap();

    let mut writer = client.new_multi_writer();
    writer.add_set_data("/a", "a1".as_bytes(), None).unwrap();
    let write = writer.commit();

    let mut reader = client.new_multi_reader();
    reader.add_get_data("/a").unwrap();
    let mut results = reader.commit().await.unwrap();
    let zk::MultiReadResult::Data { data, stat } = results.remove(0) else { panic!("expect get data result") };

    let mut write_results = write.await.unwrap();
    let zk::MultiWriteResult::SetData { stat: set_stat } = write_results.remove(0) else {
        panic!("expect set data result")
    };

    assert_that!(data).is_equal_to("a1".as_bytes().to_owned());
    assert_that!(stat).is_equal_to(set_stat);
}

#[test_log::test(tokio::test)]
async fn test_check_writer() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);

    let cluster = format!("127.0.0.1:{}", zk_port);
    let client = zk::Client::connect(&cluster).await.unwrap();

    let mut check_writer = client.new_check_writer("/check", None).unwrap();
    check_writer.add_create("/a", Default::default(), PERSISTENT_OPEN).unwrap();
    assert_that!(check_writer.commit().await.unwrap_err())
        .is_equal_to(zk::CheckWriteError::CheckFailed { source: zk::Error::NoNode });

    let (stat, _sequence) = client.create("/check", Default::default(), PERSISTENT_OPEN).await.unwrap();

    let mut check_writer = client.new_check_writer("/check", Some(stat.version + 1)).unwrap();
    check_writer.add_create("/a", Default::default(), PERSISTENT_OPEN).unwrap();
    assert_that!(check_writer.commit().await.unwrap_err())
        .is_equal_to(zk::CheckWriteError::CheckFailed { source: zk::Error::BadVersion });

    let mut check_writer = client.new_check_writer("/check", Some(stat.version)).unwrap();
    check_writer.add_create("/a", b"a", PERSISTENT_OPEN).unwrap();
    let mut results = check_writer.commit().await.unwrap();
    let created_stat = match results.remove(0) {
        zk::MultiWriteResult::Create { stat, .. } => stat,
        result => panic!("expect create result, got {:?}", result),
    };

    let (data, stat) = client.get_data("/a").await.unwrap();
    assert_that!(created_stat).is_equal_to(stat);
    assert_that!(data).is_equal_to(b"a".to_vec());
}

#[test_case("/x"; "chroot_x")]
#[test_case("/x/y"; "chroot_x_y")]
#[test_log::test(tokio::test)]
async fn test_lock_shared(chroot: &str) {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);
    let cluster = format!("127.0.0.1:{}", zk_port);

    test_lock_with_path(
        &cluster,
        chroot,
        zk::LockPrefix::new_shared("/locks/shared/n-").unwrap(),
        zk::LockPrefix::new_shared("/locks/shared/n-").unwrap(),
    )
    .await;
}

#[test_case("/x"; "chroot_x")]
#[test_case("/x/y"; "chroot_x_y")]
#[test_log::test(tokio::test)]
async fn test_lock_custom(chroot: &str) {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);
    let cluster = format!("127.0.0.1:{}", zk_port);

    let lock1_prefix = zk::LockPrefix::new_custom("/locks/custom/n-abc-".to_string(), "n-").unwrap();
    let lock2_prefix = zk::LockPrefix::new_custom("/locks/custom/n-def-".to_string(), "n-").unwrap();
    test_lock_with_path(&cluster, chroot, lock1_prefix, lock2_prefix).await;
}

#[test_case("/x"; "chroot_x")]
#[test_case("/x/y"; "chroot_x_y")]
#[test_log::test(tokio::test)]
async fn test_lock_curator(chroot: &str) {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);
    let cluster = format!("127.0.0.1:{}", zk_port);

    let lock1_prefix = zk::LockPrefix::new_curator("/locks/curator", "latch-").unwrap();
    let lock2_prefix = zk::LockPrefix::new_curator("/locks/curator", "latch-").unwrap();
    test_lock_with_path(&cluster, chroot, lock1_prefix, lock2_prefix).await;
}

#[test_log::test(tokio::test)]
async fn test_lock_no_node() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);
    let cluster = format!("127.0.0.1:{}", zk_port);

    let client = zk::Client::connect(&cluster).await.unwrap();
    let prefix = zk::LockPrefix::new_curator("/a/locks", "latch-").unwrap();
    assert_eq!(client.lock(prefix, b"", zk::Acls::anyone_all()).await.unwrap_err(), zk::Error::NoNode);
}

#[test_log::test(tokio::test)]
async fn test_lock_curator_filter() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);
    let cluster = format!("127.0.0.1:{}", zk_port);

    let client = zk::Client::connect(&cluster).await.unwrap();
    let options = zk::LockOptions::new(zk::Acls::anyone_all()).with_ancestor_options(CONTAINER_OPEN.clone()).unwrap();

    let latch_prefix = zk::LockPrefix::new_curator("/locks/curator", "latch-").unwrap();
    let _latch = client.lock(latch_prefix, b"", options.clone()).await.unwrap();

    let lock_prefix = zk::LockPrefix::new_curator("/locks/curator", "lock-").unwrap();
    let _lock = client.lock(lock_prefix, b"", options).await.unwrap();
}

#[allow(unused_must_use)] // semi-asynchronous future
async fn test_lock_with_path(
    cluster: &str,
    chroot: &str,
    lock1_prefix: zk::LockPrefix<'static>,
    lock2_prefix: zk::LockPrefix<'static>,
) {
    let client1 = connect(cluster, chroot).await;
    let client2 = connect(cluster, chroot).await;

    let options = zk::LockOptions::new(zk::Acls::anyone_all()).with_ancestor_options(CONTAINER_OPEN.clone()).unwrap();

    let lock1 = client1.lock(lock1_prefix, b"", options.clone()).await.unwrap();
    let contender2 = tokio::spawn(async move {
        let lock2 = client2.lock(lock2_prefix, b"", options).await.unwrap();
        let (data, stat) = lock2.client().get_data("/lock-path").await.unwrap();
        lock2.set_data("/lock-path", lock2.lock_path().as_bytes(), None).await.unwrap();
        lock2.delete("/tmp1", None).await.unwrap();
        (data, stat)
    });

    // Let lock2 get chance to chime in.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let lock1_path = lock1.lock_path().to_string();

    let (stat, _) = lock1.create("/lock-path", lock1_path.as_bytes(), PERSISTENT_OPEN).await.unwrap();

    // Semi-asynchronous data request, no need to await.
    lock1.create("/tmp1", b"", PERSISTENT_OPEN);
    client1.delete(&lock1_path, None).await.unwrap();
    assert_that!(lock1.create("/tmp2", b"", PERSISTENT_OPEN).await.unwrap_err())
        .is_equal_to(zk::Error::RuntimeInconsistent);

    drop(lock1);
    assert_that!(contender2.await.unwrap()).is_equal_to((lock1_path.as_bytes().to_vec(), stat));
    assert_that!(client1.check_stat("/tmp1").await.unwrap()).is_equal_to(None);

    let (data, _stat) = client1.get_data("/lock-path").await.unwrap();
    let lock2_path = String::from_utf8(data).unwrap();

    // Let background delete get chance to chime in.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_that!(client1.check_stat(&lock1_path).await.unwrap()).is_equal_to(None);
    assert_that!(client1.check_stat(&lock2_path).await.unwrap()).is_equal_to(None);
}

#[test_log::test(tokio::test)]
async fn test_no_node() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);

    let cluster = format!("127.0.0.1:{}", zk_port);

    let client = zk::Client::connect(&cluster).await.unwrap();

    assert_eq!(client.check_stat("/nonexistent").await.unwrap(), None);
    assert_eq!(client.get_data("/nonexistent").await.unwrap_err(), zk::Error::NoNode);
    assert_eq!(client.get_and_watch_data("/nonexistent").await.unwrap_err(), zk::Error::NoNode);
    assert_eq!(client.get_children("/nonexistent").await.unwrap_err(), zk::Error::NoNode);
    assert_eq!(client.get_and_watch_children("/nonexistent").await.unwrap_err(), zk::Error::NoNode);
    assert_eq!(client.list_children("/nonexistent").await.unwrap_err(), zk::Error::NoNode);
    assert_eq!(client.list_and_watch_children("/nonexistent").await.unwrap_err(), zk::Error::NoNode);
    assert_eq!(
        client.create("/nonexistent/child", Default::default(), PERSISTENT_OPEN).await.unwrap_err(),
        zk::Error::NoNode
    );
}

#[test_log::test(tokio::test)]
async fn test_request_order() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);

    let cluster = format!("127.0.0.1:{}", zk_port);
    let client = zk::Client::connect(&cluster).await.unwrap();

    let path = "/abc";
    let child_path = "/abc/efg";

    let create = client.create(path, Default::default(), PERSISTENT_OPEN);
    let get_data = client.get_and_watch_children(path);
    let get_child_data = client.get_data(child_path);
    let (child_stat, _) = client.create(child_path, Default::default(), PERSISTENT_OPEN).await.unwrap();
    let (stat, _) = create.await.unwrap();

    assert_that!(child_stat.czxid).is_greater_than(stat.czxid);

    let (children, stat1, watcher) = get_data.await.unwrap();

    assert_that!(children).is_empty();
    assert_that!(stat1).is_equal_to(stat);

    let child_event = watcher.changed().await;
    assert_that!(child_event.event_type).is_equal_to(zk::EventType::NodeChildrenChanged);
    assert_that!(child_event.path).is_equal_to(path.to_owned());

    assert_that!(get_child_data.await).is_equal_to(Err(zk::Error::NoNode));
}

#[test_log::test(tokio::test)]
async fn test_data_node() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);

    let cluster = format!("127.0.0.1:{}", zk_port);
    let client = zk::Client::connect(&cluster).await.unwrap();

    let path = "/abc";
    let data = random_data();
    let (stat, sequence) = client.create(path, &data, PERSISTENT_OPEN).await.unwrap();
    assert_eq!(sequence.into_i64(), -1);

    assert_eq!(stat, client.check_stat(path).await.unwrap().unwrap());
    assert_eq!((data, stat), client.get_data(path).await.unwrap());
    assert_eq!((vec![], stat), client.get_children(path).await.unwrap());
    assert_eq!(Vec::<String>::new(), client.list_children(path).await.unwrap());

    client.delete(path, None).await.unwrap();
    assert_eq!(client.check_stat(path).await.unwrap(), None);
}

#[test_log::test(tokio::test)]
async fn test_create_root() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);
    let cluster = format!("127.0.0.1:{}", zk_port);

    let client = zk::Client::connect(&cluster).await.unwrap().chroot("/a").unwrap();
    assert_that!(client.create("/", &vec![], PERSISTENT_OPEN).await.unwrap_err())
        .is_equal_to(zk::Error::BadArguments(&"can not create root node"));
    assert_that!(client.mkdir("/", PERSISTENT_OPEN).await.unwrap_err())
        .is_equal_to(zk::Error::BadArguments(&"can not create root node"));
}

#[test_log::test(tokio::test)]
async fn test_create_sequential() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);

    let cluster = format!("127.0.0.1:{}", zk_port);
    let client = zk::Client::connect(&cluster).await.unwrap();

    let prefix = "/PREFIX-";
    let data = random_data();
    let (stat1, sequence1) = client
        .create(prefix, &data, &zk::CreateMode::PersistentSequential.with_acls(zk::Acls::anyone_all()))
        .await
        .unwrap();
    let (stat2, sequence2) = client
        .create(prefix, &data, &zk::CreateMode::EphemeralSequential.with_acls(zk::Acls::anyone_all()))
        .await
        .unwrap();

    assert!(sequence2.into_i64() > sequence1.into_i64());

    let path1 = format!("{}{}", prefix, sequence1);
    let path2 = format!("{}{}", prefix, sequence2);
    assert_eq!((data.clone(), stat1), client.get_data(&path1).await.unwrap());
    assert_eq!((data, stat2), client.get_data(&path2).await.unwrap());
}

#[test_log::test(tokio::test)]
async fn test_create_ttl() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image_with_properties(vec![
        "-Dzookeeper.extendedTypesEnabled=true",
        "-Dznode.container.checkIntervalMs=1000",
    ]));
    let zk_port = zookeeper.get_host_port(2181);
    let cluster = format!("127.0.0.1:{}", zk_port);

    let client = zk::Client::connect(&cluster).await.unwrap();

    let ttl_options = PERSISTENT_OPEN.clone().with_ttl(Duration::from_millis(500));
    client.create("/ttl", &vec![], &ttl_options).await.unwrap();
    client.create("/ttl/child", &vec![], PERSISTENT_OPEN).await.unwrap();
    tokio::time::sleep(Duration::from_secs(4)).await;
    client.delete("/ttl/child", None).await.unwrap();
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert_that!(client.delete("/ttl", None).await.unwrap_err()).is_equal_to(zk::Error::NoNode);
}

#[test_log::test(tokio::test)]
async fn test_create_container() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image_with_properties(vec![
        "-Dzookeeper.extendedTypesEnabled=true",
        "-Dznode.container.checkIntervalMs=1000",
    ]));
    let zk_port = zookeeper.get_host_port(2181);

    let cluster = format!("127.0.0.1:{}", zk_port);
    let client = zk::Client::connect(&cluster).await.unwrap();

    client.create("/container", &vec![], &zk::CreateMode::Container.with_acls(zk::Acls::anyone_all())).await.unwrap();
    client.create("/container/child", &vec![], PERSISTENT_OPEN).await.unwrap();
    tokio::time::sleep(Duration::from_secs(4)).await;
    client.delete("/container/child", None).await.unwrap();
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert_that!(client.delete("/container", None).await.unwrap_err()).is_equal_to(zk::Error::NoNode);
}

#[test_log::test(tokio::test)]
async fn test_zookeeper34() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper34_image());
    let zk_port = zookeeper.get_host_port(2181);
    let cluster = format!("127.0.0.1:{}", zk_port);
    let client = zk::Client::builder().assume_server_version(3, 4, u32::MAX).connect(&cluster).await.unwrap();
    let (stat, _sequence) = client.create("/a", b"a1", PERSISTENT_OPEN).await.unwrap();
    assert_that!(stat.is_invalid()).is_true();

    let (data, stat) = client.get_data("/a").await.unwrap();
    assert_eq!(data, b"a1".to_vec());
    assert_eq!(stat.version, 0);

    let (data, stat, watcher) = client.get_and_watch_data("/a").await.unwrap();
    assert_eq!(data, b"a1".to_vec());
    assert_eq!(stat.version, 0);

    let stat = client.set_data("/a", b"a2", Some(0)).await.unwrap();
    assert_eq!(stat.version, 1);

    let event = watcher.changed().await;
    assert_eq!(event.path, "/a");
    assert_eq!(event.zxid, -1);
    assert_eq!(event.event_type, zk::EventType::NodeDataChanged);

    let (data, stat, watcher) = client.get_and_watch_data("/a").await.unwrap();
    assert_eq!(data, b"a2".to_vec());
    assert_eq!(stat.version, 1);

    client.delete("/a", Some(1)).await.unwrap();

    let event = watcher.changed().await;
    assert_eq!(event.path, "/a");
    assert_eq!(event.zxid, -1);
    assert_eq!(event.event_type, zk::EventType::NodeDeleted);
}

#[test_log::test(tokio::test)]
async fn test_mkdir() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);
    let cluster = format!("127.0.0.1:{}", zk_port);

    let client = zk::Client::connect(&cluster).await.unwrap();

    client.mkdir("/a/b/c/d", PERSISTENT_OPEN).await.unwrap();
    let _stat = client.check_stat("/a/b/c/d").await.unwrap().unwrap();

    client.mkdir("/a/./b/c", PERSISTENT_OPEN).await.unwrap_err();
    client.mkdir("/a/b/c", &zk::CreateMode::Ephemeral.with_acls(zk::Acls::anyone_all())).await.unwrap_err();
    client.mkdir("/a/b/c", &zk::CreateMode::PersistentSequential.with_acls(zk::Acls::anyone_all())).await.unwrap_err();

    let _client = client.clone().chroot("/x").unwrap();
    assert_that!(_client.mkdir("/a/b/c", PERSISTENT_OPEN).await.unwrap_err()).is_equal_to(zk::Error::NoNode);
}

#[test_log::test(tokio::test)]
async fn test_descendants_number() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);

    let cluster = format!("127.0.0.1:{}", zk_port);
    let client = zk::Client::connect(&cluster).await.unwrap();

    let path = "/abc";
    let child_path = "/abc/efg";
    let grandchild_path = "/abc/efg/123";

    assert_eq!(client.count_descendants_number(path).await.unwrap_err(), zk::Error::NoNode);
    assert_eq!(client.count_descendants_number(child_path).await.unwrap_err(), zk::Error::NoNode);
    assert_eq!(client.count_descendants_number(grandchild_path).await.unwrap_err(), zk::Error::NoNode);

    let root_childrens = client.count_descendants_number("/").await.unwrap();

    client.create(path, Default::default(), PERSISTENT_OPEN).await.unwrap();
    assert_eq!(client.count_descendants_number("/").await.unwrap(), 1 + root_childrens);
    assert_eq!(client.count_descendants_number(path).await.unwrap(), 0);
    assert_eq!(client.count_descendants_number(child_path).await.unwrap_err(), zk::Error::NoNode);
    assert_eq!(client.count_descendants_number(grandchild_path).await.unwrap_err(), zk::Error::NoNode);

    client.create(child_path, Default::default(), PERSISTENT_OPEN).await.unwrap();
    assert_eq!(client.count_descendants_number("/").await.unwrap(), 2 + root_childrens);
    assert_eq!(client.count_descendants_number(path).await.unwrap(), 1);
    assert_eq!(client.count_descendants_number(child_path).await.unwrap(), 0);
    assert_eq!(client.count_descendants_number(grandchild_path).await.unwrap_err(), zk::Error::NoNode);

    client.create(grandchild_path, Default::default(), PERSISTENT_OPEN).await.unwrap();
    assert_eq!(client.count_descendants_number("/").await.unwrap(), 3 + root_childrens);
    assert_eq!(client.count_descendants_number(path).await.unwrap(), 2);
    assert_eq!(client.count_descendants_number(child_path).await.unwrap(), 1);
    assert_eq!(client.count_descendants_number(grandchild_path).await.unwrap(), 0);
}

trait IntoSorted {
    fn into_sorted(self) -> Self;
}

impl<T> IntoSorted for Vec<T>
where
    T: Ord,
{
    fn into_sorted(mut self) -> Self {
        self.sort();
        self
    }
}

#[test_log::test(tokio::test)]
async fn test_ephemerals() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);

    let cluster = format!("127.0.0.1:{}", zk_port);
    let client = zk::Client::connect(&cluster).await.unwrap();

    let path = "/abc";
    let child_path = "/abc/efg";
    let grandchild_path = "/abc/efg/123";

    // No Error::NoNode for not exist path.
    assert_eq!(Vec::<String>::new(), client.list_ephemerals(path).await.unwrap());

    let acl = zk::Acls::anyone_all();
    let persistent_options = zk::CreateMode::Persistent.with_acls(acl);
    let ephemeral_options = zk::CreateMode::Ephemeral.with_acls(acl);
    let ephemeral_sequential_options = zk::CreateMode::EphemeralSequential.with_acls(acl);

    let mut root_ephemerals = Vec::new();

    client.create(path, Default::default(), &persistent_options).await.unwrap();
    client.create(child_path, Default::default(), &ephemeral_options).await.unwrap();
    root_ephemerals.push(child_path.to_string());

    assert_eq!(
        client.create(grandchild_path, Default::default(), &ephemeral_options).await.unwrap_err(),
        zk::Error::NoChildrenForEphemerals
    );

    for prefix in ["/ephemeral-i", "/abc/ephemeral-i"] {
        let (_, sequence1) = client.create(prefix, Default::default(), &ephemeral_sequential_options).await.unwrap();
        let (_, sequence2) = client.create(prefix, Default::default(), &ephemeral_sequential_options).await.unwrap();
        let (_, sequence3) = client.create(prefix, Default::default(), &ephemeral_sequential_options).await.unwrap();
        root_ephemerals.push(format!("{}{}", prefix, sequence1));
        root_ephemerals.push(format!("{}{}", prefix, sequence2));
        root_ephemerals.push(format!("{}{}", prefix, sequence3));

        assert!(sequence2 > sequence1);
        assert!(sequence3 > sequence2);
    }

    root_ephemerals.sort();
    assert_eq!(root_ephemerals, client.list_ephemerals("/").await.unwrap().into_sorted());

    // List ephemerals from ephemeral node path.
    assert_eq!(vec![child_path], client.list_ephemerals(child_path).await.unwrap().into_sorted());

    // Ephemeral paths are located at client root but not ZooKeeper root.
    let path_ephemerals: Vec<_> =
        root_ephemerals.iter().filter(|p| p.strip_prefix(path).is_some()).map(|p| p.to_string()).collect();

    let path_root_ephemerals: Vec<_> =
        root_ephemerals.iter().filter_map(|p| p.strip_prefix(path)).map(|p| p.to_string()).collect();

    let path_root_client = client.clone().chroot(path).unwrap();

    assert_eq!(path_ephemerals, client.list_ephemerals(path).await.unwrap().into_sorted());
    assert_eq!(path_root_ephemerals, path_root_client.list_ephemerals("/").await.unwrap().into_sorted());

    let child_root_client = client.clone().chroot(child_path).unwrap();
    assert_eq!(vec!["/"], child_root_client.list_ephemerals("/").await.unwrap().into_sorted());
}

#[test_log::test(tokio::test)]
async fn test_chroot() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);

    let cluster = format!("127.0.0.1:{}", zk_port);
    let client = zk::Client::connect(&cluster).await.unwrap();

    assert_eq!(client.path(), "/");
    let client = client.chroot("abc").unwrap_err();
    assert_eq!(client.path(), "/");

    let path = "/abc";
    let data = random_data();
    let child_path = "/abc/efg";
    let child_data = random_data();
    let grandchild_path = "/abc/efg/123";

    let (stat, _) = client.create(path, &data, PERSISTENT_OPEN).await.unwrap();

    let path_client = client.clone().chroot(path).unwrap();
    assert_eq!(path_client.path(), path);
    assert_eq!((data, stat), path_client.get_data("/").await.unwrap());

    let relative_child_path = child_path.strip_prefix(path).unwrap();
    let (child_stat, _) = path_client.create(relative_child_path, &child_data, PERSISTENT_OPEN).await.unwrap();
    assert_eq!((child_data.clone(), child_stat), path_client.get_data(relative_child_path).await.unwrap());
    assert_eq!((child_data.clone(), child_stat), client.get_data(child_path).await.unwrap());

    let child_client = client.clone().chroot(child_path.to_string()).unwrap();
    assert_eq!(child_client.path(), child_path);
    assert_eq!((child_data.clone(), child_stat), child_client.get_data("/").await.unwrap());

    let relative_grandchild_path = grandchild_path.strip_prefix(path).unwrap();
    let (_, grandchild_watcher) = client.check_and_watch_stat(grandchild_path).await.unwrap();
    let (_, relative_grandchild_watcher) = path_client.check_and_watch_stat(relative_grandchild_path).await.unwrap();

    client.create(grandchild_path, Default::default(), PERSISTENT_OPEN).await.unwrap();

    let grandchild_event = grandchild_watcher.changed().await;
    let relative_grandchild_event = relative_grandchild_watcher.changed().await;

    assert_eq!(grandchild_event.event_type, zk::EventType::NodeCreated);
    assert_eq!(relative_grandchild_event.event_type, zk::EventType::NodeCreated);
    assert_eq!(grandchild_event.path, grandchild_path);
    assert_eq!(relative_grandchild_event.path, relative_grandchild_path);
}

#[test_log::test(tokio::test)]
async fn test_auth() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);

    let cluster = format!("127.0.0.1:{}", zk_port);
    let client = zk::Client::connect(&cluster).await.unwrap();

    let scheme = "digest";
    let user = "bob";
    let auth = b"bob:xyz";
    let authed_user = zk::AuthUser::new(scheme, user);

    client.auth(scheme.to_string(), auth.to_vec()).await.unwrap();
    let authed_users = client.list_auth_users().await.unwrap();
    assert!(authed_users.contains(&authed_user));

    let built_client =
        zk::Client::builder().with_auth(scheme.to_string(), auth.to_vec()).connect(&cluster).await.unwrap();

    built_client.auth(scheme.to_string(), auth.to_vec()).await.unwrap();
    let authed_users = client.list_auth_users().await.unwrap();
    assert!(authed_users.contains(&authed_user));
}

#[test_log::test(tokio::test)]
async fn test_no_auth() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);

    let cluster = format!("127.0.0.1:{}", zk_port);
    let client = zk::Client::connect(&cluster).await.unwrap();

    let scheme = "digest";
    let auth = b"bob:xyz";

    client.auth(scheme.to_string(), auth.to_vec()).await.unwrap();
    client
        .create("/acl_test", b"my_data", &zk::CreateMode::Persistent.with_acls(zk::Acls::creator_all()))
        .await
        .unwrap();
    assert_eq!(client.get_data("/acl_test").await.unwrap().0, b"my_data".to_vec());

    let no_auth_client = zk::Client::connect(&cluster).await.unwrap();
    assert_eq!(no_auth_client.get_data("/acl_test").await.unwrap_err(), zk::Error::NoAuth);
    assert_eq!(no_auth_client.set_data("/acl_test", b"set_my_data", None).await.unwrap_err(), zk::Error::NoAuth);

    client
        .create("/acl_test_2", b"my_data", &zk::CreateMode::Persistent.with_acls(zk::Acls::anyone_read()))
        .await
        .unwrap();
    assert_eq!(client.get_data("/acl_test_2").await.unwrap().0, b"my_data".to_vec());
    assert_eq!(client.set_data("/acl_test_2", b"set_my_data", None).await.unwrap_err(), zk::Error::NoAuth);

    assert_eq!(no_auth_client.get_data("/acl_test_2").await.unwrap().0, b"my_data".to_vec());
    assert_eq!(no_auth_client.set_data("/acl_test_2", b"set_my_data", None).await.unwrap_err(), zk::Error::NoAuth);
}

#[test_log::test(tokio::test)]
async fn test_delete() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);

    let cluster = format!("127.0.0.1:{}", zk_port);
    let client = zk::Client::connect(&cluster).await.unwrap();

    let path = "/abc";
    let (stat, _) = client.create(path, Default::default(), PERSISTENT_OPEN).await.unwrap();

    let child_path = "/abc/efg";
    let (child_stat, _) = client.create(child_path, Default::default(), PERSISTENT_OPEN).await.unwrap();

    assert_eq!(client.delete(path, Some(stat.version)).await.unwrap_err(), zk::Error::NotEmpty);
    assert_eq!(client.delete(child_path, Some(child_stat.version + 1)).await.unwrap_err(), zk::Error::BadVersion);
    client.delete(child_path, Some(child_stat.version)).await.unwrap();
    assert_eq!(client.delete(child_path, None).await.unwrap_err(), zk::Error::NoNode);
    client.delete(path, Some(stat.version)).await.unwrap();
}

#[test_log::test(tokio::test)]
async fn test_oneshot_watcher() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);

    let cluster = format!("127.0.0.1:{}", zk_port);
    let client = zk::Client::connect(&cluster).await.unwrap();

    let path = "/abc";
    let child_path = "/abc/efg";

    // Drop or remove last watchers.
    let (_, drop_watcher) = client.check_and_watch_stat(path).await.unwrap();
    drop(drop_watcher);
    let (_, remove_watcher) = client.check_and_watch_stat(child_path).await.unwrap();
    remove_watcher.remove().await.unwrap();

    // Stat watcher for node creation.
    let (stat, stat_watcher) = client.check_and_watch_stat(path).await.unwrap();
    let (_, child_stat_watcher) = client.check_and_watch_stat(child_path).await.unwrap();
    assert_eq!(stat, None);
    let (stat, _) = client.create(path, Default::default(), PERSISTENT_OPEN).await.unwrap();

    let event = stat_watcher.changed().await;
    assert_eq!(event.event_type, zk::EventType::NodeCreated);
    assert_eq!(event.path, path);

    eprintln!("stat watcher done");

    let (_, stat_watcher) = client.check_and_watch_stat(path).await.unwrap();
    let (get_data, get_stat, get_watcher) = client.get_and_watch_data(path).await.unwrap();
    let (_, get_children_stat, get_children_watcher) = client.get_and_watch_children(path).await.unwrap();
    let (_, list_children_watcher) = client.list_and_watch_children(path).await.unwrap();
    let (_, stat_watcher_same) = client.check_and_watch_stat(path).await.unwrap();
    let (_, _, get_watcher_same) = client.get_and_watch_data(path).await.unwrap();

    assert_eq!((vec![], stat), (get_data, get_stat));
    assert_eq!(stat, get_children_stat);

    let (_, stat_receiving_watcher) = client.check_and_watch_stat(path).await.unwrap();
    let (_, _, get_receiving_watcher) = client.get_and_watch_data(path).await.unwrap();
    let (_, stat_received_watcher) = client.check_and_watch_stat(path).await.unwrap();
    let (_, _, get_received_watcher) = client.get_and_watch_data(path).await.unwrap();

    // Drop or remove watchers before event.
    let (_, drop_watcher1) = client.check_and_watch_stat(path).await.unwrap();
    let (_, _, drop_watcher2) = client.get_and_watch_data(path).await.unwrap();
    let (_, _, drop_watcher3) = client.get_and_watch_children(path).await.unwrap();
    let (_, drop_watcher4) = client.list_and_watch_children(path).await.unwrap();

    let (_, remove_watcher1) = client.check_and_watch_stat(path).await.unwrap();
    let (_, _, remove_watcher2) = client.get_and_watch_data(path).await.unwrap();
    let (_, _, remove_watcher3) = client.get_and_watch_children(path).await.unwrap();
    let (_, remove_watcher4) = client.list_and_watch_children(path).await.unwrap();
    drop(drop_watcher1);
    drop(drop_watcher2);
    drop(drop_watcher3);
    drop(drop_watcher4);
    remove_watcher1.remove().await.unwrap();
    remove_watcher2.remove().await.unwrap();
    remove_watcher3.remove().await.unwrap();
    remove_watcher4.remove().await.unwrap();

    // Child creation.
    client.create(child_path, Default::default(), PERSISTENT_OPEN).await.unwrap();

    let child_event = list_children_watcher.changed().await;
    assert_eq!(child_event.event_type, zk::EventType::NodeChildrenChanged);
    assert_eq!(child_event.path, path);
    assert_eq!(child_event, get_children_watcher.changed().await);

    let event = child_stat_watcher.changed().await;
    assert_eq!(event.event_type, zk::EventType::NodeCreated);
    assert_eq!(event.path, child_path);

    eprintln!("child creation done");

    let (_, list_children_watcher) = client.list_and_watch_children(path).await.unwrap();
    let (_, _, get_children_watcher) = client.get_and_watch_children(path).await.unwrap();

    let (_, drop_watcher1) = client.list_and_watch_children(path).await.unwrap();
    let (_, _, drop_watcher2) = client.get_and_watch_children(path).await.unwrap();

    let (_, remove_watcher1) = client.list_and_watch_children(path).await.unwrap();
    let (_, _, remove_watcher2) = client.get_and_watch_children(path).await.unwrap();

    // Child deletion.
    client.delete(child_path, None).await.unwrap();

    let child_event = list_children_watcher.changed().await;
    assert_eq!(child_event.event_type, zk::EventType::NodeChildrenChanged);
    assert_eq!(child_event.path, path);
    assert_eq!(child_event, get_children_watcher.changed().await);

    // Drop or remove watchers after event delivered.
    drop(drop_watcher1);
    drop(drop_watcher2);
    remove_watcher1.remove().await.unwrap();
    remove_watcher2.remove().await.unwrap();

    let (_, list_children_watcher) = client.list_and_watch_children(path).await.unwrap();
    let (_, _, get_children_watcher) = client.get_and_watch_children(path).await.unwrap();

    eprintln!("child deletion done");

    // Node data change.

    // Drop receiving watcher.
    let mut get_receiving_watcher_future = get_receiving_watcher.changed();
    select! {
        biased;
        _ = unsafe { Pin::new_unchecked(&mut get_receiving_watcher_future) } => {},
        _ = future::ready(()) => {},
    }
    drop(get_receiving_watcher_future);
    let mut stat_receiving_watcher_future = stat_receiving_watcher.changed();
    select! {
        biased;
        _ = unsafe { Pin::new_unchecked(&mut stat_receiving_watcher_future) } => {},
        _ = future::ready(()) => {},
    }
    drop(stat_receiving_watcher_future);

    client.set_data(path, Default::default(), None).await.unwrap();

    // Drop probably received watcher.
    let mut get_received_watcher_future = get_received_watcher.changed();
    select! {
        biased;
        _ = unsafe { Pin::new_unchecked(&mut get_received_watcher_future) } => {},
        _ = future::ready(()) => {},
    }
    drop(get_received_watcher_future);

    let node_event = get_watcher.changed().await;
    assert_eq!(node_event.event_type, zk::EventType::NodeDataChanged);
    assert_eq!(node_event.path, path);
    assert_eq!(node_event, stat_watcher.changed().await);
    assert_eq!(node_event, get_watcher_same.changed().await);
    assert_eq!(node_event, stat_watcher_same.changed().await);

    // Drop received watcher.
    let mut stat_received_watcher_future = stat_received_watcher.changed();
    select! {
        biased;
        _ = unsafe { Pin::new_unchecked(&mut stat_received_watcher_future) } => {},
        _ = future::ready(()) => {},
    }
    drop(stat_received_watcher_future);

    eprintln!("node data change done");

    let (_, _, get_watcher) = client.get_and_watch_data(path).await.unwrap();
    let (_, stat_watcher) = client.check_and_watch_stat(path).await.unwrap();

    let (_, _, drop_watcher) = client.get_and_watch_data(path).await.unwrap();
    let (_, remove_watcher) = client.check_and_watch_stat(path).await.unwrap();

    // Node deletion.
    client.delete(path, None).await.unwrap();

    // Drop or remove watchers after event happened.
    drop(drop_watcher);
    remove_watcher.remove().await.unwrap();

    let node_event = get_watcher.changed().await;
    assert_eq!(node_event.event_type, zk::EventType::NodeDeleted);
    assert_eq!(node_event.path, path);
    assert_eq!(node_event, stat_watcher.changed().await);
    assert_eq!(node_event, get_children_watcher.changed().await);
    assert_eq!(node_event, list_children_watcher.changed().await);

    eprintln!("node deletion done");
}

#[test_log::test(tokio::test)]
async fn test_config_watch() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);

    let cluster = format!("127.0.0.1:{}", zk_port);

    let connect = format!("{}/root", cluster);
    let client1 = zk::Client::connect(&connect).await.unwrap();
    let (config_bytes, stat, watcher) = client1.get_and_watch_config().await.unwrap();

    let client2 = zk::Client::connect(&cluster).await.unwrap();
    client2.auth("digest".to_string(), b"super:test".to_vec()).await.unwrap();
    client2.set_data("/zookeeper/config", &config_bytes, Some(stat.version)).await.unwrap();

    let event = watcher.changed().await;
    assert_eq!(event.event_type, zk::EventType::NodeDataChanged);
    assert_eq!(event.path, "/zookeeper/config");
}

#[test_log::test(tokio::test)]
async fn test_persistent_watcher_passive_remove() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);

    let cluster = format!("127.0.0.1:{}", zk_port);

    let client = zk::Client::connect(&cluster).await.unwrap();

    let path = "/abc";
    let child_path = "/abc/efg";
    let grandchild_path = "/abc/efg/123";

    client.create(path, Default::default(), PERSISTENT_OPEN).await.unwrap();
    client.create(child_path, Default::default(), PERSISTENT_OPEN).await.unwrap();
    client.create(grandchild_path, Default::default(), PERSISTENT_OPEN).await.unwrap();
    let (_, root_child_watcher) = client.list_and_watch_children("/").await.unwrap();

    // Two watchers on same path, drop persistent watcher has no effect.
    let (_, children_watcher) = client.list_and_watch_children(child_path).await.unwrap();
    let persistent_watcher = client.watch(child_path, zk::AddWatchMode::Persistent).await.unwrap();
    drop(persistent_watcher);

    client.delete(grandchild_path, None).await.unwrap();
    let child_event = children_watcher.changed().await;
    assert_eq!(child_event.event_type, zk::EventType::NodeChildrenChanged);
    assert_eq!(child_event.path, child_path);

    // Now, no watcher remains, this deletion will detect dangling persistent watcher.
    client.delete(child_path, None).await.unwrap();

    // No other watchers should be affected.
    client.delete(path, None).await.unwrap();
    let child_event = root_child_watcher.changed().await;
    assert_eq!(child_event.event_type, zk::EventType::NodeChildrenChanged);
    assert_eq!(child_event.path, "/");
}

#[test_log::test(tokio::test)]
async fn test_fail_watch_with_multiple_unwatching() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);
    let cluster = format!("127.0.0.1:{}", zk_port);

    let client = zk::Client::connect(&cluster).await.unwrap();

    let (_, exist_watcher1) = client.check_and_watch_stat("/a1").await.unwrap();
    let (_, exist_watcher2) = client.check_and_watch_stat("/a2").await.unwrap();

    let mut state_watcher = client.state_watcher();

    let data_watching1 = client.get_and_watch_data("/a1");
    let data_watching2 = client.get_and_watch_data("/a2");

    drop(exist_watcher1);
    drop(exist_watcher2);

    assert_that!(data_watching1.await.unwrap_err()).is_equal_to(zk::Error::NoNode);
    assert_that!(data_watching2.await.unwrap_err()).is_equal_to(zk::Error::NoNode);

    select! {
        state = state_watcher.changed() => panic!("expect no state update, but got {state}"),
        _ = tokio::time::sleep(Duration::from_millis(10)) => {},
    }
}

#[test_log::test(tokio::test)]
async fn test_fail_watch_with_concurrent_passive_remove() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);
    let cluster = format!("127.0.0.1:{}", zk_port);

    let client = zk::Client::connect(&cluster).await.unwrap();

    let recursive_watcher = client.watch("/a", zk::AddWatchMode::PersistentRecursive).await.unwrap();
    let data_watching = client.get_and_watch_data("/a");
    let persistent_watching = client.watch("/a", zk::AddWatchMode::Persistent);
    drop(recursive_watcher);

    assert_that!(data_watching.await.unwrap_err()).is_equal_to(zk::Error::NoNode);
    let mut persistent_watcher = persistent_watching.await.unwrap();

    client.create("/a", b"a", PERSISTENT_OPEN).await.unwrap();

    let event = persistent_watcher.changed().await;
    assert_that!(event.event_type).is_equal_to(zk::EventType::NodeCreated);
    assert_that!(event.path).is_same_string_to("/a");
}

#[test_log::test(tokio::test)]
async fn test_persistent_watcher() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);

    let cluster = format!("127.0.0.1:{}", zk_port);

    let client = zk::Client::connect(&cluster).await.unwrap();

    let path = "/abc";
    let child_path = "/abc/efg";
    let grandchild_path = "/abc/efg/123";
    let unrelated_path = "/xyz";

    // Remove last watchers.
    let root_recursive_watcher = client.watch("/", zk::AddWatchMode::PersistentRecursive).await.unwrap();
    let path_persistent_watcher = client.watch(path, zk::AddWatchMode::PersistentRecursive).await.unwrap();
    drop(root_recursive_watcher);
    path_persistent_watcher.remove().await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;

    let mut root_recursive_watcher = client.watch("/", zk::AddWatchMode::PersistentRecursive).await.unwrap();
    let mut path_recursive_watcher = client.watch(path, zk::AddWatchMode::PersistentRecursive).await.unwrap();
    let mut path_persistent_watcher = client.watch(path, zk::AddWatchMode::Persistent).await.unwrap();

    let root_recursive_watcher1 = client.watch("/", zk::AddWatchMode::PersistentRecursive).await.unwrap();
    let path_persistent_watcher1 = client.watch(path, zk::AddWatchMode::Persistent).await.unwrap();
    let root_recursive_watcher2 = client.watch("/", zk::AddWatchMode::PersistentRecursive).await.unwrap();
    let path_persistent_watcher2 = client.watch(path, zk::AddWatchMode::Persistent).await.unwrap();
    let mut root_recursive_watcher3 = client.watch("/", zk::AddWatchMode::PersistentRecursive).await.unwrap();
    let path_persistent_watcher3 = client.watch(path, zk::AddWatchMode::Persistent).await.unwrap();

    // Remove shared watch before events should have not effect on existing watchers.
    drop(root_recursive_watcher1);
    path_persistent_watcher1.remove().await.unwrap();

    let mut root_recursive_watcher3_changed = root_recursive_watcher3.changed();
    select! {
        biased;
        _ = unsafe { Pin::new_unchecked(&mut root_recursive_watcher3_changed) } => {},
        _ = future::ready(()) => {},
    }
    let mut path_persistent_watcher3_remove = path_persistent_watcher3.remove();
    select! {
        biased;
        _ = unsafe { Pin::new_unchecked(&mut path_persistent_watcher3_remove) } => {},
        _ = future::ready(()) => {},
    }

    // Node creation.
    client.create(path, Default::default(), PERSISTENT_OPEN).await.unwrap();
    let event = root_recursive_watcher.changed().await;
    assert_eq!(event.event_type, zk::EventType::NodeCreated);
    assert_eq!(event.path, path);
    assert_eq!(event, path_recursive_watcher.changed().await);
    assert_eq!(event, path_persistent_watcher.changed().await);

    // Child node creation.
    client.create(child_path, Default::default(), PERSISTENT_OPEN).await.unwrap();
    let event = root_recursive_watcher.changed().await;
    assert_eq!(event.event_type, zk::EventType::NodeCreated);
    assert_eq!(event.path, child_path);
    assert_eq!(event, path_recursive_watcher.changed().await);

    let child_event = path_persistent_watcher.changed().await;
    assert_eq!(child_event.event_type, zk::EventType::NodeChildrenChanged);
    assert_eq!(child_event.path, path);

    // Remove shared watch after events should have not effect on existing watchers.
    drop(root_recursive_watcher2);
    path_persistent_watcher2.remove().await.unwrap();

    // Grandchild node creation.
    client.create(grandchild_path, Default::default(), PERSISTENT_OPEN).await.unwrap();
    let event = root_recursive_watcher.changed().await;
    assert_eq!(event.event_type, zk::EventType::NodeCreated);
    assert_eq!(event.path, grandchild_path);
    assert_eq!(event, path_recursive_watcher.changed().await);

    // Grandchild node deletion.
    client.delete(grandchild_path, None).await.unwrap();
    let event = root_recursive_watcher.changed().await;
    assert_eq!(event.event_type, zk::EventType::NodeDeleted);
    assert_eq!(event.path, grandchild_path);
    assert_eq!(event, path_recursive_watcher.changed().await);

    // Child node deletion.
    client.delete(child_path, None).await.unwrap();
    let event = root_recursive_watcher.changed().await;
    assert_eq!(event.event_type, zk::EventType::NodeDeleted);
    assert_eq!(event.path, child_path);
    assert_eq!(event, path_recursive_watcher.changed().await);

    let child_event = path_persistent_watcher.changed().await;
    assert_eq!(child_event.event_type, zk::EventType::NodeChildrenChanged);
    assert_eq!(child_event.path, path);

    // Unrelated node creation.
    client.create(unrelated_path, Default::default(), PERSISTENT_OPEN).await.unwrap();
    let event = root_recursive_watcher.changed().await;
    assert_eq!(event.event_type, zk::EventType::NodeCreated);
    assert_eq!(event.path, unrelated_path);

    // Remove shared watch after events should have not effect on existing watchers.
    drop(root_recursive_watcher3_changed);
    drop(path_persistent_watcher3_remove);

    // Node deletion.
    client.delete(path, None).await.unwrap();
    let event = root_recursive_watcher.changed().await;
    assert_eq!(event.event_type, zk::EventType::NodeDeleted);
    assert_eq!(event.path, path);
    assert_eq!(event, path_recursive_watcher.changed().await);
    assert_eq!(event, path_persistent_watcher.changed().await);

    // Node recreation.
    client.create(path, Default::default(), PERSISTENT_OPEN).await.unwrap();
    let event = root_recursive_watcher.changed().await;
    assert_eq!(event.event_type, zk::EventType::NodeCreated);
    assert_eq!(event.path, path);
    assert_eq!(event, path_recursive_watcher.changed().await);
    assert_eq!(event, path_persistent_watcher.changed().await);
}

#[test_log::test(tokio::test)]
async fn test_watcher_coexist_on_same_path() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);
    let cluster = format!("127.0.0.1:{}", zk_port);

    let client = zk::Client::connect(&cluster).await.unwrap();

    let (_, exist_watcher) = client.check_and_watch_stat("/a").await.unwrap();
    let mut persistent_watcher = client.watch("/a", zk::AddWatchMode::Persistent).await.unwrap();
    let mut recursive_watcher = client.watch("/a", zk::AddWatchMode::PersistentRecursive).await.unwrap();

    let (stat, _) = client.create("/a", &vec![], PERSISTENT_OPEN).await.unwrap();

    let expected = zk::WatchedEvent::new(zk::EventType::NodeCreated, "/a".to_string()).with_zxid(stat.czxid);
    assert_that!(exist_watcher.changed().await).is_equal_to(&expected);
    assert_that!(persistent_watcher.changed().await).is_equal_to(&expected);
    assert_that!(recursive_watcher.changed().await).is_equal_to(&expected);

    let (_, _, data_watcher) = client.get_and_watch_data("/a").await.unwrap();
    let (_, _, child_watcher) = client.get_and_watch_children("/a").await.unwrap();
    let (_, exist_watcher) = client.check_and_watch_stat("/a").await.unwrap();

    let (stat, _) = client.create("/a/b", &vec![], PERSISTENT_OPEN).await.unwrap();
    let expected = zk::WatchedEvent::new(zk::EventType::NodeChildrenChanged, "/a".to_string()).with_zxid(stat.czxid);
    assert_that!(child_watcher.changed().await).is_equal_to(&expected);
    assert_that!(persistent_watcher.changed().await).is_equal_to(&expected);

    let expected = zk::WatchedEvent::new(zk::EventType::NodeCreated, "/a/b".to_string()).with_zxid(stat.czxid);
    assert_that!(recursive_watcher.changed().await).is_equal_to(&expected);

    let (_, _, child_watcher) = client.get_and_watch_children("/a").await.unwrap();

    client.delete("/a/b", None).await.unwrap();
    let stat = client.check_stat("/a").await.unwrap().unwrap();

    let expected = zk::WatchedEvent::new(zk::EventType::NodeChildrenChanged, "/a".to_string()).with_zxid(stat.pzxid);
    assert_that!(child_watcher.changed().await).is_equal_to(&expected);
    assert_that!(persistent_watcher.changed().await).is_equal_to(&expected);

    let expected = zk::WatchedEvent::new(zk::EventType::NodeDeleted, "/a/b".to_string()).with_zxid(stat.pzxid);
    assert_that!(recursive_watcher.changed().await).is_equal_to(&expected);

    let (_, _, child_watcher) = client.get_and_watch_children("/a").await.unwrap();

    client.delete("/a", None).await.unwrap();
    let stat = client.check_stat("/").await.unwrap().unwrap();
    let expected = zk::WatchedEvent::new(zk::EventType::NodeDeleted, "/a".to_string()).with_zxid(stat.pzxid);
    assert_that!(child_watcher.changed().await).is_equal_to(&expected);
    assert_that!(data_watcher.changed().await).is_equal_to(&expected);
    assert_that!(exist_watcher.changed().await).is_equal_to(&expected);
    assert_that!(persistent_watcher.changed().await).is_equal_to(&expected);
    assert_that!(recursive_watcher.changed().await).is_equal_to(&expected);

    // persistent ones still exist
    let (stat, _) = client.create("/a", &vec![], PERSISTENT_OPEN).await.unwrap();
    let expected = zk::WatchedEvent::new(zk::EventType::NodeCreated, "/a".to_string()).with_zxid(stat.mzxid);
    assert_that!(persistent_watcher.changed().await).is_equal_to(&expected);
    assert_that!(recursive_watcher.changed().await).is_equal_to(&expected);
}

// Use "current_thread" explicitly.
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_remove_no_watcher() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);
    let cluster = format!("127.0.0.1:{}", zk_port);

    let client = zk::Client::connect(&cluster).await.unwrap();

    let (_, exist_watcher) = client.check_and_watch_stat("/a").await.unwrap();
    let create = client.create("/a", &vec![], PERSISTENT_OPEN);

    // Let session task issue `create` request first, oneshot watch will be removed by server.
    tokio::task::yield_now().await;

    // Issue `RemoveWatches` which likely happen before watch event notification as it involves
    // several IO paths.
    assert_that!(exist_watcher.remove().await.unwrap_err()).is_equal_to(zk::Error::NoWatcher);
    create.await.unwrap();

    let (_, _, data_watcher) = client.get_and_watch_data("/a").await.unwrap();
    let delete = client.delete("/a", None);
    tokio::task::yield_now().await;
    assert_that!(data_watcher.remove().await.unwrap_err()).is_equal_to(zk::Error::NoWatcher);
    delete.await.unwrap();
}

#[test_log::test(tokio::test)]
async fn test_session_event() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);

    let cluster = format!("127.0.0.1:{}", zk_port);

    let client = zk::Client::connect(&cluster).await.unwrap();

    let (_, oneshot_watcher1) = client.check_and_watch_stat("/").await.unwrap();
    let (_, _, oneshot_watcher2) = client.get_and_watch_data("/").await.unwrap();
    let (_, oneshot_watcher3) = client.list_and_watch_children("/").await.unwrap();
    let (_, _, oneshot_watcher4) = client.get_and_watch_children("/").await.unwrap();

    let mut persistent_watcher = client.watch("/", zk::AddWatchMode::PersistentRecursive).await.unwrap();

    zookeeper.stop();

    let event = persistent_watcher.changed().await;
    assert_eq!(event.event_type, zk::EventType::Session);
    assert_eq!(event.session_state, zk::SessionState::Disconnected);

    let event = persistent_watcher.changed().await;
    assert_eq!(event.event_type, zk::EventType::Session);
    assert_eq!(event.session_state, zk::SessionState::Expired);

    assert_eq!(event, oneshot_watcher1.changed().await);
    assert_eq!(event, oneshot_watcher2.changed().await);
    assert_eq!(event, oneshot_watcher3.changed().await);
    assert_eq!(event, oneshot_watcher4.changed().await);

    // All operations will get session expired after observing it.
    assert_eq!(client.get_data("/a/no-exist-path").await.unwrap_err(), zk::Error::SessionExpired);
}

#[test_log::test(tokio::test)]
async fn test_state_watcher() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);

    let cluster = format!("127.0.0.1:{}", zk_port);

    let client = zk::Client::connect(&cluster).await.unwrap();
    let mut state_watcher = client.state_watcher();
    select! {
        biased;
        _ = state_watcher.changed() => panic!("expect no state update"),
        _ = future::ready(()) => {},
    }
    assert_eq!(zk::SessionState::SyncConnected, state_watcher.state());
    drop(client);
    assert_eq!(zk::SessionState::Closed, state_watcher.changed().await);
    select! {
        biased;
        _ = state_watcher.changed() => panic!("expect no state update after terminal state"),
        _ = tokio::time::sleep(Duration::from_millis(10)) => {},
    }
}

#[test_log::test(tokio::test)]
async fn test_client_drop() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);
    let cluster = format!("127.0.0.1:{}", zk_port);

    let client = zk::Client::builder().connect(&cluster).await.unwrap();

    let mut state_watcher = client.state_watcher();
    let (id, password) = client.into_session();
    assert_eq!(zk::SessionState::Closed, state_watcher.changed().await);

    zk::Client::builder().with_session(id, password).connect(&cluster).await.unwrap_err();
}

#[test_log::test(tokio::test)]
async fn test_client_detach() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);
    let cluster = format!("127.0.0.1:{}", zk_port);

    let client = zk::Client::builder().detach().connect(&cluster).await.unwrap();

    let mut state_watcher = client.state_watcher();
    let (id, password) = client.into_session();
    assert_eq!(zk::SessionState::Closed, state_watcher.changed().await);

    zk::Client::builder().with_session(id, password).connect(&cluster).await.unwrap();
}

fn generate_ca_cert() -> (Certificate, String) {
    let mut params = CertificateParams::default();
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    params.distinguished_name.push(rcgen::DnType::CommonName, "ca");
    let ca_cert = Certificate::from_params(params).unwrap();
    let ca_cert_pem = ca_cert.serialize_pem().unwrap();
    let ca_cert_key = rcgen::KeyPair::from_pem(&ca_cert.get_key_pair().serialize_pem()).unwrap();
    let ca_cert_params = CertificateParams::from_ca_cert_pem(&ca_cert_pem, ca_cert_key).unwrap();
    (Certificate::from_params(ca_cert_params).unwrap(), ca_cert_pem)
}

fn generate_server_cert() -> Certificate {
    let mut params = CertificateParams::new(vec!["127.0.0.1".to_string()]);
    params.key_usages = vec![rcgen::KeyUsagePurpose::DigitalSignature, rcgen::KeyUsagePurpose::KeyEncipherment];
    params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
    params.distinguished_name.push(rcgen::DnType::CommonName, "server");
    Certificate::from_params(params).unwrap()
}

fn generate_client_cert() -> Certificate {
    let mut params = CertificateParams::default();
    params.distinguished_name.push(rcgen::DnType::CommonName, "client");
    Certificate::from_params(params).unwrap()
}

#[test_log::test(tokio::test)]
async fn test_tls() {
    let dir = tempdir().unwrap();

    let (ca_cert, ca_cert_pem) = generate_ca_cert();
    let server_cert = generate_server_cert();
    let signed_server_cert = server_cert.serialize_pem_with_signer(&ca_cert).unwrap();

    // ZooKeeper needs a keystore with both key and signed cert.
    let server_pem = server_cert.serialize_private_key_pem() + &signed_server_cert;

    let ca_cert_file = dir.path().join("ca.cert.pem");
    fs::write(&ca_cert_file, &ca_cert_pem).unwrap();

    let server_pem_file = dir.path().join("server.pem");
    fs::write(&server_pem_file, &server_pem).unwrap();

    let config = r"
dataDir=/data
dataLogDir=/datalog
tickTime=2000
initLimit=5
syncLimit=2
autopurge.snapRetainCount=3
autopurge.purgeInterval=0
maxClientCnxns=60

# for healthcheck
clientPort=2181
secureClientPort=2182
ssl.clientAuth=need
ssl.keyStore.location=/certs/server.pem
ssl.trustStore.location=/certs/ca.cert.pem
serverCnxnFactory=org.apache.zookeeper.server.NettyServerCnxnFactory
";
    let config_file = dir.path().join("zoo.cfg");
    fs::write(&config_file, &config).unwrap();

    let docker = DockerCli::default();
    let image = zookeeper_image_with_port_and_volumes(2182, hashmap! {
        "/conf/zoo.cfg" => config_file.as_path(),
        "/certs/server.pem" => server_pem_file.as_path(),
        "/certs/ca.cert.pem" => ca_cert_file.as_path(),
    });
    let zookeeper = docker.run(image);
    let zk_port = zookeeper.get_host_port(2182);

    let client_cert = generate_client_cert();
    let signed_client_cert = client_cert.serialize_pem_with_signer(&ca_cert).unwrap();
    let client_key = client_cert.serialize_private_key_pem();

    let client = zk::Client::builder()
        .assume_tls()
        .trust_ca_pem_certs(&ca_cert_pem)
        .unwrap()
        .use_client_pem_cert(&signed_client_cert, &client_key)
        .unwrap()
        .connect(&format!("tcp+tls://127.0.0.1:{}", zk_port))
        .await
        .unwrap();
    let children = client.list_children("/").await.unwrap();
    assert_that!(children).contains("zookeeper".to_owned());
}

#[allow(dead_code)]
fn zookeeper_quorum_image(server_id: u8, dir: &TempDir, servers: &[&str]) -> RunnableImage<GenericImage> {
    let options = r"dataDir=/data
dataLogDir=/datalog
tickTime=2000
initLimit=5
syncLimit=2
autopurge.snapRetainCount=3
autopurge.purgeInterval=0
maxClientCnxns=60
standaloneEnabled=false
reconfigEnabled=true
admin.enableServer=true";
    let cfg_path = dir.path().join(format!("zoo{server_id}.cfg"));
    let myid_path = dir.path().join(format!("zoo{server_id}.myid"));
    fs::write(&cfg_path, format!("{options}\n{}\n", servers.join("\n"))).unwrap();
    fs::write(&myid_path, format!("{server_id}\n")).unwrap();
    RunnableImage::from(zookeeper_image())
        .with_network("host")
        .with_volume((cfg_path.as_path().to_str().unwrap(), "/conf/zoo.cfg"))
        .with_volume((myid_path.as_path().to_str().unwrap(), "/data/myid"))
}

/// Ideally, we can use user-defiend bridge network or `--link` to connect containers. But
/// testcontainers does not support any of them yet. So fallback to "host" network.
///
/// See:
/// * https://docs.docker.com/network/drivers/bridge/
/// * https://docs.docker.com/network/links/
#[cfg(target_os = "linux")]
#[test_log::test(tokio::test)]
async fn test_update_ensemble() {
    let dir = tempdir().unwrap();
    let docker = DockerCli::default();
    let _zoo1 =
        docker.run(zookeeper_quorum_image(1, &dir, &vec!["server.1=localhost:2001:3001:participant;localhost:4001"]));
    let _zoo2 = docker.run(zookeeper_quorum_image(2, &dir, &vec![
        "server.1=localhost:2001:3001:participant;localhost:4001",
        "server.2=localhost:2002:3002:observer;localhost:4002",
    ]));
    let _zoo3 = docker.run(zookeeper_quorum_image(3, &dir, &vec![
        "server.1=localhost:2001:3001:participant;localhost:4001",
        "server.3=localhost:2003:3003:observer;localhost:4003",
    ]));

    let zoo1_client = zk::Client::connect("localhost:4001").await.unwrap();
    let zoo2_client = zk::Client::connect("localhost:4002").await.unwrap();
    let zoo3_client = zk::Client::connect("localhost:4003").await.unwrap();

    zoo1_client.create("/xx", b"xx", PERSISTENT_OPEN).await.unwrap();

    // Assert all three servers reside in same cluster.
    zoo2_client.sync("/").await.unwrap();
    let (data2, _) = zoo2_client.get_data("/xx").await.unwrap();
    assert_eq!(data2, b"xx");

    zoo3_client.sync("/").await.unwrap();
    let (data3, _) = zoo3_client.get_data("/xx").await.unwrap();
    assert_eq!(data3, b"xx");

    let (config_bytes, config_stat) = zoo1_client.get_config().await.unwrap();
    assert_that!(String::from_utf8_lossy(&config_bytes).into_owned()).contains("server.1");
    assert_that!(String::from_utf8_lossy(&config_bytes).into_owned()).does_not_contain("server.2");
    assert_that!(String::from_utf8_lossy(&config_bytes).into_owned()).does_not_contain("server.3");
    let new_ensemble = zk::EnsembleUpdate::New {
        ensemble: vec![
            "server.1=localhost:2001:3001:participant;localhost:4001",
            "server.2=localhost:2002:3002:participant;localhost:4002",
            "server.3=localhost:2003:3003:participant;localhost:4003",
        ]
        .into_iter(),
    };
    zoo1_client.auth("digest".to_string(), b"super:test".to_vec()).await.unwrap();
    let (new_config_bytes, _) = zoo1_client.update_ensemble(new_ensemble, Some(config_stat.mzxid)).await.unwrap();
    assert_that!(String::from_utf8_lossy(&new_config_bytes).into_owned()).contains("server.1");
    assert_that!(String::from_utf8_lossy(&new_config_bytes).into_owned()).contains("server.2");
    assert_that!(String::from_utf8_lossy(&new_config_bytes).into_owned()).contains("server.3");
}
