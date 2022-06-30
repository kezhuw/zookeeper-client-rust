use std::time::Duration;

use futures::future;
use pretty_assertions::assert_eq;
use rand::distributions::Standard;
use rand::{self, Rng};
use testcontainers::clients::Cli as DockerCli;
use testcontainers::core::{Healthcheck, WaitFor};
use testcontainers::images::generic::GenericImage;
use tokio::select;
use zookeeper_client as zk;

fn random_data() -> Vec<u8> {
    let rng = rand::thread_rng();
    rng.sample_iter(Standard).take(32).collect()
}

fn zookeeper_image() -> GenericImage {
    let healthcheck = Healthcheck::default()
        .with_cmd(["./bin/zkServer.sh", "status"].iter())
        .with_interval(Duration::from_secs(6))
        .with_retries(30);
    GenericImage::new("zookeeper", "3.7.0")
        .with_healthcheck(healthcheck)
        .with_env_var(
            "SERVER_JVMFLAGS",
            "-Dzookeeper.DigestAuthenticationProvider.superDigest=super:D/InIHSb7yEEbrWz8b9l71RjZJU=",
        )
        .with_wait_for(WaitFor::Healthcheck)
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
    let create_options = zk::CreateOptions::new(zk::CreateMode::Persistent, zk::Acl::anyone_all());

    let client = zk::Client::connect(&cluster, Duration::from_secs(20)).await.unwrap();
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
}

#[tokio::test]
async fn test_example() {
    tokio::spawn(async move { example().await }).await.unwrap()
}

#[tokio::test]
async fn test_no_node() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);

    let cluster = format!("127.0.0.1:{}", zk_port);

    let client = zk::Client::connect(&cluster, Duration::from_secs(20)).await.unwrap();

    let create_options = zk::CreateOptions::new(zk::CreateMode::Persistent, zk::Acl::anyone_all());
    assert_eq!(client.check_stat("/nonexistent").await.unwrap(), None);
    assert_eq!(client.get_data("/nonexistent").await.unwrap_err(), zk::Error::NoNode);
    assert_eq!(client.get_and_watch_data("/nonexistent").await.unwrap_err(), zk::Error::NoNode);
    assert_eq!(client.get_children("/nonexistent").await.unwrap_err(), zk::Error::NoNode);
    assert_eq!(client.get_and_watch_children("/nonexistent").await.unwrap_err(), zk::Error::NoNode);
    assert_eq!(client.list_children("/nonexistent").await.unwrap_err(), zk::Error::NoNode);
    assert_eq!(client.list_and_watch_children("/nonexistent").await.unwrap_err(), zk::Error::NoNode);
    assert_eq!(
        client.create("/nonexistent/child", Default::default(), &create_options).await.unwrap_err(),
        zk::Error::NoNode
    );
}

#[tokio::test]
async fn test_data_node() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);

    let cluster = format!("127.0.0.1:{}", zk_port);
    let client = zk::Client::connect(&cluster, Duration::from_secs(20)).await.unwrap();

    let path = "/abc";
    let create_options = zk::CreateOptions::new(zk::CreateMode::Persistent, zk::Acl::anyone_all());
    let data = random_data();
    let (stat, sequence) = client.create(path, &data, &create_options).await.unwrap();
    assert_eq!(sequence.0, -1);

    assert_eq!(stat, client.check_stat(path).await.unwrap().unwrap());
    assert_eq!((data, stat), client.get_data(path).await.unwrap());
    assert_eq!((vec![], stat), client.get_children(path).await.unwrap());
    assert_eq!(Vec::<String>::new(), client.list_children(path).await.unwrap());

    client.delete(path, None).await.unwrap();
    assert_eq!(client.check_stat(path).await.unwrap(), None);
}

#[tokio::test]
async fn test_create_sequential() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);

    let cluster = format!("127.0.0.1:{}", zk_port);
    let client = zk::Client::connect(&cluster, Duration::from_secs(20)).await.unwrap();

    let prefix = "/PREFIX-";
    let data = random_data();
    let create_options = zk::CreateOptions::new(zk::CreateMode::PersistentSequential, zk::Acl::anyone_all());
    let (stat1, sequence1) = client.create(prefix, &data, &create_options).await.unwrap();
    let (stat2, sequence2) = client.create(prefix, &data, &create_options).await.unwrap();

    assert!(sequence2.0 > sequence1.0);

    let path1 = format!("{}{}", prefix, sequence1);
    let path2 = format!("{}{}", prefix, sequence2);
    assert_eq!((data.clone(), stat1), client.get_data(&path1).await.unwrap());
    assert_eq!((data, stat2), client.get_data(&path2).await.unwrap());
}

#[tokio::test]
async fn test_descendants_number() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);

    let cluster = format!("127.0.0.1:{}", zk_port);
    let client = zk::Client::connect(&cluster, Duration::from_secs(20)).await.unwrap();

    let path = "/abc";
    let child_path = "/abc/efg";
    let grandchild_path = "/abc/efg/123";
    let create_options = zk::CreateOptions::new(zk::CreateMode::Persistent, zk::Acl::anyone_all());

    assert_eq!(client.count_descendants_number(path).await.unwrap_err(), zk::Error::NoNode);
    assert_eq!(client.count_descendants_number(child_path).await.unwrap_err(), zk::Error::NoNode);
    assert_eq!(client.count_descendants_number(grandchild_path).await.unwrap_err(), zk::Error::NoNode);

    let root_childrens = client.count_descendants_number("/").await.unwrap();

    client.create(path, Default::default(), &create_options).await.unwrap();
    assert_eq!(client.count_descendants_number("/").await.unwrap(), 1 + root_childrens);
    assert_eq!(client.count_descendants_number(path).await.unwrap(), 0);
    assert_eq!(client.count_descendants_number(child_path).await.unwrap_err(), zk::Error::NoNode);
    assert_eq!(client.count_descendants_number(grandchild_path).await.unwrap_err(), zk::Error::NoNode);

    client.create(child_path, Default::default(), &create_options).await.unwrap();
    assert_eq!(client.count_descendants_number("/").await.unwrap(), 2 + root_childrens);
    assert_eq!(client.count_descendants_number(path).await.unwrap(), 1);
    assert_eq!(client.count_descendants_number(child_path).await.unwrap(), 0);
    assert_eq!(client.count_descendants_number(grandchild_path).await.unwrap_err(), zk::Error::NoNode);

    client.create(grandchild_path, Default::default(), &create_options).await.unwrap();
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

#[tokio::test]
async fn test_ephemerals() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);

    let cluster = format!("127.0.0.1:{}", zk_port);
    let client = zk::Client::connect(&cluster, Duration::from_secs(20)).await.unwrap();

    let path = "/abc";
    let child_path = "/abc/efg";
    let grandchild_path = "/abc/efg/123";

    // No Error::NoNode for not exist path.
    assert_eq!(Vec::<String>::new(), client.list_ephemerals(path).await.unwrap());

    let acl = zk::Acl::anyone_all();
    let persistent_options = zk::CreateOptions::new(zk::CreateMode::Persistent, acl);
    let ephemeral_options = zk::CreateOptions::new(zk::CreateMode::Ephemeral, acl);
    let ephemeral_sequential_options = zk::CreateOptions::new(zk::CreateMode::EphemeralSequential, acl);

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

#[tokio::test]
async fn test_chroot() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);

    let cluster = format!("127.0.0.1:{}", zk_port);
    let client = zk::Client::connect(&cluster, Duration::from_secs(20)).await.unwrap();

    let path = "/abc";
    let data = random_data();
    let child_path = "/abc/efg";
    let child_data = random_data();
    let grandchild_path = "/abc/efg/123";
    let create_options = zk::CreateOptions::new(zk::CreateMode::Persistent, zk::Acl::anyone_all());

    let (stat, _) = client.create(path, &data, &create_options).await.unwrap();

    let path_client = client.clone().chroot(path).unwrap();
    assert_eq!((data, stat), path_client.get_data("/").await.unwrap());

    let relative_child_path = child_path.strip_prefix(path).unwrap();
    let (child_stat, _) = path_client.create(relative_child_path, &child_data, &create_options).await.unwrap();
    assert_eq!((child_data.clone(), child_stat), path_client.get_data(relative_child_path).await.unwrap());
    assert_eq!((child_data.clone(), child_stat), client.get_data(child_path).await.unwrap());

    let relative_grandchild_path = grandchild_path.strip_prefix(path).unwrap();
    let (_, grandchild_watcher) = client.check_and_watch_stat(grandchild_path).await.unwrap();
    let (_, relative_grandchild_watcher) = path_client.check_and_watch_stat(relative_grandchild_path).await.unwrap();

    client.create(grandchild_path, Default::default(), &create_options).await.unwrap();

    let grandchild_event = grandchild_watcher.changed().await;
    let relative_grandchild_event = relative_grandchild_watcher.changed().await;

    assert_eq!(grandchild_event.event_type, zk::EventType::NodeCreated);
    assert_eq!(relative_grandchild_event.event_type, zk::EventType::NodeCreated);
    assert_eq!(grandchild_event.path, grandchild_path);
    assert_eq!(relative_grandchild_event.path, relative_grandchild_path);
}

#[tokio::test]
async fn test_auth() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);

    let cluster = format!("127.0.0.1:{}", zk_port);
    let client = zk::Client::connect(&cluster, Duration::from_secs(20)).await.unwrap();

    let scheme = "digest";
    let user = "bob";
    let auth = b"bob:xyz";
    let authed_user = zk::AuthUser::new(scheme, user);

    client.auth(scheme.to_string(), auth.to_vec()).await.unwrap();
    let authed_users = client.list_auth_users().await.unwrap();
    assert!(authed_users.contains(&authed_user));

    let built_client = zk::ClientBuilder::new(Duration::from_secs(20))
        .with_auth(scheme.to_string(), auth.to_vec())
        .connect(&cluster)
        .await
        .unwrap();

    built_client.auth(scheme.to_string(), auth.to_vec()).await.unwrap();
    let authed_users = client.list_auth_users().await.unwrap();
    assert!(authed_users.contains(&authed_user));
}

#[tokio::test]
async fn test_delete() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);

    let cluster = format!("127.0.0.1:{}", zk_port);
    let client = zk::Client::connect(&cluster, Duration::from_secs(20)).await.unwrap();

    let path = "/abc";
    let create_options = zk::CreateOptions::new(zk::CreateMode::Persistent, zk::Acl::anyone_all());
    let (stat, _) = client.create(path, Default::default(), &create_options).await.unwrap();

    let child_path = "/abc/efg";
    let (child_stat, _) = client.create(child_path, Default::default(), &create_options).await.unwrap();

    assert_eq!(client.delete(path, Some(stat.version)).await.unwrap_err(), zk::Error::NotEmpty);
    assert_eq!(client.delete(child_path, Some(child_stat.version + 1)).await.unwrap_err(), zk::Error::BadVersion);
    client.delete(child_path, Some(child_stat.version)).await.unwrap();
    assert_eq!(client.delete(child_path, None).await.unwrap_err(), zk::Error::NoNode);
    client.delete(path, Some(stat.version)).await.unwrap();
}

#[tokio::test]
async fn test_oneshot_watcher() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);

    let cluster = format!("127.0.0.1:{}", zk_port);
    let client = zk::Client::connect(&cluster, Duration::from_secs(20)).await.unwrap();

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
    let create_options = zk::CreateOptions::new(zk::CreateMode::Persistent, zk::Acl::anyone_all());
    let (stat, _) = client.create(path, Default::default(), &create_options).await.unwrap();

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
    client.create(child_path, Default::default(), &create_options).await.unwrap();

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

#[tokio::test]
async fn test_config_watch() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);

    let cluster = format!("127.0.0.1:{}", zk_port);

    let connect = format!("{}/root", cluster);
    let client1 = zk::Client::connect(&connect, Duration::from_secs(10)).await.unwrap();
    let (config_bytes, stat, watcher) = client1.get_and_watch_config().await.unwrap();

    let client2 = zk::Client::connect(&cluster, Duration::from_secs(10)).await.unwrap();
    client2.auth("digest".to_string(), b"super:test".to_vec()).await.unwrap();
    client2.set_data("/zookeeper/config", &config_bytes, Some(stat.version)).await.unwrap();

    let event = watcher.changed().await;
    assert_eq!(event.event_type, zk::EventType::NodeDataChanged);
    assert_eq!(event.path, "/zookeeper/config");
}

#[tokio::test]
async fn test_persistent_watcher_passive_remove() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);

    let cluster = format!("127.0.0.1:{}", zk_port);

    let client = zk::Client::connect(&cluster, Duration::from_secs(10)).await.unwrap();
    let create_options = zk::CreateOptions::new(zk::CreateMode::Persistent, zk::Acl::anyone_all());

    let path = "/abc";
    let child_path = "/abc/efg";
    let grandchild_path = "/abc/efg/123";

    client.create(path, Default::default(), &create_options).await.unwrap();
    client.create(child_path, Default::default(), &create_options).await.unwrap();
    client.create(grandchild_path, Default::default(), &create_options).await.unwrap();
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

#[tokio::test]
async fn test_persistent_watcher() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);

    let cluster = format!("127.0.0.1:{}", zk_port);

    let client = zk::Client::connect(&cluster, Duration::from_secs(10)).await.unwrap();
    let create_options = zk::CreateOptions::new(zk::CreateMode::Persistent, zk::Acl::anyone_all());

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
    client.create(path, Default::default(), &create_options).await.unwrap();
    let event = root_recursive_watcher.changed().await;
    assert_eq!(event.event_type, zk::EventType::NodeCreated);
    assert_eq!(event.path, path);
    assert_eq!(event, path_recursive_watcher.changed().await);
    assert_eq!(event, path_persistent_watcher.changed().await);

    // Child node creation.
    client.create(child_path, Default::default(), &create_options).await.unwrap();
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
    client.create(grandchild_path, Default::default(), &create_options).await.unwrap();
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
    client.create(unrelated_path, Default::default(), &create_options).await.unwrap();
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
    client.create(path, Default::default(), &create_options).await.unwrap();
    let event = root_recursive_watcher.changed().await;
    assert_eq!(event.event_type, zk::EventType::NodeCreated);
    assert_eq!(event.path, path);
    assert_eq!(event, path_recursive_watcher.changed().await);
    assert_eq!(event, path_persistent_watcher.changed().await);
}

#[tokio::test]
async fn test_session_event() {
    let docker = DockerCli::default();
    let zookeeper = docker.run(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);

    let cluster = format!("127.0.0.1:{}", zk_port);

    let client = zk::Client::connect(&cluster, Duration::from_secs(10)).await.unwrap();

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
    assert_eq!(event, oneshot_watcher1.changed().await);
    assert_eq!(event, oneshot_watcher2.changed().await);
    assert_eq!(event, oneshot_watcher3.changed().await);
    assert_eq!(event, oneshot_watcher4.changed().await);

    assert_eq!(event.event_type, zk::EventType::Session);
    assert!([zk::SessionState::Expired, zk::SessionState::Closed].contains(&event.session_state));
}
