use futures::channel::{mpsc, oneshot};
use futures::StreamExt;
use hashbrown::HashMap;
use hashlink::LinkedHashSet;
use ignore_result::Ignore;
use tracing::error;

use super::depot::Depot;
use super::event::WatcherEvent;
use super::request::{Operation, SessionOperation, StateReceiver, StateResponser};
use super::types::{EventType, SessionState, WatchMode, WatchedEvent};
use crate::error::Error;
use crate::proto::{ErrorCode, OpCode, SetWatchesRequest};
use crate::util::{Ref, ToRef};

const SET_WATCHES_MAX_BYTES: usize = 128 * 1024;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct WatcherId(u64);

impl WatcherId {
    pub fn new(id: u64) -> Self {
        WatcherId(id)
    }
}

#[derive(Debug)]
pub enum WatchReceiver {
    None,
    Oneshot(OneshotReceiver),
    Persistent(PersistentReceiver),
}

impl WatchReceiver {
    pub fn is_none(&self) -> bool {
        matches!(self, WatchReceiver::None)
    }
}

#[derive(Debug)]
pub struct OneshotReceiver {
    id: WatcherId,
    unwatch: mpsc::UnboundedSender<(WatcherId, StateResponser)>,
    receiver: Option<oneshot::Receiver<WatchedEvent>>,
}

impl OneshotReceiver {
    pub fn new(
        id: WatcherId,
        receiver: oneshot::Receiver<WatchedEvent>,
        unwatch: mpsc::UnboundedSender<(WatcherId, StateResponser)>,
    ) -> Self {
        OneshotReceiver { id, receiver: Some(receiver), unwatch }
    }

    unsafe fn into_unwatch(self) -> mpsc::UnboundedSender<(WatcherId, StateResponser)> {
        let unwatch = std::ptr::read(&self.unwatch);
        std::ptr::read(&self.receiver);
        std::mem::forget(self);
        unwatch
    }

    pub async fn recv(mut self) -> WatchedEvent {
        let receiver = self.receiver.take().unwrap();
        let event = receiver.await.unwrap();
        unsafe { self.into_unwatch() };
        event
    }

    pub async fn remove(self) -> Result<(), Error> {
        let id = self.id;
        let unwatch = unsafe { self.into_unwatch() };
        let (sender, receiver) = oneshot::channel();
        unwatch.unbounded_send((id, StateResponser::new(sender))).ignore();
        let receiver = StateReceiver::new(OpCode::RemoveWatches, receiver);
        receiver.await?;
        Ok(())
    }
}

impl Drop for OneshotReceiver {
    fn drop(&mut self) {
        self.unwatch.unbounded_send((self.id, Default::default())).ignore();
    }
}

#[derive(Debug)]
pub struct PersistentReceiver {
    id: WatcherId,
    unwatch: mpsc::UnboundedSender<(WatcherId, StateResponser)>,
    receiver: mpsc::UnboundedReceiver<WatchedEvent>,
}

impl PersistentReceiver {
    pub fn new(
        id: WatcherId,
        receiver: mpsc::UnboundedReceiver<WatchedEvent>,
        unwatch: mpsc::UnboundedSender<(WatcherId, StateResponser)>,
    ) -> Self {
        PersistentReceiver { id, receiver, unwatch }
    }

    unsafe fn into_unwatch(self) -> mpsc::UnboundedSender<(WatcherId, StateResponser)> {
        let unwatch = std::ptr::read(&self.unwatch);
        std::ptr::read(&self.receiver);
        std::mem::forget(self);
        unwatch
    }

    pub async fn recv(&mut self) -> WatchedEvent {
        self.receiver.next().await.unwrap()
    }

    pub async fn remove(self) -> Result<(), Error> {
        let id = self.id;
        let unwatch = unsafe { self.into_unwatch() };
        let (sender, receiver) = oneshot::channel();
        unwatch.unbounded_send((id, StateResponser::new(sender))).ignore();
        let receiver = StateReceiver::new(OpCode::RemoveWatches, receiver);
        receiver.await?;
        Ok(())
    }
}

impl Drop for PersistentReceiver {
    fn drop(&mut self) {
        self.unwatch.unbounded_send((self.id, Default::default())).ignore();
    }
}

enum WatchSender {
    Oneshot(oneshot::Sender<WatchedEvent>),
    Persistent(mpsc::UnboundedSender<WatchedEvent>),
}

impl WatchSender {
    fn into_oneshot(self) -> oneshot::Sender<WatchedEvent> {
        if let WatchSender::Oneshot(sender) = self {
            sender
        } else {
            unreachable!("not oneshot sender")
        }
    }

    fn get_persistent(&self) -> &mpsc::UnboundedSender<WatchedEvent> {
        if let WatchSender::Persistent(sender) = self {
            sender
        } else {
            unreachable!("not persistent sender")
        }
    }
}

struct Watch {
    watchers: Vec<Watcher>,
}

impl Watch {
    fn new() -> Self {
        Watch { watchers: Vec::with_capacity(5) }
    }

    fn is_empty(&self) -> bool {
        self.watchers.is_empty()
    }

    fn should_cache(&self) -> bool {
        self.watchers.capacity() <= 0x10
    }

    fn iter(&self) -> std::slice::Iter<'_, Watcher> {
        self.watchers.iter()
    }

    fn has_mode(&self, mode: WatchMode) -> bool {
        self.watchers.iter().any(|watch| watch.kind.into_remove_mode() == mode)
    }

    fn remove_watcher(&mut self, id: WatcherId) -> Option<Watcher> {
        if let Some(i) = self.watchers.iter().position(|watch| watch.id == id) {
            Some(self.watchers.swap_remove(i))
        } else {
            None
        }
    }

    fn send(&mut self, event: &WatcherEvent, watching_paths: &mut HashMap<WatcherId, &'static str>) {
        for i in (0..self.watchers.len()).rev() {
            let watcher = &self.watchers[i];
            if !watcher.interest(event) {
                continue;
            }
            match &watcher.sender {
                WatchSender::Oneshot(_) => {
                    let watcher = self.watchers.swap_remove(i);
                    watching_paths.remove(&watcher.id);
                    let sender = watcher.sender.into_oneshot();
                    sender.send(event.to_value()).ignore();
                },
                WatchSender::Persistent(sender) => sender.unbounded_send(event.to_value()).ignore(),
            }
        }
    }

    fn add(&mut self, watcher: Watcher) {
        self.watchers.push(watcher);
    }
}

pub struct WatchManager {
    next_watcher_id: u64,
    watches: HashMap<String, Watch>,
    watching_paths: HashMap<WatcherId, &'static str>,

    // Usually, oneshot watches are repeated on same paths.
    cached_paths: LinkedHashSet<String>,
    cached_watches: Vec<Watch>,

    unwatch_sender: mpsc::UnboundedSender<(WatcherId, StateResponser)>,
}

impl WatchManager {
    pub fn new() -> (Self, mpsc::UnboundedReceiver<(WatcherId, StateResponser)>) {
        let (unwatch_sender, unwatch_receiver) = mpsc::unbounded();
        let manager = WatchManager {
            cached_paths: LinkedHashSet::with_capacity(1000),
            cached_watches: Vec::with_capacity(100),

            next_watcher_id: 1,
            watches: HashMap::with_capacity(20),
            watching_paths: HashMap::with_capacity(20),

            unwatch_sender,
        };
        (manager, unwatch_receiver)
    }

    fn new_watcher_id(&mut self) -> WatcherId {
        let id = self.next_watcher_id;
        self.next_watcher_id = id + 1;
        WatcherId::new(id)
    }

    fn add_watch(&mut self, path: &str, watcher: Watcher) {
        let watcher_id = watcher.id;
        if let Some((path, watch)) = self.watches.get_key_value_mut(path) {
            let watch_path = unsafe { std::mem::transmute::<&str, &'_ str>(path) };
            self.watching_paths.insert(watcher_id, watch_path);
            watch.add(watcher);
            return;
        }
        let path = self.cached_paths.take(path).unwrap_or_else(|| String::from(path));
        let watch_path = unsafe { std::mem::transmute::<&str, &'_ str>(path.as_str()) };
        self.watching_paths.insert(watcher_id, watch_path);
        let mut watch = self.cached_watches.pop().unwrap_or_else(Watch::new);
        watch.add(watcher);
        self.watches.insert(path, watch);
    }

    fn add_oneshot_watch(&mut self, path: &str, kind: WatcherKind) -> OneshotReceiver {
        let id = self.new_watcher_id();
        let (sender, receiver) = oneshot::channel();
        let watcher = Watcher { id, kind, sender: WatchSender::Oneshot(sender) };
        self.add_watch(path, watcher);
        OneshotReceiver::new(id, receiver, self.unwatch_sender.clone())
    }

    fn add_persistent_watch(&mut self, path: &str, kind: WatcherKind) -> PersistentReceiver {
        let id = self.new_watcher_id();
        let (sender, receiver) = mpsc::unbounded();
        let watcher = Watcher { id, kind, sender: WatchSender::Persistent(sender) };
        self.add_watch(path, watcher);
        PersistentReceiver::new(id, receiver, self.unwatch_sender.clone())
    }

    fn add_data_watch(&mut self, path: &str) -> OneshotReceiver {
        self.add_oneshot_watch(path, WatcherKind::Data)
    }

    fn add_exist_watch(&mut self, path: &str) -> OneshotReceiver {
        self.add_oneshot_watch(path, WatcherKind::Exist)
    }

    fn add_child_watch(&mut self, path: &str) -> OneshotReceiver {
        self.add_oneshot_watch(path, WatcherKind::Child)
    }

    pub fn create_watcher(
        &mut self,
        path: &str,
        watch_mode: WatchMode,
        op_code: OpCode,
        rc: ErrorCode,
    ) -> WatchReceiver {
        if rc != ErrorCode::Ok {
            if rc == ErrorCode::NoNode && op_code == OpCode::Exists {
                return WatchReceiver::Oneshot(self.add_exist_watch(path));
            }
            return WatchReceiver::None;
        }
        if op_code == OpCode::GetData || op_code == OpCode::Exists {
            return WatchReceiver::Oneshot(self.add_data_watch(path));
        } else if op_code == OpCode::GetChildren || op_code == OpCode::GetChildren2 {
            return WatchReceiver::Oneshot(self.add_child_watch(path));
        }
        if watch_mode == WatchMode::PersistentNode {
            return WatchReceiver::Persistent(self.add_persistent_watch(path, WatcherKind::PersistentNode));
        }
        assert!(watch_mode == WatchMode::PersistentRecursive);
        WatchReceiver::Persistent(self.add_persistent_watch(path, WatcherKind::PersistentRecursive))
    }

    fn remove_watches(&mut self, path: &str) {
        let (path, watch) = self.watches.remove_entry(path).unwrap();
        if self.cached_paths.len() >= self.cached_paths.capacity() {
            self.cached_paths.pop_front();
        }
        self.cached_paths.insert(path);
        if watch.should_cache() {
            self.cached_watches.push(watch);
        }
    }

    pub fn dispatch_session_state(&mut self, state: SessionState) {
        let _event = WatchedEvent::new_session(state);
        let event = _event.to_ref();
        self.watches.values_mut().for_each(|watch| {
            watch.send(&event, &mut self.watching_paths);
        });
        if event.session_state.is_terminated() {
            self.watches.clear();
        }
    }

    pub fn dispatch_server_event(&mut self, event: WatcherEvent, depot: &mut Depot) {
        use EventType::*;
        match event.event_type {
            NodeCreated | NodeDeleted | NodeDataChanged | NodeChildrenChanged => self.dispatch_path_event(event, depot),
            _ => error!("unexpected server watch event {:?}", event),
        }
    }

    fn dispatch_path_event(&mut self, event: WatcherEvent, depot: &mut Depot) {
        let mut path = event.path;
        let mut has_watch = false;
        if let Some(watch) = self.watches.get_mut(path) {
            watch.send(&event, &mut self.watching_paths);
            if watch.is_empty() {
                self.remove_watches(path);
            }
            has_watch = true;
        }
        let is_children_event = event.event_type == EventType::NodeChildrenChanged;
        while !is_children_event && path.len() > 1 {
            let i = path.rfind('/').unwrap_or(0).max(1);
            path = unsafe { path.get_unchecked(..i) };
            if let Some(watch) = self.watches.get_mut(path) {
                for watcher in watch.iter().filter(|watcher| watcher.kind == WatcherKind::PersistentRecursive) {
                    watcher.sender.get_persistent().unbounded_send(event.to_value()).ignore();
                    has_watch = true;
                }
            }
        }
        if !has_watch {
            // Probably a dangling persistent watcher.
            depot.push_remove_watch(event.path, WatchMode::Any, StateResponser::none());
        }
    }

    fn try_remove_watcher(&mut self, watcher_id: WatcherId, depot: &Depot) -> Option<(&str, WatchMode)> {
        let path = self.watching_paths.remove(&watcher_id)?;
        let watch = self.watches.get_mut(path)?;
        let watcher = watch.remove_watcher(watcher_id)?;
        let mut mode = watcher.kind.into_remove_mode();
        if watch.is_empty() {
            self.remove_watches(path);
            if mode != WatchMode::Any && !depot.has_watching_requests(path) {
                mode = WatchMode::Any;
            }
        } else if mode == WatchMode::Any || watch.has_mode(mode) {
            return None;
        }
        Some((path, mode))
    }

    pub fn remove_watcher(&mut self, watcher_id: WatcherId, responser: StateResponser, depot: &mut Depot) {
        if let Some((path, mode)) = self.try_remove_watcher(watcher_id, depot) {
            depot.push_remove_watch(path, mode, responser);
        } else {
            responser.send_empty();
        }
    }

    fn send_and_clear_watches(&self, last_zxid: i64, paths: &mut [Vec<&str>; 5], i: usize, depot: &mut Depot) {
        let (n, op_code) = if i <= 2 { (3, OpCode::SetWatches) } else { (5, OpCode::SetWatches2) };
        let request = SetWatchesRequest { relative_zxid: last_zxid, paths: &paths[..n] };
        let operation = SessionOperation::new(op_code, &request);
        depot.push_operation(Operation::Session(operation));
        paths[..=i].iter_mut().for_each(|v| v.clear());
    }

    pub fn resend_watches(&self, last_zxid: i64, depot: &mut Depot) {
        if self.watches.is_empty() {
            return;
        }
        let mut paths = [vec![], vec![], vec![], vec![], vec![]];
        let mut index = 0;
        let mut bytes = 0;
        for (path, watch) in self.watches.iter() {
            let mut contains = [false; 5];
            for watcher in watch.iter() {
                let i = watcher.kind.set_watches_index();
                if !contains[i] {
                    contains[i] = true;
                    paths[i].push(path.as_str());
                    index = index.max(index);
                    bytes += path.len();
                    if bytes > SET_WATCHES_MAX_BYTES {
                        self.send_and_clear_watches(last_zxid, &mut paths, index, depot);
                        index = 0;
                        bytes = 0;
                    }
                }
            }
        }
        if bytes != 0 {
            self.send_and_clear_watches(last_zxid, &mut paths, index, depot);
        }
    }
}

struct Watcher {
    id: WatcherId,
    kind: WatcherKind,
    sender: WatchSender,
}

impl Watcher {
    fn interest(&self, event: &WatcherEvent) -> bool {
        use EventType::*;
        match event.event_type {
            NodeCreated | NodeDataChanged => self.kind != WatcherKind::Child,
            NodeChildrenChanged => self.kind == WatcherKind::Child || self.kind == WatcherKind::PersistentNode,
            NodeDeleted => true,
            Session => event.session_state.is_terminated() || self.kind.is_persistent(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum WatcherKind {
    Data,
    Exist,
    Child,
    PersistentNode,
    PersistentRecursive,
}

impl From<WatcherKind> for WatchMode {
    fn from(kind: WatcherKind) -> Self {
        match kind {
            WatcherKind::Data | WatcherKind::Exist => WatchMode::Data,
            WatcherKind::Child => WatchMode::Child,
            WatcherKind::PersistentNode => WatchMode::PersistentNode,
            WatcherKind::PersistentRecursive => WatchMode::PersistentRecursive,
        }
    }
}

impl WatcherKind {
    fn is_persistent(self) -> bool {
        use WatcherKind::*;
        matches!(self, PersistentNode | PersistentRecursive)
    }

    fn set_watches_index(self) -> usize {
        use WatcherKind::*;
        match self {
            Data => 0,
            Exist => 1,
            Child => 2,
            PersistentNode => 3,
            PersistentRecursive => 4,
        }
    }

    fn into_remove_mode(self) -> WatchMode {
        use WatcherKind::*;
        match self {
            Data | Exist => WatchMode::Data,
            Child => WatchMode::Child,
            PersistentNode | PersistentRecursive => WatchMode::Any,
        }
    }
}
