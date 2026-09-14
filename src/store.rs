use crate::{base62_decode, base62_encode, validate_url};
use parking_lot::{Mutex, RwLock};
use rusqlite::Connection;
use std::{
    fs::{File, OpenOptions},
    io,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

/// Single writer, sharded read cache. SQLite commits precede publication and acknowledgment.
/// A process lock prevents two independent caches from serving the same database.
pub struct Store {
    shards: Box<[RwLock<Vec<Arc<str>>>]>,
    writer: Mutex<Option<Connection>>,
    count: AtomicU64,
    capacity: usize,
    _lock: Option<File>,
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}
impl Store {
    pub fn new() -> Self {
        Self::with_shards(64)
    }
    pub fn with_shards(n: usize) -> Self {
        Self::memory(n, 1_000_000)
    }
    pub fn memory(n: usize, capacity: usize) -> Self {
        assert!(n.is_power_of_two() && n <= 4096 && capacity > 0);
        Self {
            shards: (0..n).map(|_| RwLock::new(Vec::new())).collect(),
            writer: Mutex::new(None),
            count: AtomicU64::new(0),
            capacity,
            _lock: None,
        }
    }
    pub fn open(path: &Path, shards: usize, capacity: usize) -> io::Result<Self> {
        let mut store = Self::memory(shards, capacity);
        let mut lock_path = path.as_os_str().to_owned();
        lock_path.push(".lock");
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)?;
        lock.try_lock().map_err(io::Error::other)?;
        let db = Connection::open(path).map_err(io::Error::other)?;
        db.busy_timeout(std::time::Duration::from_secs(2))
            .map_err(io::Error::other)?;
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA fullfsync=ON; CREATE TABLE IF NOT EXISTS urls (id INTEGER PRIMARY KEY, url TEXT NOT NULL);").map_err(io::Error::other)?;
        {
            let mut stmt = db
                .prepare("SELECT id, url FROM urls ORDER BY id")
                .map_err(io::Error::other)?;
            let rows = stmt
                .query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))
                .map_err(io::Error::other)?;
            for row in rows {
                let (id, url) = row.map_err(io::Error::other)?;
                let id = u64::try_from(id).map_err(io::Error::other)?;
                if id != store.count.load(Ordering::Relaxed) || id >= capacity as u64 {
                    return Err(io::Error::other(
                        "database IDs are not contiguous or exceed --max-urls",
                    ));
                }
                validate_url(&url).map_err(io::Error::other)?;
                store.publish(id, &url);
            }
        }
        store.writer = Mutex::new(Some(db));
        store._lock = Some(lock);
        Ok(store)
    }
    fn publish(&self, id: u64, url: &str) {
        self.shards[id as usize & (self.shards.len() - 1)]
            .write()
            .push(Arc::from(url));
        self.count.store(id + 1, Ordering::Release);
    }
    /// Blocking in durable mode: call from spawn_blocking, never a Tokio worker.
    pub fn shorten(&self, url: &str) -> io::Result<String> {
        validate_url(url).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let db = self.writer.lock();
        let id = self.count.load(Ordering::Relaxed);
        if id >= self.capacity as u64 {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "URL capacity reached",
            ));
        }
        if let Some(db) = db.as_ref() {
            db.execute("INSERT INTO urls (id,url) VALUES (?1,?2)", (id as i64, url))
                .map_err(io::Error::other)?;
        }
        self.publish(id, url);
        Ok(base62_encode(id))
    }
    pub fn resolve(&self, code: &str) -> Option<Arc<str>> {
        if code.len() > 11 || (code.len() > 1 && code.starts_with('0')) {
            return None;
        }
        let id = usize::try_from(base62_decode(code)?).ok()?;
        self.shards[id & (self.shards.len() - 1)]
            .read()
            .get(id / self.shards.len())
            .cloned()
    }
    pub fn len(&self) -> usize {
        self.count.load(Ordering::Acquire) as usize
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn capacity(&self) -> usize {
        self.capacity
    }
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }
}
