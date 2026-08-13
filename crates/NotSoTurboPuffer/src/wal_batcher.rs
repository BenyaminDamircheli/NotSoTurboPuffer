use anyhow::{Context, Result, anyhow};
use rkyv::to_bytes;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, RwLock, oneshot};

use crate::config::get_config;
use crate::engine::WalRecord;
use crate::s3client;

#[derive(Clone, Debug)]
pub struct BatchConfig {
    pub max_batch_size: usize,
    pub max_batch_time: Duration,
    pub max_batch_bytes: usize,
}

impl BatchConfig {
    pub async fn from_system_config() -> Result<Self> {
        let config = get_config().await?;
        Ok(Self {
            max_batch_size: config.batching.max_batch_size,
            max_batch_time: config.batching.max_batch_time,
            max_batch_bytes: config.batching.max_batch_bytes,
        })
    }
}

type BatchMap = Arc<RwLock<HashMap<String, Arc<Mutex<WriteBatch>>>>>;

pub struct WriteRequest {
    pub namespace: String,
    pub records: Vec<WalRecord>,
    pub response_tx: oneshot::Sender<Result<String>>,
}

pub struct WriteBatch {
    requests: Vec<WriteRequest>,
    created_at: Instant,
    bytes: usize,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self {
            requests: Vec::new(),
            created_at: Instant::now(),
            bytes: 0,
        }
    }

    fn add_request(&mut self, request: WriteRequest, estimated_size: usize) {
        self.bytes += estimated_size;
        self.requests.push(request);
    }

    fn is_empty(&self) -> bool {
        self.requests.is_empty()
    }

    fn should_flush(&self, config: &BatchConfig) -> bool {
        self.requests.len() >= config.max_batch_size
            || self.created_at.elapsed() >= config.max_batch_time
            || self.bytes >= config.max_batch_bytes
    }

    fn age(&self) -> Duration {
        self.created_at.elapsed()
    }
}

pub struct WalBatcher {
    config: BatchConfig,
    batches: Arc<RwLock<HashMap<String, Arc<Mutex<WriteBatch>>>>>,
    shutdown_tx: Option<tokio::sync::mpsc::Sender<()>>,
}

impl WalBatcher {
    pub fn new(config: BatchConfig) -> Self {
        Self {
            config,
            batches: Arc::new(RwLock::new(HashMap::new())),
            shutdown_tx: None,
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel(1);
        self.shutdown_tx = Some(shutdown_tx);

        let batches = self.batches.clone();
        let config = self.config.clone();

        let check_interval = Duration::from_millis(
            u64::try_from(config.max_batch_time.as_millis() / 10)
                .map_err(|_| anyhow!("Max batch time too large for u64"))?,
        );

        tokio::spawn(async move {
            let mut flush_interval = tokio::time::interval(check_interval);
            loop {
                tokio::select! {
                    _ = flush_interval.tick() => {
                        if let Err(e) = Self::flush_ready_batches(&batches, &config).await {
                            tracing::error!("Error flushing batches: {:?}", e);
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                }
            }
        });
        tracing::info!("WAL batcher started with config: {:?}", self.config);
        Ok(())
    }

    async fn submit_write(&self, namespace: String, records: Vec<WalRecord>) {
        let (response_tx, response_rx) = oneshot::channel();

        let estimated_size = records.len() * 1024;

        let request: WriteRequest = WriteRequest {
            namespace: namespace.clone(),
            records,
            response_tx,
        };

        let batch_mutex = {
            let mut batches = self.batches.write().await;
            batches
                .entry(namespace.clone())
                .or_insert_with(|| Arc::new(Mutex::new(WriteBatch::new())))
                .clone()
        };

        let should_flush = {
            let mut batch = batch_mutex.lock().await;
            batch.add_request(request, estimated_size);
            batch.should_flush(&self.config)
        };

        if should_flush {
            tokio::spawn(async move {
                if let Err(e) = Self::flush_batch(&namespace, batch_mutex).await {
                    tracing::error!("Error flushing immediate batch for {}: {:?}", namespace, e);
                }
            });
        }
    }

    async fn flush_ready_batches(batches: &BatchMap, config: &BatchConfig) -> Result<()> {
        let namespaces_to_flush: Vec<String> = {
            let batches = batches.read().await;
            let mut to_flush = Vec::new();
            for (namespace, batch) in batches.iter() {
                let batch = batch.lock().await;
                if !batch.is_empty() && batch.should_flush(config) {
                    to_flush.push(namespace.clone());
                }
            }
            to_flush
        };

        for namespace in namespaces_to_flush {
            let batch_mutex = {
                let batches = batches.read().await;
                batches.get(&namespace).cloned()
            };

            if let Some(batch_mutex) = batch_mutex {
                tokio::spawn(async move {
                    if let Err(e) = Self::flush_batch(&namespace, batch_mutex).await {
                        tracing::error!("Error flushing batch for {}: {:?}", namespace, e);
                    }
                });
            }
        }

        Ok(())
    }

    async fn flush_batch(namespace: &str, batch_mutex: Arc<Mutex<WriteBatch>>) -> Result<()> {
        let batch = {
            let mut batch_guard = batch_mutex.lock().await;
            if batch_guard.is_empty() {
                return Ok(());
            }
            std::mem::replace(&mut *batch_guard, WriteBatch::new())
        };

        let request_count = batch.requests.len();
        let batch_age = batch.age();

        tracing::debug!(
            "Flushing batch for namespace '{}': {} requests, age: {:?}, {} bytes",
            namespace,
            request_count,
            batch_age,
            batch.bytes
        );

        let flush_start = std::time::Instant::now();

        let mut all_records = Vec::new();
        for request in &batch.requests {
            all_records.extend_from_slice(&request.records);
        }

        let bytes = to_bytes::<rkyv::rancor::Error>(&all_records)
            .with_context(|| format!("Failed to serialize batch for namespace: {namespace}"))?;

        let ulid = ulid::Ulid::from_bytes(bytes.as_slice().try_into().unwrap());
        let wal_key = format!("wal/{}", ulid.to_string());

        let s3_result = s3client::put_object_if_not_exists(namespace, &wal_key, &bytes).await;

        let flush_duration = flush_start.elapsed();

        match s3_result {
            Ok(_) => {
                tracing::info!(
                    "Flushing batch for namespace: {} took {:?}",
                    namespace,
                    flush_duration
                );

                for request in batch.requests {
                    let _ = request.response_tx.send(Ok(()));
                }
            }
            Err(e) => {
                tracing::error!(
                    "Failed to flush batch for namespace: {} took {:?}: {:?}",
                    namespace,
                    flush_duration,
                    e
                );
                for request in batch.requests {
                    let _ = request.response_tx.send(Err(anyhow::anyhow!(
                        "Failed to write WAL batch for namespace '{namespace}': {e}"
                    )));
                }
            }
        }
        Ok(())
    }
}
