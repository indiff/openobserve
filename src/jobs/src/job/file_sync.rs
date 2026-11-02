// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::Duration,
};

use axum::body::Bytes;
use config::{DEFAULT_ORG, get_config};
use ingestion_common::{IngestUser, IngestionRequest, SystemJobType};
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{self, File},
    io::{AsyncBufReadExt, AsyncSeekExt, BufReader, SeekFrom},
};

const POLL_INTERVAL: Duration = Duration::from_secs(1);
const MAX_BATCH_RECORDS: usize = 256;
const MAX_BATCH_BYTES: usize = 1024 * 1024;
const CHECKPOINT_FILE: &str = "sync_log_checkpoints.json";

#[derive(Default, Deserialize, Serialize)]
struct Checkpoints {
    files: HashMap<String, FileCheckpoint>,
}

#[derive(Clone, Default, Deserialize, Serialize)]
struct FileCheckpoint {
    position: u64,
    device: Option<u64>,
    inode: Option<u64>,
}

impl FileCheckpoint {
    fn reset_if_changed(&mut self, device: Option<u64>, inode: Option<u64>, len: u64) -> bool {
        let changed = self.position > len
            || (self.device.is_some()
                && self.inode.is_some()
                && (self.device != device || self.inode != inode));
        if changed {
            self.position = 0;
        }
        self.device = device;
        self.inode = inode;
        changed
    }
}

pub async fn run() {
    let checkpoint_path = Path::new(&get_config().common.data_dir).join(CHECKPOINT_FILE);
    let mut checkpoints = match load_checkpoints(&checkpoint_path).await {
        Ok(checkpoints) => checkpoints,
        Err(e) => {
            log::error!("[FILE_SYNC] failed to load checkpoints: {e}");
            Checkpoints::default()
        }
    };

    loop {
        for directory in get_config()
            .common
            .sync_log_dir
            .split(',')
            .map(str::trim)
            .filter(|directory| !directory.is_empty())
        {
            let directory = PathBuf::from(directory);
            if let Err(e) = sync_directory(&directory, &checkpoint_path, &mut checkpoints).await {
                log::error!(
                    "[FILE_SYNC] failed to sync directory {}: {e}",
                    directory.display()
                );
            }
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn load_checkpoints(path: &Path) -> anyhow::Result<Checkpoints> {
    match fs::read(path).await {
        Ok(data) => Ok(serde_json::from_slice(&data)?),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Checkpoints::default()),
        Err(e) => Err(e.into()),
    }
}

async fn save_checkpoints(path: &Path, checkpoints: &Checkpoints) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("checkpoint path has no parent"))?;
    fs::create_dir_all(parent).await?;
    let temporary_path = path.with_extension("tmp");
    fs::write(&temporary_path, serde_json::to_vec(checkpoints)?).await?;
    fs::rename(temporary_path, path).await?;
    Ok(())
}

async fn sync_directory(
    directory: &Path,
    checkpoint_path: &Path,
    checkpoints: &mut Checkpoints,
) -> anyhow::Result<()> {
    let stream_name = directory
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("directory has no stream name"))?
        .to_string_lossy();
    let mut entries = match fs::read_dir(directory).await {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };

    while let Some(entry) = entries.next_entry().await? {
        if !entry.file_type().await?.is_file() {
            continue;
        }
        if let Err(e) = sync_file(&entry.path(), &stream_name, checkpoint_path, checkpoints).await {
            log::error!(
                "[FILE_SYNC] failed to sync file {}: {e}",
                entry.path().display()
            );
        }
    }
    Ok(())
}

async fn sync_file(
    path: &Path,
    stream_name: &str,
    checkpoint_path: &Path,
    checkpoints: &mut Checkpoints,
) -> anyhow::Result<()> {
    let file = File::open(path).await?;
    let metadata = file.metadata().await?;
    let (device, inode) = file_identity(&metadata);
    let key = path.to_string_lossy().into_owned();
    let checkpoint_changed = {
        let checkpoint = checkpoints.files.entry(key.clone()).or_default();
        checkpoint.reset_if_changed(device, inode, metadata.len())
    };
    if checkpoint_changed {
        save_checkpoints(checkpoint_path, checkpoints).await?;
    }

    let mut reader = BufReader::new(file);
    let position = checkpoints.files[&key].position;
    reader.seek(SeekFrom::Start(position)).await?;
    let mut batch = Vec::new();
    let mut batch_bytes = 0;
    let mut batch_end = position;
    let mut line = Vec::new();

    loop {
        line.clear();
        let bytes_read = reader.read_until(b'\n', &mut line).await?;
        if bytes_read == 0 {
            break;
        }
        let Some(record) = record_from_line(&line) else {
            break;
        };
        batch_end += bytes_read as u64;
        batch_bytes += bytes_read;
        batch.push(record);
        if batch.len() >= MAX_BATCH_RECORDS || batch_bytes >= MAX_BATCH_BYTES {
            ingest_batch(stream_name, &batch).await?;
            checkpoints.files.get_mut(&key).unwrap().position = batch_end;
            save_checkpoints(checkpoint_path, checkpoints).await?;
            batch.clear();
            batch_bytes = 0;
        }
    }

    if !batch.is_empty() {
        ingest_batch(stream_name, &batch).await?;
        checkpoints.files.get_mut(&key).unwrap().position = batch_end;
        save_checkpoints(checkpoint_path, checkpoints).await?;
    }
    Ok(())
}

async fn ingest_batch(stream_name: &str, records: &[serde_json::Value]) -> anyhow::Result<()> {
    let response = openobserve_core::logs::ingest::ingest(
        openobserve_core::ingestion::get_thread_id(),
        DEFAULT_ORG,
        stream_name,
        IngestionRequest::JSON(Bytes::from(serde_json::to_vec(records)?)),
        IngestUser::SystemJob(SystemJobType::FileSync),
        None,
        false,
    )
    .await?;
    if response.code != 200
        || response.write_failed
        || response.stream_skipped
        || response
            .status
            .iter()
            .any(|status| status.status.failed > 0)
    {
        anyhow::bail!("ingestion was not fully successful: {response:?}");
    }
    Ok(())
}

fn record_from_line(line: &[u8]) -> Option<serde_json::Value> {
    if !line.ends_with(b"\n") {
        return None;
    }
    let line = line.strip_suffix(b"\n").unwrap_or(line);
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    Some(serde_json::json!({
        "message": String::from_utf8_lossy(line).into_owned()
    }))
}

#[cfg(unix)]
fn file_identity(metadata: &std::fs::Metadata) -> (Option<u64>, Option<u64>) {
    use std::os::unix::fs::MetadataExt;

    (Some(metadata.dev()), Some(metadata.ino()))
}

#[cfg(not(unix))]
fn file_identity(_metadata: &std::fs::Metadata) -> (Option<u64>, Option<u64>) {
    (None, None)
}

#[cfg(test)]
mod tests {
    use super::{FileCheckpoint, record_from_line};

    #[test]
    fn accepts_complete_utf8_and_replaces_invalid_bytes() {
        assert_eq!(
            record_from_line(b"database alert\n").unwrap()["message"],
            "database alert"
        );
        assert_eq!(record_from_line(b"\xff\n").unwrap()["message"], "\u{fffd}");
    }

    #[test]
    fn strips_windows_line_ending_and_preserves_empty_lines() {
        assert_eq!(record_from_line(b"entry\r\n").unwrap()["message"], "entry");
        assert_eq!(record_from_line(b"\n").unwrap()["message"], "");
    }

    #[test]
    fn incomplete_line_is_left_for_the_next_poll() {
        assert!(record_from_line(b"unfinished").is_none());
    }

    #[test]
    fn checkpoint_resets_when_file_is_truncated_or_replaced() {
        let mut checkpoint = FileCheckpoint {
            position: 100,
            device: Some(1),
            inode: Some(2),
        };
        assert!(checkpoint.reset_if_changed(Some(1), Some(3), 200));
        assert_eq!(checkpoint.position, 0);

        checkpoint.position = 100;
        assert!(checkpoint.reset_if_changed(Some(1), Some(3), 50));
        assert_eq!(checkpoint.position, 0);
    }

    #[test]
    fn checkpoint_continues_when_file_identity_is_unchanged() {
        let mut checkpoint = FileCheckpoint {
            position: 100,
            device: Some(1),
            inode: Some(2),
        };
        assert!(!checkpoint.reset_if_changed(Some(1), Some(2), 200));
        assert_eq!(checkpoint.position, 100);
    }
}
