use crate::{discovery::DiscoverySignal, survival::parse_pulse_mint_line};
use std::{path::PathBuf, time::Duration};
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncSeekExt, BufReader},
    sync::mpsc,
};
use tracing::{info, warn};

pub fn spawn_pulse_tail(path: PathBuf, poll_interval: Duration) -> mpsc::Receiver<DiscoverySignal> {
    let (tx, rx) = mpsc::channel(1024);
    tokio::spawn(async move {
        let mut offset = 0u64;
        loop {
            match read_appended(&path, offset, &tx).await {
                Ok(next_offset) => offset = next_offset,
                Err(err) => warn!("Pulse tail read failed path={}: {err:#}", path.display()),
            }
            tokio::time::sleep(poll_interval).await;
        }
    });
    rx
}

async fn read_appended(
    path: &PathBuf,
    offset: u64,
    out: &mpsc::Sender<DiscoverySignal>,
) -> anyhow::Result<u64> {
    let mut file = match File::open(path).await {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(err) => return Err(err.into()),
    };
    let len = file.metadata().await?.len();
    let start = offset.min(len);
    file.seek(std::io::SeekFrom::Start(start)).await?;
    let mut reader = BufReader::new(file);
    let mut line = String::new();
    let mut next_offset = start;

    loop {
        line.clear();
        let bytes = reader.read_line(&mut line).await?;
        if bytes == 0 {
            break;
        }
        next_offset = next_offset.saturating_add(bytes as u64);
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        match parse_pulse_mint_line(trimmed) {
            Ok(Some(pulse)) => {
                let signal = DiscoverySignal::from(pulse);
                info!(
                    "received Pulse discovery mint={} source={}",
                    signal.mint, signal.source
                );
                if out.send(signal).await.is_err() {
                    return Ok(next_offset);
                }
            }
            Ok(None) => {}
            Err(err) => warn!("invalid Pulse row ignored: {err:#}"),
        }
    }

    Ok(next_offset)
}
