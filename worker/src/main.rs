use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use base64::Engine;
use clap::Parser;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};
use uuid::Uuid;

const RESPONSE_TOPIC: &str = "inference_responses";
const PRESENCE_TOPIC: &str = "worker_presence";
const SUBSTRATE_MAX_WAIT_SECS: u64 = 20;

#[derive(Parser, Debug)]
#[command(
    name = "inference-router-worker",
    about = "Ollama-backed inference worker for project-inference-router"
)]
struct Args {
    #[arg(long, env = "TENANT_BEARER")]
    bearer: Option<String>,

    #[arg(long, env = "SUBSTRATE_BASE", default_value = "https://mq.connected-cloud.io/v1")]
    substrate_base: String,

    #[arg(long, env = "MODEL", default_value = "llama3.2:1b")]
    model: String,

    #[arg(long, env = "OLLAMA_BASE", default_value = "http://localhost:11434")]
    ollama_base: String,

    #[arg(long, env = "REGION", default_value = "local-dev")]
    region: String,

    #[arg(long, env = "HEARTBEAT_SECS", default_value_t = 10)]
    heartbeat_secs: u64,
}

#[derive(Clone)]
struct Cfg {
    bearer: String,
    base: String,
    model: String,
    normalized_model: String,
    queue: String,
    ollama_base: String,
    region: String,
    worker_id: String,
}

#[derive(Deserialize)]
struct WorkItem {
    corr_id: String,
    #[allow(dead_code)]
    model: String,
    prompt: String,
    #[serde(default)]
    response_queue: Option<String>,
}

#[derive(Deserialize)]
struct ReceiveResponse {
    #[serde(default)]
    count: u32,
    #[serde(default)]
    messages: Vec<ReceivedMessage>,
}

#[derive(Deserialize)]
struct ReceivedMessage {
    ack_token: String,
    body_b64: String,
}

#[derive(Serialize)]
struct Heartbeat<'a> {
    worker_id: &'a str,
    model: &'a str,
    region: &'a str,
    ts: u64,
}

#[derive(Serialize)]
struct FinalResponse<'a> {
    corr_id: &'a str,
    model: &'a str,
    response: &'a str,
    tokens: u32,
}

#[derive(Serialize)]
struct ErrorResponse<'a> {
    corr_id: &'a str,
    model: &'a str,
    error: &'a str,
}

#[derive(Serialize)]
struct OllamaRequest<'a> {
    model: &'a str,
    prompt: &'a str,
    stream: bool,
}

#[derive(Deserialize)]
struct OllamaChunk {
    #[serde(default)]
    response: String,
    #[serde(default)]
    done: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "worker=info,reqwest=warn".into()),
        )
        .init();

    let args = Args::parse();
    let bearer = args
        .bearer
        .map(Ok)
        .unwrap_or_else(load_bearer_from_disk)?
        .trim()
        .to_string();

    let normalized_model = normalize_name(&args.model);
    let queue = format!("inference_req_{}", normalized_model);
    let worker_id = format!("wkr_{}", Uuid::new_v4().simple());

    let cfg = Arc::new(Cfg {
        bearer,
        base: args.substrate_base.trim_end_matches('/').to_string(),
        model: args.model,
        normalized_model,
        queue,
        ollama_base: args.ollama_base.trim_end_matches('/').to_string(),
        region: args.region,
        worker_id,
    });

    info!(
        worker_id = %cfg.worker_id,
        model = %cfg.model,
        queue = %cfg.queue,
        "worker starting"
    );

    ensure_resources(&cfg).await?;

    let hb_cfg = cfg.clone();
    tokio::spawn(async move {
        if let Err(e) = heartbeat_loop(hb_cfg, args.heartbeat_secs).await {
            error!(error = ?e, "heartbeat loop exited");
        }
    });

    consume_loop(cfg).await
}

fn load_bearer_from_disk() -> Result<String> {
    let candidates = [
        std::path::PathBuf::from("../.tenant-bearer"),
        std::path::PathBuf::from(".tenant-bearer"),
    ];
    for path in &candidates {
        if path.exists() {
            return std::fs::read_to_string(path)
                .with_context(|| format!("reading {}", path.display()));
        }
    }
    Err(anyhow!(
        "no bearer provided (--bearer or TENANT_BEARER env), and ../.tenant-bearer not found"
    ))
}

async fn ensure_resources(cfg: &Cfg) -> Result<()> {
    // Idempotent: substrate returns 201 for both first and subsequent create.
    let client = client()?;
    create_resource(&client, cfg, "/queues", &cfg.queue).await?;
    create_resource(&client, cfg, "/topics", RESPONSE_TOPIC).await?;
    create_resource(&client, cfg, "/topics", PRESENCE_TOPIC).await?;
    Ok(())
}

async fn heartbeat_loop(cfg: Arc<Cfg>, interval_secs: u64) -> Result<()> {
    let client = client()?;
    let mut tick = tokio::time::interval(Duration::from_secs(interval_secs));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        if let Err(e) = publish_heartbeat(&client, &cfg).await {
            warn!(error = ?e, "heartbeat publish failed");
        }
    }
}

async fn publish_heartbeat(client: &reqwest::Client, cfg: &Cfg) -> Result<()> {
    let hb = Heartbeat {
        worker_id: &cfg.worker_id,
        model: &cfg.model,
        region: &cfg.region,
        ts: now_secs(),
    };
    let url = format!("{}/topics/{}/publish", cfg.base, PRESENCE_TOPIC);
    let resp = client
        .post(url)
        .bearer_auth(&cfg.bearer)
        .header(
            "X-MQ-Subject",
            format!("presence.{}.{}", cfg.worker_id, cfg.normalized_model),
        )
        .json(&hb)
        .send()
        .await?;
    if !resp.status().is_success() {
        let s = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow!("heartbeat {}: {}", s, body));
    }
    Ok(())
}

async fn consume_loop(cfg: Arc<Cfg>) -> Result<()> {
    let client = client()?;
    loop {
        match receive_messages(&client, &cfg).await {
            Ok(messages) => {
                for msg in messages {
                    let cfg = cfg.clone();
                    let client = client.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_message(&client, &cfg, msg).await {
                            error!(error = ?e, "request handling failed");
                        }
                    });
                }
            }
            Err(e) => {
                warn!(error = ?e, "receive failed, backing off");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
}

async fn receive_messages(
    client: &reqwest::Client,
    cfg: &Cfg,
) -> Result<Vec<ReceivedMessage>> {
    let url = format!(
        "{}/queues/{}/messages?wait={}",
        cfg.base, cfg.queue, SUBSTRATE_MAX_WAIT_SECS
    );
    let resp = client.get(url).bearer_auth(&cfg.bearer).send().await?;
    let status = resp.status();
    let bytes = resp.bytes().await?;
    if !status.is_success() {
        return Err(anyhow!("receive {}: {}", status, String::from_utf8_lossy(&bytes)));
    }
    let parsed: ReceiveResponse =
        serde_json::from_slice(&bytes).context("queue receive shape")?;
    if parsed.count == 0 {
        return Ok(vec![]);
    }
    Ok(parsed.messages)
}

async fn handle_message(
    client: &reqwest::Client,
    cfg: &Cfg,
    msg: ReceivedMessage,
) -> Result<()> {
    let body_bytes = base64::engine::general_purpose::STANDARD
        .decode(msg.body_b64.as_bytes())
        .context("body_b64 decode")?;
    let item: WorkItem = serde_json::from_slice(&body_bytes).context("decoding work item")?;

    info!(corr_id = %item.corr_id, "received request");

    let result = run_inference(client, cfg, &item).await;
    if let Err(ref e) = result {
        error!(corr_id = %item.corr_id, error = ?e, "inference failed");
        let _ = publish_error(client, cfg, &item, &e.to_string()).await;
    } else {
        info!(corr_id = %item.corr_id, "request complete");
    }
    // Ack regardless of inference success — we don't redrive a bad request.
    if let Err(e) = ack(client, cfg, &msg.ack_token).await {
        warn!(error = ?e, "ack failed");
    }
    result
}

async fn run_inference(
    client: &reqwest::Client,
    cfg: &Cfg,
    item: &WorkItem,
) -> Result<()> {
    let url = format!("{}/api/generate", cfg.ollama_base);
    let body = OllamaRequest {
        model: &cfg.model,
        prompt: &item.prompt,
        stream: true,
    };
    let resp = client.post(url).json(&body).send().await?;
    if !resp.status().is_success() {
        let s = resp.status();
        let txt = resp.text().await.unwrap_or_default();
        return Err(anyhow!("ollama {}: {}", s, txt));
    }
    let mut stream = resp.bytes_stream();
    let mut buf: Vec<u8> = Vec::new();
    let mut accumulator = String::new();
    let mut token_count: u32 = 0;
    let mut done = false;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        buf.extend_from_slice(&chunk);
        while let Some(pos) = buf.iter().position(|&b| b == b'\n') {
            let line: Vec<u8> = buf.drain(..=pos).collect();
            let line = &line[..line.len() - 1];
            if line.is_empty() {
                continue;
            }
            let parsed: OllamaChunk = match serde_json::from_slice(line) {
                Ok(v) => v,
                Err(e) => {
                    warn!(error = ?e, "skipping unparsable ollama chunk");
                    continue;
                }
            };
            if !parsed.response.is_empty() {
                token_count += 1;
                accumulator.push_str(&parsed.response);
                publish_token(client, cfg, &item.corr_id, &parsed.response).await?;
            }
            if parsed.done {
                done = true;
            }
        }
        if done {
            break;
        }
    }

    publish_done(client, cfg, &item.corr_id, token_count).await?;
    if let Some(rq) = &item.response_queue {
        let final_msg = FinalResponse {
            corr_id: &item.corr_id,
            model: &cfg.model,
            response: &accumulator,
            tokens: token_count,
        };
        send_to_queue(client, cfg, rq, &serde_json::to_vec(&final_msg)?).await?;
    }
    Ok(())
}

async fn publish_token(
    client: &reqwest::Client,
    cfg: &Cfg,
    corr_id: &str,
    token: &str,
) -> Result<()> {
    publish_to_responses(
        client,
        cfg,
        corr_id,
        "token",
        &serde_json::json!({ "token": token }),
    )
    .await
}

async fn publish_done(
    client: &reqwest::Client,
    cfg: &Cfg,
    corr_id: &str,
    tokens: u32,
) -> Result<()> {
    publish_to_responses(
        client,
        cfg,
        corr_id,
        "done",
        &serde_json::json!({ "tokens": tokens }),
    )
    .await
}

async fn publish_error(
    client: &reqwest::Client,
    cfg: &Cfg,
    item: &WorkItem,
    err: &str,
) -> Result<()> {
    publish_to_responses(
        client,
        cfg,
        &item.corr_id,
        "error",
        &serde_json::json!({ "error": err }),
    )
    .await?;
    if let Some(rq) = &item.response_queue {
        let payload = ErrorResponse {
            corr_id: &item.corr_id,
            model: &cfg.model,
            error: err,
        };
        send_to_queue(client, cfg, rq, &serde_json::to_vec(&payload)?).await?;
    }
    Ok(())
}

async fn publish_to_responses(
    client: &reqwest::Client,
    cfg: &Cfg,
    corr_id: &str,
    leaf: &str,
    body: &serde_json::Value,
) -> Result<()> {
    let url = format!("{}/topics/{}/publish", cfg.base, RESPONSE_TOPIC);
    let subject = format!("inference.{}.{}", corr_id, leaf);
    let resp = client
        .post(url)
        .bearer_auth(&cfg.bearer)
        .header("X-MQ-Subject", subject)
        .json(body)
        .send()
        .await?;
    if !resp.status().is_success() {
        let s = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow!("publish {}: {}", s, body));
    }
    Ok(())
}

async fn send_to_queue(
    client: &reqwest::Client,
    cfg: &Cfg,
    queue: &str,
    body: &[u8],
) -> Result<()> {
    let url = format!("{}/queues/{}/messages", cfg.base, queue);
    let resp = client
        .post(url)
        .bearer_auth(&cfg.bearer)
        .header("content-type", "application/octet-stream")
        .body(body.to_vec())
        .send()
        .await?;
    if !resp.status().is_success() {
        let s = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow!("send to {}: {}: {}", queue, s, body));
    }
    Ok(())
}

async fn ack(client: &reqwest::Client, cfg: &Cfg, token: &str) -> Result<()> {
    let url = format!("{}/queues/{}/messages/{}", cfg.base, cfg.queue, token);
    let resp = client.delete(url).bearer_auth(&cfg.bearer).send().await?;
    if !resp.status().is_success() {
        let s = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow!("ack {}: {}", s, body));
    }
    Ok(())
}

async fn create_resource(
    client: &reqwest::Client,
    cfg: &Cfg,
    path: &str,
    name: &str,
) -> Result<()> {
    let url = format!("{}{}", cfg.base, path);
    let resp = client
        .post(url)
        .bearer_auth(&cfg.bearer)
        .json(&serde_json::json!({ "name": name }))
        .send()
        .await?;
    if resp.status().is_success() {
        return Ok(());
    }
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    Err(anyhow!("create {} {}: {}: {}", path, name, status, body))
}

fn client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent(concat!("inference-router-worker/", env!("CARGO_PKG_VERSION")))
        .timeout(Duration::from_secs(120))
        .connect_timeout(Duration::from_secs(10))
        .build()
        .context("building http client")
}

fn normalize_name(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '_' | '-' => c,
            _ => '-',
        })
        .collect::<String>()
        .chars()
        .take(64)
        .collect()
}

fn now_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
