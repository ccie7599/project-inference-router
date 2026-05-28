use anyhow::{anyhow, Context, Result};
use base64::Engine;
use serde::{Deserialize, Serialize};
use spin_sdk::http::{IntoResponse, Method, Request, Response};
use spin_sdk::http_component;
use spin_sdk::key_value::Store;
use spin_sdk::variables;
use uuid::Uuid;

const RESPONSE_TOPIC: &str = "inference_responses";
const SUBSTRATE_MAX_WAIT_SECS: u64 = 20;

#[derive(Deserialize)]
struct InferenceRequest {
    model: String,
    prompt: String,
    #[serde(default)]
    stream: bool,
}

#[derive(Serialize)]
struct WorkItem<'a> {
    corr_id: &'a str,
    model: &'a str,
    prompt: &'a str,
    response_queue: Option<&'a str>,
}

#[derive(Deserialize)]
struct PresenceMessage {
    worker_id: String,
    model: String,
    #[serde(default)]
    region: Option<String>,
    ts: u64,
}

#[derive(Serialize, Deserialize, Clone)]
struct PresenceRecord {
    worker_id: String,
    model: String,
    region: Option<String>,
    last_seen: u64,
}

#[derive(Deserialize)]
struct SubscriptionCreated {
    id: String,
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

struct Cfg {
    bearer: String,
    base: String,
    presence_max_age_secs: u64,
    reqres_poll_secs: u64,
}

impl Cfg {
    fn load() -> Result<Self> {
        let mut poll: u64 = variables::get("reqres_poll_secs")?
            .parse()
            .context("reqres_poll_secs must be a number")?;
        if poll > SUBSTRATE_MAX_WAIT_SECS {
            poll = SUBSTRATE_MAX_WAIT_SECS;
        }
        Ok(Cfg {
            bearer: variables::get("tenant_bearer")
                .context("variable tenant_bearer not set")?,
            base: variables::get("substrate_base")?,
            presence_max_age_secs: variables::get("presence_max_age_secs")?
                .parse()
                .context("presence_max_age_secs must be a number")?,
            reqres_poll_secs: poll,
        })
    }
}

#[http_component]
async fn handle(req: Request) -> Result<impl IntoResponse> {
    let method = req.method().clone();
    let path = req
        .path_and_query()
        .unwrap_or("/")
        .split('?')
        .next()
        .unwrap_or("/")
        .to_string();

    match (&method, path.as_str()) {
        (&Method::Get, "/healthz") => Ok(text(200, "ok\n")),
        (&Method::Post, "/v1/inference") => handle_inference(req).await,
        (&Method::Post, "/v1/internal/presence") => handle_presence(req).await,
        _ => Ok(text(404, "not found\n")),
    }
}

async fn handle_inference(req: Request) -> Result<Response> {
    let cfg = Cfg::load()?;
    let body = req.into_body();
    let parsed: InferenceRequest = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return Ok(text(400, &format!("bad json: {e}\n"))),
    };

    let store = Store::open_default()?;
    if pick_worker(&store, &parsed.model, cfg.presence_max_age_secs)?.is_none() {
        return Ok(text(503, "no-workers\n"));
    }

    let corr_id = format!("corr_{}", Uuid::new_v4().simple());
    let req_queue = format!("inference_req_{}", normalize_name(&parsed.model));

    if parsed.stream {
        let sub_id = create_subscription(&cfg, &corr_id).await?;
        publish_request(&cfg, &req_queue, &corr_id, &parsed.model, &parsed.prompt, None).await?;
        let url = format!(
            "{}/subscriptions/{}/stream?bearer={}",
            cfg.base, sub_id, cfg.bearer
        );
        return Ok(Response::builder()
            .status(302)
            .header("location", url)
            .header("cache-control", "no-store")
            .body(())
            .build());
    }

    // req/res
    let resp_queue = format!("inference_resp_{}", corr_id);
    create_queue(&cfg, &resp_queue).await?;
    let publish_result =
        publish_request(&cfg, &req_queue, &corr_id, &parsed.model, &parsed.prompt, Some(&resp_queue))
            .await;
    if let Err(e) = publish_result {
        let _ = delete_queue(&cfg, &resp_queue).await;
        return Err(e);
    }
    let received = receive_one(&cfg, &resp_queue, cfg.reqres_poll_secs).await;
    let _ = delete_queue(&cfg, &resp_queue).await;
    match received {
        Ok(Some(body_bytes)) => Ok(Response::builder()
            .status(200)
            .header("content-type", "application/json")
            .body(body_bytes)
            .build()),
        Ok(None) => Ok(text(504, "worker did not respond in time\n")),
        Err(e) => Ok(text(502, &format!("substrate error: {e}\n"))),
    }
}

async fn handle_presence(req: Request) -> Result<Response> {
    let body = req.into_body();
    let msg: PresenceMessage = serde_json::from_slice(&body)
        .map_err(|e| anyhow!("bad presence payload: {e}"))?;
    let store = Store::open_default()?;
    let key = format!("presence:{}:{}", normalize_name(&msg.model), msg.worker_id);
    let record = PresenceRecord {
        worker_id: msg.worker_id,
        model: msg.model,
        region: msg.region,
        last_seen: msg.ts,
    };
    store.set_json(&key, &record)?;
    Ok(text(200, "ok\n"))
}

fn pick_worker(
    store: &Store,
    model: &str,
    max_age_secs: u64,
) -> Result<Option<PresenceRecord>> {
    let prefix = format!("presence:{}:", normalize_name(model));
    let now = now_secs();
    let mut best: Option<PresenceRecord> = None;
    for key in store.get_keys()? {
        if !key.starts_with(&prefix) {
            continue;
        }
        let Some(record) = store.get_json::<PresenceRecord>(&key)? else {
            continue;
        };
        if now.saturating_sub(record.last_seen) > max_age_secs {
            continue;
        }
        if best.as_ref().map_or(true, |b| record.last_seen > b.last_seen) {
            best = Some(record);
        }
    }
    Ok(best)
}

async fn create_subscription(cfg: &Cfg, corr_id: &str) -> Result<String> {
    let body = serde_json::json!({
        "topic": RESPONSE_TOPIC,
        "filter": format!("inference.{}.>", corr_id),
        "delivery": {"type": "sse"}
    });
    let resp = post_json(cfg, "/subscriptions", &body).await?;
    let parsed: SubscriptionCreated =
        serde_json::from_slice(&resp).context("subscription create response")?;
    Ok(parsed.id)
}

async fn create_queue(cfg: &Cfg, name: &str) -> Result<()> {
    let body = serde_json::json!({ "name": name });
    let _ = post_json(cfg, "/queues", &body).await?;
    Ok(())
}

async fn delete_queue(cfg: &Cfg, name: &str) -> Result<()> {
    let url = format!("{}/queues/{}", cfg.base, name);
    let req = Request::builder()
        .method(Method::Delete)
        .uri(url)
        .header("authorization", format!("Bearer {}", cfg.bearer))
        .build();
    let _resp: Response = spin_sdk::http::send(req).await?;
    Ok(())
}

async fn publish_request(
    cfg: &Cfg,
    queue: &str,
    corr_id: &str,
    model: &str,
    prompt: &str,
    response_queue: Option<&str>,
) -> Result<()> {
    let item = WorkItem { corr_id, model, prompt, response_queue };
    let bytes = serde_json::to_vec(&item)?;
    let url = format!("{}/queues/{}/messages", cfg.base, queue);
    let req = Request::builder()
        .method(Method::Post)
        .uri(url)
        .header("authorization", format!("Bearer {}", cfg.bearer))
        .header("content-type", "application/octet-stream")
        .body(bytes)
        .build();
    let resp: Response = spin_sdk::http::send(req).await?;
    let status: u16 = (*resp.status()).into();
    if status >= 300 {
        return Err(anyhow!(
            "publish to {} returned {}: {}",
            queue,
            status,
            String::from_utf8_lossy(resp.body())
        ));
    }
    Ok(())
}

/// Long-poll one message from the queue. Acks before returning.
/// Returns the raw body bytes (after base64 decode), or None if the queue stayed empty.
async fn receive_one(cfg: &Cfg, queue: &str, wait_secs: u64) -> Result<Option<Vec<u8>>> {
    let wait = wait_secs.min(SUBSTRATE_MAX_WAIT_SECS);
    let url = format!("{}/queues/{}/messages?wait={}", cfg.base, queue, wait);
    let req = Request::builder()
        .method(Method::Get)
        .uri(url)
        .header("authorization", format!("Bearer {}", cfg.bearer))
        .build();
    let resp: Response = spin_sdk::http::send(req).await?;
    let status: u16 = (*resp.status()).into();
    if status >= 300 {
        return Err(anyhow!(
            "receive from {} returned {}: {}",
            queue,
            status,
            String::from_utf8_lossy(resp.body())
        ));
    }
    let parsed: ReceiveResponse =
        serde_json::from_slice(resp.body()).context("queue receive shape")?;
    if parsed.count == 0 || parsed.messages.is_empty() {
        return Ok(None);
    }
    // Take the first message, ack it. (Receive can return multiple; for v0.1
    // req/res, we only expect one because the response queue is per-corr-id.)
    let msg = parsed.messages.into_iter().next().unwrap();
    let body_bytes = base64::engine::general_purpose::STANDARD
        .decode(msg.body_b64.as_bytes())
        .context("body_b64 decode")?;
    ack(cfg, queue, &msg.ack_token).await?;
    Ok(Some(body_bytes))
}

async fn ack(cfg: &Cfg, queue: &str, ack_token: &str) -> Result<()> {
    let url = format!("{}/queues/{}/messages/{}", cfg.base, queue, ack_token);
    let req = Request::builder()
        .method(Method::Delete)
        .uri(url)
        .header("authorization", format!("Bearer {}", cfg.bearer))
        .build();
    let resp: Response = spin_sdk::http::send(req).await?;
    let status: u16 = (*resp.status()).into();
    if status >= 300 {
        return Err(anyhow!(
            "ack on {} returned {}: {}",
            queue,
            status,
            String::from_utf8_lossy(resp.body())
        ));
    }
    Ok(())
}

async fn post_json(cfg: &Cfg, path: &str, body: &serde_json::Value) -> Result<Vec<u8>> {
    let url = format!("{}{}", cfg.base, path);
    let bytes = serde_json::to_vec(body)?;
    let req = Request::builder()
        .method(Method::Post)
        .uri(url.clone())
        .header("authorization", format!("Bearer {}", cfg.bearer))
        .header("content-type", "application/json")
        .body(bytes)
        .build();
    let resp: Response = spin_sdk::http::send(req).await?;
    let status: u16 = (*resp.status()).into();
    if status >= 300 {
        return Err(anyhow!(
            "POST {} -> {}: {}",
            url,
            status,
            String::from_utf8_lossy(resp.body())
        ));
    }
    Ok(resp.body().to_vec())
}

fn text(status: u16, msg: &str) -> Response {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .body(msg.as_bytes().to_vec())
        .build()
}

/// Normalize a free-form name (model, etc.) to the substrate's queue/topic name rule
/// `[A-Za-z0-9_-]{1,64}` — every disallowed char becomes `-`.
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
