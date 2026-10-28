//! webhook の送信 (`tenko::deliver`) の Workers での部品: 外向きの HTTP (`worker::Fetch`) と待ち (`worker::Delay`)。
//!
//! rust の `ReqwestWebhookClient` と同じく、リダイレクトは追わない (302 で内部へ回り込ませない)。打ち切りは 7 秒 (rust は 10 秒)。
//! 値と future は JS の値で `Send` でないので、`worker::send` の `SendFuture` で包む (Workers は単一 thread)。
//! このファイルは wasm 専用で、CI のテストと coverage の gate の外 (確かめるのは本番の実物)。

use std::time::Duration;

use futures_util::future::{select, BoxFuture, Either};
use tenko::deliver::{HttpResult, Sleeper, WebhookHttp};
use wasm_bindgen::JsValue;
use worker::send::SendFuture;
use worker::{Delay, Fetch, Headers, Method, Request, RequestInit, RequestRedirect};

/// 1 回の送信の上限。rust は 10 秒だが、3 回 × 7 秒 + 待ち 1 秒 + 5 秒 = 最長 27 秒で `wait_until` (応答の後 30 秒まで) に収める
const TIMEOUT: Duration = Duration::from_secs(7);

pub struct WorkerHttp;

async fn post(url: &str, headers: Vec<(&'static str, String)>, body: String) -> HttpResult {
    let h = Headers::new();
    for (k, v) in headers {
        h.set(k, &v).map_err(|_| "bad header".to_owned())?;
    }
    let mut init = RequestInit::new();
    init.with_method(Method::Post)
        .with_headers(h)
        .with_redirect(RequestRedirect::Manual)
        .with_body(Some(JsValue::from_str(&body)));
    let req = Request::new_with_init(url, &init).map_err(|_| "bad request".to_owned())?;
    let send = Box::pin(async move {
        let mut resp = Fetch::Request(req)
            .send()
            .await
            .map_err(|_| "request failed".to_owned())?;
        let text = resp.text().await.unwrap_or_default();
        Ok((resp.status_code(), text))
    });
    match select(send, Box::pin(Delay::from(TIMEOUT))).await {
        Either::Left((r, _)) => r,
        Either::Right(_) => Err("timeout".to_owned()),
    }
}

impl WebhookHttp for WorkerHttp {
    fn post<'a>(
        &'a self,
        url: &'a str,
        headers: Vec<(&'static str, String)>,
        body: String,
    ) -> BoxFuture<'a, HttpResult> {
        Box::pin(SendFuture::new(post(url, headers, body)))
    }
}

pub struct WorkerSleeper;

impl Sleeper for WorkerSleeper {
    fn sleep(&self, secs: u64) -> BoxFuture<'_, ()> {
        Box::pin(SendFuture::new(Delay::from(Duration::from_secs(secs))))
    }
}
