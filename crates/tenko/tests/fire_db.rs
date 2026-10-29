//! ほかの worker から webhook を預かる口 (`POST /internal/webhooks/fire`) を通す。
//! 口は DB を読まないが、組み立ては worker と同じ (実行用ロール `alc_api_rt` の接続) にする。

mod support;

use serde_json::json;
use support::{call, call_with, tenant};
use uuid::Uuid;

const URI: &str = "/internal/webhooks/fire";

/// 4 つの種類を積む。payload は渡されたまま (送るのは応答の後の deliver)
#[tokio::test]
async fn fire_queues_known_events() {
    let app = support::app().await;
    let t = tenant().await;
    for event_type in tenko::fire::EVENT_TYPES {
        let payload = json!({ "event": event_type, "tenant_id": t, "data": { "n": 1 } });
        let got = call(
            &app,
            "POST",
            URI,
            Some(t),
            Some(json!({ "event_type": event_type, "payload": payload })),
        )
        .await;
        assert_eq!(got.status, 202);
        assert!(got.body.is_empty());
    }
    let queued = app.outbox.take();
    assert_eq!(queued.len(), 4);
    for ((tenant_id, event), expected) in queued.iter().zip(tenko::fire::EVENT_TYPES) {
        assert_eq!(*tenant_id, t);
        assert_eq!(event.event_type, expected);
        assert_eq!(event.payload["tenant_id"], json!(t));
    }
    assert!(app.logs().is_empty());
}

/// 知らない種類・テナントの違う payload・`tenant_id` の無い payload は 400、テナントのヘッダーが無ければ 401。
/// dev端末の要求は 202 で受けて積まない。`/api` の下には無い
#[tokio::test]
async fn fire_rejects_and_skips() {
    let app = support::app().await;
    let t = tenant().await;
    let other = Uuid::new_v4();
    let body = |event_type: &str, payload: serde_json::Value| {
        Some(json!({ "event_type": event_type, "payload": payload }))
    };

    let unknown = call(
        &app,
        "POST",
        URI,
        Some(t),
        body("tenko_overdue", json!({ "tenant_id": t })),
    )
    .await;
    assert_eq!(unknown.status, 400);
    let mismatch = call(
        &app,
        "POST",
        URI,
        Some(t),
        body("alcohol_detected", json!({ "tenant_id": other })),
    )
    .await;
    assert_eq!(mismatch.status, 400);
    let missing = call(
        &app,
        "POST",
        URI,
        Some(t),
        body("alcohol_detected", json!({})),
    )
    .await;
    assert_eq!(missing.status, 400);
    let not_object = call(
        &app,
        "POST",
        URI,
        Some(t),
        body("alcohol_detected", json!("x")),
    )
    .await;
    assert_eq!(not_object.status, 400);
    let no_tenant = call(
        &app,
        "POST",
        URI,
        None,
        body("alcohol_detected", json!({ "tenant_id": t })),
    )
    .await;
    assert_eq!(no_tenant.status, 401);

    let dev = call_with(
        &app,
        "POST",
        URI,
        Some(t),
        &[("X-Device-Dev", "1")],
        body("alcohol_detected", json!({ "tenant_id": t })),
    )
    .await;
    assert_eq!(dev.status, 202);

    let under_api = call(
        &app,
        "POST",
        "/api/internal/webhooks/fire",
        Some(t),
        body("alcohol_detected", json!({ "tenant_id": t })),
    )
    .await;
    assert_eq!(under_api.status, 404);

    assert!(app.outbox.take().is_empty());
}
