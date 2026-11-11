//! 点呼の異常のメールの送り手 ([`tenko::mail::Mailer`]) の Workers での実装: Cloudflare の `send_email` の binding
//! `ANOMALY_EMAIL` (構造化の `Email` を `send_with_builder` で送る。MIME は手で組まない)。
//! ippoan/alc-measurements-worker の src/mail.rs (アルコール検知メール) の写しで、添付は持たない。
//!
//! 差出人は変数 `NOTIFY_EMAIL_FROM`。宛先 (`to`) は notify のグループ「点呼の異常」の email で、Cloudflare アカウントで
//! 確認済みの送り先だけが届く (binding に `destination_address` を書かない = 確認済みの全員に送れる形)。
//!
//! `Mailer` は `Send + Sync` で `Send` な future を返す。`SendEmail` と future は JS の値で `Send` でないので、
//! `worker::send` の `SendWrapper`・`SendFuture` で包む (Workers は単一 thread)。
//!
//! エラーは段の名前だけを持つ `MailError` に落とす。**宛先・本文・ランタイムの生のエラー文はログに出さない。**
//! このファイルは wasm 専用で、CI のテストと coverage の gate の外。

use futures_util::future::BoxFuture;
use tenko::mail::{Mail, MailError, Mailer};
use worker::send::{SendFuture, SendWrapper};
use worker::{SendEmail, SendEmailBuilder};

/// `send_email` の binding と差出人
pub struct EmailSender {
    binding: SendWrapper<SendEmail>,
    from: String,
}

impl EmailSender {
    pub fn new(binding: SendEmail, from: String) -> Self {
        Self {
            binding: SendWrapper::new(binding),
            from,
        }
    }
}

impl Mailer for EmailSender {
    fn send<'a>(&'a self, mail: &'a Mail) -> BoxFuture<'a, Result<(), MailError>> {
        Box::pin(SendFuture::new(async move {
            let email = SendEmailBuilder::builder(&self.from, &mail.to, &mail.subject)
                .text(&mail.body)
                .build();
            self.binding
                .send_with_builder(&email)
                .await
                .map(|_| ())
                .map_err(|_| MailError("send"))
        }))
    }
}
