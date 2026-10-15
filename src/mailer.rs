//! SMTP transport for invite-by-email (CLAUDE-394).
//!
//! [`Mailer`] is built fresh from [`crate::config::EmailConfig::from_env`] on
//! every send (CLAUDE-428: the SMTP transport is environment-variable-only, no
//! Settings-page form), so a changed environment applies on the next daemon
//! restart. It is deliberately not `Debug`: it holds the plaintext SMTP
//! credential for the lifetime of one send.

use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};

use crate::config::{EmailConfig, SmtpEncryption};
use crate::error::Result;

/// How long a single SMTP send may take before it is treated as failed.
const SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// A built SMTP transport plus the From mailbox to send with. Not `Debug`:
/// the transport holds the credential passed to [`Mailer::new`].
pub struct Mailer {
    transport: AsyncSmtpTransport<Tokio1Executor>,
    from: String,
}

impl Mailer {
    /// Build a transport from `cfg`, using its resolved [`EmailConfig::password`]
    /// (empty when no `smtp_username` is set). Fails when `cfg` is not
    /// "configured" ([`EmailConfig::configured`]) or `smtp_host`/`from` do not
    /// parse.
    pub fn new(cfg: &EmailConfig) -> Result<Self> {
        let host = cfg
            .smtp_host
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("email is not configured: no smtp_host"))?;
        let from = cfg
            .from
            .clone()
            .ok_or_else(|| anyhow::anyhow!("email is not configured: no from address"))?;

        let default_port = match cfg.smtp_encryption {
            SmtpEncryption::Starttls => 587,
            SmtpEncryption::Tls => 465,
            SmtpEncryption::None => 25,
        };
        let port = cfg.smtp_port.unwrap_or(default_port);

        let builder = match cfg.smtp_encryption {
            SmtpEncryption::Starttls => AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(host)
                .map_err(|e| anyhow::anyhow!(format!("invalid SMTP host `{host}`: {e}")))?,
            SmtpEncryption::Tls => AsyncSmtpTransport::<Tokio1Executor>::relay(host)
                .map_err(|e| anyhow::anyhow!(format!("invalid SMTP host `{host}`: {e}")))?,
            SmtpEncryption::None => AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(host),
        };
        let mut builder = builder.port(port).timeout(Some(SEND_TIMEOUT));
        if let Some(username) = cfg.smtp_username.as_deref() {
            builder = builder.credentials(Credentials::new(
                username.to_string(),
                cfg.password().unwrap_or_default().to_string(),
            ));
        }

        Ok(Self {
            transport: builder.build(),
            from,
        })
    }

    /// Send a plain-text message to `to`. The subject and body must never
    /// contain the invite token, the full invite link, or the SMTP password;
    /// the caller is responsible for that.
    pub async fn send(&self, to: &str, subject: &str, body: &str) -> Result<()> {
        let message = Message::builder()
            .from(
                self.from
                    .parse()
                    .map_err(|e| anyhow::anyhow!(format!("invalid From address: {e}")))?,
            )
            .to(to
                .parse()
                .map_err(|e| anyhow::anyhow!(format!("invalid recipient address: {e}")))?)
            .subject(subject)
            .body(body.to_string())
            .map_err(|e| anyhow::anyhow!(format!("building the message: {e}")))?;

        self.transport
            .send(message)
            .await
            .map_err(|e| anyhow::anyhow!(format!("SMTP send failed: {e}")))?;
        Ok(())
    }
}
