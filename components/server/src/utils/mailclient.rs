use anyhow::Result;
use lettre::message::header::ContentType;
use lettre::transport::smtp::authentication::Credentials;
use lettre::{Message, SmtpTransport, Transport};

pub struct MailClient {
    creds: Credentials,
    server: String,
    sender_email: String,
}

impl MailClient {
    pub fn new() -> Result<Self> {
        let smtp_user = dotenvy::var("SMTP_USER")?;
        let smtp_password = dotenvy::var("SMTP_PASSWORD")?;
        let smtp_server = dotenvy::var("SMTP_SERVER")?;
        let sender_email = dotenvy::var("SMTP_SEND_ADDRESS")?;

        Ok(MailClient {
            creds: Credentials::new(smtp_user, smtp_password),
            server: smtp_server,
            sender_email,
        })
    }

    pub fn send_message(&self, recepient: &str, message: String, subject: &str) -> Result<()> {
        // Open a remote connection to gmail
        let mailer = SmtpTransport::relay(&self.server)?
            .credentials(self.creds.clone())
            .build();

        let email = Message::builder()
            .from(self.sender_email.parse()?)
            .to(recepient.parse()?)
            .subject(subject)
            .header(ContentType::TEXT_PLAIN)
            .body(message)?;

        let _ = mailer.send(&email)?;
        Ok(())
    }
}
