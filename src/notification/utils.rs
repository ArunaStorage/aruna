use aruna_rust_api::api::notification::services::v2::Reply;
use base64::{engine::general_purpose, Engine};
use hmac::{Hmac, Mac};
use rand::{distributions::Alphanumeric, Rng};
use sha2::Sha256;

use crate::database::enums::ObjectType;

// ------------------------------------------- //
// ----- Subject Generation ------------------ //
// ------------------------------------------- //
///ToDo: Rust Doc
pub fn generate_resource_subject(
    resource_id: &str,
    resource_variant: ObjectType,
    include_sub_resources: bool,
) -> String {
    // No one cares about the specific graph hierarchy anymore
    let base_subject = match resource_variant {
        ObjectType::PROJECT => format!("AOS.RESOURCE._.{}.", resource_id),
        ObjectType::COLLECTION => format!("AOS.RESOURCE._.*._.{}.", resource_id),
        ObjectType::DATASET => format!("AOS.RESOURCE._.*._.*._.{}.", resource_id),
        ObjectType::OBJECT => format!("AOS.RESOURCE._.*._.*._.*._.{}.", resource_id),
    };

    if include_sub_resources {
        format!("{}_", base_subject)
    } else {
        format!("{}>", base_subject)
    }
}

///ToDo: Rust Doc
pub fn generate_resource_message_subject(
    resource_id: &str,
    resource_variant: ObjectType,
) -> String {
    // No one cares about the specific graph anymore
    match resource_variant {
        ObjectType::PROJECT => format!("AOS.RESOURCE._.{}._", resource_id),
        ObjectType::COLLECTION => format!("AOS.RESOURCE._.*._.{}._", resource_id),
        ObjectType::DATASET => format!("AOS.RESOURCE._.*._.*._.{}._", resource_id),
        ObjectType::OBJECT => format!("AOS.RESOURCE._.*._.*._.*._.{}._", resource_id),
    }
}

//ToDo: Rust Doc
pub fn generate_user_subject(user_id: &str) -> String {
    format!("AOS.USER.{}.>", user_id)
}

///ToDo: Rust Doc
pub fn generate_user_message_subject(user_id: &str) -> String {
    format!("AOS.USER.{}._", user_id)
}

//ToDo: Rust Doc
pub fn generate_announcement_subject() -> String {
    "AOS.ANNOUNCEMENT".to_string()
}

//ToDo: Rust Doc
pub fn generate_announcement_message_subject() -> String {
    todo!()
}

// ------------------------------------------- //
// ----- Reply Validation -------------------- //
// ------------------------------------------- //
type HmacSha256 = Hmac<Sha256>;

///ToDo: Rust Doc
pub fn calculate_reply_hmac(reply: String, secret: String) -> Reply {
    // Generate random salt value
    let salt = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect();

    // Calculate hmac
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC can take key of any size");
    mac.update(format!("{}-{}", reply, salt).as_bytes());

    // Encode hmac in base64
    let base64_hmac = general_purpose::STANDARD.encode(mac.finalize().into_bytes());

    // Return reply
    Reply {
        reply,
        salt,
        hmac: base64_hmac,
    }
}

///ToDo: Rust Doc
pub fn validate_reply_msg(reply: Reply, secret: String) -> anyhow::Result<bool> {
    // Calculate hmac
    let mut mac = match HmacSha256::new_from_slice(secret.as_bytes()) {
        Ok(hmac256) => hmac256,
        Err(_) => return Err(anyhow::anyhow!("Invalid key length for hmac")),
    };
    mac.update(format!("{}-{}", reply.reply, reply.salt).as_bytes());

    // Encode updated hmac in base64
    let base64_hmac = general_purpose::STANDARD.encode(mac.finalize().into_bytes());

    // Check if hmacs are equal
    Ok(base64_hmac == reply.hmac)
}
