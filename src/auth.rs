//! SCRAM-SHA-256 authentication (RFC 5802 / RFC 7677), as used by MongoDB
//! drivers. Only salted keys are stored, never passwords.

use crate::error::{Error, Result};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use bson::{Bson, Document, doc};
use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};

pub const MECHANISM: &str = "SCRAM-SHA-256";
pub const ITERATIONS: u32 = 15000;

fn hmac(key: &[u8], msg: &[u8]) -> Vec<u8> {
    let mut m = <Hmac<Sha256> as KeyInit>::new_from_slice(key).expect("hmac accepts any key length");
    m.update(msg);
    m.finalize().into_bytes().to_vec()
}

fn sha256(b: &[u8]) -> Vec<u8> {
    Sha256::digest(b).to_vec()
}

/// Builds the stored credential document for a new user.
pub fn make_credentials(password: &str, salt: &[u8], iterations: u32) -> Document {
    let mut salted = [0u8; 32];
    pbkdf2::pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, iterations, &mut salted);
    let client_key = hmac(&salted, b"Client Key");
    let stored_key = sha256(&client_key);
    let server_key = hmac(&salted, b"Server Key");
    doc! {
        "iterationCount": iterations as i32,
        "salt": B64.encode(salt),
        "storedKey": B64.encode(stored_key),
        "serverKey": B64.encode(server_key),
    }
}

pub fn user_doc(db: &str, user: &str, password: &str, roles: Bson) -> Document {
    let salt: Vec<u8> = (0..28).map(|_| rand::random::<u8>()).collect();
    doc! {
        "_id": format!("{db}.{user}"),
        "user": user,
        "db": db,
        "credentials": { MECHANISM: make_credentials(password, &salt, ITERATIONS) },
        "roles": roles,
        "mechanisms": [MECHANISM],
    }
}

/// Whether a user's roles only grant read access.
pub fn is_read_only(user: &Document) -> bool {
    let Ok(roles) = user.get_array("roles") else { return false };
    !roles.is_empty()
        && roles.iter().all(|r| {
            let name = match r {
                Bson::String(s) => s.as_str(),
                Bson::Document(d) => d.get_str("role").unwrap_or(""),
                _ => "",
            };
            matches!(name, "read" | "readAnyDatabase")
        })
}

fn parse_attrs(msg: &str) -> Vec<(char, String)> {
    msg.split(',')
        .filter_map(|p| {
            let mut c = p.chars();
            let k = c.next()?;
            let rest = c.as_str().strip_prefix('=')?;
            Some((k, rest.to_string()))
        })
        .collect()
}

fn attr(attrs: &[(char, String)], k: char) -> Option<&str> {
    attrs.iter().find(|(a, _)| *a == k).map(|(_, v)| v.as_str())
}

fn decode_username(s: &str) -> String {
    s.replace("=2C", ",").replace("=3D", "=")
}

/// Server side of one SCRAM conversation.
pub struct Conversation {
    pub db: String,
    pub user: String,
    client_first_bare: String,
    server_first: String,
    nonce: String,
    creds: Document,
    pub done: bool,
    pub verified: bool,
}

fn auth_error() -> Error {
    Error::auth_failed("Authentication failed.")
}

impl Conversation {
    /// Handles `client-first-message`; `lookup` returns the stored user document.
    pub fn start(db: &str, payload: &[u8], lookup: impl FnOnce(&str) -> Result<Option<Document>>) -> Result<(Conversation, Vec<u8>)> {
        let msg = std::str::from_utf8(payload).map_err(|_| auth_error())?;
        let bare = msg.strip_prefix("n,,").ok_or_else(|| Error::bad_value("SCRAM: unsupported channel binding or authzid"))?;
        let attrs = parse_attrs(bare);
        let user = decode_username(attr(&attrs, 'n').ok_or_else(auth_error)?);
        let client_nonce = attr(&attrs, 'r').ok_or_else(auth_error)?.to_string();
        let doc = lookup(&user)?.ok_or_else(auth_error)?;
        let creds = doc
            .get_document("credentials")
            .ok()
            .and_then(|c| c.get_document(MECHANISM).ok())
            .cloned()
            .ok_or_else(auth_error)?;
        let server_nonce: String = B64.encode((0..24).map(|_| rand::random::<u8>()).collect::<Vec<u8>>());
        let nonce = format!("{client_nonce}{server_nonce}");
        let server_first = format!(
            "r={nonce},s={},i={}",
            creds.get_str("salt").map_err(|_| auth_error())?,
            creds.get_i32("iterationCount").map_err(|_| auth_error())?
        );
        let conv = Conversation {
            db: db.to_string(),
            user,
            client_first_bare: bare.to_string(),
            server_first: server_first.clone(),
            nonce,
            creds,
            done: false,
            verified: false,
        };
        Ok((conv, server_first.into_bytes()))
    }

    /// Handles `client-final-message`, returning `server-final-message`.
    pub fn finish(&mut self, payload: &[u8]) -> Result<Vec<u8>> {
        if self.verified {
            // The optional empty exchange after verification.
            self.done = true;
            return Ok(Vec::new());
        }
        let msg = std::str::from_utf8(payload).map_err(|_| auth_error())?;
        let (without_proof, proof) = msg.rsplit_once(",p=").ok_or_else(auth_error)?;
        let attrs = parse_attrs(without_proof);
        if attr(&attrs, 'r') != Some(self.nonce.as_str()) {
            return Err(auth_error());
        }
        if attr(&attrs, 'c') != Some("biws") {
            return Err(auth_error());
        }
        let proof = B64.decode(proof).map_err(|_| auth_error())?;
        let stored_key = B64.decode(self.creds.get_str("storedKey").map_err(|_| auth_error())?).map_err(|_| auth_error())?;
        let server_key = B64.decode(self.creds.get_str("serverKey").map_err(|_| auth_error())?).map_err(|_| auth_error())?;
        let auth_message = format!("{},{},{}", self.client_first_bare, self.server_first, without_proof);
        let client_signature = hmac(&stored_key, auth_message.as_bytes());
        if proof.len() != client_signature.len() {
            return Err(auth_error());
        }
        let client_key: Vec<u8> = proof.iter().zip(&client_signature).map(|(a, b)| a ^ b).collect();
        // Constant-time comparison of H(ClientKey) with StoredKey.
        let computed = sha256(&client_key);
        let diff = computed.iter().zip(&stored_key).fold(0u8, |acc, (a, b)| acc | (a ^ b));
        if diff != 0 || computed.len() != stored_key.len() {
            return Err(auth_error());
        }
        self.verified = true;
        let server_signature = hmac(&server_key, auth_message.as_bytes());
        Ok(format!("v={}", B64.encode(server_signature)).into_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Runs the client side of SCRAM-SHA-256 against the server code.
    #[test]
    fn full_exchange() {
        let user = user_doc("admin", "alice", "s3cret", Bson::Array(vec![Bson::String("root".into())]));
        let client_nonce = "rOprNGfwEbeRWgbNEkqO";
        let client_first_bare = format!("n=alice,r={client_nonce}");
        let (mut conv, server_first) =
            Conversation::start("admin", format!("n,,{client_first_bare}").as_bytes(), |_| Ok(Some(user.clone()))).unwrap();
        let server_first = String::from_utf8(server_first).unwrap();
        let attrs = parse_attrs(&server_first);
        let salt = B64.decode(attr(&attrs, 's').unwrap()).unwrap();
        let iters: u32 = attr(&attrs, 'i').unwrap().parse().unwrap();
        let nonce = attr(&attrs, 'r').unwrap();
        let mut salted = [0u8; 32];
        pbkdf2::pbkdf2_hmac::<Sha256>(b"s3cret", &salt, iters, &mut salted);
        let client_key = hmac(&salted, b"Client Key");
        let stored_key = sha256(&client_key);
        let without_proof = format!("c=biws,r={nonce}");
        let auth_message = format!("{client_first_bare},{server_first},{without_proof}");
        let sig = hmac(&stored_key, auth_message.as_bytes());
        let proof: Vec<u8> = client_key.iter().zip(&sig).map(|(a, b)| a ^ b).collect();
        let final_msg = format!("{without_proof},p={}", B64.encode(&proof));
        let server_final = conv.finish(final_msg.as_bytes()).unwrap();
        let server_key = hmac(&salted, b"Server Key");
        let expected = format!("v={}", B64.encode(hmac(&server_key, auth_message.as_bytes())));
        assert_eq!(String::from_utf8(server_final).unwrap(), expected);

        // wrong password
        let (mut conv, _) = Conversation::start("admin", format!("n,,{client_first_bare}").as_bytes(), |_| Ok(Some(user.clone()))).unwrap();
        let bad: Vec<u8> = proof.iter().map(|b| b ^ 1).collect();
        let _ = conv.finish(format!("c=biws,r={},p={}", conv.nonce.clone(), B64.encode(bad)).as_bytes()).unwrap_err();
    }
}
