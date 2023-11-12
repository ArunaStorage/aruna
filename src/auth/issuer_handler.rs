use anyhow::{bail, Result};
use chrono::{NaiveDateTime, Utc};
use jsonwebtoken::{
    decode_header,
    jwk::{Jwk, JwkSet},
    DecodingKey,
};

use super::token_handler::ArunaTokenClaims;

pub struct Issuer {
    pub issuer_url: String,
    pub pubkey_endpoint: String,
    pub jwks: JwkSet,
    pub last_updated: NaiveDateTime,
    pub audiences: Vec<String>,
}

impl Issuer {
    pub async fn new(
        issuer_url: String,
        pubkey_endpoint: String,
        audiences: Vec<String>,
    ) -> Result<Self> {
        let (jwks, last_updated) = Self::fetch_jwks(&pubkey_endpoint).await?;
        Ok(Self {
            issuer_url,
            pubkey_endpoint,
            jwks,
            last_updated,
            audiences,
        })
    }

    pub async fn fetch_jwks(endpoint: &str) -> Result<(JwkSet, NaiveDateTime)> {
        let client = reqwest::Client::new();
        let res = client.get(endpoint).send().await?;
        let jwks: JwkSet = res.json().await?;
        Ok((jwks, Utc::now().naive_utc()))
    }

    pub async fn refresh_jwks(&mut self) -> Result<()> {
        if self.last_updated + chrono::Duration::minutes(5) > Utc::now().naive_utc() {
            bail!("JWKS was updated less than 5 minutes ago");
        }
        let (jwks, last_updated) = Self::fetch_jwks(&self.pubkey_endpoint).await?;
        self.jwks = jwks;
        self.last_updated = last_updated;
        Ok(())
    }

    pub async fn check_token(&mut self, kid: &str, token: &str) -> Result<ArunaTokenClaims> {
        match self.jwks.find(kid) {
            Some(jwk) => Self::get_validate_claims(token, &jwk, &self.audiences),
            None => {
                self.refresh_jwks().await?;
                if let Some(jwk) = self.jwks.find(kid) {
                    Self::get_validate_claims(token, &jwk, &self.audiences)
                } else {
                    bail!("No matching key found")
                }
            }
        }
    }

    pub fn get_validate_claims(
        token: &str,
        jwk: &Jwk,
        audiences: &[String],
    ) -> Result<ArunaTokenClaims> {
        // if !jwk.is_supported() {
        //     return Err(anyhow::anyhow!("Unsupported algorithm"));
        // }
        let header = decode_header(token)?;
        let alg = header.alg;
        let mut validation = jsonwebtoken::Validation::new(alg);
        validation.set_audience(audiences);
        let tokendata = jsonwebtoken::decode::<ArunaTokenClaims>(
            token,
            &DecodingKey::from_jwk(jwk)?,
            &validation,
        )?;
        Ok(tokendata.claims)
    }
}
