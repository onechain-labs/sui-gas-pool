// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::anyhow;
use fastcrypto::encoding::{Base64, Encoding};
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use shared_crypto::intent::{Intent, IntentMessage};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use sui_types::base_types::SuiAddress;
use sui_types::crypto::{Signature, SuiKeyPair};
use sui_types::signature::GenericSignature;
use sui_types::transaction::{TransactionData, TransactionDataAPI};

#[async_trait::async_trait]
pub trait TxSigner: Send + Sync {
    async fn sign_transaction(&self, tx_data: &TransactionData)
        -> anyhow::Result<GenericSignature>;
    fn get_addresses(&self) -> Vec<SuiAddress>;
    fn is_valid_address(&self, address: &SuiAddress) -> bool {
        self.get_addresses()
            .iter()
            .find(|&&a| a == *address)
            .is_some()
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SignatureResponse {
    signature: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SuiAddressResponse {
    sui_pubkey_address: SuiAddress,
}

pub struct SidecarTxSigner {
    sidecar_url: String,
    client: Client,
    sui_address: SuiAddress,
}

impl SidecarTxSigner {
    pub async fn new(sidecar_url: String) -> Arc<Self> {
        let client = Client::new();
        let resp = client
            .get(format!("{}/{}", sidecar_url, "get-pubkey-address"))
            .send()
            .await
            .unwrap_or_else(|err| panic!("Failed to get pubkey address: {}", err));
        let sui_address = resp
            .json::<SuiAddressResponse>()
            .await
            .unwrap_or_else(|err| panic!("Failed to parse address response: {}", err))
            .sui_pubkey_address;
        Arc::new(Self {
            sidecar_url,
            client,
            sui_address,
        })
    }
}

#[async_trait::async_trait]
impl TxSigner for SidecarTxSigner {
    async fn sign_transaction(
        &self,
        tx_data: &TransactionData,
    ) -> anyhow::Result<GenericSignature> {
        let bytes = Base64::encode(bcs::to_bytes(&tx_data)?);
        let resp = self
            .client
            .post(format!("{}/{}", self.sidecar_url, "sign-transaction"))
            .header("Content-Type", "application/json")
            .json(&json!({"txBytes": bytes}))
            .send()
            .await?;
        let sig_bytes = resp.json::<SignatureResponse>().await?;
        let sig = GenericSignature::from_str(&sig_bytes.signature)
            .map_err(|err| anyhow!(err.to_string()))?;
        Ok(sig)
    }

    fn get_addresses(&self) -> Vec<SuiAddress> {
        vec![self.sui_address]
    }
}

pub struct TestTxSigner {
    keypair_map: HashMap<SuiAddress, SuiKeyPair>,
}

impl TestTxSigner {
    pub fn new(keypair: Vec<SuiKeyPair>) -> Arc<Self> {
        Arc::new(Self {
            keypair_map: keypair
                .into_iter()
                .map(|k| ((&k.public()).into(), k))
                .collect(),
        })
    }
}

#[async_trait::async_trait]
impl TxSigner for TestTxSigner {
    async fn sign_transaction(
        &self,
        tx_data: &TransactionData,
    ) -> anyhow::Result<GenericSignature> {
        let gas_owner = tx_data.gas_owner();
        let keypair = self
            .keypair_map
            .get(&gas_owner)
            .ok_or(anyhow!("Not found ${} keypair", gas_owner))?;
        let intent_msg = IntentMessage::new(Intent::sui_transaction(), tx_data);
        let sponsor_sig = Signature::new_secure(&intent_msg, keypair).into();
        Ok(sponsor_sig)
    }

    fn get_addresses(&self) -> Vec<SuiAddress> {
        self.keypair_map.keys().cloned().collect()
    }
}
