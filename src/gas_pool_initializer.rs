// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::retry_forever;
use crate::storage::Storage;
use crate::sui_client::SuiClient;
use crate::tx_signer::TxSigner;
use crate::types::GasCoin;
use parking_lot::Mutex;
use std::cmp::min;
use std::collections::VecDeque;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;
use sui_json_rpc_types::SuiTransactionBlockEffectsAPI;
use sui_types::coin::{PAY_MODULE_NAME, PAY_SPLIT_N_FUNC_NAME};
use sui_types::gas_coin::GAS;
use sui_types::programmable_transaction_builder::ProgrammableTransactionBuilder;
use sui_types::transaction::{Argument, Transaction, TransactionData};
use sui_types::SUI_FRAMEWORK_PACKAGE_ID;
use tap::TapFallible;
use tokio::task::JoinHandle;
use tokio::time::Instant;
#[cfg(not(test))]
use tokio_retry::strategy::FixedInterval;
#[cfg(not(test))]
use tokio_retry::Retry;
use tracing::{debug, error, info};

pub struct GasPoolInitializer {}

#[derive(Clone)]
struct CoinSplitEnv {
    target_init_coin_balance: u64,
    gas_cost_per_object: u64,
    signer: Arc<dyn TxSigner>,
    sui_client: SuiClient,
    task_queue: Arc<Mutex<VecDeque<JoinHandle<Vec<GasCoin>>>>>,
    total_coin_count: Arc<AtomicUsize>,
    rgp: u64,
}

impl CoinSplitEnv {
    fn enqueue_task(&self, coin: GasCoin) -> Option<GasCoin> {
        if coin.balance <= (self.gas_cost_per_object + self.target_init_coin_balance) * 2 {
            debug!(
                "Skip splitting coin {:?} because it has small balance",
                coin
            );
            return Some(coin);
        }
        let env = self.clone();
        let task = tokio::task::spawn(async move { env.split_one_gas_coin(coin).await });
        self.task_queue.lock().push_back(task);
        None
    }

    fn increment_total_coin_count_by(&self, delta: usize) {
        println!(
            "Number of coins got so far: {}",
            self.total_coin_count
                .fetch_add(delta, std::sync::atomic::Ordering::Relaxed)
                + delta
        );
    }

    async fn split_one_gas_coin(self, mut coin: GasCoin) -> Vec<GasCoin> {
        let rgp = self.rgp;
        let split_count = min(
            // Max number of object mutations per transaction is 2048.
            2000,
            coin.balance / (self.gas_cost_per_object + self.target_init_coin_balance),
        );
        debug!(
            "Evenly splitting coin {:?} into {} coins",
            coin, split_count
        );
        let budget = self.gas_cost_per_object * split_count;
        let effects = loop {
            let mut pt_builder = ProgrammableTransactionBuilder::new();
            let pure_arg = pt_builder.pure(split_count).unwrap();
            pt_builder.programmable_move_call(
                SUI_FRAMEWORK_PACKAGE_ID,
                PAY_MODULE_NAME.into(),
                PAY_SPLIT_N_FUNC_NAME.into(),
                vec![GAS::type_tag()],
                vec![Argument::GasCoin, pure_arg],
            );
            let pt = pt_builder.finish();
            let sponsor_address = self.signer.get_address();
            let tx_data = TransactionData::new_programmable(
                sponsor_address,
                vec![coin.object_ref],
                pt,
                budget,
                rgp,
            );
            let sig = retry_forever!(async {
                self.signer
                    .sign_transaction(&tx_data)
                    .await
                    .tap_err(|err| error!("Failed to sign transaction: {:?}", err))
            })
            .unwrap();
            let tx = Transaction::from_data(tx_data, vec![sig]);
            debug!(
                "Sending transaction for execution. Tx digest: {:?}",
                tx.digest()
            );
            let result = self
                .sui_client
                .execute_transaction(tx.clone(), Duration::from_secs(20))
                .await;
            match result {
                Ok(effects) => {
                    assert!(
                        effects.status().is_ok(),
                        "Transaction failed. This should never happen. Tx: {:?}, effects: {:?}",
                        tx,
                        effects
                    );
                    break effects;
                }
                Err(e) => {
                    error!("Failed to execute transaction: {:?}", e);
                    coin = self
                        .sui_client
                        .get_latest_gas_objects([coin.object_ref.0])
                        .await
                        .into_iter()
                        .next()
                        .unwrap()
                        .1
                        .unwrap();
                    continue;
                }
            }
        };
        let mut result = vec![];
        let new_coin_balance = (coin.balance - budget) / split_count;
        for created in effects.created() {
            result.extend(self.enqueue_task(GasCoin {
                object_ref: created.reference.to_object_ref(),
                balance: new_coin_balance,
            }));
        }
        let remaining_coin_balance = (coin.balance - new_coin_balance * (split_count - 1)) as i64
            - effects.gas_cost_summary().net_gas_usage();
        result.extend(self.enqueue_task(GasCoin {
            object_ref: effects.gas_object().reference.to_object_ref(),
            balance: remaining_coin_balance as u64,
        }));
        self.increment_total_coin_count_by(result.len() - 1);
        result
    }
}

impl GasPoolInitializer {
    async fn split_gas_coins(coins: Vec<GasCoin>, env: CoinSplitEnv) -> Vec<GasCoin> {
        let total_balance: u64 = coins.iter().map(|c| c.balance).sum();
        println!(
            "Splitting {} coins with total balance of {} into smaller coins with target balance of {}. This will result in close to {} coins",
            coins.len(),
            total_balance,
            env.target_init_coin_balance,
            total_balance / env.target_init_coin_balance,
        );
        let mut result = vec![];
        for coin in coins {
            result.extend(env.enqueue_task(coin));
        }
        loop {
            let Some(task) = env.task_queue.lock().pop_front() else {
                break;
            };
            result.extend(task.await.unwrap());
        }
        let new_total_balance: u64 = result.iter().map(|c| c.balance).sum();
        println!(
            "Splitting finished. Got {} coins. New total balance: {}. Spent {} gas in total",
            result.len(),
            new_total_balance,
            total_balance - new_total_balance
        );
        result
    }

    pub async fn run(
        fullnode_url: &str,
        storage: &Arc<dyn Storage>,
        force_init_gas_pool: bool,
        target_init_coin_balance: u64,
        signer: Arc<dyn TxSigner>,
    ) {
        let sponsor_address = signer.get_address();
        let available_coin_count = storage
            .get_available_coin_count(sponsor_address)
            .await
            .unwrap();
        if available_coin_count > 0 && !force_init_gas_pool {
            info!(
                "The account already owns {} gas coins. Skipping gas pool initialization",
                available_coin_count
            );
            return;
        }
        let start = Instant::now();
        info!("Initializing gas pool. Deleting all existing available gas coins from the store");
        storage
            .remove_all_available_coins(sponsor_address)
            .await
            .unwrap();
        let sui_client = SuiClient::new(fullnode_url).await;
        let coins = sui_client.get_all_owned_sui_coins(sponsor_address).await;
        let total_coin_count = Arc::new(AtomicUsize::new(coins.len()));
        let rgp = sui_client.get_reference_gas_price().await;
        if coins.is_empty() {
            error!("The account doesn't own any gas coins");
            return;
        }
        let gas_cost_per_object = sui_client
            .calibrate_gas_cost_per_object(sponsor_address, &coins[0])
            .await;
        info!("Calibrated gas cost per object: {:?}", gas_cost_per_object);
        let result = Self::split_gas_coins(
            coins,
            CoinSplitEnv {
                target_init_coin_balance,
                gas_cost_per_object,
                signer,
                sui_client,
                task_queue: Default::default(),
                total_coin_count,
                rgp,
            },
        )
        .await;
        for chunk in result.chunks(5000) {
            storage
                .add_new_coins(sponsor_address, chunk.to_vec())
                .await
                .unwrap();
        }
        println!("Pool initialization took {:?}s", start.elapsed().as_secs());
    }
}

#[cfg(test)]
mod tests {
    use crate::gas_pool_initializer::GasPoolInitializer;
    use crate::storage::connect_storage_for_testing;
    use crate::test_env::start_sui_cluster;
    use sui_types::gas_coin::MIST_PER_SUI;

    // TODO: Add more accurate tests.

    #[tokio::test]
    async fn test_basic_init_flow() {
        telemetry_subscribers::init_for_testing();
        let (cluster, signer) = start_sui_cluster(vec![1000 * MIST_PER_SUI]).await;
        let sponsor = signer.get_address();
        let fullnode_url = cluster.fullnode_handle.rpc_url;
        let storage = connect_storage_for_testing().await;
        GasPoolInitializer::run(fullnode_url.as_str(), &storage, false, MIST_PER_SUI, signer).await;
        assert!(storage.get_available_coin_count(sponsor).await.unwrap() > 900);
    }

    #[tokio::test]
    async fn test_init_non_even_split() {
        telemetry_subscribers::init_for_testing();
        let (cluster, signer) = start_sui_cluster(vec![10000000 * MIST_PER_SUI]).await;
        let sponsor = signer.get_address();
        let fullnode_url = cluster.fullnode_handle.rpc_url;
        let storage = connect_storage_for_testing().await;
        let target_init_coin_balance = 12345 * MIST_PER_SUI;
        GasPoolInitializer::run(
            fullnode_url.as_str(),
            &storage,
            false,
            target_init_coin_balance,
            signer,
        )
        .await;
        assert!(storage.get_available_coin_count(sponsor).await.unwrap() > 800);
    }
}
