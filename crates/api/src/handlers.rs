//! HTTP request handlers for the balance API

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use scorpius_core::{BalanceSnapshot, Money, UserId};
use serde::Serialize;
use std::str::FromStr;

use crate::state::AppState;

/// Response structure for a single balance
#[derive(Debug, Serialize)]
pub struct BalanceResponse {
    pub user_id: u64,
    pub asset: String,
    pub available: MoneyResponse,
    pub locked: MoneyResponse,
    pub total: MoneyResponse,
}

/// Response structure for money amounts
#[derive(Debug, Serialize)]
pub struct MoneyResponse {
    pub amount: i64,
    pub scale: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub formatted: Option<String>,
}

impl From<Money> for MoneyResponse {
    fn from(money: Money) -> Self {
        Self {
            amount: money.amount,
            scale: money.scale,
            formatted: Some(format_money(money)),
        }
    }
}

/// Response structure for user balances list
#[derive(Debug, Serialize)]
pub struct UserBalancesResponse {
    pub user_id: u64,
    pub balances: Vec<BalanceResponse>,
}

/// Response structure for error messages
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
}

/// Format money amount as a decimal string
fn format_money(money: Money) -> String {
    let amount_str = money.amount.to_string();
    let scale = money.scale as usize;

    if scale == 0 {
        return amount_str;
    }

    if amount_str.len() <= scale {
        // Need leading zeros
        let zeros_needed = scale - amount_str.len() + 1;
        format!("0.{}", "0".repeat(zeros_needed - 1) + &amount_str)
    } else {
        // Insert decimal point
        let (integer, fractional) = amount_str.split_at(amount_str.len() - scale);
        format!("{}.{}", integer, fractional.trim_end_matches('0'))
    }
}

/// Convert BalanceSnapshot to BalanceResponse
fn balance_to_response(snapshot: BalanceSnapshot) -> BalanceResponse {
    let total = Money::new(
        snapshot.available.amount + snapshot.locked.amount,
        snapshot.available.scale.max(snapshot.locked.scale),
    );

    BalanceResponse {
        user_id: snapshot.user_id.0,
        asset: snapshot.asset,
        available: snapshot.available.into(),
        locked: snapshot.locked.into(),
        total: total.into(),
    }
}

/// Get all balances for a specific user
///
/// GET /balances/{user_id}
///
/// Returns 200 OK with all balances for the user, or 404 if user has no balances
/// Note: This endpoint is eventually consistent and may show slightly stale data
pub async fn get_user_balances(
    State(state): State<AppState>,
    Path(user_id_str): Path<String>,
) -> Result<Json<UserBalancesResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Parse user_id
    let user_id = u64::from_str(&user_id_str)
        .map(UserId)
        .map_err(|_| {
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "Invalid user_id".to_string(),
                    details: Some(format!("user_id must be a valid number, got: {}", user_id_str)),
                }),
            )
        })?;

    // Query balances from read model
    let balances = state
        .read_model
        .get_user_balances(user_id)
        .await
        .map_err(|e| {
            tracing::error!("Error querying balances for user {}: {}", user_id.0, e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Database error".to_string(),
                    details: Some("Failed to query balances".to_string()),
                }),
            )
        })?;

    if balances.is_empty() {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "No balances found".to_string(),
                details: Some(format!("User {} has no balances", user_id.0)),
            }),
        ));
    }

    let balance_responses: Vec<BalanceResponse> = balances
        .into_iter()
        .map(balance_to_response)
        .collect();

    Ok(Json(UserBalancesResponse {
        user_id: user_id.0,
        balances: balance_responses,
    }))
}

/// Get balance for a specific user and asset
///
/// GET /balances/{user_id}/{asset}
///
/// Returns 200 OK with the balance, or 404 if the balance doesn't exist
/// Note: This endpoint is eventually consistent and may show slightly stale data
pub async fn get_balance(
    State(state): State<AppState>,
    Path((user_id_str, asset)): Path<(String, String)>,
) -> Result<Json<BalanceResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Parse user_id
    let user_id = u64::from_str(&user_id_str)
        .map(UserId)
        .map_err(|_| {
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "Invalid user_id".to_string(),
                    details: Some(format!("user_id must be a valid number, got: {}", user_id_str)),
                }),
            )
        })?;

    // Query balance from read model
    let balance = state
        .read_model
        .get_balance(user_id, &asset)
        .await
        .map_err(|e| {
            tracing::error!(
                "Error querying balance for user {} and asset {}: {}",
                user_id.0,
                asset,
                e
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Database error".to_string(),
                    details: Some("Failed to query balance".to_string()),
                }),
            )
        })?;

    match balance {
        Some(snapshot) => Ok(Json(balance_to_response(snapshot))),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "Balance not found".to_string(),
                details: Some(format!(
                    "No balance found for user {} and asset {}",
                    user_id.0, asset
                )),
            }),
        )),
    }
}
