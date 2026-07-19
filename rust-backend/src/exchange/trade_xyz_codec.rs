use k256::ecdsa::{Signature as EcdsaSignature, SigningKey};
use rust_decimal::Decimal;
use serde::Serialize;
use sha3::{Digest, Keccak256};
use thiserror::Error;

use crate::domain::{ClientOrderId, OrderSide};

const CLOID_MAGIC: u128 = 0b10_1101;
const CLOID_MAGIC_SHIFT: u32 = 122;
const NO_LEVEL: u8 = 127;
const QUOTE_ASSET: &str = "USDC";
const DEX_PREFIX: &str = "xyz:";

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TradeXyzCodecError {
    #[error("TRADE.XYZ requires a 32-byte hexadecimal agent private key")]
    InvalidPrivateKey,
    #[error("TRADE.XYZ account address must be a 20-byte hexadecimal address")]
    InvalidAddress,
    #[error("strategy order ID cannot be represented as a Hyperliquid cloid")]
    UnsupportedClientOrderId,
    #[error("TRADE.XYZ symbol must be an uppercase base asset followed by USDC")]
    InvalidSymbol,
    #[error("Hyperliquid action serialization failed")]
    MessagePack,
    #[error("Hyperliquid action signing failed")]
    Signing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ClientIdentity {
    prefix: u8,
    run: u64,
    level: u8,
    side: OrderSide,
    sequence: u64,
}

pub(crate) fn encode_cloid(client_order_id: &ClientOrderId) -> Result<String, TradeXyzCodecError> {
    let identity = parse_client_identity(client_order_id.as_str())?;
    let side = u128::from(identity.side == OrderSide::Sell);
    let encoded = (CLOID_MAGIC << CLOID_MAGIC_SHIFT)
        | (u128::from(identity.prefix) << 120)
        | (u128::from(identity.run) << 72)
        | (u128::from(identity.level) << 65)
        | (side << 64)
        | u128::from(identity.sequence);
    Ok(format!("0x{encoded:032x}"))
}

pub(crate) fn decode_cloid(value: &str) -> Option<ClientOrderId> {
    let raw = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))?;
    if raw.len() != 32 || !raw.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    let encoded = u128::from_str_radix(raw, 16).ok()?;
    if encoded >> CLOID_MAGIC_SHIFT != CLOID_MAGIC {
        return None;
    }
    let prefix = ((encoded >> 120) & 0b11) as u8;
    let run = ((encoded >> 72) & 0xffff_ffff_ffff) as u64;
    let level = ((encoded >> 65) & 0x7f) as u8;
    let side = if ((encoded >> 64) & 1) == 0 {
        OrderSide::Buy
    } else {
        OrderSide::Sell
    };
    let sequence = encoded as u64;
    let prefix_text = match prefix {
        0 => "o",
        1 => "g",
        2 => "c",
        3 => "r",
        _ => return None,
    };
    let side_text = match side {
        OrderSide::Buy => "B",
        OrderSide::Sell => "S",
    };
    let run_text = format!("{run:012x}");
    let value = match (prefix_text, level) {
        ("o" | "c", NO_LEVEL) => format!("{prefix_text}_{run_text}_{side_text}_{sequence}"),
        ("g" | "r", 0..=100) => {
            format!("{prefix_text}_{run_text}_{level}_{side_text}_{sequence}")
        }
        _ => return None,
    };
    ClientOrderId::parse(value).ok()
}

fn parse_client_identity(value: &str) -> Result<ClientIdentity, TradeXyzCodecError> {
    let parts = value.split('_').collect::<Vec<_>>();
    let (prefix, run, level, side, sequence) = match parts.as_slice() {
        [prefix @ ("o" | "c"), run, side, sequence] => (*prefix, *run, NO_LEVEL, *side, *sequence),
        [prefix @ ("g" | "r"), run, level, side, sequence] => {
            let level = level
                .parse::<u8>()
                .ok()
                .filter(|level| *level <= 100)
                .ok_or(TradeXyzCodecError::UnsupportedClientOrderId)?;
            (*prefix, *run, level, *side, *sequence)
        }
        _ => return Err(TradeXyzCodecError::UnsupportedClientOrderId),
    };
    if run.len() != 12 || !run.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(TradeXyzCodecError::UnsupportedClientOrderId);
    }
    let run =
        u64::from_str_radix(run, 16).map_err(|_| TradeXyzCodecError::UnsupportedClientOrderId)?;
    let prefix = match prefix {
        "o" => 0,
        "g" => 1,
        "c" => 2,
        "r" => 3,
        _ => return Err(TradeXyzCodecError::UnsupportedClientOrderId),
    };
    let side = match side {
        "B" => OrderSide::Buy,
        "S" => OrderSide::Sell,
        _ => return Err(TradeXyzCodecError::UnsupportedClientOrderId),
    };
    let sequence = sequence
        .parse::<u64>()
        .map_err(|_| TradeXyzCodecError::UnsupportedClientOrderId)?;
    Ok(ClientIdentity {
        prefix,
        run,
        level,
        side,
        sequence,
    })
}

pub(crate) fn normalize_address(value: &str) -> Result<String, TradeXyzCodecError> {
    let raw = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .ok_or(TradeXyzCodecError::InvalidAddress)?;
    if raw.len() != 40 || !raw.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(TradeXyzCodecError::InvalidAddress);
    }
    Ok(format!("0x{}", raw.to_ascii_lowercase()))
}

pub(crate) fn exchange_coin(symbol: &str) -> Result<String, TradeXyzCodecError> {
    let base = symbol
        .strip_suffix(QUOTE_ASSET)
        .filter(|base| {
            !base.is_empty()
                && base
                    .bytes()
                    .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
        })
        .ok_or(TradeXyzCodecError::InvalidSymbol)?;
    Ok(format!("{DEX_PREFIX}{base}"))
}

pub(crate) fn local_symbol(coin: &str) -> Result<String, TradeXyzCodecError> {
    let base = coin
        .strip_prefix(DEX_PREFIX)
        .filter(|base| {
            !base.is_empty()
                && base
                    .bytes()
                    .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
        })
        .ok_or(TradeXyzCodecError::InvalidSymbol)?;
    Ok(format!("{base}{QUOTE_ASSET}"))
}

pub(crate) fn wire_decimal(value: Decimal) -> String {
    value.normalize().to_string()
}

pub(crate) fn quantity_step(size_decimals: u32) -> Option<Decimal> {
    (size_decimals <= 28).then(|| Decimal::new(1, size_decimals))
}

pub(crate) fn maximum_decimal_price_tick(size_decimals: u32) -> Option<Decimal> {
    (size_decimals <= 6).then(|| Decimal::new(1, 6 - size_decimals))
}

pub(crate) fn effective_price_tick(mark_price: Decimal, size_decimals: u32) -> Option<Decimal> {
    if mark_price <= Decimal::ZERO || size_decimals > 6 {
        return None;
    }
    let text = mark_price.normalize().to_string();
    let allowed_by_significant_digits = if mark_price >= Decimal::ONE {
        let integer_digits = text.split('.').next()?.trim_start_matches('0').len() as u32;
        5_u32.saturating_sub(integer_digits)
    } else {
        let fraction = text.split('.').nth(1).unwrap_or_default();
        let leading_zeroes = fraction.bytes().take_while(|byte| *byte == b'0').count() as u32;
        leading_zeroes.saturating_add(5)
    };
    let decimals = (6_u32.saturating_sub(size_decimals)).min(allowed_by_significant_digits);
    Some(Decimal::new(1, decimals))
}

pub(crate) fn valid_price(value: Decimal, size_decimals: u32) -> bool {
    if value <= Decimal::ZERO || size_decimals > 6 {
        return false;
    }
    let normalized = value.normalize();
    if normalized.fract().is_zero() {
        return true;
    }
    if normalized.scale() > 6_u32.saturating_sub(size_decimals) {
        return false;
    }
    significant_digits(normalized) <= 5
}

fn significant_digits(value: Decimal) -> usize {
    let text = value.abs().normalize().to_string();
    text.bytes()
        .filter(|byte| byte.is_ascii_digit())
        .skip_while(|byte| *byte == b'0')
        .count()
}

#[derive(Clone)]
pub(crate) struct HyperliquidSigner {
    signing_key: SigningKey,
    address: String,
}

impl HyperliquidSigner {
    pub(crate) fn from_private_key(value: &str) -> Result<Self, TradeXyzCodecError> {
        let raw = value
            .strip_prefix("0x")
            .or_else(|| value.strip_prefix("0X"))
            .unwrap_or(value);
        if raw.len() != 64 || !raw.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(TradeXyzCodecError::InvalidPrivateKey);
        }
        let bytes = hex::decode(raw).map_err(|_| TradeXyzCodecError::InvalidPrivateKey)?;
        let signing_key =
            SigningKey::from_slice(&bytes).map_err(|_| TradeXyzCodecError::InvalidPrivateKey)?;
        let public_key = signing_key.verifying_key().to_encoded_point(false);
        let digest = Keccak256::digest(&public_key.as_bytes()[1..]);
        let address = format!("0x{}", hex::encode(&digest[12..]));
        Ok(Self {
            signing_key,
            address,
        })
    }

    pub(crate) fn address(&self) -> &str {
        &self.address
    }

    pub(crate) fn sign_action<A: Serialize>(
        &self,
        action: &A,
        nonce: u64,
        mainnet: bool,
    ) -> Result<HyperliquidSignature, TradeXyzCodecError> {
        let mut encoded =
            rmp_serde::to_vec_named(action).map_err(|_| TradeXyzCodecError::MessagePack)?;
        encoded.extend_from_slice(&nonce.to_be_bytes());
        encoded.push(0); // No vault address.
        let connection_id: [u8; 32] = Keccak256::digest(encoded).into();
        let digest = agent_eip712_digest(connection_id, mainnet);
        let (signature, recovery_id) = self
            .signing_key
            .sign_prehash_recoverable(&digest)
            .map_err(|_| TradeXyzCodecError::Signing)?;
        HyperliquidSignature::from_parts(signature, recovery_id.to_byte())
    }
}

impl std::fmt::Debug for HyperliquidSigner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HyperliquidSigner")
            .field("address", &self.address)
            .field("private_key", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct HyperliquidSignature {
    pub r: String,
    pub s: String,
    pub v: u8,
}

impl HyperliquidSignature {
    fn from_parts(signature: EcdsaSignature, recovery_id: u8) -> Result<Self, TradeXyzCodecError> {
        if recovery_id > 1 {
            return Err(TradeXyzCodecError::Signing);
        }
        let bytes = signature.to_bytes();
        Ok(Self {
            r: format!("0x{}", hex::encode(&bytes[..32])),
            s: format!("0x{}", hex::encode(&bytes[32..])),
            v: 27 + recovery_id,
        })
    }

    #[cfg(test)]
    fn joined_hex(&self) -> String {
        format!(
            "0x{}{}{:02x}",
            self.r.trim_start_matches("0x"),
            self.s.trim_start_matches("0x"),
            self.v
        )
    }
}

fn agent_eip712_digest(connection_id: [u8; 32], mainnet: bool) -> [u8; 32] {
    let domain_type = keccak(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
    );
    let agent_type = keccak(b"Agent(string source,bytes32 connectionId)");
    let mut domain = Vec::with_capacity(160);
    domain.extend_from_slice(&domain_type);
    domain.extend_from_slice(&keccak(b"Exchange"));
    domain.extend_from_slice(&keccak(b"1"));
    let mut chain_id = [0_u8; 32];
    chain_id[30..].copy_from_slice(&1337_u16.to_be_bytes());
    domain.extend_from_slice(&chain_id);
    domain.extend_from_slice(&[0_u8; 32]);
    let domain_hash = keccak(&domain);

    let mut agent = Vec::with_capacity(96);
    agent.extend_from_slice(&agent_type);
    agent.extend_from_slice(&keccak(if mainnet { b"a" } else { b"b" }));
    agent.extend_from_slice(&connection_id);
    let agent_hash = keccak(&agent);

    let mut message = Vec::with_capacity(66);
    message.extend_from_slice(b"\x19\x01");
    message.extend_from_slice(&domain_hash);
    message.extend_from_slice(&agent_hash);
    keccak(&message)
}

fn keccak(value: &[u8]) -> [u8; 32] {
    Keccak256::digest(value).into()
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct OrderAction {
    #[serde(rename = "type")]
    action_type: &'static str,
    pub orders: Vec<WireOrder>,
    grouping: &'static str,
}

impl OrderAction {
    pub(crate) fn single(order: WireOrder) -> Self {
        Self {
            action_type: "order",
            orders: vec![order],
            grouping: "na",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct WireOrder {
    #[serde(rename = "a")]
    pub asset: u32,
    #[serde(rename = "b")]
    pub is_buy: bool,
    #[serde(rename = "p")]
    pub price: String,
    #[serde(rename = "s")]
    pub size: String,
    #[serde(rename = "r")]
    pub reduce_only: bool,
    #[serde(rename = "t")]
    pub order_type: WireOrderType,
    #[serde(rename = "c", skip_serializing_if = "Option::is_none")]
    pub cloid: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct WireOrderType {
    pub limit: WireLimit,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct WireLimit {
    pub tif: &'static str,
}

impl WireOrderType {
    pub(crate) fn gtc() -> Self {
        Self {
            limit: WireLimit { tif: "Gtc" },
        }
    }

    pub(crate) fn post_only() -> Self {
        Self {
            limit: WireLimit { tif: "Alo" },
        }
    }

    pub(crate) fn immediate_or_cancel() -> Self {
        Self {
            limit: WireLimit { tif: "Ioc" },
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct CancelAction {
    #[serde(rename = "type")]
    action_type: &'static str,
    pub cancels: Vec<WireCancel>,
}

impl CancelAction {
    pub(crate) fn single(asset: u32, order_id: u64) -> Self {
        Self {
            action_type: "cancel",
            cancels: vec![WireCancel { asset, order_id }],
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct WireCancel {
    #[serde(rename = "a")]
    asset: u32,
    #[serde(rename = "o")]
    order_id: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct UpdateLeverageAction {
    #[serde(rename = "type")]
    action_type: &'static str,
    pub asset: u32,
    #[serde(rename = "isCross")]
    pub is_cross: bool,
    pub leverage: u16,
}

impl UpdateLeverageAction {
    pub(crate) fn isolated(asset: u32, leverage: u16) -> Self {
        Self {
            action_type: "updateLeverage",
            asset,
            is_cross: false,
            leverage,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn official_public_test_vector_key() -> String {
        hex::encode([
            0xe9, 0x08, 0xf8, 0x6d, 0xbb, 0x4d, 0x55, 0xac, 0x87, 0x63, 0x78, 0x56, 0x5a, 0xaf,
            0xea, 0xbc, 0x18, 0x7f, 0x66, 0x90, 0xf0, 0x46, 0x45, 0x93, 0x97, 0xb1, 0x7d, 0x9b,
            0x9a, 0x19, 0x68, 0x8e,
        ])
    }

    fn client(value: &str) -> ClientOrderId {
        ClientOrderId::parse(value).unwrap()
    }

    #[test]
    fn cloid_round_trip_preserves_every_strategy_identity_component() {
        for value in [
            "o_012345abcdef_S_1",
            "g_012345abcdef_0_B_2",
            "g_ffffffffffff_100_S_999999999999999",
            "c_012345abcdef_B_3",
            "r_012345abcdef_77_S_4",
        ] {
            let encoded = encode_cloid(&client(value)).unwrap();
            assert_eq!(encoded.len(), 34);
            assert_eq!(decode_cloid(&encoded), Some(client(value)));
        }
    }

    #[test]
    fn manual_and_noncanonical_cloids_are_never_adopted() {
        assert_eq!(decode_cloid("0x00000000000000000000000000000001"), None);
        assert!(ClientOrderId::parse("g_ffffffffffff_100_S_1000000000000000").is_err());
        assert!(encode_cloid(&client("g_LEGACY001_1_B_2")).is_err());
        assert!(encode_cloid(&client("o_012345abcdef_1_B_2")).is_err());
    }

    #[test]
    fn official_hyperliquid_order_signature_vector_matches_byte_for_byte() {
        let signer =
            HyperliquidSigner::from_private_key(&official_public_test_vector_key()).unwrap();
        let action = OrderAction::single(WireOrder {
            asset: 1,
            is_buy: true,
            price: "2000.0".into(),
            size: "3.5".into(),
            reduce_only: false,
            order_type: WireOrderType::immediate_or_cancel(),
            cloid: None,
        });
        let signature = signer.sign_action(&action, 1_583_838, true).unwrap();
        assert_eq!(
            signature.joined_hex(),
            "0x77957e58e70f43b6b68581f2dc42011fc384538a2e5b7bf42d5b936f19fbb67360721a8598727230f67080efee48c812a6a4442013fd3b0eed509171bef9f23f1c"
        );
    }

    #[test]
    fn symbol_and_precision_contracts_are_strict() {
        assert_eq!(exchange_coin("MUUSDC").unwrap(), "xyz:MU");
        assert_eq!(local_symbol("xyz:MU").unwrap(), "MUUSDC");
        assert!(exchange_coin("MUUSDT").is_err());
        assert!(valid_price(Decimal::new(12345, 4), 2));
        assert!(!valid_price(Decimal::new(123456, 5), 2));
        assert_eq!(quantity_step(3), Some(Decimal::new(1, 3)));
        assert_eq!(maximum_decimal_price_tick(3), Some(Decimal::new(1, 3)));
    }
}
