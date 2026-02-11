//! Validator gRPC protocol types for direct submission to Sui validators.
//!
//! These types mirror Sui's `sui.validator.Validator` gRPC service, which is exposed
//! on port 8080 and uses ProstCodec (standard protobuf). The service is defined
//! programmatically in Sui's `build.rs` — no `.proto` file exists.
//!
//! Reference: `sui-types/src/messages_grpc.rs`

use tonic::transport::Channel;

// =========== SubmitTransaction types ===========

/// Submit transaction request. Matches Sui's `RawSubmitTxRequest`.
#[derive(Clone, prost::Message)]
pub struct RawSubmitTxRequest {
    /// BCS-serialized `Transaction` objects (Envelope<SenderSignedData, EmptySignInfo>).
    #[prost(bytes = "vec", repeated, tag = "1")]
    pub transactions: Vec<Vec<u8>>,

    /// The type of submission.
    #[prost(enumeration = "SubmitTxType", tag = "2")]
    pub submit_type: i32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum SubmitTxType {
    Default = 0,
    Ping = 1,
    SoftBundle = 2,
}

/// Submit transaction response. Matches Sui's `RawSubmitTxResponse`.
#[derive(Clone, prost::Message)]
pub struct RawSubmitTxResponse {
    #[prost(message, repeated, tag = "1")]
    pub results: Vec<RawSubmitTxResult>,
}

#[derive(Clone, prost::Message)]
pub struct RawSubmitTxResult {
    #[prost(oneof = "RawValidatorSubmitStatus", tags = "1, 2, 3")]
    pub inner: Option<RawValidatorSubmitStatus>,
}

#[derive(Clone, prost::Oneof)]
pub enum RawValidatorSubmitStatus {
    /// Transaction submitted to consensus. Value is BCS-serialized ConsensusPosition.
    #[prost(bytes = "vec", tag = "1")]
    Submitted(Vec<u8>),

    /// Transaction already executed (finalized).
    #[prost(message, tag = "2")]
    Executed(RawExecutedStatus),

    /// Transaction rejected from consensus submission.
    #[prost(message, tag = "3")]
    Rejected(RawRejectedStatus),
}

#[derive(Clone, prost::Message)]
pub struct RawExecutedStatus {
    /// BCS-serialized TransactionEffectsDigest.
    #[prost(bytes = "vec", tag = "1")]
    pub effects_digest: Vec<u8>,

    #[prost(message, optional, tag = "2")]
    pub details: Option<RawExecutedData>,

    #[prost(bool, tag = "3")]
    pub fast_path: bool,
}

#[derive(Clone, prost::Message)]
pub struct RawExecutedData {
    /// BCS-serialized TransactionEffects.
    #[prost(bytes = "vec", tag = "1")]
    pub effects: Vec<u8>,

    /// BCS-serialized TransactionEvents.
    #[prost(bytes = "vec", optional, tag = "2")]
    pub events: Option<Vec<u8>>,

    #[prost(bytes = "vec", repeated, tag = "3")]
    pub input_objects: Vec<Vec<u8>>,

    #[prost(bytes = "vec", repeated, tag = "4")]
    pub output_objects: Vec<Vec<u8>>,
}

#[derive(Clone, prost::Message)]
pub struct RawRejectedStatus {
    /// BCS-serialized SuiError.
    #[prost(bytes = "vec", optional, tag = "1")]
    pub error: Option<Vec<u8>>,
}

// =========== WaitForEffects types ===========

/// Wait for effects request. Matches Sui's `RawWaitForEffectsRequest`.
#[derive(Clone, prost::Message)]
pub struct RawWaitForEffectsRequest {
    /// BCS-serialized TransactionDigest (32 bytes).
    #[prost(bytes = "vec", optional, tag = "1")]
    pub transaction_digest: Option<Vec<u8>>,

    /// BCS-serialized ConsensusPosition (from SubmitTransaction response).
    #[prost(bytes = "vec", optional, tag = "2")]
    pub consensus_position: Option<Vec<u8>>,

    /// Whether to include effects content, events, input/output objects.
    #[prost(bool, tag = "3")]
    pub include_details: bool,

    #[prost(enumeration = "PingType", optional, tag = "4")]
    pub ping_type: Option<i32>,
}

/// Wait for effects response. Matches Sui's `RawWaitForEffectsResponse`.
#[derive(Clone, prost::Message)]
pub struct RawWaitForEffectsResponse {
    #[prost(oneof = "RawValidatorTransactionStatus", tags = "1, 2, 3")]
    pub inner: Option<RawValidatorTransactionStatus>,
}

#[derive(Clone, prost::Oneof)]
pub enum RawValidatorTransactionStatus {
    #[prost(message, tag = "1")]
    Executed(RawExecutedStatus),

    #[prost(message, tag = "2")]
    Rejected(RawRejectedStatus),

    #[prost(message, tag = "3")]
    Expired(RawExpiredStatus),
}

#[derive(Clone, prost::Message)]
pub struct RawExpiredStatus {
    #[prost(uint64, tag = "1")]
    pub epoch: u64,

    #[prost(uint32, optional, tag = "2")]
    pub round: Option<u32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum PingType {
    Consensus = 0,
    FastPath = 1,
}

// =========== Manual tonic gRPC client ===========

/// Client for the `sui.validator.Validator` gRPC service (port 8080, plaintext).
///
/// This is a manual tonic client — no `.proto` file needed. The service uses
/// ProstCodec (standard protobuf) and `.allow_insecure(true)` on the server side.
pub struct SuiValidatorClient {
    inner: tonic::client::Grpc<Channel>,
}

impl SuiValidatorClient {
    pub fn new(channel: Channel) -> Self {
        let inner = tonic::client::Grpc::new(channel);
        Self { inner }
    }

    /// Call `sui.validator.Validator/SubmitTransaction`.
    pub async fn submit_transaction(
        &mut self,
        request: impl tonic::IntoRequest<RawSubmitTxRequest>,
    ) -> Result<tonic::Response<RawSubmitTxResponse>, tonic::Status> {
        self.inner
            .ready()
            .await
            .map_err(|e| tonic::Status::unknown(format!("Service not ready: {}", e)))?;
        let codec = tonic::codec::ProstCodec::default();
        let path = tonic::codegen::http::uri::PathAndQuery::from_static(
            "/sui.validator.Validator/SubmitTransaction",
        );
        self.inner.unary(request.into_request(), path, codec).await
    }

    /// Call `sui.validator.Validator/WaitForEffects`.
    pub async fn wait_for_effects(
        &mut self,
        request: impl tonic::IntoRequest<RawWaitForEffectsRequest>,
    ) -> Result<tonic::Response<RawWaitForEffectsResponse>, tonic::Status> {
        self.inner
            .ready()
            .await
            .map_err(|e| tonic::Status::unknown(format!("Service not ready: {}", e)))?;
        let codec = tonic::codec::ProstCodec::default();
        let path = tonic::codegen::http::uri::PathAndQuery::from_static(
            "/sui.validator.Validator/WaitForEffects",
        );
        self.inner.unary(request.into_request(), path, codec).await
    }
}

// =========== BCS Transaction builder ===========

/// Compute the Sui TransactionDigest from raw TransactionData BCS bytes.
///
/// Sui's `Signable` trait (in `sui-types/src/crypto.rs`) hashes as:
///   `Blake2b-256("TransactionData::" || tx_data_bcs)`
///
/// The input is the raw TransactionData BCS from the PTB builder, NOT the full
/// Transaction envelope (which includes intent prefix, signatures, vec wrappers).
///
/// Returns base58-encoded digest string (for display/logging).
pub fn tx_digest_of_tx_data(tx_data_bcs: &[u8]) -> String {
    crate::signer::bs58_encode(&tx_digest_hash(tx_data_bcs))
}

/// Compute the raw 32-byte TransactionDigest hash from TransactionData BCS bytes.
///
/// Uses Sui's `Signable` trait convention: `Blake2b-256("TransactionData::" || bcs_data)`.
fn tx_digest_hash(tx_data_bcs: &[u8]) -> Vec<u8> {
    use blake2::{digest::consts::U32, Blake2b, Digest};
    type Blake2b256 = Blake2b<U32>;
    let mut hasher = Blake2b256::new();
    hasher.update(b"TransactionData::");
    hasher.update(tx_data_bcs);
    hasher.finalize().to_vec()
}

/// BCS-serialize a TransactionDigest from raw TransactionData BCS bytes.
///
/// Computes `tx_digest_hash(tx_data_bcs)` and wraps it in BCS format:
/// ULEB128(32) + 32 hash bytes = 33 bytes total.
///
/// Sui's `Digest` type uses `serde_with::Bytes` for BCS serialization, which calls
/// `serialize_bytes()`. BCS encodes `serialize_bytes` as ULEB128(length) + raw bytes.
pub fn bcs_serialize_digest(tx_data_bcs: &[u8]) -> Vec<u8> {
    let hash = tx_digest_hash(tx_data_bcs);
    let mut out = Vec::with_capacity(1 + hash.len());
    encode_uleb128(&mut out, hash.len());
    out.extend_from_slice(&hash);
    out
}

/// Construct BCS-serialized `Transaction` (`Envelope<SenderSignedData, EmptySignInfo>`)
/// from raw `TransactionData` BCS bytes + signature bytes.
///
/// BCS layout:
/// ```text
///   01                      // SenderSignedData: Vec length = 1
///   00 00 00                // Intent: scope=0, version=0, app_id=0
///   [tx_data_bcs...]        // TransactionData (already BCS from PTB builder)
///   01                      // Vec<GenericSignature> length = 1
///   ULEB128(sig_len)        // signature byte length (97 for Ed25519)
///   [sig_bytes...]          // flag(1) + ed25519_sig(64) + pubkey(32)
/// ```
pub fn build_bcs_transaction(tx_data_bcs: &[u8], sig_bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 3 + tx_data_bcs.len() + 1 + 2 + sig_bytes.len());

    // SenderSignedData: Vec<SenderSignedTransaction> with 1 element
    out.push(0x01);

    // IntentMessage<TransactionData>
    // Intent: scope=TransactionData(0), version=V0(0), app_id=Sui(0)
    out.extend_from_slice(&[0x00, 0x00, 0x00]);

    // TransactionData (already BCS-encoded)
    out.extend_from_slice(tx_data_bcs);

    // tx_signatures: Vec<GenericSignature> with 1 element
    out.push(0x01);

    // GenericSignature serialized as BCS bytes: ULEB128(len) + raw bytes
    encode_uleb128(&mut out, sig_bytes.len());
    out.extend_from_slice(sig_bytes);

    // EmptySignInfo: zero-size, nothing to serialize

    out
}

/// BCS-serialize an already-computed 32-byte digest.
///
/// Wraps the raw digest bytes as ULEB128(32) + 32 bytes = 33 bytes total.
/// Used for `WaitForEffectsRequest.transaction_digest`.
pub fn bcs_wrap_digest(digest: &[u8; 32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(33);
    encode_uleb128(&mut out, 32);
    out.extend_from_slice(digest);
    out
}

/// Encode a usize as ULEB128 (unsigned LEB128).
fn encode_uleb128(buf: &mut Vec<u8>, mut value: usize) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            buf.push(byte);
            break;
        } else {
            buf.push(byte | 0x80);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uleb128_single_byte() {
        let mut buf = Vec::new();
        encode_uleb128(&mut buf, 0);
        assert_eq!(buf, vec![0x00]);

        let mut buf = Vec::new();
        encode_uleb128(&mut buf, 1);
        assert_eq!(buf, vec![0x01]);

        let mut buf = Vec::new();
        encode_uleb128(&mut buf, 97);
        assert_eq!(buf, vec![0x61]);

        let mut buf = Vec::new();
        encode_uleb128(&mut buf, 127);
        assert_eq!(buf, vec![0x7f]);
    }

    #[test]
    fn test_uleb128_multi_byte() {
        let mut buf = Vec::new();
        encode_uleb128(&mut buf, 128);
        assert_eq!(buf, vec![0x80, 0x01]);

        let mut buf = Vec::new();
        encode_uleb128(&mut buf, 300);
        assert_eq!(buf, vec![0xac, 0x02]);
    }

    #[test]
    fn test_build_bcs_transaction_structure() {
        let tx_data = vec![0xaa, 0xbb, 0xcc]; // fake 3-byte TransactionData
        let sig = vec![0x00; 97]; // Ed25519 sig: flag(0) + sig(64) + pubkey(32)

        let bcs_tx = build_bcs_transaction(&tx_data, &sig);

        // [0]: 0x01 (vec len = 1)
        assert_eq!(bcs_tx[0], 0x01);
        // [1..4]: intent [0, 0, 0]
        assert_eq!(&bcs_tx[1..4], &[0x00, 0x00, 0x00]);
        // [4..7]: tx_data
        assert_eq!(&bcs_tx[4..7], &[0xaa, 0xbb, 0xcc]);
        // [7]: 0x01 (signatures vec len = 1)
        assert_eq!(bcs_tx[7], 0x01);
        // [8]: 0x61 (ULEB128(97))
        assert_eq!(bcs_tx[8], 0x61);
        // [9..106]: 97 bytes of signature
        assert_eq!(bcs_tx[9..].len(), 97);
        // Total: 1 + 3 + 3 + 1 + 1 + 97 = 106
        assert_eq!(bcs_tx.len(), 106);
    }

    #[test]
    fn test_bcs_serialize_digest() {
        // Digest is computed from raw TransactionData BCS, not full envelope
        let tx_data = vec![0xaa; 50];

        let bcs_digest = bcs_serialize_digest(&tx_data);

        // Should be 33 bytes: ULEB128(32) + 32 hash bytes
        assert_eq!(bcs_digest.len(), 33);
        // First byte is ULEB128(32) = 0x20
        assert_eq!(bcs_digest[0], 0x20);
        // Remaining 32 bytes are the Blake2b-256 hash (with type prefix)
        assert_eq!(&bcs_digest[1..], &tx_digest_hash(&tx_data)[..]);
    }

    #[test]
    fn test_tx_digest_of_tx_data_deterministic() {
        let tx_data = vec![0xbb; 100];

        let digest1 = tx_digest_of_tx_data(&tx_data);
        let digest2 = tx_digest_of_tx_data(&tx_data);
        assert_eq!(digest1, digest2);
        assert!(!digest1.is_empty());
    }

    #[test]
    fn test_tx_digest_has_type_prefix() {
        use blake2::{digest::consts::U32, Blake2b, Digest};
        type Blake2b256 = Blake2b<U32>;

        let tx_data = vec![0xcc; 64];

        // Manually compute with the Signable type prefix
        let mut hasher = Blake2b256::new();
        hasher.update(b"TransactionData::");
        hasher.update(&tx_data);
        let expected = hasher.finalize().to_vec();

        // Our function should produce the same hash
        let actual = tx_digest_hash(&tx_data);
        assert_eq!(actual, expected);

        // Verify it differs from a plain hash (without prefix)
        let plain_hash = Blake2b256::digest(&tx_data).to_vec();
        assert_ne!(actual, plain_hash, "Digest must include type prefix");
    }

    #[test]
    fn test_build_bcs_transaction_large_signature() {
        // Test with a signature > 127 bytes to exercise multi-byte ULEB128
        let tx_data = vec![0xff; 10];
        let sig = vec![0x42; 200]; // 200 bytes, ULEB128 = [0xc8, 0x01]

        let bcs_tx = build_bcs_transaction(&tx_data, &sig);

        // 1 + 3 + 10 + 1 + 2 + 200 = 217
        assert_eq!(bcs_tx.len(), 217);
        // ULEB128(200) = [0xc8, 0x01]
        assert_eq!(bcs_tx[15], 0xc8);
        assert_eq!(bcs_tx[16], 0x01);
    }

    #[test]
    fn test_submit_tx_type_values() {
        assert_eq!(SubmitTxType::Default as i32, 0);
        assert_eq!(SubmitTxType::Ping as i32, 1);
        assert_eq!(SubmitTxType::SoftBundle as i32, 2);
    }

    #[test]
    fn test_raw_submit_tx_request_prost_roundtrip() {
        use prost::Message;

        let request = RawSubmitTxRequest {
            transactions: vec![vec![1, 2, 3]],
            submit_type: SubmitTxType::Default as i32,
        };

        let encoded = request.encode_to_vec();
        let decoded = RawSubmitTxRequest::decode(encoded.as_slice()).unwrap();

        assert_eq!(decoded.transactions, vec![vec![1u8, 2, 3]]);
        assert_eq!(decoded.submit_type, 0);
    }

    #[test]
    fn test_raw_submit_tx_response_submitted() {
        use prost::Message;

        let response = RawSubmitTxResponse {
            results: vec![RawSubmitTxResult {
                inner: Some(RawValidatorSubmitStatus::Submitted(vec![0xde, 0xad])),
            }],
        };

        let encoded = response.encode_to_vec();
        let decoded = RawSubmitTxResponse::decode(encoded.as_slice()).unwrap();

        assert_eq!(decoded.results.len(), 1);
        match &decoded.results[0].inner {
            Some(RawValidatorSubmitStatus::Submitted(pos)) => assert_eq!(pos, &[0xde, 0xad]),
            other => panic!("Expected Submitted, got {:?}", other),
        }
    }

    #[test]
    fn test_raw_submit_tx_response_executed() {
        use prost::Message;

        let response = RawSubmitTxResponse {
            results: vec![RawSubmitTxResult {
                inner: Some(RawValidatorSubmitStatus::Executed(RawExecutedStatus {
                    effects_digest: vec![0x42; 32],
                    details: None,
                    fast_path: true,
                })),
            }],
        };

        let encoded = response.encode_to_vec();
        let decoded = RawSubmitTxResponse::decode(encoded.as_slice()).unwrap();

        match &decoded.results[0].inner {
            Some(RawValidatorSubmitStatus::Executed(status)) => {
                assert_eq!(status.effects_digest, vec![0x42; 32]);
                assert!(status.fast_path);
                assert!(status.details.is_none());
            }
            other => panic!("Expected Executed, got {:?}", other),
        }
    }

    #[test]
    fn test_raw_submit_tx_response_rejected() {
        use prost::Message;

        let response = RawSubmitTxResponse {
            results: vec![RawSubmitTxResult {
                inner: Some(RawValidatorSubmitStatus::Rejected(RawRejectedStatus {
                    error: Some(vec![0xff]),
                })),
            }],
        };

        let encoded = response.encode_to_vec();
        let decoded = RawSubmitTxResponse::decode(encoded.as_slice()).unwrap();

        match &decoded.results[0].inner {
            Some(RawValidatorSubmitStatus::Rejected(status)) => {
                assert_eq!(status.error, Some(vec![0xff]));
            }
            other => panic!("Expected Rejected, got {:?}", other),
        }
    }

    #[test]
    fn test_wait_for_effects_request_roundtrip() {
        use prost::Message;

        let request = RawWaitForEffectsRequest {
            transaction_digest: Some(vec![0x42; 32]),
            consensus_position: Some(vec![0xde, 0xad]),
            include_details: true,
            ping_type: None,
        };

        let encoded = request.encode_to_vec();
        let decoded = RawWaitForEffectsRequest::decode(encoded.as_slice()).unwrap();

        assert_eq!(decoded.transaction_digest, Some(vec![0x42; 32]));
        assert_eq!(decoded.consensus_position, Some(vec![0xde, 0xad]));
        assert!(decoded.include_details);
        assert!(decoded.ping_type.is_none());
    }

    #[test]
    fn test_wait_for_effects_response_executed() {
        use prost::Message;

        let response = RawWaitForEffectsResponse {
            inner: Some(RawValidatorTransactionStatus::Executed(RawExecutedStatus {
                effects_digest: vec![0x42; 32],
                details: Some(RawExecutedData {
                    effects: vec![1, 2, 3],
                    events: Some(vec![4, 5, 6]),
                    input_objects: vec![],
                    output_objects: vec![],
                }),
                fast_path: false,
            })),
        };

        let encoded = response.encode_to_vec();
        let decoded = RawWaitForEffectsResponse::decode(encoded.as_slice()).unwrap();

        match decoded.inner {
            Some(RawValidatorTransactionStatus::Executed(status)) => {
                assert_eq!(status.effects_digest, vec![0x42; 32]);
                let details = status.details.unwrap();
                assert_eq!(details.effects, vec![1, 2, 3]);
                assert_eq!(details.events, Some(vec![4, 5, 6]));
                assert!(!status.fast_path);
            }
            other => panic!("Expected Executed, got {:?}", other),
        }
    }

    #[test]
    fn test_wait_for_effects_response_expired() {
        use prost::Message;

        let response = RawWaitForEffectsResponse {
            inner: Some(RawValidatorTransactionStatus::Expired(RawExpiredStatus {
                epoch: 42,
                round: Some(100),
            })),
        };

        let encoded = response.encode_to_vec();
        let decoded = RawWaitForEffectsResponse::decode(encoded.as_slice()).unwrap();

        match decoded.inner {
            Some(RawValidatorTransactionStatus::Expired(status)) => {
                assert_eq!(status.epoch, 42);
                assert_eq!(status.round, Some(100));
            }
            other => panic!("Expected Expired, got {:?}", other),
        }
    }

    #[test]
    fn test_wait_for_effects_response_rejected() {
        use prost::Message;

        let response = RawWaitForEffectsResponse {
            inner: Some(RawValidatorTransactionStatus::Rejected(RawRejectedStatus {
                error: None,
            })),
        };

        let encoded = response.encode_to_vec();
        let decoded = RawWaitForEffectsResponse::decode(encoded.as_slice()).unwrap();

        match decoded.inner {
            Some(RawValidatorTransactionStatus::Rejected(status)) => {
                assert!(status.error.is_none());
            }
            other => panic!("Expected Rejected, got {:?}", other),
        }
    }

    #[test]
    fn test_executed_data_with_objects() {
        use prost::Message;

        let data = RawExecutedData {
            effects: vec![1, 2, 3],
            events: Some(vec![4, 5]),
            input_objects: vec![vec![10, 11], vec![12, 13]],
            output_objects: vec![vec![20, 21]],
        };

        let encoded = data.encode_to_vec();
        let decoded = RawExecutedData::decode(encoded.as_slice()).unwrap();

        assert_eq!(decoded.effects, vec![1, 2, 3]);
        assert_eq!(decoded.events, Some(vec![4, 5]));
        assert_eq!(decoded.input_objects.len(), 2);
        assert_eq!(decoded.output_objects.len(), 1);
    }
}
