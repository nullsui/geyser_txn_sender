//! Minimal BCS-decodable types matching Sui's on-chain serialization layout.
//!
//! These types are used to decode `TransactionEffects` and `TransactionEvents`
//! from the raw BCS bytes returned by validators, without pulling in the full
//! `sui-types` crate (~200k lines).
//!
//! BCS is order-dependent: field order and enum variant order must exactly match
//! the upstream Sui definitions. Sourced from `sui-types` on GitHub.

use serde::Deserialize;
use std::fmt;

// ============================================================
// Primitive type aliases
// ============================================================

pub type EpochId = u64;
pub type SequenceNumber = u64;
pub type CodeOffset = u16;
pub type CommandIndex = u64;

// ============================================================
// Digest types — BCS-serialized with length prefix (Bytes annotation)
// ============================================================

/// Generic 32-byte digest. Sui's `Digest` uses `serde_with::Bytes` for BCS,
/// which means ULEB128(32) + 32 raw bytes in the BCS stream.
/// We deserialize as `Vec<u8>` which matches this layout.
#[derive(Clone, Debug, Deserialize)]
pub struct Digest(pub Vec<u8>);

#[derive(Clone, Debug, Deserialize)]
pub struct TransactionDigest(pub Digest);

#[derive(Clone, Debug, Deserialize)]
pub struct ObjectDigest(pub Digest);

#[derive(Clone, Debug, Deserialize)]
pub struct TransactionEventsDigest(pub Digest);

#[derive(Clone, Debug, Deserialize)]
pub struct EffectsAuxDataDigest(pub Digest);

/// (SequenceNumber, ObjectDigest) — used in V2 effects
pub type VersionDigest = (SequenceNumber, ObjectDigest);

// ============================================================
// Address types — BCS-serialized as raw fixed [u8; 32] (no length prefix)
// ============================================================

/// Move's AccountAddress — 32 raw bytes in BCS.
#[derive(Clone, Debug, Deserialize)]
pub struct AccountAddress(pub [u8; 32]);

/// Sui object ID, wraps AccountAddress.
#[derive(Clone, Debug, Deserialize)]
pub struct ObjectID(pub AccountAddress);

/// Sui address — 32 raw bytes in BCS.
#[derive(Clone, Debug, Deserialize)]
pub struct SuiAddress(pub [u8; 32]);

/// (ObjectID, SequenceNumber, ObjectDigest)
pub type ObjectRef = (ObjectID, SequenceNumber, ObjectDigest);

// ============================================================
// Owner
// ============================================================

#[derive(Clone, Debug, Deserialize)]
pub enum Owner {
    AddressOwner(SuiAddress),
    ObjectOwner(SuiAddress),
    Shared {
        initial_shared_version: SequenceNumber,
    },
    Immutable,
    ConsensusAddressOwner {
        start_version: SequenceNumber,
        owner: SuiAddress,
    },
}

// ============================================================
// Gas
// ============================================================

#[derive(Clone, Debug, Deserialize)]
pub struct GasCostSummary {
    pub computation_cost: u64,
    pub storage_cost: u64,
    pub storage_rebate: u64,
    pub non_refundable_storage_fee: u64,
}

// ============================================================
// Execution status
// ============================================================

#[derive(Clone, Debug, Deserialize)]
pub enum ExecutionStatus {
    Success,
    Failure {
        error: ExecutionFailureStatus,
        command: Option<CommandIndex>,
    },
}

/// All 42 variants in exact BCS order. Must match upstream `sui-types`.
#[derive(Clone, Debug, Deserialize)]
pub enum ExecutionFailureStatus {
    InsufficientGas,
    InvalidGasObject,
    InvariantViolation,
    FeatureNotYetSupported,
    MoveObjectTooBig {
        object_size: u64,
        max_object_size: u64,
    },
    MovePackageTooBig {
        object_size: u64,
        max_object_size: u64,
    },
    CircularObjectOwnership {
        object: ObjectID,
    },
    InsufficientCoinBalance,
    CoinBalanceOverflow,
    PublishErrorNonZeroAddress,
    SuiMoveVerificationError,
    MovePrimitiveRuntimeError(MoveLocationOpt),
    MoveAbort(MoveLocation, u64),
    VMVerificationOrDeserializationError,
    VMInvariantViolation,
    FunctionNotFound,
    ArityMismatch,
    TypeArityMismatch,
    NonEntryFunctionInvoked,
    CommandArgumentError {
        arg_idx: u16,
        kind: CommandArgumentError,
    },
    TypeArgumentError {
        argument_idx: u16,
        kind: TypeArgumentError,
    },
    UnusedValueWithoutDrop {
        result_idx: u16,
        secondary_idx: u16,
    },
    InvalidPublicFunctionReturnType {
        idx: u16,
    },
    InvalidTransferObject,
    EffectsTooLarge {
        current_size: u64,
        max_size: u64,
    },
    PublishUpgradeMissingDependency,
    PublishUpgradeDependencyDowngrade,
    PackageUpgradeError {
        upgrade_error: PackageUpgradeError,
    },
    WrittenObjectsTooLarge {
        current_size: u64,
        max_size: u64,
    },
    CertificateDenied,
    SuiMoveVerificationTimedout,
    SharedObjectOperationNotAllowed,
    InputObjectDeleted,
    ExecutionCancelledDueToSharedObjectCongestion {
        congested_objects: CongestedObjects,
    },
    AddressDeniedForCoin {
        address: SuiAddress,
        coin_type: String,
    },
    CoinTypeGlobalPause {
        coin_type: String,
    },
    ExecutionCancelledDueToRandomnessUnavailable,
    MoveVectorElemTooBig {
        value_size: u64,
        max_scaled_size: u64,
    },
    MoveRawValueTooBig {
        value_size: u64,
        max_scaled_size: u64,
    },
    InvalidLinkage,
    InsufficientFundsForWithdraw,
    NonExclusiveWriteInputObjectModified {
        id: ObjectID,
    },
}

#[derive(Clone, Debug, Deserialize)]
pub struct MoveLocation {
    pub module: ModuleId,
    pub function: u16,
    pub instruction: CodeOffset,
    pub function_name: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct MoveLocationOpt(pub Option<MoveLocation>);

#[derive(Clone, Debug, Deserialize)]
pub enum CommandArgumentError {
    TypeMismatch,
    InvalidBCSBytes,
    InvalidUsageOfPureArg,
    InvalidArgumentToPrivateEntryFunction,
    IndexOutOfBounds { idx: u16 },
    SecondaryIndexOutOfBounds { result_idx: u16, secondary_idx: u16 },
    InvalidResultArity { result_idx: u16 },
    InvalidGasCoinUsage,
    InvalidValueUsage,
    InvalidObjectByValue,
    InvalidObjectByMutRef,
    SharedObjectOperationNotAllowed,
    InvalidArgumentArity,
    InvalidTransferObject,
    InvalidMakeMoveVecNonObjectArgument,
    ArgumentWithoutValue,
    CannotMoveBorrowedValue,
    CannotWriteToExtendedReference,
    InvalidReferenceArgument,
}

#[derive(Clone, Debug, Deserialize)]
pub enum TypeArgumentError {
    TypeNotFound,
    ConstraintNotSatisfied,
}

#[derive(Clone, Debug, Deserialize)]
pub enum PackageUpgradeError {
    UnableToFetchPackage {
        package_id: ObjectID,
    },
    NotAPackage {
        object_id: ObjectID,
    },
    IncompatibleUpgrade,
    DigestDoesNotMatch {
        digest: Vec<u8>,
    },
    UnknownUpgradePolicy {
        policy: u8,
    },
    PackageIDDoesNotMatch {
        package_id: ObjectID,
        ticket_id: ObjectID,
    },
}

#[derive(Clone, Debug, Deserialize)]
pub struct CongestedObjects(pub Vec<ObjectID>);

// ============================================================
// Object changes (V2 effects)
// ============================================================

#[derive(Clone, Debug, Deserialize)]
pub enum IDOperation {
    None,
    Created,
    Deleted,
}

#[derive(Clone, Debug, Deserialize)]
pub enum ObjectIn {
    NotExist,
    Exist((VersionDigest, Owner)),
}

#[derive(Clone, Debug, Deserialize)]
pub enum ObjectOut {
    NotExist,
    ObjectWrite((ObjectDigest, Owner)),
    PackageWrite(VersionDigest),
    AccumulatorWriteV1(AccumulatorWriteV1),
}

#[derive(Clone, Debug, Deserialize)]
pub struct EffectsObjectChange {
    pub input_state: ObjectIn,
    pub output_state: ObjectOut,
    pub id_operation: IDOperation,
}

#[derive(Clone, Debug, Deserialize)]
pub struct AccumulatorAddress {
    pub address: SuiAddress,
    pub ty: TypeTag,
}

#[derive(Clone, Debug, Deserialize)]
pub enum AccumulatorOperation {
    Merge,
    Split,
}

#[derive(Clone, Debug, Deserialize)]
pub enum AccumulatorValue {
    Integer(u64),
    IntegerTuple(u64, u64),
    /// NonEmpty<(u64, Digest)> serializes as Vec in BCS (at least 1 element).
    EventDigest(Vec<(u64, Digest)>),
}

#[derive(Clone, Debug, Deserialize)]
pub struct AccumulatorWriteV1 {
    pub address: AccumulatorAddress,
    pub operation: AccumulatorOperation,
    pub value: AccumulatorValue,
}

#[derive(Clone, Debug, Deserialize)]
pub enum UnchangedConsensusKind {
    ReadOnlyRoot(VersionDigest),
    MutateConsensusStreamEnded(SequenceNumber),
    ReadConsensusStreamEnded(SequenceNumber),
    Cancelled(SequenceNumber),
    PerEpochConfig,
}

// ============================================================
// TransactionEffects V1 and V2
// ============================================================

#[derive(Clone, Debug, Deserialize)]
pub enum TransactionEffects {
    V1(TransactionEffectsV1),
    V2(TransactionEffectsV2),
}

#[derive(Clone, Debug, Deserialize)]
pub struct TransactionEffectsV1 {
    pub status: ExecutionStatus,
    pub executed_epoch: EpochId,
    pub gas_used: GasCostSummary,
    pub modified_at_versions: Vec<(ObjectID, SequenceNumber)>,
    pub shared_objects: Vec<ObjectRef>,
    pub transaction_digest: TransactionDigest,
    pub created: Vec<(ObjectRef, Owner)>,
    pub mutated: Vec<(ObjectRef, Owner)>,
    pub unwrapped: Vec<(ObjectRef, Owner)>,
    pub deleted: Vec<ObjectRef>,
    pub unwrapped_then_deleted: Vec<ObjectRef>,
    pub wrapped: Vec<ObjectRef>,
    pub gas_object: (ObjectRef, Owner),
    pub events_digest: Option<TransactionEventsDigest>,
    pub dependencies: Vec<TransactionDigest>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct TransactionEffectsV2 {
    pub status: ExecutionStatus,
    pub executed_epoch: EpochId,
    pub gas_used: GasCostSummary,
    pub transaction_digest: TransactionDigest,
    pub gas_object_index: Option<u32>,
    pub events_digest: Option<TransactionEventsDigest>,
    pub dependencies: Vec<TransactionDigest>,
    pub lamport_version: SequenceNumber,
    pub changed_objects: Vec<(ObjectID, EffectsObjectChange)>,
    pub unchanged_consensus_objects: Vec<(ObjectID, UnchangedConsensusKind)>,
    pub aux_data_digest: Option<EffectsAuxDataDigest>,
}

// ============================================================
// Events
// ============================================================

#[derive(Clone, Debug, Deserialize)]
pub struct TransactionEvents {
    pub data: Vec<Event>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Event {
    pub package_id: ObjectID,
    pub transaction_module: Identifier,
    pub sender: SuiAddress,
    pub type_: StructTag,
    pub contents: Vec<u8>,
}

// ============================================================
// Move core types (for StructTag / TypeTag)
// ============================================================

/// Move identifier — serialized as a String in BCS.
#[derive(Clone, Debug, Deserialize)]
pub struct Identifier(pub String);

/// Move ModuleId — (address, module name).
#[derive(Clone, Debug, Deserialize)]
pub struct ModuleId {
    pub address: AccountAddress,
    pub name: Identifier,
}

#[derive(Clone, Debug, Deserialize)]
pub struct StructTag {
    pub address: AccountAddress,
    pub module: Identifier,
    pub name: Identifier,
    pub type_params: Vec<TypeTag>,
}

#[derive(Clone, Debug, Deserialize)]
pub enum TypeTag {
    Bool,
    U8,
    U64,
    U128,
    Address,
    Signer,
    Vector(Box<TypeTag>),
    Struct(Box<StructTag>),
    U16,
    U32,
    U256,
}

// ============================================================
// Display implementations for JSON output
// ============================================================

impl fmt::Display for Digest {
    /// Base58-encode the digest bytes (Sui convention for digests).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", crate::signer::bs58_encode(&self.0))
    }
}

impl fmt::Display for TransactionDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for ObjectDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for AccountAddress {
    /// 0x-prefixed hex (Sui convention for addresses/object IDs).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x{}", hex::encode(self.0))
    }
}

impl fmt::Display for ObjectID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for SuiAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x{}", hex::encode(self.0))
    }
}

impl fmt::Display for Identifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for StructTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}::{}::{}", self.address, self.module, self.name)?;
        if !self.type_params.is_empty() {
            write!(f, "<")?;
            for (i, tp) in self.type_params.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}", tp)?;
            }
            write!(f, ">")?;
        }
        Ok(())
    }
}

impl fmt::Display for TypeTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TypeTag::Bool => write!(f, "bool"),
            TypeTag::U8 => write!(f, "u8"),
            TypeTag::U16 => write!(f, "u16"),
            TypeTag::U32 => write!(f, "u32"),
            TypeTag::U64 => write!(f, "u64"),
            TypeTag::U128 => write!(f, "u128"),
            TypeTag::U256 => write!(f, "u256"),
            TypeTag::Address => write!(f, "address"),
            TypeTag::Signer => write!(f, "signer"),
            TypeTag::Vector(inner) => write!(f, "vector<{}>", inner),
            TypeTag::Struct(st) => write!(f, "{}", st),
        }
    }
}

impl Owner {
    /// Convert to the JSON owner format used by Sui's RPC.
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Owner::AddressOwner(addr) => serde_json::json!({
                "AddressOwner": addr.to_string()
            }),
            Owner::ObjectOwner(addr) => serde_json::json!({
                "ObjectOwner": addr.to_string()
            }),
            Owner::Shared {
                initial_shared_version,
            } => serde_json::json!({
                "Shared": {
                    "initial_shared_version": initial_shared_version.to_string()
                }
            }),
            Owner::Immutable => serde_json::json!("Immutable"),
            Owner::ConsensusAddressOwner {
                start_version,
                owner,
            } => serde_json::json!({
                "ConsensusAddressOwner": {
                    "start_version": start_version.to_string(),
                    "owner": owner.to_string()
                }
            }),
        }
    }
}

// ============================================================
// Decode helpers
// ============================================================

/// Decode BCS effects bytes into TransactionEffects.
pub fn decode_effects(bytes: &[u8]) -> Result<TransactionEffects, String> {
    bcs::from_bytes::<TransactionEffects>(bytes)
        .map_err(|e| format!("BCS decode TransactionEffects failed: {}", e))
}

/// Decode BCS events bytes into TransactionEvents.
pub fn decode_events(bytes: &[u8]) -> Result<TransactionEvents, String> {
    bcs::from_bytes::<TransactionEvents>(bytes)
        .map_err(|e| format!("BCS decode TransactionEvents failed: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display_account_address() {
        let addr = AccountAddress([0xab; 32]);
        assert_eq!(
            addr.to_string(),
            "0xabababababababababababababababababababababababababababababababab"
        );
    }

    #[test]
    fn test_display_struct_tag() {
        let tag = StructTag {
            address: AccountAddress([0x02; 32]),
            module: Identifier("coin".to_string()),
            name: Identifier("Coin".to_string()),
            type_params: vec![TypeTag::Struct(Box::new(StructTag {
                address: AccountAddress([0x02; 32]),
                module: Identifier("sui".to_string()),
                name: Identifier("SUI".to_string()),
                type_params: vec![],
            }))],
        };
        let s = tag.to_string();
        assert!(s.contains("::coin::Coin<"));
        assert!(s.contains("::sui::SUI>"));
    }

    #[test]
    fn test_decode_real_mainnet_effects_v2() {
        // Real BCS effects from mainnet tx GgwcDRqgCzGGZBVKs9aT2vyVNHnFd2SwAGhpbDBHyuUb
        // epoch 1035, V2, success, 0 created, 2 mutated, 0 deleted, 1 dep
        let hex = "01000b04000000000000000000000000000000000000000000000000000000000000000000000000000020e91ab32f6c869d6e49685b1e1d92d81ff9e8c7d4050dc559a10a50a5ce90a690000001200dd93b3279f50f5b4ae068181d6636f35f9c9500f3d56a80514b50fdb35933d1f2a58f180000000002000000000000000000000000000000000000000000000000000000000000000801f1a58f1800000000202df245fa08c7a4d38df65f086feeaded5ab42b1cd313701d5de1588dd365e42e0230ef70130000000001201c7ddcdbd3bf23b4c4dbcd79ad7cd5b0ecbf102a943684e0a480437969e2139c0230ef701300000000009c2eab63ecbda68309aba3e36fa98cdafd50d4f67d310c2c86ef9a8e4d7d024b01f1a58f180000000020019512a90eb4c8fa377752b834d78f6ff598bd7cc42cfafaaa7432c9592ab04d01754526047e6e997e6c348e7c3491c57b79e22c3efab204b9f0e72c85249c595901204b7544486fcc764c6517ef49bb9ad9aa9338126d97c3cc148fb4243ed3ac25da01754526047e6e997e6c348e7c3491c57b79e22c3efab204b9f0e72c85249c5959000000";
        let bytes = hex::decode(hex).unwrap();

        let effects = decode_effects(&bytes).expect("BCS decode should succeed");

        match effects {
            TransactionEffects::V2(v2) => {
                // Status should be success
                assert!(matches!(v2.status, ExecutionStatus::Success));
                // Epoch 1035
                assert_eq!(v2.executed_epoch, 1035);
                // Gas all zero (epoch change tx)
                assert_eq!(v2.gas_used.computation_cost, 0);
                assert_eq!(v2.gas_used.storage_cost, 0);
                assert_eq!(v2.gas_used.storage_rebate, 0);
                assert_eq!(v2.gas_used.non_refundable_storage_fee, 0);
                // 1 dependency
                assert_eq!(v2.dependencies.len(), 1);
                // 2 changed objects (both mutated)
                assert_eq!(v2.changed_objects.len(), 2);
            }
            TransactionEffects::V1(_) => panic!("Expected V2, got V1"),
        }
    }

    #[test]
    fn test_decode_real_mainnet_effects_v2_with_gas() {
        // Real BCS effects from mainnet tx 3wbPm7YC2BXR9g5aRxmT8AgF8bDcTjAr66Y1DFChFKTM
        // epoch 1035, V2, success, 0 created, 2 mutated, 1 event, 3 deps
        let hex = "01000b040000000000003045080000000000e01f28000000000028b9270000000000b866000000000000202bb3a00bcc01a1683a637c136614fa9a784307f0a82f759dc77fad67143f0090010100000001204670c2c05352f06afcdcc3ceaa4001d1ba3b2d71421b4c92ec8fa3a3d8dabec60320883728f9dd038e6b704abb6e8e76e296fb8d8c80074aa2589d06303df6b0698420c0d8fcc890f9097b749a5c01546188641a4885c8f54c9b843dc21930d60fb40420ede60f78e728cc4780642761560f789a51e92b30a074097e67995ab8d4f3775a6dfea02d0000000002376f38f31e8c18b3351996a4458e7da54e83d420f677a260ffd04a07c75e79a801e766a02d0000000020db963de407343230a9cbf9d709e59957d3f3edea433ae6a0637dd6314696365a00fcc4ce1cfd2ea0fedae6eb7ee85f93ecd44243f3509bbc1b4db612140ee967660120815facf0b01a55e3efa28a695c45d5143da71aa1709bfffd9a62b0867b3f065500fcc4ce1cfd2ea0fedae6eb7ee85f93ecd44243f3509bbc1b4db612140ee9676600be94ae61adb1a2e5c118383a2a9990151d5d23f153c6ee28e3cfeb4065b1fd2a016cfea02d00000000202de4c47f3afa59875107b64280d80238930b0accd4ca06581732ce2b571b1a070032048ef28c4b3ca1def525a2e411d779f4302e73958637210c3a42d6e7fd27eb0120c20015809d8b2aa09f67ae6eae6ab59cae96d8575679b7733b01392e867c237a0032048ef28c4b3ca1def525a2e411d779f4302e73958637210c3a42d6e7fd27eb000000";
        let bytes = hex::decode(hex).unwrap();
        assert_eq!(bytes.len(), 575);

        let effects = decode_effects(&bytes).expect("BCS decode should succeed");

        match effects {
            TransactionEffects::V2(v2) => {
                assert!(matches!(v2.status, ExecutionStatus::Success));
                assert_eq!(v2.executed_epoch, 1035);
                // Real gas costs
                assert_eq!(v2.gas_used.computation_cost, 542000);
                assert_eq!(v2.gas_used.storage_cost, 2629600);
                assert_eq!(v2.gas_used.storage_rebate, 2603304);
                assert_eq!(v2.gas_used.non_refundable_storage_fee, 26296);
                // Dependencies
                assert_eq!(v2.dependencies.len(), 3);
                // Changed objects (2 mutated)
                assert_eq!(v2.changed_objects.len(), 2);
                // Has events digest
                assert!(v2.events_digest.is_some());
                // Gas object index should be set
                assert!(v2.gas_object_index.is_some());
            }
            TransactionEffects::V1(_) => panic!("Expected V2, got V1"),
        }
    }

    #[test]
    fn test_owner_to_json() {
        let owner = Owner::AddressOwner(SuiAddress([0x01; 32]));
        let json = owner.to_json();
        assert!(json.get("AddressOwner").is_some());

        let owner = Owner::Shared {
            initial_shared_version: 42,
        };
        let json = owner.to_json();
        let shared = json.get("Shared").unwrap();
        assert_eq!(shared["initial_shared_version"], "42");

        let owner = Owner::Immutable;
        let json = owner.to_json();
        assert_eq!(json, "Immutable");
    }
}
