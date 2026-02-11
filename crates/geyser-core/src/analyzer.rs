use crate::committee::{AuthorityName, Committee};

/// Classification of a transaction's object access path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TxPath {
    /// All objects are owned — eligible for fast path (no consensus).
    OwnedOnly,
    /// At least one shared object — requires consensus ordering.
    Shared,
}

impl std::fmt::Display for TxPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TxPath::OwnedOnly => write!(f, "owned"),
            TxPath::Shared => write!(f, "shared"),
        }
    }
}

/// Analyzes transactions to determine their execution path and optimal submission strategy.
pub struct TxAnalyzer;

impl TxAnalyzer {
    pub fn new() -> Self {
        Self
    }

    /// Classify a transaction based on its input objects.
    ///
    /// Inspects the transaction kind to determine if any shared objects are accessed.
    /// This is done by checking for `SharedObject` input types in the transaction data.
    pub fn classify_from_tx_kind(&self, tx_json: &serde_json::Value) -> TxPath {
        // Check if any input objects are shared
        if let Some(inputs) = tx_json.get("inputs").and_then(|v| v.as_array()) {
            for input in inputs {
                if let Some(obj_type) = input.get("type").and_then(|v| v.as_str()) {
                    if obj_type == "sharedObject" {
                        return TxPath::Shared;
                    }
                }
                // Also check for SharedMoveObject in BCS-decoded inputs
                if let Some(kind) = input.get("objectType").and_then(|v| v.as_str()) {
                    if kind.contains("Shared") {
                        return TxPath::Shared;
                    }
                }
            }
        }

        // Check the transaction kind for MoveCall targets that use shared objects
        if let Some(kind) = tx_json.get("kind") {
            if self.has_shared_objects(kind) {
                return TxPath::Shared;
            }
        }

        TxPath::OwnedOnly
    }

    /// Simple heuristic: classify by checking if the raw BCS bytes contain shared object markers.
    /// For BCS-encoded TransactionData, shared objects have SharedObject { id, initial_shared_version, mutable }.
    /// This is a best-effort classification when full deserialization isn't available.
    pub fn classify_heuristic(&self, tx_bytes: &[u8]) -> TxPath {
        Self::classify_from_bcs_prefix(tx_bytes).unwrap_or(TxPath::Shared)
    }

    /// Get the consensus-optimal validator ordering for a transaction digest.
    /// Replicates `committee.shuffle_by_stake_from_tx_digest()` +
    /// `submission_position()` from consensus_adapter.rs.
    pub fn get_submission_position(
        &self,
        committee: &Committee,
        tx_digest: &[u8; 32],
    ) -> Vec<AuthorityName> {
        committee.shuffle_by_stake_from_digest(tx_digest)
    }

    /// Determine the recommended amplification factor based on TX path.
    pub fn recommended_amplification(&self, path: TxPath, max: usize) -> usize {
        match path {
            // Fast path: fewer validators needed (only need 1 for submission)
            TxPath::OwnedOnly => 3.min(max),
            // Consensus: more validators for faster ordering
            TxPath::Shared => max,
        }
    }

    fn has_shared_objects(&self, kind: &serde_json::Value) -> bool {
        // Recursively check for shared object references
        match kind {
            serde_json::Value::Object(map) => {
                for (key, value) in map {
                    if key == "Shared" || key == "sharedObject" {
                        return true;
                    }
                    if self.has_shared_objects(value) {
                        return true;
                    }
                }
                false
            }
            serde_json::Value::Array(arr) => arr.iter().any(|v| self.has_shared_objects(v)),
            _ => false,
        }
    }

    fn classify_from_bcs_prefix(tx_bytes: &[u8]) -> Option<TxPath> {
        let mut offset = 0usize;

        // TransactionData enum tag: V1 = 0
        let tx_data_variant = Self::read_uleb(tx_bytes, &mut offset)?;
        if tx_data_variant != 0 {
            return Some(TxPath::Shared);
        }

        // TransactionKind enum tag:
        // 0 => ProgrammableTransaction, 10 => ProgrammableSystemTransaction.
        let kind_variant = Self::read_uleb(tx_bytes, &mut offset)?;
        if kind_variant != 0 && kind_variant != 10 {
            return Some(TxPath::Shared);
        }

        // ProgrammableTransaction starts with inputs: Vec<CallArg>.
        let inputs_len = Self::read_uleb(tx_bytes, &mut offset)? as usize;
        for _ in 0..inputs_len {
            if Self::call_arg_is_shared(tx_bytes, &mut offset)? {
                return Some(TxPath::Shared);
            }
        }

        Some(TxPath::OwnedOnly)
    }

    fn call_arg_is_shared(bytes: &[u8], offset: &mut usize) -> Option<bool> {
        match Self::read_uleb(bytes, offset)? {
            // CallArg::Pure(Vec<u8>)
            0 => {
                Self::skip_bytes(bytes, offset)?;
                Some(false)
            }
            // CallArg::Object(ObjectArg)
            1 => Self::object_arg_is_shared(bytes, offset),
            // CallArg::FundsWithdrawal(...) => conservatively treat as shared
            2 => Some(true),
            _ => None,
        }
    }

    fn object_arg_is_shared(bytes: &[u8], offset: &mut usize) -> Option<bool> {
        match Self::read_uleb(bytes, offset)? {
            // ObjectArg::ImmOrOwnedObject(ObjectRef)
            0 => {
                Self::skip_object_ref(bytes, offset)?;
                Some(false)
            }
            // ObjectArg::SharedObject { id, initial_shared_version, mutability }
            1 => {
                Self::skip_fixed(bytes, offset, 32)?; // ObjectID
                Self::skip_fixed(bytes, offset, 8)?; // initial_shared_version (u64 LE)
                let _ = Self::read_uleb(bytes, offset)?; // mutability enum
                Some(true)
            }
            // ObjectArg::Receiving(ObjectRef)
            2 => {
                Self::skip_object_ref(bytes, offset)?;
                Some(false)
            }
            _ => None,
        }
    }

    fn skip_object_ref(bytes: &[u8], offset: &mut usize) -> Option<()> {
        Self::skip_fixed(bytes, offset, 32)?; // ObjectID
        Self::skip_fixed(bytes, offset, 8)?; // SequenceNumber (u64 LE)
        Self::skip_bytes(bytes, offset)?; // ObjectDigest (BCS bytes)
        Some(())
    }

    fn skip_bytes(bytes: &[u8], offset: &mut usize) -> Option<()> {
        let len = Self::read_uleb(bytes, offset)? as usize;
        Self::skip_fixed(bytes, offset, len)
    }

    fn skip_fixed(bytes: &[u8], offset: &mut usize, len: usize) -> Option<()> {
        let end = offset.checked_add(len)?;
        if end > bytes.len() {
            return None;
        }
        *offset = end;
        Some(())
    }

    fn read_uleb(bytes: &[u8], offset: &mut usize) -> Option<u64> {
        let mut value: u64 = 0;
        let mut shift = 0u32;

        loop {
            let byte = *bytes.get(*offset)?;
            *offset += 1;

            value |= ((byte & 0x7f) as u64) << shift;
            if byte & 0x80 == 0 {
                return Some(value);
            }

            shift += 7;
            if shift >= 64 {
                return None;
            }
        }
    }
}

impl Default for TxAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::committee::{Committee, ValidatorInfo};

    fn test_committee() -> Committee {
        let validators = (0..4)
            .map(|i| {
                let mut name = [0u8; 32];
                name[0] = i;
                ValidatorInfo {
                    name,
                    network_address: format!("127.0.0.1:{}", 8000 + i as u16),
                    stake: 2500,
                    description: format!("v{}", i),
                }
            })
            .collect();
        Committee::new(1, validators)
    }

    #[test]
    fn test_classify_owned_tx() {
        let analyzer = TxAnalyzer::new();
        let tx = serde_json::json!({
            "inputs": [
                { "type": "object", "objectId": "0x1" },
                { "type": "pure", "value": "100" }
            ]
        });
        assert_eq!(analyzer.classify_from_tx_kind(&tx), TxPath::OwnedOnly);
    }

    #[test]
    fn test_classify_shared_tx() {
        let analyzer = TxAnalyzer::new();
        let tx = serde_json::json!({
            "inputs": [
                { "type": "sharedObject", "objectId": "0x1", "initialSharedVersion": 1 }
            ]
        });
        assert_eq!(analyzer.classify_from_tx_kind(&tx), TxPath::Shared);
    }

    #[test]
    fn test_submission_position() {
        let analyzer = TxAnalyzer::new();
        let committee = test_committee();
        let digest = [42u8; 32];
        let order = analyzer.get_submission_position(&committee, &digest);
        assert_eq!(order.len(), 4);
    }

    #[test]
    fn test_recommended_amplification() {
        let analyzer = TxAnalyzer::new();
        assert_eq!(analyzer.recommended_amplification(TxPath::OwnedOnly, 5), 3);
        assert_eq!(analyzer.recommended_amplification(TxPath::Shared, 5), 5);
        assert_eq!(analyzer.recommended_amplification(TxPath::OwnedOnly, 2), 2);
    }

    fn encode_uleb(mut value: u64) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            out.push(byte);
            if value == 0 {
                return out;
            }
        }
    }

    fn build_owned_object_call_arg() -> Vec<u8> {
        let mut out = Vec::new();
        out.extend(encode_uleb(1)); // CallArg::Object
        out.extend(encode_uleb(0)); // ObjectArg::ImmOrOwnedObject
        out.extend([0u8; 32]); // ObjectID
        out.extend(1u64.to_le_bytes()); // version
        out.extend(encode_uleb(32)); // digest len
        out.extend([0u8; 32]); // digest bytes
        out
    }

    fn build_shared_object_call_arg() -> Vec<u8> {
        let mut out = Vec::new();
        out.extend(encode_uleb(1)); // CallArg::Object
        out.extend(encode_uleb(1)); // ObjectArg::SharedObject
        out.extend([0u8; 32]); // ObjectID
        out.extend(1u64.to_le_bytes()); // initial_shared_version
        out.extend(encode_uleb(0)); // SharedObjectMutability::Mutable
        out
    }

    fn wrap_programmable_tx_inputs(inputs: Vec<Vec<u8>>) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend(encode_uleb(0)); // TransactionData::V1
        out.extend(encode_uleb(0)); // TransactionKind::ProgrammableTransaction
        out.extend(encode_uleb(inputs.len() as u64)); // inputs length
        for input in inputs {
            out.extend(input);
        }
        out
    }

    #[test]
    fn test_classify_heuristic_owned_from_bcs() {
        let analyzer = TxAnalyzer::new();
        let tx = wrap_programmable_tx_inputs(vec![build_owned_object_call_arg()]);
        assert_eq!(analyzer.classify_heuristic(&tx), TxPath::OwnedOnly);
    }

    #[test]
    fn test_classify_heuristic_shared_from_bcs() {
        let analyzer = TxAnalyzer::new();
        let tx = wrap_programmable_tx_inputs(vec![build_shared_object_call_arg()]);
        assert_eq!(analyzer.classify_heuristic(&tx), TxPath::Shared);
    }
}
