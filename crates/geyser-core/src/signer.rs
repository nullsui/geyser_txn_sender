use anyhow::{anyhow, Result};
use blake2::{digest::consts::U32, Blake2b, Digest};

type Blake2b256 = Blake2b<U32>;
use ed25519_dalek::{Signer, SigningKey, VerifyingKey};

/// Sui signature scheme flag for ed25519.
const ED25519_FLAG: u8 = 0x00;

/// An ed25519 keypair for Sui transaction signing.
#[derive(Clone)]
pub struct SuiKeypair {
    signing_key: SigningKey,
    verifying_key: VerifyingKey,
}

impl SuiKeypair {
    /// Create from a 32-byte ed25519 secret key.
    pub fn from_secret_key(secret: &[u8; 32]) -> Result<Self> {
        let signing_key = SigningKey::from_bytes(secret);
        let verifying_key = signing_key.verifying_key();
        Ok(Self {
            signing_key,
            verifying_key,
        })
    }

    /// Derive the Sui address (0x-prefixed hex, 64 chars) from the public key.
    /// Sui address = Blake2b-256(scheme_flag || pubkey_bytes)
    pub fn address(&self) -> String {
        let mut hasher = Blake2b256::new();
        hasher.update([ED25519_FLAG]);
        hasher.update(self.verifying_key.as_bytes());
        let hash = hasher.finalize();
        format!("0x{}", hex::encode(hash))
    }

    /// Get the 32-byte public key.
    pub fn public_key_bytes(&self) -> &[u8; 32] {
        self.verifying_key.as_bytes()
    }

    /// Sign a transaction and produce Sui's signature format:
    /// `[scheme_flag || sig_bytes(64) || pubkey_bytes(32)]` = 97 bytes total.
    ///
    /// The message to sign is Blake2b-256(intent_prefix || tx_bytes).
    /// Intent prefix for transaction signing: [0, 0, 0] (IntentScope::TransactionData, V0, Sui).
    pub fn sign_transaction(&self, tx_bytes: &[u8]) -> Vec<u8> {
        // Compute intent message: Blake2b-256(intent_prefix || tx_bytes)
        let mut hasher = Blake2b256::new();
        hasher.update([0u8, 0, 0]); // Intent prefix: TransactionData, V0, Sui
        hasher.update(tx_bytes);
        let digest = hasher.finalize();

        // Sign the digest
        let signature = self.signing_key.sign(&digest);

        // Sui signature format: flag || sig || pubkey
        let mut result = Vec::with_capacity(1 + 64 + 32);
        result.push(ED25519_FLAG);
        result.extend_from_slice(&signature.to_bytes());
        result.extend_from_slice(self.verifying_key.as_bytes());
        result
    }
}

/// Load all keypairs from a Sui keystore file.
/// The file is a JSON array of base64-encoded strings.
/// Each string is: `[scheme_flag(1) || secret_key(32)]` (33 bytes total for ed25519).
pub fn load_keystore(path: &str) -> Result<Vec<SuiKeypair>> {
    let content = std::fs::read_to_string(path)?;
    let keys: Vec<String> = serde_json::from_str(&content)?;

    let mut keypairs = Vec::new();
    for key_b64 in &keys {
        let bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, key_b64)?;

        if bytes.is_empty() {
            continue;
        }

        let scheme = bytes[0];
        if scheme == ED25519_FLAG {
            if bytes.len() < 33 {
                return Err(anyhow!(
                    "Ed25519 key too short: {} bytes (expected 33)",
                    bytes.len()
                ));
            }
            let mut secret = [0u8; 32];
            secret.copy_from_slice(&bytes[1..33]);
            keypairs.push(SuiKeypair::from_secret_key(&secret)?);
        }
        // Skip non-ed25519 keys silently
    }

    Ok(keypairs)
}

/// Load a specific keypair from the keystore that matches the given address.
pub fn load_keypair_for_address(keystore_path: &str, address: &str) -> Result<SuiKeypair> {
    let keypairs = load_keystore(keystore_path)?;
    let normalized = address.to_lowercase();

    for kp in &keypairs {
        if kp.address() == normalized {
            return Ok(kp.clone());
        }
    }

    let available: Vec<String> = keypairs.iter().map(|k| k.address()).collect();
    Err(anyhow!(
        "No keypair found for address {}. Available: {:?}",
        address,
        available
    ))
}

/// Compute the raw 32-byte TransactionDigest from TransactionData BCS bytes.
///
/// Uses Sui's `Signable` trait convention:
///   `Blake2b-256("TransactionData::" || tx_data_bcs)`
///
/// This is NOT the same as the signing digest (which uses intent prefix [0,0,0]).
pub fn tx_digest_bytes(tx_bytes: &[u8]) -> Vec<u8> {
    let mut hasher = Blake2b256::new();
    hasher.update(b"TransactionData::");
    hasher.update(tx_bytes);
    hasher.finalize().to_vec()
}

/// Compute the TransactionDigest from TransactionData BCS bytes, base58-encoded.
///
/// Uses Sui's `Signable` trait: `Blake2b-256("TransactionData::" || tx_data_bcs)`.
pub fn tx_digest(tx_bytes: &[u8]) -> String {
    bs58_encode(&tx_digest_bytes(tx_bytes))
}

/// Simple base58 encoding (Sui uses base58 for digests).
pub(crate) fn bs58_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    if bytes.is_empty() {
        return String::new();
    }

    // Count leading zeros
    let leading_zeros = bytes.iter().take_while(|&&b| b == 0).count();

    // Convert to base58
    let mut digits: Vec<u8> = Vec::new();
    for &byte in bytes {
        let mut carry = byte as u32;
        for d in digits.iter_mut() {
            carry += (*d as u32) * 256;
            *d = (carry % 58) as u8;
            carry /= 58;
        }
        while carry > 0 {
            digits.push((carry % 58) as u8);
            carry /= 58;
        }
    }

    let mut result = String::with_capacity(leading_zeros + digits.len());
    for _ in 0..leading_zeros {
        result.push('1');
    }
    for &d in digits.iter().rev() {
        result.push(ALPHABET[d as usize] as char);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keypair_address_derivation() {
        // Create a deterministic keypair
        let secret = [42u8; 32];
        let kp = SuiKeypair::from_secret_key(&secret).unwrap();
        let addr = kp.address();

        // Should be 0x + 64 hex chars
        assert!(addr.starts_with("0x"));
        assert_eq!(addr.len(), 66);

        // Should be deterministic
        let kp2 = SuiKeypair::from_secret_key(&secret).unwrap();
        assert_eq!(kp.address(), kp2.address());
    }

    #[test]
    fn test_sign_transaction_format() {
        let secret = [1u8; 32];
        let kp = SuiKeypair::from_secret_key(&secret).unwrap();

        let tx_bytes = b"fake transaction data";
        let sig = kp.sign_transaction(tx_bytes);

        // Should be 97 bytes: flag(1) + sig(64) + pubkey(32)
        assert_eq!(sig.len(), 97);
        assert_eq!(sig[0], ED25519_FLAG);
    }

    #[test]
    fn test_sign_deterministic() {
        let secret = [7u8; 32];
        let kp = SuiKeypair::from_secret_key(&secret).unwrap();

        let tx = b"some tx";
        let sig1 = kp.sign_transaction(tx);
        let sig2 = kp.sign_transaction(tx);
        assert_eq!(sig1, sig2);
    }

    #[test]
    fn test_tx_digest() {
        let tx_bytes = b"test transaction";
        let digest = tx_digest(tx_bytes);
        assert!(!digest.is_empty());

        // Should be deterministic
        assert_eq!(digest, tx_digest(tx_bytes));
    }

    #[test]
    fn test_tx_digest_bytes() {
        let tx_bytes = b"test transaction";
        let raw = tx_digest_bytes(tx_bytes);

        // Should be 32 bytes (Blake2b-256)
        assert_eq!(raw.len(), 32);

        // Should be deterministic
        assert_eq!(raw, tx_digest_bytes(tx_bytes));

        // base58 of raw bytes should match tx_digest()
        assert_eq!(tx_digest(tx_bytes), super::bs58_encode(&raw));
    }

    #[test]
    fn test_tx_digest_uses_signable_prefix() {
        // TransactionDigest uses Sui's Signable trait: Blake2b-256("TransactionData::" || tx_data)
        // NOT the signing digest which uses intent prefix [0,0,0].
        let tx_data = b"test data";

        let digest = tx_digest_bytes(tx_data);

        // Manually compute with Signable prefix
        let mut hasher = Blake2b256::new();
        hasher.update(b"TransactionData::");
        hasher.update(tx_data);
        let expected = hasher.finalize().to_vec();

        assert_eq!(digest, expected);

        // Verify it differs from intent-prefix hash (the signing digest)
        let mut hasher2 = Blake2b256::new();
        hasher2.update([0u8, 0, 0]);
        hasher2.update(tx_data);
        let intent_hash = hasher2.finalize().to_vec();

        assert_ne!(
            digest, intent_hash,
            "TransactionDigest must NOT use intent prefix"
        );
    }

    #[test]
    fn test_tx_digest_real_mainnet() {
        // Real mainnet tx: 3wbPm7YC2BXR9g5aRxmT8AgF8bDcTjAr66Y1DFChFKTM
        // rawTransaction envelope (base64 from sui_getTransactionBlock showRawInput):
        // Envelope = [0x01] [0x00,0x00,0x00] [TransactionData 309 bytes] [signatures...]
        // TransactionData = bytes[4..313] of the envelope.
        let envelope_b64 = "AQAAAAAAAgEAN2848x6MGLM1GZakRY59pU6D1CD2d6Jg/9BKB8deeajnZqAtAAAAACDblj3kBzQyMKnL+dcJ5ZlX0/Pt6kM65qBjfdYxRpY2WgAIwNQBAAAAAAABABP+OnQilGut/wQr4ObbuwaG+/8/q8DIbO3C16ApSG7OCWFydGlwZWRpYQ11cGRhdGVfcG9pbnRzAAIBAAABAQD8xM4c/S6g/trm637oX5Ps1EJD81CbvBtNthIUDulnZgG+lK5hrbGi5cEYODoqmZAVHV0j8VPG7ijjz+tAZbH9Kmz+oC0AAAAAIC3kxH86+lmHUQe2QoDYAjiTCwrM1MoGWBcyzitXGxoHMgSO8oxLPKHe9SWi5BHXefQwLnOVhjchDDpC1uf9J+seAgAAAAAAAICWmAAAAAAAAAJhAHDLhU94CvQBERo4e2C98Y8JdaTRDyX7A47l/vjjPHbhHqzysCnshlsFTYyN8mYSmH38gQ8u5EXInE9sBJdLQQyGlIKT/eDFYM585OFTSOGlC2YQsH65k9oBmZtfOh/HcmEAbOG5IFBmBj3DWyYtnSNUDBFOk8NnCn6o7XUdPM6NZQBQsW1ahPkbm7kZm1ntAThCjtDnv+hSUicN7Nb8zePsDuBmODwg2d+vc+OrzgxgSH/Xp36SPKCLtdR+MushOYE4";
        let envelope =
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, envelope_b64)
                .unwrap();

        // Extract TransactionData: bytes[4..313]
        let tx_data = &envelope[4..313];

        let digest = tx_digest(tx_data);
        assert_eq!(
            digest, "3wbPm7YC2BXR9g5aRxmT8AgF8bDcTjAr66Y1DFChFKTM",
            "TransactionDigest must match known mainnet value"
        );
    }
}
