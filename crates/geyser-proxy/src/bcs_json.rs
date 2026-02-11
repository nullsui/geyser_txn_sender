//! Convert BCS-encoded TransactionEffects + TransactionEvents into
//! standard `SuiTransactionBlockResponse` JSON.

use base64::Engine;
use geyser_core::sui_types::*;
use serde_json::{json, Value};

/// Which response fields the caller wants (mirrors Sui's showOptions).
pub struct ShowOptions {
    pub show_effects: bool,
    pub show_events: bool,
    pub show_object_changes: bool,
    pub show_balance_changes: bool,
    pub show_raw_effects: bool,
}

impl ShowOptions {
    /// Parse showOptions from the JSON-RPC params (index 2).
    pub fn from_params(options: Option<&Value>) -> Self {
        let opts = options.and_then(|v| v.as_object());
        let get_bool = |key: &str, default: bool| -> bool {
            opts.and_then(|o| o.get(key))
                .and_then(|v| v.as_bool())
                .unwrap_or(default)
        };
        Self {
            show_effects: get_bool("showEffects", true),
            show_events: get_bool("showEvents", false),
            show_object_changes: get_bool("showObjectChanges", false),
            show_balance_changes: get_bool("showBalanceChanges", false),
            show_raw_effects: get_bool("showRawEffects", false),
        }
    }
}

/// Build a standard `SuiTransactionBlockResponse` JSON-RPC response
/// from BCS-encoded effects and events.
pub fn effects_to_sui_response(
    effects_bytes: &[u8],
    events_bytes: Option<&[u8]>,
    tx_digest: &str,
    show: &ShowOptions,
    id: &Value,
) -> Result<Value, String> {
    tracing::debug!(
        effects_len = effects_bytes.len(),
        first_byte = effects_bytes.first().copied().unwrap_or(0),
        "BCS decode: attempting effects decode"
    );
    let effects = decode_effects(effects_bytes)?;

    let mut result = json!({
        "digest": tx_digest,
    });
    let result_obj = result.as_object_mut().unwrap();

    if show.show_effects {
        result_obj.insert("effects".to_string(), effects_to_json(&effects, tx_digest));
    }

    if show.show_events {
        if let Some(eb) = events_bytes {
            match decode_events(eb) {
                Ok(events) => {
                    result_obj.insert("events".to_string(), events_to_json(&events, tx_digest));
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to decode events BCS, omitting events");
                    result_obj.insert("events".to_string(), json!([]));
                }
            }
        } else {
            result_obj.insert("events".to_string(), json!([]));
        }
    }

    if show.show_object_changes {
        result_obj.insert(
            "objectChanges".to_string(),
            object_changes_from_effects(&effects),
        );
    }

    if show.show_balance_changes {
        // Balance changes are not available from effects alone
        result_obj.insert("balanceChanges".to_string(), json!([]));
    }

    if show.show_raw_effects {
        let raw_b64 = base64::engine::general_purpose::STANDARD.encode(effects_bytes);
        result_obj.insert("rawEffects".to_string(), json!(raw_b64));
    }

    Ok(json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result
    }))
}

// ============================================================
// Effects → JSON
// ============================================================

fn effects_to_json(effects: &TransactionEffects, tx_digest: &str) -> Value {
    match effects {
        TransactionEffects::V1(v1) => effects_v1_to_json(v1, tx_digest),
        TransactionEffects::V2(v2) => effects_v2_to_json(v2, tx_digest),
    }
}

fn effects_v1_to_json(v1: &TransactionEffectsV1, _tx_digest: &str) -> Value {
    let gas_ref = &v1.gas_object;
    json!({
        "messageVersion": "v1",
        "status": status_to_json(&v1.status),
        "executedEpoch": v1.executed_epoch.to_string(),
        "gasUsed": gas_cost_to_json(&v1.gas_used),
        "transactionDigest": v1.transaction_digest.to_string(),
        "created": v1.created.iter().map(|(r, o)| obj_ref_owner_json(r, o)).collect::<Vec<_>>(),
        "mutated": v1.mutated.iter().map(|(r, o)| obj_ref_owner_json(r, o)).collect::<Vec<_>>(),
        "unwrapped": v1.unwrapped.iter().map(|(r, o)| obj_ref_owner_json(r, o)).collect::<Vec<_>>(),
        "deleted": v1.deleted.iter().map(obj_ref_json).collect::<Vec<_>>(),
        "unwrappedThenDeleted": v1.unwrapped_then_deleted.iter().map(obj_ref_json).collect::<Vec<_>>(),
        "wrapped": v1.wrapped.iter().map(obj_ref_json).collect::<Vec<_>>(),
        "gasObject": obj_ref_owner_json(&gas_ref.0, &gas_ref.1),
        "dependencies": v1.dependencies.iter().map(|d| d.to_string()).collect::<Vec<_>>(),
    })
}

fn effects_v2_to_json(v2: &TransactionEffectsV2, _tx_digest: &str) -> Value {
    // Partition changed_objects into created/mutated/deleted
    let mut created = Vec::new();
    let mut mutated = Vec::new();
    let mut deleted = Vec::new();
    let mut gas_object_json = Value::Null;

    for (i, (obj_id, change)) in v2.changed_objects.iter().enumerate() {
        let is_gas = v2.gas_object_index.map_or(false, |gi| gi as usize == i);

        match (&change.id_operation, &change.output_state) {
            (_, ObjectOut::NotExist) | (IDOperation::Deleted, _) => {
                // Deleted object — use lamport_version as the version
                deleted.push(json!({
                    "objectId": obj_id.to_string(),
                    "version": v2.lamport_version.to_string(),
                    "digest": "",
                }));
            }
            (IDOperation::Created, ObjectOut::ObjectWrite((digest, owner))) => {
                let entry = json!({
                    "owner": owner.to_json(),
                    "reference": {
                        "objectId": obj_id.to_string(),
                        "version": v2.lamport_version.to_string(),
                        "digest": digest.to_string(),
                    }
                });
                if is_gas {
                    gas_object_json = entry.clone();
                }
                created.push(entry);
            }
            (_, ObjectOut::ObjectWrite((digest, owner))) => {
                let entry = json!({
                    "owner": owner.to_json(),
                    "reference": {
                        "objectId": obj_id.to_string(),
                        "version": v2.lamport_version.to_string(),
                        "digest": digest.to_string(),
                    }
                });
                if is_gas {
                    gas_object_json = entry.clone();
                }
                mutated.push(entry);
            }
            (_, ObjectOut::PackageWrite((version, digest))) => {
                mutated.push(json!({
                    "owner": Owner::Immutable.to_json(),
                    "reference": {
                        "objectId": obj_id.to_string(),
                        "version": version.to_string(),
                        "digest": digest.to_string(),
                    }
                }));
            }
            (_, ObjectOut::AccumulatorWriteV1(_)) => {
                // Accumulator writes don't map to standard created/mutated
            }
        }
    }

    json!({
        "messageVersion": "v1",
        "status": status_to_json(&v2.status),
        "executedEpoch": v2.executed_epoch.to_string(),
        "gasUsed": gas_cost_to_json(&v2.gas_used),
        "transactionDigest": v2.transaction_digest.to_string(),
        "created": created,
        "mutated": mutated,
        "deleted": deleted,
        "gasObject": gas_object_json,
        "dependencies": v2.dependencies.iter().map(|d| d.to_string()).collect::<Vec<_>>(),
    })
}

// ============================================================
// Events → JSON
// ============================================================

fn events_to_json(events: &TransactionEvents, tx_digest: &str) -> Value {
    let items: Vec<Value> = events
        .data
        .iter()
        .enumerate()
        .map(|(i, event)| {
            let bcs_b64 = base64::engine::general_purpose::STANDARD.encode(&event.contents);
            json!({
                "id": {
                    "txDigest": tx_digest,
                    "eventSeq": i.to_string(),
                },
                "packageId": event.package_id.to_string(),
                "transactionModule": event.transaction_module.to_string(),
                "sender": event.sender.to_string(),
                "type": event.type_.to_string(),
                "bcs": bcs_b64,
            })
        })
        .collect();
    Value::Array(items)
}

// ============================================================
// Object changes from effects
// ============================================================

fn object_changes_from_effects(effects: &TransactionEffects) -> Value {
    match effects {
        TransactionEffects::V1(v1) => object_changes_v1(v1),
        TransactionEffects::V2(v2) => object_changes_v2(v2),
    }
}

fn object_changes_v1(v1: &TransactionEffectsV1) -> Value {
    let mut changes = Vec::new();
    for (obj_ref, owner) in &v1.created {
        changes.push(json!({
            "type": "created",
            "objectId": obj_ref.0.to_string(),
            "version": obj_ref.1.to_string(),
            "digest": obj_ref.2.to_string(),
            "owner": owner.to_json(),
        }));
    }
    for (obj_ref, owner) in &v1.mutated {
        changes.push(json!({
            "type": "mutated",
            "objectId": obj_ref.0.to_string(),
            "version": obj_ref.1.to_string(),
            "digest": obj_ref.2.to_string(),
            "owner": owner.to_json(),
        }));
    }
    for obj_ref in &v1.deleted {
        changes.push(json!({
            "type": "deleted",
            "objectId": obj_ref.0.to_string(),
            "version": obj_ref.1.to_string(),
            "digest": obj_ref.2.to_string(),
        }));
    }
    Value::Array(changes)
}

fn object_changes_v2(v2: &TransactionEffectsV2) -> Value {
    let mut changes = Vec::new();
    for (obj_id, change) in &v2.changed_objects {
        match (&change.id_operation, &change.output_state) {
            (_, ObjectOut::NotExist) | (IDOperation::Deleted, _) => {
                changes.push(json!({
                    "type": "deleted",
                    "objectId": obj_id.to_string(),
                    "version": v2.lamport_version.to_string(),
                }));
            }
            (IDOperation::Created, ObjectOut::ObjectWrite((digest, owner))) => {
                changes.push(json!({
                    "type": "created",
                    "objectId": obj_id.to_string(),
                    "version": v2.lamport_version.to_string(),
                    "digest": digest.to_string(),
                    "owner": owner.to_json(),
                }));
            }
            (_, ObjectOut::ObjectWrite((digest, owner))) => {
                changes.push(json!({
                    "type": "mutated",
                    "objectId": obj_id.to_string(),
                    "version": v2.lamport_version.to_string(),
                    "digest": digest.to_string(),
                    "owner": owner.to_json(),
                }));
            }
            (_, ObjectOut::PackageWrite((version, digest))) => {
                changes.push(json!({
                    "type": "published",
                    "objectId": obj_id.to_string(),
                    "version": version.to_string(),
                    "digest": digest.to_string(),
                }));
            }
            (_, ObjectOut::AccumulatorWriteV1(_)) => {}
        }
    }
    Value::Array(changes)
}

// ============================================================
// Helpers
// ============================================================

fn status_to_json(status: &ExecutionStatus) -> Value {
    match status {
        ExecutionStatus::Success => json!({"status": "success"}),
        ExecutionStatus::Failure { error, command } => {
            let mut obj = json!({
                "status": "failure",
                "error": format!("{:?}", error),
            });
            if let Some(cmd) = command {
                obj["command"] = json!(cmd);
            }
            obj
        }
    }
}

fn gas_cost_to_json(gas: &GasCostSummary) -> Value {
    json!({
        "computationCost": gas.computation_cost.to_string(),
        "storageCost": gas.storage_cost.to_string(),
        "storageRebate": gas.storage_rebate.to_string(),
        "nonRefundableStorageFee": gas.non_refundable_storage_fee.to_string(),
    })
}

fn obj_ref_owner_json(obj_ref: &ObjectRef, owner: &Owner) -> Value {
    json!({
        "owner": owner.to_json(),
        "reference": {
            "objectId": obj_ref.0.to_string(),
            "version": obj_ref.1.to_string(),
            "digest": obj_ref.2.to_string(),
        }
    })
}

fn obj_ref_json(obj_ref: &ObjectRef) -> Value {
    json!({
        "objectId": obj_ref.0.to_string(),
        "version": obj_ref.1.to_string(),
        "digest": obj_ref.2.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_effects_to_sui_response_real_mainnet() {
        // Real BCS effects from mainnet tx 3wbPm7YC2BXR9g5aRxmT8AgF8bDcTjAr66Y1DFChFKTM
        let hex = "01000b040000000000003045080000000000e01f28000000000028b9270000000000b866000000000000202bb3a00bcc01a1683a637c136614fa9a784307f0a82f759dc77fad67143f0090010100000001204670c2c05352f06afcdcc3ceaa4001d1ba3b2d71421b4c92ec8fa3a3d8dabec60320883728f9dd038e6b704abb6e8e76e296fb8d8c80074aa2589d06303df6b0698420c0d8fcc890f9097b749a5c01546188641a4885c8f54c9b843dc21930d60fb40420ede60f78e728cc4780642761560f789a51e92b30a074097e67995ab8d4f3775a6dfea02d0000000002376f38f31e8c18b3351996a4458e7da54e83d420f677a260ffd04a07c75e79a801e766a02d0000000020db963de407343230a9cbf9d709e59957d3f3edea433ae6a0637dd6314696365a00fcc4ce1cfd2ea0fedae6eb7ee85f93ecd44243f3509bbc1b4db612140ee967660120815facf0b01a55e3efa28a695c45d5143da71aa1709bfffd9a62b0867b3f065500fcc4ce1cfd2ea0fedae6eb7ee85f93ecd44243f3509bbc1b4db612140ee9676600be94ae61adb1a2e5c118383a2a9990151d5d23f153c6ee28e3cfeb4065b1fd2a016cfea02d00000000202de4c47f3afa59875107b64280d80238930b0accd4ca06581732ce2b571b1a070032048ef28c4b3ca1def525a2e411d779f4302e73958637210c3a42d6e7fd27eb0120c20015809d8b2aa09f67ae6eae6ab59cae96d8575679b7733b01392e867c237a0032048ef28c4b3ca1def525a2e411d779f4302e73958637210c3a42d6e7fd27eb000000";
        let effects_bytes = hex::decode(hex).unwrap();

        let show = ShowOptions {
            show_effects: true,
            show_events: false,
            show_object_changes: true,
            show_balance_changes: false,
            show_raw_effects: false,
        };

        let id = json!(1);
        let result = effects_to_sui_response(
            &effects_bytes,
            None,
            "3wbPm7YC2BXR9g5aRxmT8AgF8bDcTjAr66Y1DFChFKTM",
            &show,
            &id,
        )
        .expect("effects_to_sui_response should succeed");

        // Verify JSON structure
        assert_eq!(result["jsonrpc"], "2.0");
        assert_eq!(result["id"], 1);

        let r = &result["result"];
        assert_eq!(r["digest"], "3wbPm7YC2BXR9g5aRxmT8AgF8bDcTjAr66Y1DFChFKTM");

        // Effects
        let eff = &r["effects"];
        assert_eq!(eff["messageVersion"], "v1");
        assert_eq!(eff["status"]["status"], "success");
        assert_eq!(eff["executedEpoch"], "1035");
        assert_eq!(eff["gasUsed"]["computationCost"], "542000");
        assert_eq!(eff["gasUsed"]["storageCost"], "2629600");
        assert_eq!(eff["gasUsed"]["storageRebate"], "2603304");
        assert_eq!(eff["gasUsed"]["nonRefundableStorageFee"], "26296");

        // Mutated objects (2)
        let mutated = eff["mutated"].as_array().unwrap();
        assert_eq!(mutated.len(), 2);

        // Created should be empty
        let created = eff["created"].as_array().unwrap();
        assert_eq!(created.len(), 0);

        // Dependencies (3)
        let deps = eff["dependencies"].as_array().unwrap();
        assert_eq!(deps.len(), 3);

        // Gas object should be set
        assert!(eff["gasObject"].is_object());

        // Object changes
        let changes = r["objectChanges"].as_array().unwrap();
        assert_eq!(changes.len(), 2);
        assert!(changes.iter().all(|c| c["type"] == "mutated"));
    }
}
