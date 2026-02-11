use crate::committee::CommitteeManager;
use anyhow::{anyhow, Result};
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{info, warn};

/// Epoch state machine, matching Sui's reconfiguration states:
/// AcceptAllCerts → RejectUserCerts → RejectAllCerts → RejectAllTx
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EpochState {
    /// Normal operation — accepting all transactions.
    Accepting,
    /// Epoch transition detected — buffering submissions.
    Transitioning,
    /// New epoch started — resuming with new committee.
    NewEpoch(u64),
}

/// Monitors epoch transitions and manages the accepting/transitioning state.
pub struct EpochMonitor {
    state: Arc<RwLock<EpochState>>,
    committee_manager: Arc<CommitteeManager>,
}

impl EpochMonitor {
    pub fn new(committee_manager: Arc<CommitteeManager>) -> Self {
        Self {
            state: Arc::new(RwLock::new(EpochState::Accepting)),
            committee_manager,
        }
    }

    /// Check if the monitor is accepting transactions.
    pub fn check_accepting(&self) -> Result<()> {
        let state = *self.state.read();
        match state {
            EpochState::Accepting | EpochState::NewEpoch(_) => Ok(()),
            EpochState::Transitioning => {
                Err(anyhow!("Epoch transition in progress, try again shortly"))
            }
        }
    }

    /// Get the current epoch state.
    pub fn state(&self) -> EpochState {
        *self.state.read()
    }

    /// Get the current epoch number.
    pub fn epoch(&self) -> u64 {
        self.committee_manager.epoch()
    }

    /// Signal that a validator returned EpochEnding error.
    pub fn signal_epoch_ending(&self) {
        let mut state = self.state.write();
        if *state == EpochState::Accepting {
            info!("Epoch ending detected, transitioning");
            *state = EpochState::Transitioning;
        }
    }

    /// Signal that a new epoch has started.
    pub fn signal_new_epoch(&self, epoch: u64) {
        let mut state = self.state.write();
        info!(epoch, "New epoch detected, resuming");
        *state = EpochState::NewEpoch(epoch);
    }

    /// Resume accepting after new epoch.
    pub fn resume_accepting(&self) {
        let mut state = self.state.write();
        *state = EpochState::Accepting;
    }

    /// Start the background epoch monitoring task.
    /// Polls the committee periodically and detects epoch changes.
    pub fn start_monitor_task(
        self: &Arc<Self>,
        poll_interval: Duration,
    ) -> tokio::task::JoinHandle<()> {
        let monitor = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = time::interval(poll_interval);
            let mut last_epoch = monitor.committee_manager.epoch();

            loop {
                interval.tick().await;

                match monitor.committee_manager.refresh().await {
                    Ok(changed) => {
                        if changed {
                            let new_epoch = monitor.committee_manager.epoch();
                            info!(
                                old_epoch = last_epoch,
                                new_epoch, "Epoch transition completed"
                            );
                            monitor.signal_new_epoch(new_epoch);

                            // Brief pause then resume accepting
                            time::sleep(Duration::from_millis(500)).await;
                            monitor.resume_accepting();
                            last_epoch = new_epoch;
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to refresh committee");
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_epoch_state_transitions() {
        let cm = Arc::new(CommitteeManager::new(
            "https://fullnode.mainnet.sui.io:443".to_string(),
        ));
        let monitor = EpochMonitor::new(cm);

        // Initially accepting
        assert_eq!(monitor.state(), EpochState::Accepting);
        assert!(monitor.check_accepting().is_ok());

        // Signal epoch ending
        monitor.signal_epoch_ending();
        assert_eq!(monitor.state(), EpochState::Transitioning);
        assert!(monitor.check_accepting().is_err());

        // Signal new epoch
        monitor.signal_new_epoch(42);
        assert_eq!(monitor.state(), EpochState::NewEpoch(42));
        assert!(monitor.check_accepting().is_ok());

        // Resume
        monitor.resume_accepting();
        assert_eq!(monitor.state(), EpochState::Accepting);
    }
}
