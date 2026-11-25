//! Session restart/resume seam. A venue session restarts at sequence 1; the
//! client's own `requested_sequence_number` config field is that replay knob
//! (`client_soupbintcp::config` doc: "1 replays the session from the start").
//! An adapter can additionally hand back a Glimpse resume sequence via a
//! `Control::RecoveryComplete` record.

use client_soupbintcp::SoupBinClientConfig;
use md_events::ControlKind;

/// Resume point tracked across a live session's lifetime, so a reconnect
/// picks the right starting sequence instead of always doing a full
/// sequence-1 replay.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RestartState {
    /// SoupBinTCP `next_expected_sequence` captured before a dropped socket.
    /// `None` means no live session has run yet, or the venue itself
    /// restarted its session numbering, so the next reconnect replays from
    /// sequence 1.
    pub soupbin_resume_seq: Option<u64>,
    /// Last Glimpse resume sequence observed from a `RecoveryComplete`
    /// Control record.
    pub glimpse_resume_seq: Option<u64>,
    /// Primary leg `next_expected_sequence` captured when a dual source
    /// returns. `None` until a dual session has run.
    pub primary_resume_seq: Option<u64>,
    /// Secondary leg `next_expected_sequence` captured when a dual source
    /// returns. Legs run independent SoupBinTCP sessions with independent
    /// sequence spaces, so each keeps its own resume point.
    pub secondary_resume_seq: Option<u64>,
}

impl RestartState {
    /// Build the login config for a reconnect: resumes mid-session at the
    /// captured SoupBinTCP sequence, then a Glimpse recovery point, or
    /// sequence-1 replay when neither is available.
    pub fn resume_login_config(&self, base: &SoupBinClientConfig) -> SoupBinClientConfig {
        SoupBinClientConfig {
            requested_sequence_number: self
                .soupbin_resume_seq
                .or(self.glimpse_resume_seq)
                .unwrap_or(1),
            ..base.clone()
        }
    }

    /// Per-leg login configs for a dual reconnect. Each leg resumes at
    /// its own captured socket sequence, then the shared Glimpse recovery
    /// point, then sequence-1 replay.
    pub fn resume_dual_login_configs(
        &self,
        base: &SoupBinClientConfig,
    ) -> (SoupBinClientConfig, SoupBinClientConfig) {
        let leg = |resume: Option<u64>| SoupBinClientConfig {
            requested_sequence_number: resume.or(self.glimpse_resume_seq).unwrap_or(1),
            ..base.clone()
        };
        (leg(self.primary_resume_seq), leg(self.secondary_resume_seq))
    }

    /// Records the socket-level resume point, typically read from
    /// `LiveSoupBinSource::next_expected_sequence` before a planned or
    /// observed disconnect.
    pub fn capture_soupbin_resume(&mut self, next_expected: u64) {
        self.soupbin_resume_seq = Some(next_expected);
    }

    /// Records both leg resume points of a dual source, read from
    /// `DualLiveSource::next_expected_sequences` when a dual session returns.
    pub fn capture_dual_resume(&mut self, primary_next: u64, secondary_next: u64) {
        self.primary_resume_seq = Some(primary_next);
        self.secondary_resume_seq = Some(secondary_next);
    }

    /// Clears the socket-level resume point: the venue restarted its own
    /// session (fresh session id at login), so the next reconnect must
    /// re-stream from sequence 1 rather than resume mid-session.
    pub fn clear_soupbin_resume(&mut self) {
        self.soupbin_resume_seq = None;
    }

    /// Clears every captured point before a sequence-1 full replay.
    pub fn reset_for_replay(&mut self) {
        self.soupbin_resume_seq = None;
        self.glimpse_resume_seq = None;
        self.primary_resume_seq = None;
        self.secondary_resume_seq = None;
    }

    /// Feeds a drained Control record; records the Glimpse resume
    /// sequence on `RecoveryComplete`, ignores every other kind.
    pub fn observe_control(&mut self, kind: ControlKind, aux: u64) {
        if kind == ControlKind::RecoveryComplete {
            self.glimpse_resume_seq = Some(aux);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A fresh RestartState has no captured resume point, so the very first
    // reconnect (or a venue restart) replays from sequence 1.
    #[test]
    fn defaults_to_sequence_one_replay() {
        let state = RestartState::default();
        let cfg = state.resume_login_config(&SoupBinClientConfig::default());
        assert_eq!(cfg.requested_sequence_number, 1);
    }

    // Once a socket-level sequence is captured, reconnect resumes there
    // instead of replaying the whole session.
    #[test]
    fn resumes_at_captured_sequence() {
        let mut state = RestartState::default();
        state.capture_soupbin_resume(4_207);
        let cfg = state.resume_login_config(&SoupBinClientConfig::default());
        assert_eq!(cfg.requested_sequence_number, 4_207);
    }

    // Clearing the captured sequence (venue-side session restart) falls back
    // to the sequence-1 replay path again.
    #[test]
    fn clearing_resume_falls_back_to_sequence_one() {
        let mut state = RestartState::default();
        state.capture_soupbin_resume(4_207);
        state.clear_soupbin_resume();
        let cfg = state.resume_login_config(&SoupBinClientConfig::default());
        assert_eq!(cfg.requested_sequence_number, 1);
    }

    // The Glimpse resume sequence is captured off RecoveryComplete only;
    // other control kinds never touch it.
    #[test]
    fn glimpse_resume_captured_from_recovery_complete_only() {
        let mut state = RestartState::default();
        state.observe_control(ControlKind::Heartbeat, 99);
        assert_eq!(
            state.glimpse_resume_seq, None,
            "heartbeat is not a resume point"
        );
        state.observe_control(ControlKind::RecoveryComplete, 5_150);
        assert_eq!(state.glimpse_resume_seq, Some(5_150));
        let cfg = state.resume_login_config(&SoupBinClientConfig::default());
        assert_eq!(cfg.requested_sequence_number, 5_150);
    }

    #[test]
    fn socket_resume_takes_precedence_over_glimpse() {
        let mut state = RestartState::default();
        state.observe_control(ControlKind::RecoveryComplete, 5_150);
        state.capture_soupbin_resume(5_200);
        let cfg = state.resume_login_config(&SoupBinClientConfig::default());
        assert_eq!(cfg.requested_sequence_number, 5_200);
    }

    #[test]
    fn full_replay_clears_every_resume_point() {
        let mut state = RestartState {
            soupbin_resume_seq: Some(5_200),
            glimpse_resume_seq: Some(5_150),
            primary_resume_seq: Some(4_000),
            secondary_resume_seq: Some(6_000),
        };
        state.reset_for_replay();
        assert_eq!(state, RestartState::default());
    }

    // Captured per-leg sequences must come back on the matching leg's login
    // config: swapped legs would resume each feed at the other's sequence.
    #[test]
    fn dual_resume_roundtrips_per_leg() {
        let mut state = RestartState::default();
        state.capture_dual_resume(4_000, 6_000);
        let (primary, secondary) = state.resume_dual_login_configs(&SoupBinClientConfig::default());
        assert_eq!(primary.requested_sequence_number, 4_000);
        assert_eq!(secondary.requested_sequence_number, 6_000);
    }

    // Without socket captures both legs fall back to the Glimpse recovery
    // point, then to sequence 1 when no Glimpse point exists either.
    #[test]
    fn dual_resume_falls_back_to_glimpse_then_sequence_one() {
        let mut state = RestartState::default();
        let base = SoupBinClientConfig::default();
        let (primary, secondary) = state.resume_dual_login_configs(&base);
        assert_eq!(primary.requested_sequence_number, 1);
        assert_eq!(secondary.requested_sequence_number, 1);

        state.observe_control(ControlKind::RecoveryComplete, 5_150);
        let (primary, secondary) = state.resume_dual_login_configs(&base);
        assert_eq!(primary.requested_sequence_number, 5_150);
        assert_eq!(secondary.requested_sequence_number, 5_150);
    }
}
