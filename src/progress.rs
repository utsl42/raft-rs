// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use eraftpb::ConfState;
use errors::Error;
use fxhash::{FxBuildHasher, FxHashMap, FxHashSet};
use std::cmp;
use std::collections::{HashMap, HashSet};

// Since it's an integer, it rounds for us.
fn majority(total: usize) -> usize {
    (total / 2) + 1
}

/// The state of the progress.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum ProgressState {
    /// Whether it's probing.
    Probe,
    /// Whether it's replicating.
    Replicate,
    /// Whethers it's a snapshot.
    Snapshot,
}

impl Default for ProgressState {
    fn default() -> ProgressState {
        ProgressState::Probe
    }
}

#[derive(Clone, Debug, Default)]
pub struct Configuration {
    pub voters: FxHashSet<u64>,
    pub learners: FxHashSet<u64>,
}

impl<'a> From<&'a ConfState> for Configuration {
    fn from(conf_state: &'a ConfState) -> Self {
        Self {
            voters: conf_state.get_nodes().iter().cloned().collect(),
            learners: conf_state.get_learners().iter().cloned().collect(),
        }
    }
}

impl Into<ConfState> for Configuration {
    fn into(self) -> ConfState {
        let mut state = ConfState::new();
        state.set_nodes(self.voters.iter().cloned().collect());
        state.set_learners(self.learners.iter().cloned().collect());
        state
    }
}

impl Configuration {
    fn with_capacity(voters: usize, learners: usize) -> Self {
        Self {
            voters: HashSet::with_capacity_and_hasher(voters, FxBuildHasher::default()),
            learners: HashSet::with_capacity_and_hasher(learners, FxBuildHasher::default()),
        }
    }

    // Validates that the configuration not problematic.
    //
    // Namely:
    // * There can be no overlap of voters and learners.
    // * There must be at least one voter.
    pub fn valid(&self) -> Result<(), Error> {
        if let Some(id) = self.voters.intersection(&self.learners).next() {
            Err(Error::Exists(*id, "learners"))?;
        } else if self.voters.is_empty() {
            Err(Error::ConfigInvalid(
                "There must be at least one voter.".into(),
            ))?;
        }
        Ok(())
    }

    fn has_quorum(&self, potential_quorum: &FxHashSet<u64>) -> bool {
        self.voters.intersection(potential_quorum).count() >= majority(self.voters.len())
    }
}

/// The status of an election according to a Candidate node.
///
/// This is returned by `progress_set.election_status(vote_map)`
#[derive(Clone, Copy, Debug)]
pub enum CandidacyStatus {
    /// The election has been won by this Raft.
    Elected,
    /// It is still possible to win the election.
    Eligible,
    /// It is no longer possible to win the election.
    Ineligible,
}

/// The configuration state of the `ProgressSet`. It is used to determine how quorums are accounted for.
#[derive(Clone, Debug, Default)]
pub struct ConfigurationState {
    /// The cluster is not in a changing configuration. It has only one quorum. It is in a steady state.
    current: Configuration,
    /// The cluster is currently in a changing configuration. It has two seperate quorums which must agree before progressing.
    next: Option<Configuration>,
}

impl ConfigurationState {
    /// Create a new configuration state with the given sizes.
    pub fn with_capacity(voters: usize, learners: usize) -> Self {
        Self {
            current: Configuration::with_capacity(voters, learners),
            next: None,
        }
    }

    /// Adds a voter node.
    pub fn insert_voter(&mut self, id: u64) -> Result<(), Error> {
        if self.learners().contains(&id) {
            Err(Error::Exists(id, "learners"))?;
        } else if self.voters().contains(&id) {
            Err(Error::Exists(id, "voters"))?;
        }
        self.current.voters.insert(id);
        self.next.as_mut().map(|config| config.voters.insert(id));
        Ok(())
    }

    /// Returns the ids of all known voters.
    #[inline]
    pub fn voters(&self) -> FxHashSet<u64> {
        match self.next {
            Some(ref next) => self
                .current
                .voters
                .union(&next.voters)
                .cloned()
                .collect::<FxHashSet<u64>>(),
            None => self.current.voters.clone(),
        }
    }

    /// Returns the ids of all known learners.
    #[inline]
    pub fn learners(&self) -> FxHashSet<u64> {
        match self.next {
            Some(ref next) => self
                .current
                .learners
                .union(&next.learners)
                .cloned()
                .collect::<FxHashSet<u64>>(),
            None => self.current.learners.clone(),
        }
    }
    /// Promote a learner to a peer.
    pub fn promote_learner(&mut self, id: u64) -> Result<(), Error> {
        if !self.current.learners.remove(&id) {
            // Wasn't already a voter. We can't promote what doesn't exist.
            return Err(Error::Exists(id, "learners"));
        }
        if !self.current.voters.insert(id) {
            // Already existed, the caller should know this was a noop.
            return Err(Error::Exists(id, "voters"));
        }
        if let Some(ref mut next) = self.next {
            if !next.learners.remove(&id) {
                // Wasn't already a voter. We can't promote what doesn't exist.
                // Rollback change above.
                self.current.voters.remove(&id);
                self.current.learners.insert(id);
                return Err(Error::Exists(id, "next learners"));
            }
            if !next.voters.insert(id) {
                // Already existed, the caller should know this was a noop.
                // Rollback change above.
                self.current.voters.remove(&id);
                self.current.learners.insert(id);
                return Err(Error::Exists(id, "next voters"));
            }
        }
        Ok(())
    }

    /// Adds a learner to the cluster
    pub fn insert_learner(&mut self, id: u64) -> Result<(), Error> {
        if self.learners().contains(&id) {
            Err(Error::Exists(id, "learners"))?;
        } else if self.voters().contains(&id) {
            Err(Error::Exists(id, "voters"))?;
        }
        self.current.learners.insert(id);
        if let Some(ref mut next) = self.next {
            next.learners.insert(id);
        }
        Ok(())
    }

    /// Removes the peer from the set of voters or learners.
    pub fn remove(&mut self, id: u64) {
        self.current.learners.remove(&id);
        self.current.voters.remove(&id);
        if let Some(ref mut next) = self.next {
            next.learners.remove(&id);
            next.voters.remove(&id);
        };
    }

    pub fn has_quorum(&self, potential_quorum: &FxHashSet<u64>) -> bool {
        self.current.has_quorum(potential_quorum) && self
            .next
            .as_ref()
            .map(|next| next.has_quorum(potential_quorum))
            // If `next` is `None` we don't consider it, so just `true` it.
            .unwrap_or(true)
    }
}

/// `ProgressSet` contains several `Progress`es,
/// which could be `Leader`, `Follower` and `Learner`.
#[derive(Default, Clone)]
pub struct ProgressSet {
    progress: FxHashMap<u64, Progress>,
    configuration: ConfigurationState,
}

impl ProgressSet {
    /// Creates a new ProgressSet.
    pub fn new() -> Self {
        ProgressSet {
            progress: Default::default(),
            configuration: Default::default(),
        }
    }

    /// Create a progress sete with the specified sizes already reserved.
    pub fn with_capacity(voters: usize, learners: usize) -> Self {
        ProgressSet {
            progress: HashMap::with_capacity_and_hasher(
                voters + learners,
                FxBuildHasher::default(),
            ),
            configuration: ConfigurationState::with_capacity(voters, learners),
        }
    }

    /// Returns the status of voters.
    #[inline]
    pub fn voters(&self) -> impl Iterator<Item = (&u64, &Progress)> {
        let set = self.configuration.voters();
        self.progress.iter().filter(move |(&k, _)| set.contains(&k))
    }

    /// Returns the status of learners.
    #[inline]
    pub fn learners(&self) -> impl Iterator<Item = (&u64, &Progress)> {
        let set = self.configuration.learners();
        self.progress.iter().filter(move |(&k, _)| set.contains(&k))
    }

    /// Returns the mutable status of voters.
    #[inline]
    pub fn voters_mut(&mut self) -> impl Iterator<Item = (&u64, &mut Progress)> {
        let ids = self.configuration.voters();
        self.progress
            .iter_mut()
            .filter(move |(k, _)| ids.contains(k))
    }

    /// Returns the mutable status of learners.
    #[inline]
    pub fn learners_mut(&mut self) -> impl Iterator<Item = (&u64, &mut Progress)> {
        let ids = self.configuration.learners();
        self.progress
            .iter_mut()
            .filter(move |(k, _)| ids.contains(k))
    }

    /// Returns the ids of all known voters.
    #[inline]
    pub fn voter_ids(&self) -> FxHashSet<u64> {
        self.configuration.voters()
    }

    /// Returns the ids of all known learners.
    #[inline]
    pub fn learner_ids(&self) -> FxHashSet<u64> {
        self.configuration.learners()
    }

    /// Grabs a reference to the progress of a node.
    #[inline]
    pub fn get(&self, id: u64) -> Option<&Progress> {
        self.progress.get(&id)
    }

    /// Grabs a mutable reference to the progress of a node.
    #[inline]
    pub fn get_mut(&mut self, id: u64) -> Option<&mut Progress> {
        self.progress.get_mut(&id)
    }

    /// Returns an iterator across all the nodes and their progress.
    #[inline]
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&u64, &Progress)> {
        self.progress.iter()
    }

    /// Returns a mutable iterator across all the nodes and their progress.
    #[inline]
    pub fn iter_mut(&mut self) -> impl ExactSizeIterator<Item = (&u64, &mut Progress)> {
        self.progress.iter_mut()
    }

    /// Adds a voter node
    pub fn insert_voter(&mut self, id: u64, pr: Progress) -> Result<(), Error> {
        debug!("Inserting voter with id {}.", id);
        self.configuration.insert_voter(id)?;
        self.progress.insert(id, pr);
        self.assert_progress_and_configuration_consistent();
        Ok(())
    }

    /// Adds a learner to the cluster
    pub fn insert_learner(&mut self, id: u64, pr: Progress) -> Result<(), Error> {
        debug!("Inserting learner with id {}.", id);
        self.configuration.insert_learner(id)?;
        self.progress.insert(id, pr);
        self.assert_progress_and_configuration_consistent();
        Ok(())
    }

    /// Removes the peer from the set of voters or learners.
    pub fn remove(&mut self, id: u64) -> Option<Progress> {
        debug!("Removing peer with id {}.", id);
        self.configuration.remove(id);
        let removed = self.progress.remove(&id);
        self.assert_progress_and_configuration_consistent();
        removed
    }

    /// Promote a learner to a peer.
    pub fn promote_learner(&mut self, id: u64) -> Result<(), Error> {
        debug!("Promote learner with id {}.", id);
        self.configuration.promote_learner(id)?;
        self.assert_progress_and_configuration_consistent();
        Ok(())
    }

    #[inline(always)]
    fn assert_progress_and_configuration_consistent(&self) {
        debug_assert!(
            self.configuration
                .voters()
                .union(&self.configuration.learners())
                .all(|v| self.progress.contains_key(v))
        );
        debug_assert!(
            self.progress
                .keys()
                .all(|v| self.configuration.learners().contains(v)
                    || self.configuration.voters().contains(v))
        );
        assert_eq!(
            self.configuration.voters().len() + self.configuration.learners().len(),
            self.progress.len()
        );
    }

    /// Returns the minimal committed index for the cluster.
    ///
    /// Eg. If the matched indexes are [2,2,2,4,5], it will return 2.
    pub fn minimum_committed_index(&self) -> u64 {
        let mut matched = self
            .voters()
            .map(|(_id, peer)| peer.matched)
            .collect::<Vec<_>>();
        // Reverse sort.
        matched.sort_by(|a, b| b.cmp(a));
        // Smallest that the majority has commited.
        matched[matched.len() / 2]
    }

    /// Returns the Candidate's eligibility in the current election.
    ///
    /// If it is still eligible, it should continue polling nodes and checking.
    /// Eventually, the election will result in this returning either `Elected`
    /// or `Ineligible`, meaning the election can be concluded.
    pub fn candidacy_status<'a>(
        &self,
        id: u64,
        votes: impl IntoIterator<Item = (&'a u64, &'a bool)>,
    ) -> CandidacyStatus {
        let (accepted, total) =
            votes
                .into_iter()
                .fold((0, 0), |(mut accepted, mut total), (_, nominated)| {
                    if *nominated {
                        accepted += 1;
                    }
                    total += 1;
                    (accepted, total)
                });
        let quorum = majority(self.voter_ids().len());
        let rejected = total - accepted;

        info!(
            "{} [quorum: {}] has received {} votes and {} vote rejections",
            id, quorum, accepted, rejected,
        );

        if accepted >= quorum {
            CandidacyStatus::Elected
        } else if rejected == quorum {
            CandidacyStatus::Ineligible
        } else {
            CandidacyStatus::Eligible
        }
    }

    /// Determines if the current quorum is active according to the this raft node.
    /// Doing this will set the `recent_active` of each peer to false.
    ///
    /// This should only be called by the leader.
    pub fn quorum_recently_active(&mut self, perspective_of: u64) -> bool {
        let mut active = 0;
        for (&id, pr) in self.voters_mut() {
            if id == perspective_of {
                active += 1;
                continue;
            }
            if pr.recent_active { active += 1; }
            pr.recent_active = false;
        }
        for (&_id, pr) in self.learners_mut() {
            pr.recent_active = false;
        }
        active >= majority(self.voter_ids().len())
    }

    /// Determine if a quorum is formed from the given set of nodes.
    #[inline]
    pub fn has_quorum(&self, potential_quorum: &FxHashSet<u64>) -> bool {
        self.configuration.has_quorum(potential_quorum)
    }

    /// Determine if the ProgressSet is represented by a transition state under Joint Consensus.
    pub fn is_in_transition(&self) -> bool {
        self.configuration.next.is_some()
    }

    /// Enter a joint consensus state to transition to the specified configuration.
    ///
    /// The `next` provided should be derived from the `ConfChange` message.
    ///
    /// Once this state is entered the leader should replicate the `ConfChange` message. After the
    /// majority of nodes, in both the current and the `next`, have committed the union state. At
    /// this point the leader can call `commit_config_transition` and replicate a message
    /// commiting the change.
    ///
    /// Valid transitions:
    /// * Non-existing -> Learner
    /// * Non-existing -> Voter
    /// * Learner -> Voter
    /// * Learner -> Non-existing
    /// * Voter -> Non-existing
    ///
    /// Errors:
    /// * Voter -> Learner
    /// * Member as voter and learner.
    /// * Empty voter set.
    pub fn begin_config_transition(&mut self, next: impl Into<Configuration>) -> Result<(), Error> {
        let next = next.into();
        next.valid()?;
        // Demotion check.
        if let Some(&demoted) = self
            .configuration
            .voters()
            .intersection(&next.learners)
            .next()
        {
            Err(Error::Exists(demoted, "learners"))?;
        }
        debug!("Beginning member configuration transition. End state will be voters ({:?}), Learners: ({:?})", next.voters, next.learners);
        for id in next.voters.iter().chain(&next.learners) {
            // TODO: Fill ins_size with correct value.
            let new_progress = Progress::new(1, 10);
            self.progress
                .entry(*id)
                .or_insert_with(|| new_progress);
        }
        self.configuration.next = Some(next);
        // Now we create progresses for any that do not exist.
        Ok(())
    }

    /// Finalizes the joint consensus state and transitions solely to the new state.
    ///
    /// This should be called only after calling `begin_config_transition` and the the majority
    /// of nodes in both the `current` and the `next` state have commited the changes.
    pub fn commit_config_transition(&mut self) -> Result<(), Error> {
        let next = self.configuration.next.take();
        match next {
            None => Err(Error::NoPendingTransition)?,
            Some(next) => {
                debug!("Commiting member configration transition. State is now voters ({:?}), Learners: ({:?})", next.voters, next.learners);
                self.configuration.current = next;
            }
        }
        Ok(())
    }
}

/// The progress of catching up from a restart.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct Progress {
    /// How much state is matched.
    pub matched: u64,
    /// The next index to apply
    pub next_idx: u64,
    /// When in ProgressStateProbe, leader sends at most one replication message
    /// per heartbeat interval. It also probes actual progress of the follower.
    ///
    /// When in ProgressStateReplicate, leader optimistically increases next
    /// to the latest entry sent after sending replication message. This is
    /// an optimized state for fast replicating log entries to the follower.
    ///
    /// When in ProgressStateSnapshot, leader should have sent out snapshot
    /// before and stop sending any replication message.
    pub state: ProgressState,
    /// Paused is used in ProgressStateProbe.
    /// When Paused is true, raft should pause sending replication message to this peer.
    pub paused: bool,
    /// This field is used in ProgressStateSnapshot.
    /// If there is a pending snapshot, the pendingSnapshot will be set to the
    /// index of the snapshot. If pendingSnapshot is set, the replication process of
    /// this Progress will be paused. raft will not resend snapshot until the pending one
    /// is reported to be failed.
    pub pending_snapshot: u64,

    /// This is true if the progress is recently active. Receiving any messages
    /// from the corresponding follower indicates the progress is active.
    /// RecentActive can be reset to false after an election timeout.
    pub recent_active: bool,

    /// Inflights is a sliding window for the inflight messages.
    /// When inflights is full, no more message should be sent.
    /// When a leader sends out a message, the index of the last
    /// entry should be added to inflights. The index MUST be added
    /// into inflights in order.
    /// When a leader receives a reply, the previous inflights should
    /// be freed by calling inflights.freeTo.
    pub ins: Inflights,
}

impl Progress {
    /// Creates a new progress with the given settings.
    pub fn new(next_idx: u64, ins_size: usize) -> Self {
        Progress {
            next_idx,
            ins: Inflights::new(ins_size),
            ..Default::default()
        }
    }

    fn reset_state(&mut self, state: ProgressState) {
        self.paused = false;
        self.pending_snapshot = 0;
        self.state = state;
        self.ins.reset();
    }

    /// Changes the progress to a probe.
    pub fn become_probe(&mut self) {
        // If the original state is ProgressStateSnapshot, progress knows that
        // the pending snapshot has been sent to this peer successfully, then
        // probes from pendingSnapshot + 1.
        if self.state == ProgressState::Snapshot {
            let pending_snapshot = self.pending_snapshot;
            self.reset_state(ProgressState::Probe);
            self.next_idx = cmp::max(self.matched + 1, pending_snapshot + 1);
        } else {
            self.reset_state(ProgressState::Probe);
            self.next_idx = self.matched + 1;
        }
    }

    /// Changes the progress to a Replicate.
    pub fn become_replicate(&mut self) {
        self.reset_state(ProgressState::Replicate);
        self.next_idx = self.matched + 1;
    }

    /// Changes the progress to a snapshot.
    pub fn become_snapshot(&mut self, snapshot_idx: u64) {
        self.reset_state(ProgressState::Snapshot);
        self.pending_snapshot = snapshot_idx;
    }

    /// Sets the snapshot to failure.
    pub fn snapshot_failure(&mut self) {
        self.pending_snapshot = 0;
    }

    /// Unsets pendingSnapshot if Match is equal or higher than
    /// the pendingSnapshot
    pub fn maybe_snapshot_abort(&self) -> bool {
        self.state == ProgressState::Snapshot && self.matched >= self.pending_snapshot
    }

    /// Returns false if the given n index comes from an outdated message.
    /// Otherwise it updates the progress and returns true.
    pub fn maybe_update(&mut self, n: u64) -> bool {
        let need_update = self.matched < n;
        if need_update {
            self.matched = n;
            self.resume();
        };

        if self.next_idx < n + 1 {
            self.next_idx = n + 1
        }

        need_update
    }

    /// Optimistically advance the index
    pub fn optimistic_update(&mut self, n: u64) {
        self.next_idx = n + 1;
    }

    /// Returns false if the given index comes from an out of order message.
    /// Otherwise it decreases the progress next index to min(rejected, last)
    /// and returns true.
    pub fn maybe_decr_to(&mut self, rejected: u64, last: u64) -> bool {
        if self.state == ProgressState::Replicate {
            // the rejection must be stale if the progress has matched and "rejected"
            // is smaller than "match".
            if rejected <= self.matched {
                return false;
            }
            self.next_idx = self.matched + 1;
            return true;
        }

        // the rejection must be stale if "rejected" does not match next - 1
        if self.next_idx == 0 || self.next_idx - 1 != rejected {
            return false;
        }

        self.next_idx = cmp::min(rejected, last + 1);
        if self.next_idx < 1 {
            self.next_idx = 1;
        }
        self.resume();
        true
    }

    /// Determine whether progress is paused.
    pub fn is_paused(&self) -> bool {
        match self.state {
            ProgressState::Probe => self.paused,
            ProgressState::Replicate => self.ins.full(),
            ProgressState::Snapshot => true,
        }
    }

    /// Resume progress
    pub fn resume(&mut self) {
        self.paused = false;
    }

    /// Pause progress.
    pub fn pause(&mut self) {
        self.paused = true;
    }
}

/// A buffer of inflight messages.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct Inflights {
    // the starting index in the buffer
    start: usize,
    // number of inflights in the buffer
    count: usize,

    // ring buffer
    buffer: Vec<u64>,
}

impl Inflights {
    /// Creates a new buffer for inflight messages.
    pub fn new(cap: usize) -> Inflights {
        Inflights {
            buffer: Vec::with_capacity(cap),
            ..Default::default()
        }
    }

    /// Returns true if the inflights is full.
    pub fn full(&self) -> bool {
        self.count == self.cap()
    }

    /// The buffer capacity.
    pub fn cap(&self) -> usize {
        self.buffer.capacity()
    }

    /// Adds an inflight into inflights
    pub fn add(&mut self, inflight: u64) {
        if self.full() {
            panic!("cannot add into a full inflights")
        }

        let mut next = self.start + self.count;
        if next >= self.cap() {
            next -= self.cap();
        }
        assert!(next <= self.buffer.len());
        if next == self.buffer.len() {
            self.buffer.push(inflight);
        } else {
            self.buffer[next] = inflight;
        }
        self.count += 1;
    }

    /// Frees the inflights smaller or equal to the given `to` flight.
    pub fn free_to(&mut self, to: u64) {
        if self.count == 0 || to < self.buffer[self.start] {
            // out of the left side of the window
            return;
        }

        let mut i = 0usize;
        let mut idx = self.start;
        while i < self.count {
            if to < self.buffer[idx] {
                // found the first large inflight
                break;
            }

            // increase index and maybe rotate
            idx += 1;
            if idx >= self.cap() {
                idx -= self.cap();
            }

            i += 1;
        }

        // free i inflights and set new start index
        self.count -= i;
        self.start = idx;
    }

    /// Frees the first buffer entry.
    pub fn free_first_one(&mut self) {
        let start = self.buffer[self.start];
        self.free_to(start);
    }

    /// Frees all inflights.
    pub fn reset(&mut self) {
        self.count = 0;
        self.start = 0;
    }
}

#[cfg(test)]
mod test {
    use progress::Inflights;
    use setup_for_test;

    #[test]
    fn test_inflight_add() {
        setup_for_test();
        let mut inflight = Inflights::new(10);
        for i in 0..5 {
            inflight.add(i);
        }

        let wantin = Inflights {
            start: 0,
            count: 5,
            buffer: vec![0, 1, 2, 3, 4],
        };

        assert_eq!(inflight, wantin);

        for i in 5..10 {
            inflight.add(i);
        }

        let wantin2 = Inflights {
            start: 0,
            count: 10,
            buffer: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        };

        assert_eq!(inflight, wantin2);

        let mut inflight2 = Inflights {
            start: 5,
            buffer: Vec::with_capacity(10),
            ..Default::default()
        };
        inflight2.buffer.extend_from_slice(&[0, 0, 0, 0, 0]);

        for i in 0..5 {
            inflight2.add(i);
        }

        let wantin21 = Inflights {
            start: 5,
            count: 5,
            buffer: vec![0, 0, 0, 0, 0, 0, 1, 2, 3, 4],
        };

        assert_eq!(inflight2, wantin21);

        for i in 5..10 {
            inflight2.add(i);
        }

        let wantin22 = Inflights {
            start: 5,
            count: 10,
            buffer: vec![5, 6, 7, 8, 9, 0, 1, 2, 3, 4],
        };

        assert_eq!(inflight2, wantin22);
    }

    #[test]
    fn test_inflight_free_to() {
        setup_for_test();
        let mut inflight = Inflights::new(10);
        for i in 0..10 {
            inflight.add(i);
        }

        inflight.free_to(4);

        let wantin = Inflights {
            start: 5,
            count: 5,
            buffer: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        };

        assert_eq!(inflight, wantin);

        inflight.free_to(8);

        let wantin2 = Inflights {
            start: 9,
            count: 1,
            buffer: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        };

        assert_eq!(inflight, wantin2);

        for i in 10..15 {
            inflight.add(i);
        }

        inflight.free_to(12);

        let wantin3 = Inflights {
            start: 3,
            count: 2,
            buffer: vec![10, 11, 12, 13, 14, 5, 6, 7, 8, 9],
        };

        assert_eq!(inflight, wantin3);

        inflight.free_to(14);

        let wantin4 = Inflights {
            start: 5,
            count: 0,
            buffer: vec![10, 11, 12, 13, 14, 5, 6, 7, 8, 9],
        };

        assert_eq!(inflight, wantin4);
    }

    #[test]
    fn test_inflight_free_first_one() {
        setup_for_test();
        let mut inflight = Inflights::new(10);
        for i in 0..10 {
            inflight.add(i);
        }

        inflight.free_first_one();

        let wantin = Inflights {
            start: 1,
            count: 9,
            buffer: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        };

        assert_eq!(inflight, wantin);
    }
}

// TODO: Reorganize this whole file into separate files.
// See https://github.com/pingcap/raft-rs/issues/125
#[cfg(test)]
mod test_progress_set {
    use Result;
    use {Progress, ProgressSet};

    const CANARY: u64 = 123;

    #[test]
    fn test_insert_redundant_voter() -> Result<()> {
        let mut set = ProgressSet::default();
        let default_progress = Progress::default();
        let canary_progress = Progress {
            matched: CANARY,
            ..Default::default()
        };
        set.insert_voter(1, default_progress.clone())?;
        assert!(
            set.insert_voter(1, canary_progress).is_err(),
            "Should return an error on redundant insert."
        );
        assert_eq!(
            *set.get(1).expect("Should be inserted."),
            default_progress,
            "The ProgressSet was mutated in a `insert_voter` that returned error."
        );
        Ok(())
    }

    #[test]
    fn test_insert_redundant_learner() -> Result<()> {
        let mut set = ProgressSet::default();
        let default_progress = Progress::default();
        let canary_progress = Progress {
            matched: CANARY,
            ..Default::default()
        };
        set.insert_voter(1, default_progress.clone())?;
        assert!(
            set.insert_voter(1, canary_progress).is_err(),
            "Should return an error on redundant insert."
        );
        assert_eq!(
            *set.get(1).expect("Should be inserted."),
            default_progress,
            "The ProgressSet was mutated in a `insert_learner` that returned error."
        );
        Ok(())
    }

    #[test]
    fn test_insert_learner_that_is_voter() -> Result<()> {
        let mut set = ProgressSet::default();
        let default_progress = Progress::default();
        let canary_progress = Progress {
            matched: CANARY,
            ..Default::default()
        };
        set.insert_voter(1, default_progress.clone())?;
        assert!(
            set.insert_learner(1, canary_progress).is_err(),
            "Should return an error on invalid learner insert."
        );
        assert_eq!(
            *set.get(1).expect("Should be inserted."),
            default_progress,
            "The ProgressSet was mutated in a `insert_learner` that returned error."
        );
        Ok(())
    }

    #[test]
    fn test_insert_voter_that_is_learner() -> Result<()> {
        let mut set = ProgressSet::default();
        let default_progress = Progress::default();
        let canary_progress = Progress {
            matched: CANARY,
            ..Default::default()
        };
        set.insert_learner(1, default_progress.clone())?;
        assert!(
            set.insert_voter(1, canary_progress).is_err(),
            "Should return an error on invalid voter insert."
        );
        assert_eq!(
            *set.get(1).expect("Should be inserted."),
            default_progress,
            "The ProgressSet was mutated in a `insert_voter` that returned error."
        );
        Ok(())
    }

    #[test]
    fn test_promote_learner_already_voter() -> Result<()> {
        let mut set = ProgressSet::default();
        let default_progress = Progress::default();
        set.insert_voter(1, default_progress)?;
        let pre = set.get(1).expect("Should have been inserted").clone();
        assert!(
            set.promote_learner(1).is_err(),
            "Should return an error on invalid promote_learner."
        );
        assert!(
            set.promote_learner(2).is_err(),
            "Should return an error on invalid promote_learner."
        );
        assert_eq!(pre, *set.get(1).expect("Peer should not have been deleted"));
        Ok(())
    }
}
