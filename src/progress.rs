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
use std::collections::hash_set::Union;
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
/// A Raft internal representation of a Configuration. This is corallary to a ConfState, but optimized for `contains` calls.
pub struct Configuration {
    /// The voter set.
    pub voters: FxHashSet<u64>,
    /// The learner set.
    pub learners: FxHashSet<u64>,
}

impl Configuration {
    /// Create a new configuration with the given configuration.
    pub fn new(
        voters: impl IntoIterator<Item = u64>,
        learners: impl IntoIterator<Item = u64>,
    ) -> Self {
        Self {
            voters: voters.into_iter().collect(),
            learners: learners.into_iter().collect(),
        }
    }
}

impl<'a, I> From<(I, I)> for Configuration
where
    I: IntoIterator<Item = &'a u64>,
{
    fn from((voters, learners): (I, I)) -> Self {
        Self {
            voters: voters.into_iter().cloned().collect(),
            learners: learners.into_iter().cloned().collect(),
        }
    }
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

    /// Validates that the configuration not problematic.
    ///
    /// Namely:
    /// * There can be no overlap of voters and learners.
    /// * There must be at least one voter.
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

/// `ProgressSet` contains several `Progress`es,
/// which could be `Leader`, `Follower` and `Learner`.
#[derive(Default, Clone)]
pub struct ProgressSet {
    progress: FxHashMap<u64, Progress>,
    configuration: Configuration,
    next_configuration: Option<Configuration>,
    configuration_capacity: (usize, usize),
}

impl ProgressSet {
    /// Creates a new ProgressSet.
    pub fn new() -> Self {
        Self::with_capacity(0, 0)
    }

    /// Create a progress sete with the specified sizes already reserved.
    pub fn with_capacity(voters: usize, learners: usize) -> Self {
        let configuration_capacity = (voters, learners);
        ProgressSet {
            progress: HashMap::with_capacity_and_hasher(
                voters + learners,
                FxBuildHasher::default(),
            ),
            configuration_capacity,
            configuration: Configuration::with_capacity(voters, learners),
            next_configuration: Option::default(),
        }
    }

    /// Returns the status of voters.
    ///
    /// **Note:** Do not use this for majority/quorum calculation. The Raft node may be
    /// transitioning to a new configuration and have two qourums. Use `has_quorum` instead.
    #[inline]
    pub fn voters(&self) -> impl Iterator<Item = (&u64, &Progress)> {
        let set = self.voter_ids();
        self.progress.iter().filter(move |(&k, _)| set.contains(&k))
    }

    /// Returns the status of learners.
    ///
    /// **Note:** Do not use this for majority/quorum calculation. The Raft node may be
    /// transitioning to a new configuration and have two qourums. Use `has_quorum` instead.
    #[inline]
    pub fn learners(&self) -> impl Iterator<Item = (&u64, &Progress)> {
        let set = self.learner_ids();
        self.progress.iter().filter(move |(&k, _)| set.contains(&k))
    }

    /// Returns the mutable status of voters.
    ///
    /// **Note:** Do not use this for majority/quorum calculation. The Raft node may be
    /// transitioning to a new configuration and have two qourums. Use `has_quorum` instead.
    #[inline]
    pub fn voters_mut(&mut self) -> impl Iterator<Item = (&u64, &mut Progress)> {
        let ids = self.voter_ids();
        self.progress
            .iter_mut()
            .filter(move |(k, _)| ids.contains(k))
    }

    /// Returns the mutable status of learners.
    ///
    /// **Note:** Do not use this for majority/quorum calculation. The Raft node may be
    /// transitioning to a new configuration and have two qourums. Use `has_quorum` instead.
    #[inline]
    pub fn learners_mut(&mut self) -> impl Iterator<Item = (&u64, &mut Progress)> {
        let ids = self.learner_ids();
        self.progress
            .iter_mut()
            .filter(move |(k, _)| ids.contains(k))
    }

    /// Returns the ids of all known voters.
    ///
    /// **Note:** Do not use this for majority/quorum calculation. The Raft node may be
    /// transitioning to a new configuration and have two qourums. Use `has_quorum` instead.
    #[inline]
    pub fn voter_ids(&self) -> FxHashSet<u64> {
        match self.next_configuration {
            Some(ref next) => self
                .configuration
                .voters
                .union(&next.voters)
                .cloned()
                .collect::<FxHashSet<u64>>(),
            None => self.configuration.voters.clone(),
        }
    }

    /// Returns the ids of all known learners.
    ///
    /// **Note:** Do not use this for majority/quorum calculation. The Raft node may be
    /// transitioning to a new configuration and have two qourums. Use `has_quorum` instead.
    #[inline]
    pub fn learner_ids(&self) -> FxHashSet<u64> {
        match self.next_configuration {
            Some(ref next) => self
                .configuration
                .learners
                .union(&next.learners)
                .cloned()
                .collect::<FxHashSet<u64>>(),
            None => self.configuration.learners.clone(),
        }
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
    ///
    /// **Note:** Do not use this for majority/quorum calculation. The Raft node may be
    /// transitioning to a new configuration and have two qourums. Use `has_quorum` instead.
    #[inline]
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&u64, &Progress)> {
        self.progress.iter()
    }

    /// Returns a mutable iterator across all the nodes and their progress.
    ///
    /// **Note:** Do not use this for majority/quorum calculation. The Raft node may be
    /// transitioning to a new configuration and have two qourums. Use `has_quorum` instead.
    #[inline]
    pub fn iter_mut(&mut self) -> impl ExactSizeIterator<Item = (&u64, &mut Progress)> {
        self.progress.iter_mut()
    }

    /// Adds a voter node
    pub fn insert_voter(&mut self, id: u64, pr: Progress) -> Result<(), Error> {
        debug!("Inserting voter with id {}.", id);

        if self.learner_ids().contains(&id) {
            Err(Error::Exists(id, "learners"))?;
        } else if self.voter_ids().contains(&id) {
            Err(Error::Exists(id, "voters"))?;
        }
        self.configuration.voters.insert(id);
        self.next_configuration
            .as_mut()
            .map(|config| config.voters.insert(id));

        self.progress.insert(id, pr);
        self.assert_progress_and_configuration_consistent();
        Ok(())
    }

    /// Adds a learner to the cluster
    pub fn insert_learner(&mut self, id: u64, pr: Progress) -> Result<(), Error> {
        debug!("Inserting learner with id {}.", id);

        if self.learner_ids().contains(&id) {
            Err(Error::Exists(id, "learners"))?;
        } else if self.voter_ids().contains(&id) {
            Err(Error::Exists(id, "voters"))?;
        }
        self.configuration.learners.insert(id);
        if let Some(ref mut next) = self.next_configuration {
            next.learners.insert(id);
        }

        self.progress.insert(id, pr);
        self.assert_progress_and_configuration_consistent();
        Ok(())
    }

    /// Removes the peer from the set of voters or learners.
    pub fn remove(&mut self, id: u64) -> Option<Progress> {
        debug!("Removing peer with id {}.", id);

        self.configuration.learners.remove(&id);
        self.configuration.voters.remove(&id);
        if let Some(ref mut next) = self.next_configuration {
            next.learners.remove(&id);
            next.voters.remove(&id);
        };

        let removed = self.progress.remove(&id);
        self.assert_progress_and_configuration_consistent();
        removed
    }

    /// Promote a learner to a peer.
    pub fn promote_learner(&mut self, id: u64) -> Result<(), Error> {
        debug!("Promote learner with id {}.", id);

        if !self.configuration.learners.remove(&id) {
            // Wasn't already a voter. We can't promote what doesn't exist.
            return Err(Error::Exists(id, "learners"));
        }
        if !self.configuration.voters.insert(id) {
            // Already existed, the caller should know this was a noop.
            return Err(Error::Exists(id, "voters"));
        }
        if let Some(ref mut next) = self.next_configuration {
            if !next.learners.remove(&id) {
                // Wasn't already a voter. We can't promote what doesn't exist.
                // Rollback change above.
                self.configuration.voters.remove(&id);
                self.configuration.learners.insert(id);
                return Err(Error::Exists(id, "next learners"));
            }
            if !next.voters.insert(id) {
                // Already existed, the caller should know this was a noop.
                // Rollback change above.
                self.configuration.voters.remove(&id);
                self.configuration.learners.insert(id);
                return Err(Error::Exists(id, "next voters"));
            }
        }

        self.assert_progress_and_configuration_consistent();
        Ok(())
    }

    #[inline(always)]
    fn assert_progress_and_configuration_consistent(&self) {
        debug_assert!(
            self.voter_ids()
                .union(&self.learner_ids())
                .all(|v| self.progress.contains_key(v))
        );
        debug_assert!(
            self.progress
                .keys()
                .all(|v| self.learner_ids().contains(v) || self.voter_ids().contains(v))
        );
        assert_eq!(
            self.voter_ids().len() + self.learner_ids().len(),
            self.progress.len()
        );
    }

    /// Returns the minimal committed index for the cluster.
    ///
    /// Eg. If the matched indexes are [2,2,2,4,5], it will return 2.
    pub fn minimum_committed_index(&self) -> u64 {
        let mut matched = self
            .configuration.voters.iter()
            .map(|id| {
                let peer = &self.progress[id];
                peer.matched })
            .collect::<Vec<_>>();
        // Reverse sort.
        matched.sort_by(|a, b| b.cmp(a));
        let mut mci = matched[matched.len() / 2];

        if let Some(next) = &self.next_configuration {
            let mut matched = next.voters.iter()
            .map(|id| {
                let peer = &self.progress[id];
                peer.matched })
            .collect::<Vec<_>>();
            matched.sort_by(|a, b| b.cmp(a));
            // Smallest that the majority has commited.
            let next_mci = matched[matched.len() / 2];
            if next_mci < mci {
                mci = next_mci;
            }
        }
        mci
    }

    /// Returns the Candidate's eligibility in the current election.
    ///
    /// If it is still eligible, it should continue polling nodes and checking.
    /// Eventually, the election will result in this returning either `Elected`
    /// or `Ineligible`, meaning the election can be concluded.
    pub fn candidacy_status<'a>(
        &self,
        votes: impl IntoIterator<Item = (&'a u64, &'a bool)>,
    ) -> CandidacyStatus {
        let (accepts, rejects) = votes.into_iter().fold(
            (FxHashSet::default(), FxHashSet::default()),
            |(mut accepts, mut rejects), (&id, &accepted)| {
                if accepted {
                    accepts.insert(id);
                } else {
                    rejects.insert(id);
                }
                (accepts, rejects)
            },
        );
        if self.configuration.has_quorum(&accepts) {
            return CandidacyStatus::Elected;
        } else if self.configuration.has_quorum(&rejects) {
            return CandidacyStatus::Ineligible;
        }
        if let Some(ref next) = self.next_configuration {
            if next.has_quorum(&accepts) {
                return CandidacyStatus::Elected;
            } else if next.has_quorum(&rejects) {
                return CandidacyStatus::Ineligible;
            }
        }

        CandidacyStatus::Eligible
    }

    /// Determines if the current quorum is active according to the this raft node.
    /// Doing this will set the `recent_active` of each peer to false.
    ///
    /// This should only be called by the leader.
    pub fn quorum_recently_active(&mut self, perspective_of: u64) -> bool {
        let mut active = FxHashSet::default();
        for (&id, pr) in self.voters_mut() {
            if id == perspective_of {
                active.insert(id);
                continue;
            }
            if pr.recent_active {
                active.insert(id);
            }
            pr.recent_active = false;
        }
        for (&_id, pr) in self.learners_mut() {
            pr.recent_active = false;
        }
        self.configuration.has_quorum(&active) &&
            // If `next` is `None` we don't consider it, so just `true` it.
            self.next_configuration.as_ref().map(|next| next.has_quorum(&active)).unwrap_or(true)
    }

    /// Determine if a quorum is formed from the given set of nodes.
    ///
    /// This is the only correct way to verify you have reached a quorum for the whole group.
    #[inline]
    pub fn has_quorum(&self, potential_quorum: &FxHashSet<u64>) -> bool {
        self.configuration.has_quorum(potential_quorum) && self
            .next_configuration
            .as_ref()
            .map(|next| next.has_quorum(potential_quorum))
            // If `next` is `None` we don't consider it, so just `true` it.
            .unwrap_or(true)
    }

    /// Determine if the ProgressSet is represented by a transition state under Joint Consensus.
    #[inline]
    pub fn is_in_transition(&self) -> bool {
        self.next_configuration.is_some()
    }

    /// Enter a joint consensus state to transition to the specified configuration.
    ///
    /// The `next` provided should be derived from the `ConfChange` message. `progress` is used as a basis for created peer `Progress` values. You are only expected to set `ins` from the `raft.max_inflights` value.
    ///
    /// Once this state is entered the leader should replicate the `ConfChange` message. After the
    /// majority of nodes, in both the current and the `next`, have committed the union state. At
    /// this point the leader can call `finalize_config_transition` and replicate a message
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
    pub fn begin_config_transition(&mut self, next: impl Into<Configuration>, mut progress: Progress) -> Result<(), Error> {
        let next = next.into();
        next.valid()?;
        // Demotion check.
        if let Some(&demoted) = self
            .configuration
            .voters
            .intersection(&next.learners)
            .next()
        {
            Err(Error::Exists(demoted, "learners"))?;
        }
        debug!("Beginning member configuration transition. End state will be voters {:?}, Learners: {:?}", next.voters, next.learners);

        // When a node is first added/promoted, we should mark it as recently active.
        // Otherwise, check_quorum may cause us to step down if it is invoked
        // before the added node has a chance to commuicate with us.
        progress.recent_active = true;
        progress.paused = false;
        for id in next.voters.iter().chain(&next.learners) {
            self.progress.entry(*id).or_insert_with(|| progress.clone());
        }
        self.next_configuration = Some(next);
        // Now we create progresses for any that do not exist.
        Ok(())
    }

    /// Finalizes the joint consensus state and transitions solely to the new state.
    ///
    /// This should be called only after calling `begin_config_transition` and the the majority
    /// of nodes in both the `current` and the `next` state have commited the changes.
    pub fn finalize_config_transition(&mut self) -> Result<(), Error> {
        let next = self.next_configuration.take();
        match next {
            None => Err(Error::NoPendingTransition)?,
            Some(next) => {
                debug!("Inserted finalize member configration transition command. State will be {:?}, Learners: {:?}", next.voters, next.learners);
                self.configuration = next;
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
#[derive(Debug, Default, PartialEq)]
pub struct Inflights {
    // the starting index in the buffer
    start: usize,
    // number of inflights in the buffer
    count: usize,

    // ring buffer
    buffer: Vec<u64>,
}

// The `buffer` must have it's capacity set correctly on clone, normally it does not.
impl Clone for Inflights {
    fn clone(&self) -> Self {
        let mut buffer = self.buffer.clone();
        buffer.reserve(self.buffer.capacity() - self.buffer.len());
        Inflights {
            start: self.start,
            count: self.count,
            buffer: buffer,
        }
    }
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
    use fxhash::FxHashSet;
    use {progress::Configuration, Progress, ProgressSet, Result};

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

    #[test]
    fn test_config_transition_remove_voter() -> Result<()> {
        check_set_nodes(&vec![1, 2], &vec![], &vec![1], &vec![])
    }

    #[test]
    fn test_config_transition_remove_learner() -> Result<()> {
        check_set_nodes(&vec![1], &vec![2], &vec![1], &vec![])
    }

    #[test]
    fn test_config_transition_conflicting_sets() {
        assert!(check_set_nodes(&vec![1], &vec![], &vec![1], &vec![1]).is_err())
    }

    #[test]
    fn test_config_transition_empty_sets() {
        assert!(check_set_nodes(&vec![], &vec![], &vec![], &vec![]).is_err())
    }

    #[test]
    fn test_config_transition_empty_voters() {
        assert!(check_set_nodes(&vec![1], &vec![], &vec![], &vec![]).is_err())
    }

    #[test]
    fn test_config_transition_add_voter() -> Result<()> {
        check_set_nodes(&vec![1], &vec![], &vec![1, 2], &vec![])
    }

    #[test]
    fn test_config_transition_add_learner() -> Result<()> {
        check_set_nodes(&vec![1], &vec![], &vec![1], &vec![2])
    }

    #[test]
    fn test_config_transition_promote_learner() -> Result<()> {
        check_set_nodes(&vec![1], &vec![2], &vec![1, 2], &vec![])
    }

    fn check_set_nodes<'a>(
        start_voters: impl IntoIterator<Item = &'a u64>,
        start_learners: impl IntoIterator<Item = &'a u64>,
        end_voters: impl IntoIterator<Item = &'a u64>,
        end_learners: impl IntoIterator<Item = &'a u64>,
    ) -> Result<()> {
        let start_voters = start_voters
            .into_iter()
            .cloned()
            .collect::<FxHashSet<u64>>();
        let start_learners = start_learners
            .into_iter()
            .cloned()
            .collect::<FxHashSet<u64>>();
        let end_voters = end_voters.into_iter().cloned().collect::<FxHashSet<u64>>();
        let end_learners = end_learners
            .into_iter()
            .cloned()
            .collect::<FxHashSet<u64>>();
        let transition_voters = start_voters
            .union(&end_voters)
            .cloned()
            .collect::<FxHashSet<u64>>();
        let transition_learners = start_learners
            .union(&end_learners)
            .cloned()
            .collect::<FxHashSet<u64>>();

        let mut set = ProgressSet::default();
        let default_progress = Progress::default();

        for starter in start_voters {
            set.insert_voter(starter, default_progress.clone())?;
        }
        for starter in start_learners {
            set.insert_learner(starter, default_progress.clone())?;
        }
        set.begin_config_transition(Configuration::new(end_voters.clone(), end_learners.clone()), default_progress)?;
        assert!(set.is_in_transition());
        assert_eq!(
            set.voter_ids(),
            transition_voters,
            "Transition state voters inaccurate"
        );
        assert_eq!(
            set.learner_ids(),
            transition_learners,
            "Transition state learners inaccurate."
        );

        set.finalize_config_transition()?;
        assert!(!set.is_in_transition());
        assert_eq!(set.voter_ids(), end_voters, "End state voters inaccurate");
        assert_eq!(
            set.learner_ids(),
            end_learners,
            "End state learners inaccurate"
        );
        Ok(())
    }
}
