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
//
//
use fxhash::FxHashSet;
use protobuf::{self, RepeatedField};
use raft::{
    eraftpb::{ConfChange, ConfChangeType, ConfState, Entry, EntryType, Message, MessageType},
    storage::MemStorage,
    Config, Configuration, Raft, Result, NO_LIMIT,
};
use std::ops::{Deref, DerefMut};
use test_util::{new_message, setup_for_test, Network};

// Test that the API itself works.
//
// * Errors are returned from misuse.
// * Happy path returns happy values.
mod api {
    use super::*;
    // Test that the cluster can transition from a single node to a whole cluster.
    #[test]
    fn can_transition() -> Result<()> {
        setup_for_test();
        let mut raft = Raft::new(
            &Config {
                id: 1,
                tag: "1".into(),
                peers: vec![1],
                learners: vec![],
                ..Default::default()
            },
            MemStorage::new(),
        )?;
        let begin_entry = begin_entry(&[1, 2, 3], &[4], raft.raft_log.last_index() + 1);
        raft.begin_membership_change(&begin_entry)?;
        let finalize_entry = finalize_entry(raft.raft_log.last_index() + 1);
        raft.finalize_membership_change(&finalize_entry)?;
        Ok(())
    }

    // Test if the process rejects an overlapping voter and learner set.
    #[test]
    fn checks_for_overlapping_membership() -> Result<()> {
        setup_for_test();
        let mut raft = Raft::new(
            &Config {
                id: 1,
                tag: "1".into(),
                peers: vec![1],
                learners: vec![],
                ..Default::default()
            },
            MemStorage::new(),
        )?;
        let begin_entry = begin_entry(&[1, 2, 3], &[1, 2, 3], raft.raft_log.last_index() + 1);
        assert!(raft.begin_membership_change(&begin_entry).is_err());
        Ok(())
    }

    // Test if the process rejects an voter demotion.
    #[test]
    fn checks_for_voter_demotion() -> Result<()> {
        setup_for_test();
        let mut raft = Raft::new(
            &Config {
                id: 1,
                tag: "1".into(),
                peers: vec![1, 2, 3],
                learners: vec![4],
                ..Default::default()
            },
            MemStorage::new(),
        )?;
        let begin_entry = begin_entry(&[1, 2], &[3, 4], raft.raft_log.last_index() + 1);
        assert!(raft.begin_membership_change(&begin_entry).is_err());
        Ok(())
    }

    // Test if the process rejects an voter demotion.
    #[test]
    fn finalize_before_begin_fails_gracefully() -> Result<()> {
        setup_for_test();
        let mut raft = Raft::new(
            &Config {
                id: 1,
                tag: "1".into(),
                peers: vec![1, 2, 3],
                learners: vec![4],
                ..Default::default()
            },
            MemStorage::new(),
        )?;
        let finalize_entry = finalize_entry(raft.raft_log.last_index() + 1);
        assert!(raft.finalize_membership_change(&finalize_entry).is_err());
        Ok(())
    }
}

// Test that small cluster is able to progress through adding a voter.
mod three_peers_add_voter {
    use super::*;

    /// In a steady state transition should proceed without issue.
    #[test]
    fn stable() -> Result<()> {
        setup_for_test();
        let leader = 1;
        let old_configuration = ([1, 2, 3], []);
        let new_configuration = ([1, 2, 3, 4], []);
        let mut scenario = Scenario::new(
            leader,
            (old_configuration.0.as_ref(), old_configuration.1.as_ref()),
            (new_configuration.0.as_ref(), new_configuration.1.as_ref()),
        )?;
        scenario.spawn_new_peers()?;
        scenario.propose_change_message()?;

        info!("Allowing quorum to commit");
        scenario.expect_read_and_dispatch_messages_from(&[1, 2, 3])?;

        info!("Advancing leader, now entered the joint");
        scenario.assert_can_apply_transition_entry_at_index(&[1], 2, ConfChangeType::BeginConfChange);
        scenario.assert_in_transition(&[1]);

        info!("Leader replicates the commit and finalize entry.");
        scenario.expect_read_and_dispatch_messages_from(&[1])?;
        scenario.assert_can_apply_transition_entry_at_index(&[2, 3], 2, ConfChangeType::BeginConfChange);
        scenario.assert_in_transition(&[1, 2, 3]);

        info!("Allowing new peers to catch up.");
        scenario.expect_read_and_dispatch_messages_from(&[4, 1, 4])?;
        scenario.assert_can_apply_transition_entry_at_index(&[4], 2, ConfChangeType::BeginConfChange);
        scenario.assert_in_transition(&[1, 2, 3, 4]);

        info!("Cluster leaving the joint.");
        scenario.expect_read_and_dispatch_messages_from(&[3, 2, 1])?;
        scenario.assert_can_apply_transition_entry_at_index(&[1, 2, 3, 4], 3, ConfChangeType::FinalizeConfChange);
        scenario.assert_not_in_transition(&[1, 2, 3, 4]);

        Ok(())
    }
}

// Test that small cluster is able to progress through adding a learner.
mod three_peers_add_learner {
    use super::*;

    /// In a steady state transition should proceed without issue.
    #[test]
    fn stable() -> Result<()> {
        setup_for_test();
        let leader = 1;
        let old_configuration = ([1, 2, 3], []);
        let new_configuration = ([1, 2, 3], [4]);
        let mut scenario = Scenario::new(
            leader,
            (old_configuration.0.as_ref(), old_configuration.1.as_ref()),
            (new_configuration.0.as_ref(), new_configuration.1.as_ref()),
        )?;
        scenario.spawn_new_peers()?;
        scenario.propose_change_message()?;

        info!("Allowing quorum to commit");
        scenario.expect_read_and_dispatch_messages_from(&[1, 2, 3])?;

        info!("Advancing leader, now entered the joint");
        scenario.assert_can_apply_transition_entry_at_index(&[1], 2, ConfChangeType::BeginConfChange);
        scenario.assert_in_transition(&[1]);

        info!("Leader replicates the commit and finalize entry.");
        scenario.expect_read_and_dispatch_messages_from(&[1])?;
        scenario.assert_can_apply_transition_entry_at_index(&[2, 3], 2, ConfChangeType::BeginConfChange);
        scenario.assert_in_transition(&[1, 2, 3]);

        info!("Allowing new peers to catch up.");
        scenario.expect_read_and_dispatch_messages_from(&[4, 1, 4])?;
        scenario.assert_can_apply_transition_entry_at_index(&[4], 2, ConfChangeType::BeginConfChange);
        scenario.assert_in_transition(&[1, 2, 3, 4]);

        info!("Cluster leaving the joint.");
        scenario.expect_read_and_dispatch_messages_from(&[3, 2, 1])?;
        scenario.assert_can_apply_transition_entry_at_index(&[1, 2, 3, 4], 3, ConfChangeType::FinalizeConfChange);
        scenario.assert_not_in_transition(&[1, 2, 3, 4]);

        Ok(())
    }
}

// Test that small cluster is able to progress through replacing a voter.
mod three_peers_replace_voter {
    use super::*;

    /// In a steady state transition should proceed without issue.
    #[test]
    fn stable() -> Result<()> {
        setup_for_test();
        let leader = 1;
        let old_configuration = ([1, 2, 3], []);
        let new_configuration = ([1, 2, 4], []);
        let mut scenario = Scenario::new(
            leader,
            (old_configuration.0.as_ref(), old_configuration.1.as_ref()),
            (new_configuration.0.as_ref(), new_configuration.1.as_ref()),
        )?;
        scenario.spawn_new_peers()?;
        scenario.propose_change_message()?;

        info!("Allowing quorum to commit");
        scenario.expect_read_and_dispatch_messages_from(&[1, 2, 3])?;

        info!("Advancing leader, now entered the joint");
        scenario.assert_can_apply_transition_entry_at_index(&[1], 2, ConfChangeType::BeginConfChange);
        scenario.assert_in_transition(&[1]);

        info!("Leader replicates the commit and finalize entry.");
        scenario.expect_read_and_dispatch_messages_from(&[1])?;
        scenario.assert_can_apply_transition_entry_at_index(&[2], 2, ConfChangeType::BeginConfChange);
        scenario.assert_in_transition(&[1, 2]);

        info!("Allowing new peers to catch up.");
        scenario.expect_read_and_dispatch_messages_from(&[4, 1, 4])?;
        scenario.assert_can_apply_transition_entry_at_index(&[4], 2, ConfChangeType::BeginConfChange);
        scenario.assert_in_transition(&[1, 2, 4]);

        info!("Cluster leaving the joint.");
        scenario.expect_read_and_dispatch_messages_from(&[2, 1, 4])?;
        scenario.assert_can_apply_transition_entry_at_index(&[1, 2, 4], 3, ConfChangeType::FinalizeConfChange);
        scenario.assert_not_in_transition(&[1, 2, 4]);

        Ok(())
    }

    // Ensure if a peer in the old quorum fails, but the quorum is still big enough, it's ok.
    #[test]
    fn pending_delete_fails_after_begin() -> Result<()> {
        setup_for_test();
        let leader = 1;
        let old_configuration = ([1, 2, 3], []);
        let new_configuration = ([1, 2, 4], []);
        let mut scenario = Scenario::new(
            leader,
            (old_configuration.0.as_ref(), old_configuration.1.as_ref()),
            (new_configuration.0.as_ref(), new_configuration.1.as_ref()),
        )?;
        scenario.spawn_new_peers()?;
        scenario.propose_change_message()?;

        info!("Allowing quorum to commit");
        scenario.expect_read_and_dispatch_messages_from(&[1, 2, 3])?;

        info!("Advancing leader, now entered the joint");
        scenario.assert_can_apply_transition_entry_at_index(&[1], 2, ConfChangeType::BeginConfChange);
        scenario.assert_in_transition(&[1]);

        scenario.isolate(3); // Take 3 down.

        info!("Leader replicates the commit and finalize entry.");
        scenario.expect_read_and_dispatch_messages_from(&[1])?;
        scenario.assert_can_apply_transition_entry_at_index(&[2], 2, ConfChangeType::BeginConfChange);
        scenario.assert_in_transition(&[1, 2]);

        info!("Allowing new peers to catch up.");
        scenario.expect_read_and_dispatch_messages_from(&[4, 1, 4])?;
        scenario.assert_can_apply_transition_entry_at_index(&[4], 2, ConfChangeType::BeginConfChange);
        scenario.assert_in_transition(&[1, 2, 4]);

        info!("Cluster leaving the joint.");
        scenario.expect_read_and_dispatch_messages_from(&[2, 1, 4])?;
        scenario.assert_can_apply_transition_entry_at_index(&[1, 2, 4], 3, ConfChangeType::FinalizeConfChange);
        scenario.assert_not_in_transition(&[1, 2, 4]);

        Ok(())
    }

    // Ensure if a peer in the new quorum fails, but the quorum is still big enough, it's ok.
    #[test]
    fn pending_create_with_quorum_fails_after_begin() -> Result<()> {
        setup_for_test();
        let leader = 1;
        let old_configuration = ([1, 2, 3], []);
        let new_configuration = ([1, 2, 4], []);
        let mut scenario = Scenario::new(
            leader,
            (old_configuration.0.as_ref(), old_configuration.1.as_ref()),
            (new_configuration.0.as_ref(), new_configuration.1.as_ref()),
        )?;
        scenario.spawn_new_peers()?;
        scenario.propose_change_message()?;

        info!("Allowing quorum to commit");
        scenario.expect_read_and_dispatch_messages_from(&[1, 2, 3])?;

        info!("Advancing leader, now entered the joint");
        scenario.assert_can_apply_transition_entry_at_index(&[1], 2, ConfChangeType::BeginConfChange);
        scenario.assert_in_transition(&[1]);

        scenario.isolate(4); // Take 4 down.

        info!("Leader replicates the commit and finalize entry.");
        scenario.expect_read_and_dispatch_messages_from(&[1])?;
        scenario.assert_can_apply_transition_entry_at_index(&[2, 3], 2, ConfChangeType::BeginConfChange);
        scenario.assert_in_transition(&[1, 2, 3]);

        info!("Cluster leaving the joint.");
        scenario.expect_read_and_dispatch_messages_from(&[2, 1])?;
        scenario.assert_can_apply_transition_entry_at_index(&[1, 2, 3], 3, ConfChangeType::FinalizeConfChange);
        scenario.assert_not_in_transition(&[1, 2, 3]);

        Ok(())
    }

    // Ensure if the peer pending a deletion and the peer pending a creation both fail it's still ok (so long as both quorums hold).
    #[test]
    fn pending_create_and_destroy_both_fail() -> Result<()> {
        setup_for_test();
        let leader = 1;
        let old_configuration = ([1, 2, 3], []);
        let new_configuration = ([1, 2, 4], []);
        let mut scenario = Scenario::new(
            leader,
            (old_configuration.0.as_ref(), old_configuration.1.as_ref()),
            (new_configuration.0.as_ref(), new_configuration.1.as_ref()),
        )?;
        scenario.spawn_new_peers()?;
        scenario.propose_change_message()?;

        info!("Allowing quorum to commit");
        scenario.expect_read_and_dispatch_messages_from(&[1, 2, 3])?;

        info!("Advancing leader, now entered the joint");
        scenario.assert_can_apply_transition_entry_at_index(&[1], 2, ConfChangeType::BeginConfChange);
        scenario.assert_in_transition(&[1]);

        scenario.isolate(3); // Take 3 down.
        scenario.isolate(4); // Take 4 down.

        info!("Leader replicates the commit and finalize entry.");
        scenario.expect_read_and_dispatch_messages_from(&[1])?;
        scenario.assert_can_apply_transition_entry_at_index(&[2], 2, ConfChangeType::BeginConfChange);
        scenario.assert_in_transition(&[1, 2]);

        info!("Cluster leaving the joint.");
        scenario.expect_read_and_dispatch_messages_from(&[2, 1])?;
        scenario.assert_can_apply_transition_entry_at_index(&[1, 2], 3, ConfChangeType::FinalizeConfChange);
        scenario.assert_not_in_transition(&[1, 2]);

        Ok(())
    }
}

// Test that small cluster is able to progress through adding a learner.
mod three_peers_to_five_with_learner {
    use super::*;

    /// In a steady state transition should proceed without issue.
    #[test]
    fn stable() -> Result<()> {
        setup_for_test();
        let leader = 1;
        let old_configuration = ([1, 2, 3], []);
        let new_configuration = ([1, 2, 3, 4, 5], [6]);
        let mut scenario = Scenario::new(
            leader,
            (old_configuration.0.as_ref(), old_configuration.1.as_ref()),
            (new_configuration.0.as_ref(), new_configuration.1.as_ref()),
        )?;
        scenario.spawn_new_peers()?;
        scenario.propose_change_message()?;

        info!("Allowing quorum to commit");
        scenario.expect_read_and_dispatch_messages_from(&[1, 2, 3])?;

        info!("Advancing leader, now entered the joint");
        scenario.assert_can_apply_transition_entry_at_index(&[1], 2, ConfChangeType::BeginConfChange);
        scenario.assert_in_transition(&[1]);

        info!("Leader replicates the commit and finalize entry.");
        scenario.expect_read_and_dispatch_messages_from(&[1])?;
        scenario.assert_can_apply_transition_entry_at_index(&[2, 3], 2, ConfChangeType::BeginConfChange);
        scenario.assert_in_transition(&[1, 2, 3]);

        info!("Allowing new peers to catch up.");
        scenario.expect_read_and_dispatch_messages_from(&[4, 5, 6, 1, 4, 5, 6])?;
        scenario.assert_can_apply_transition_entry_at_index(&[4, 5, 6], 2, ConfChangeType::BeginConfChange);
        scenario.assert_in_transition(&[1, 2, 3, 4, 5, 6]);

        info!("Cluster leaving the joint.");
        scenario.expect_read_and_dispatch_messages_from(&[3, 2, 1])?;
        scenario.assert_can_apply_transition_entry_at_index(&[1, 2, 3, 4, 5, 6], 3, ConfChangeType::FinalizeConfChange);
        scenario.assert_not_in_transition(&[1, 2, 3, 4, 5, 6]);

        Ok(())
    }

    /// In this, a single node (of 3) halts during the transition.
    #[test]
    fn minority_old_followers_halt_at_start() -> Result<()> {
        setup_for_test();
        let leader = 1;
        let old_configuration = ([1, 2, 3], []);
        let new_configuration = ([1, 2, 3, 4, 5], [6]);
        let mut scenario = Scenario::new(
            leader,
            (old_configuration.0.as_ref(), old_configuration.1.as_ref()),
            (new_configuration.0.as_ref(), new_configuration.1.as_ref()),
        )?;
        scenario.spawn_new_peers()?;
        scenario.isolate(3);
        scenario.propose_change_message()?;

        info!("Allowing quorum to commit");
        scenario.expect_read_and_dispatch_messages_from(&[1, 2])?;

        info!("Advancing leader, now entered the joint");
        scenario.assert_can_apply_transition_entry_at_index(&[1], 2, ConfChangeType::BeginConfChange);
        scenario.assert_in_transition(&[1]);

        info!("Leader replicates the commit and finalize entry.");
        scenario.expect_read_and_dispatch_messages_from(&[1])?;
        scenario.assert_can_apply_transition_entry_at_index(&[2], 2, ConfChangeType::BeginConfChange);
        scenario.assert_in_transition(&[1, 2]);
        scenario.assert_not_in_transition(&[3]);

        info!("Allowing new peers to catch up.");
        scenario.expect_read_and_dispatch_messages_from(&[4, 5, 6, 1])?;
        scenario.assert_can_apply_transition_entry_at_index(&[4, 5, 6], 2, ConfChangeType::BeginConfChange);
        scenario.assert_in_transition(&[1, 2, 4, 5, 6]);
        scenario.assert_not_in_transition(&[3]);

        scenario.expect_read_and_dispatch_messages_from(&[4, 5, 6])?;

        info!("Cluster leaving the joint.");
        {
            let mut leader = scenario.peers.get_mut(&1).unwrap();
            let ticks = leader.get_heartbeat_timeout();
            for _ in 0..=ticks {
                leader.tick();
            }
        }
        scenario.expect_read_and_dispatch_messages_from(&[2, 1, 4, 5, 6, 1, 4, 5, 6, 1])?;
        scenario.assert_can_apply_transition_entry_at_index(&[1, 2, 4, 5], 3, ConfChangeType::FinalizeConfChange);
        scenario.assert_not_in_transition(&[1, 2, 4, 5]);
        scenario.assert_not_in_transition(&[3]);

        Ok(())
    }
}

/// A test harness providing some useful utility and shorthand functions appropriate for this test suite.
/// 
/// Since it derefs into `Network` it can be used the same way.
struct Scenario {
    leader: u64,
    old_configuration: Configuration,
    new_configuration: Configuration,
    network: Network,

}
impl Deref for Scenario {
    type Target = Network;
    fn deref(&self) -> &Network {
        &self.network
    }
}
impl DerefMut for Scenario {
    fn deref_mut(&mut self) -> &mut Network {
        &mut self.network
    }
}

impl Scenario {
    /// Create a new scenario with the given state.
    fn new(
        leader: u64,
        old_configuration: impl Into<Configuration>,
        new_configuration: impl Into<Configuration>,
    ) -> Result<Scenario> {
        let old_configuration = old_configuration.into();
        let new_configuration = new_configuration.into();
        info!(
            "Beginning scenario, old: {:?}, new: {:?}",
            old_configuration, new_configuration
        );
        let starting_peers = old_configuration
            .voters
            .iter()
            .map(|&id| {
                Some(
                    Raft::new(
                        &Config {
                            id,
                            peers: old_configuration.voters.iter().cloned().collect(),
                            learners: old_configuration.learners.iter().cloned().collect(),
                            ..Default::default()
                        },
                        MemStorage::new(),
                    ).unwrap()
                    .into(),
                )
            }).collect();
        let mut scenario = Scenario {
            leader,
            old_configuration,
            new_configuration,
            network: Network::new(starting_peers),
        };
        // Elect the leader.
        info!("Sending MsgHup to predetermined leader ({})", leader);
        let message = new_message(leader, leader, MessageType::MsgHup, 0);
        scenario.send(vec![message]);
        Ok(scenario)
    }

    /// Creates any peers which are pending creation.
    /// 
    /// This *only* creates the peers and adds them to the `Network`. It does not take other action. Newly created peers are only aware of the leader and themself.
    fn spawn_new_peers(&mut self) -> Result<()> {
        let storage = MemStorage::new();
        let new_peers = self.new_peers();
        info!("Creating new peers. {:?}", new_peers);
        for id in new_peers.voters {
            let raft = Raft::new(
                &Config {
                    id,
                    peers: vec![self.leader, id],
                    learners: vec![],
                    ..Default::default()
                },
                storage.clone(),
            )?;
            self.peers.insert(id, raft.into());
        }
        for id in new_peers.learners {
            let raft = Raft::new(
                &Config {
                    id,
                    peers: vec![self.leader],
                    learners: vec![id],
                    ..Default::default()
                },
                storage.clone(),
            )?;
            self.peers.insert(id, raft.into());
        }
        Ok(())
    }

    /// Return a configuration containing only the peers pending creation.
    fn new_peers(&self) -> Configuration {
        let all_old = self
            .old_configuration
            .voters
            .union(&self.old_configuration.learners)
            .cloned()
            .collect::<FxHashSet<_>>();
        Configuration::new(
            self.new_configuration.voters.difference(&all_old).cloned(),
            self.new_configuration
                .learners
                .difference(&all_old)
                .cloned(),
        )
    }

    /// Send the message which proposes the configuration change.
    fn propose_change_message(&mut self) -> Result<()> {
        info!(
            "Proposing change message. Target: {:?}",
            self.new_configuration
        );
        let message = propose_change_message(
            self.leader,
            &self.new_configuration.voters,
            &self.new_configuration.learners,
            self.peers[&1].raft_log.last_index() + 1,
        );
        self.dispatch(vec![message])
    }

    /// Checks that the given peers are not in a transition state.
    fn assert_not_in_transition<'a>(&self, peers: impl IntoIterator<Item = &'a u64>) {
        for peer in peers.into_iter().map(|id| &self.peers[id]) {
            assert!(
                !peer.is_in_transition(),
                "Peer {} should not have been in transition.",
                peer.id
            );
        }
    }

    // Checks that the given peers are in a transition state.
    fn assert_in_transition<'a>(&self, peers: impl IntoIterator<Item = &'a u64>) {
        for peer in peers.into_iter().map(|id| &self.peers[id]) {
            assert!(
                peer.is_in_transition(),
                "Peer {} should have been in transition.",
                peer.id
            );
        }
    }

    /// Reads the pending entries to be applied to a raft peer, checks one is of the expected variant, and applies it. Then, it advances the node to that point in the configuration change.
    fn expect_apply_transition_entry<'a>(
        &mut self,
        peers: impl IntoIterator<Item = &'a u64>,
        entry_type: ConfChangeType,
    ) -> Result<()> {
        for peer in peers {
            debug!(
                "Advancing peer {}, expecting a {:?} entry.",
                peer, entry_type
            );
            let peer = self.network.peers.get_mut(peer).unwrap();
            if let Some(entries) = peer.raft_log.next_entries() {
                peer.mut_store().wl().append(&entries).unwrap();
                let mut found = false;
                for entry in &entries {
                    if entry.get_entry_type() == EntryType::EntryConfChange {
                        let conf_change =
                            protobuf::parse_from_bytes::<ConfChange>(entry.get_data())?;
                        if conf_change.get_change_type() == entry_type {
                            found = true;
                            if entry_type == ConfChangeType::BeginConfChange {
                                peer.begin_membership_change(&entry)?;
                            } else {
                                peer.finalize_membership_change(&entry)?;
                            }
                        }
                    }
                    if found {
                        peer.raft_log.stable_to(entry.get_index(), entry.get_term());
                        peer.raft_log.commit_to(entry.get_index());
                        peer.commit_apply(entry.get_index());
                        peer.tick();
                        break;
                    }
                }
                assert!(
                    found,
                    "{:?} message not found for peer {}. Got: {:?}",
                    entry_type, peer.id, entries
                );
            } else {
                panic!("Didn't have any entries {}", peer.id);
            }
        }
        Ok(())
    }

    fn read_and_dispatch_messages_from<'a>(
        &mut self,
        peers: impl IntoIterator<Item = &'a u64>,
    ) -> Result<()> {
        let peers = peers.into_iter().cloned();
        for peer in peers {
            info!("Reading and dispatching messages from {}", peer);
            let messages = self.peers.get_mut(&peer).unwrap().read_messages();
            self.dispatch(messages)?;
        }
        Ok(())
    }

    /// Reads messages from each peer in a given list, and dispatches their message before moving to the next peer.
    /// 
    /// Expects each peer to have a message. If the message is not defintely sent use `read_and_dispatch_messages_from`.
    fn expect_read_and_dispatch_messages_from<'a>(
        &mut self,
        peers: impl IntoIterator<Item = &'a u64>,
    ) -> Result<()> {
        let peers = peers.into_iter().cloned();
        for (step, peer) in peers.enumerate() {
            info!(
                "Expecting and dispatching messages from {} at step {}.",
                peer, step
            );
            let messages = self.peers.get_mut(&peer).unwrap().read_messages();
            trace!("{} sends messages: {:?}", peer, messages);
            assert!(
                !messages.is_empty(),
                "Expected peer {} to have messages at step {}.",
                peer,
                step
            );
            self.dispatch(messages)?;
        }
        Ok(())
    }

    // Verify there is a transition entry at the given index of the given variant.
    fn assert_transition_entry_at<'a>(
        &self,
        peers: impl IntoIterator<Item = &'a u64>,
        index: u64,
        entry_type: ConfChangeType,
    ) {
        let peers = peers.into_iter().cloned();
        for peer in peers {
            let entry = &self.peers[&peer]
                .raft_log
                .slice(index, index + 1, NO_LIMIT)
                .unwrap()[0];
            assert_eq!(entry.get_entry_type(), EntryType::EntryConfChange);
            let conf_change = protobuf::parse_from_bytes::<ConfChange>(entry.get_data()).unwrap();
            assert_eq!(conf_change.get_change_type(), entry_type);
        }
    }

    fn assert_can_apply_transition_entry_at_index<'a>(
        &mut self,
        peers: impl IntoIterator<Item = &'a u64>,
        index: u64,
        entry_type: ConfChangeType,
    ) {
        let peers = peers.into_iter().collect::<Vec<_>>();
        self.expect_apply_transition_entry(peers.clone(), entry_type);
        self.assert_transition_entry_at(peers, index, entry_type)
    }
}

fn conf_state<'a>(
    voters: impl IntoIterator<Item = &'a u64>,
    learners: impl IntoIterator<Item = &'a u64>,
) -> ConfState {
    let voters = voters.into_iter().cloned().collect::<Vec<_>>();
    let learners = learners.into_iter().cloned().collect::<Vec<_>>();
    let mut conf_state = ConfState::new();
    conf_state.set_nodes(voters);
    conf_state.set_learners(learners);
    conf_state
}

fn begin_entry<'a>(
    voters: impl IntoIterator<Item = &'a u64>,
    learners: impl IntoIterator<Item = &'a u64>,
    index: u64,
) -> Entry {
    let conf_state = conf_state(voters, learners);
    let mut conf_change = ConfChange::new();
    conf_change.set_change_type(ConfChangeType::BeginConfChange);
    conf_change.set_configuration(conf_state);
    let data = protobuf::Message::write_to_bytes(&conf_change).unwrap();
    let mut entry = Entry::new();
    entry.set_entry_type(EntryType::EntryConfChange);
    entry.set_data(data);
    entry.set_index(index);
    entry
}

fn finalize_entry(index: u64) -> Entry {
    let mut conf_change = ConfChange::new();
    conf_change.set_change_type(ConfChangeType::FinalizeConfChange);
    let data = protobuf::Message::write_to_bytes(&conf_change).unwrap();
    let mut entry = Entry::new();
    entry.set_entry_type(EntryType::EntryConfChange);
    entry.set_index(index);
    entry.set_data(data);
    entry
}

fn propose_change_message<'a>(
    recipient: u64,
    voters: impl IntoIterator<Item = &'a u64>,
    learners: impl IntoIterator<Item = &'a u64>,
    index: u64,
) -> Message {
    let begin_entry = begin_entry(voters, learners, index);
    let mut message = Message::new();
    message.set_to(recipient);
    message.set_msg_type(MessageType::MsgPropose);
    message.set_index(index);
    message.set_entries(RepeatedField::from_vec(vec![begin_entry]));
    message
}
