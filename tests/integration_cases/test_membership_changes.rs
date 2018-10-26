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
use raft::{
    eraftpb::{ConfState, Entry, EntryType, ConfChange, ConfChangeType, Message, MessageType},
    storage::MemStorage,
    Config, ProgressSet, Configuration, Raft, StateRole, Result,
};
use protobuf::{self, RepeatedField};
use std::ops::{Deref, DerefMut};
use test_util::{setup_for_test, Interface, Network, new_message};

fn conf_state<'a>(voters: impl IntoIterator<Item= &'a u64>, learners: impl IntoIterator<Item = &'a u64>) -> ConfState {
    let voters = voters.into_iter().cloned().collect::<Vec<_>>();
    let learners = learners.into_iter().cloned().collect::<Vec<_>>();
    let mut conf_state = ConfState::new();
    conf_state.set_nodes(voters);
    conf_state.set_learners(learners);
    conf_state
}

fn begin_entry<'a>(voters: impl IntoIterator<Item= &'a u64>, learners: impl IntoIterator<Item = &'a u64>, index: u64) -> Entry {
    let conf_state = conf_state(voters, learners);
    let mut conf_change = ConfChange::new();
    conf_change.set_change_type(ConfChangeType::BeginSetNodes);
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
    conf_change.set_change_type(ConfChangeType::CommitSetNodes);
    let data = protobuf::Message::write_to_bytes(&conf_change).unwrap();
    let mut entry = Entry::new();
    entry.set_entry_type(EntryType::EntryConfChange);
    entry.set_index(index);
    entry.set_data(data);
    entry
}

fn propose_change_message<'a>(recipient: u64, voters: impl IntoIterator<Item= &'a u64>, learners: impl IntoIterator<Item = &'a u64>, index: u64) -> Message {
    let mut begin_entry = begin_entry(voters, learners, index);
    let mut message = Message::new();
    message.set_to(recipient);
    message.set_msg_type(MessageType::MsgPropose);
    message.set_index(index);
    message.set_entries(RepeatedField::from_vec(vec![begin_entry]));
    message
}

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
        let mut raft = Raft::new(&Config {
            id: 1,
            tag: "1".into(),
            peers: vec![1],
            learners: vec![],
            ..Default::default()
        }, MemStorage::new())?;
        let begin_entry = begin_entry(&[1,2,3], &[4], raft.raft_log.last_index() + 1);
        raft.begin_membership_change(&begin_entry)?;
        let finalize_entry = finalize_entry(raft.raft_log.last_index() + 1);
        raft.finalize_membership_change(&finalize_entry)?;
        Ok(())
    }
    
    // Test if the process rejects an overlapping voter and learner set.
    #[test]
    fn checks_for_overlapping_membership() -> Result<()> {
        setup_for_test();
        let mut raft = Raft::new(&Config {
            id: 1,
            tag: "1".into(),
            peers: vec![1],
            learners: vec![],
            ..Default::default()
        }, MemStorage::new())?;
        let begin_entry = begin_entry(&[1,2,3], &[1, 2, 3], raft.raft_log.last_index() + 1);
        assert!(raft.begin_membership_change(&begin_entry).is_err());
        Ok(())
    }
    
    // Test if the process rejects an voter demotion.
    #[test]
    fn checks_for_voter_demotion() -> Result<()> {
        setup_for_test();
        let mut raft = Raft::new(&Config {
            id: 1,
            tag: "1".into(),
            peers: vec![1, 2, 3],
            learners: vec![4],
            ..Default::default()
        }, MemStorage::new())?;
        let begin_entry = begin_entry(&[1,2], &[3, 4], raft.raft_log.last_index() + 1);
        assert!(raft.begin_membership_change(&begin_entry).is_err());
        Ok(())
    }
    
    // Test if the process rejects an voter demotion.
    #[test]
    fn finalize_before_begin_fails_gracefully() -> Result<()> {
        setup_for_test();
        let mut raft = Raft::new(&Config {
            id: 1,
            tag: "1".into(),
            peers: vec![1, 2, 3],
            learners: vec![4],
            ..Default::default()
        }, MemStorage::new())?;
        let finalize_entry = finalize_entry(raft.raft_log.last_index() + 1);
        assert!(raft.finalize_membership_change(&finalize_entry).is_err());
        Ok(())
    }
}

// Test that a single peer is able to progress into a cluster.
mod three_peer_cluster_adds_voter {
    use super::*;
    
    /// In a steady state transition should proceed without issue.
    #[test]
    fn stable() -> Result<()> {
        setup_for_test();
        let leader = 1;
        let old_configuration = ([1,2,3], []);
        let new_configuration = ([1,2,3, 4], []);
        let mut scenario = Scenario::new(
            leader,
            (old_configuration.0.as_ref(), old_configuration.1.as_ref()),
            (new_configuration.0.as_ref(), new_configuration.1.as_ref()),
        )?;
        // Elect the leader.
        let message = new_message(1, 1, MessageType::MsgHup, 0);
        scenario.send(vec![message]);

        info!("Initializing the new Rafts.");
        for id in 4..=4 {
            let storage = MemStorage::new();
            scenario.peers.insert(id, Interface::new(Raft::new(&Config {
                id: id,
                peers: vec![1, id],
                learners: vec![],
                ..Default::default()
            }, storage.clone())?));
        }

        info!("Proposing a change");
        let propose_message = propose_change_message(
            1,
            &[1, 2, 3, 4],
            &[],
            scenario.peers[&1].raft_log.last_index() + 1
        );
        scenario.dispatch(vec![propose_message]);

        info!("Step the clsuter. First, the leader sends appends...");
        let messages = scenario.peers.get_mut(&1).unwrap().read_messages();
        scenario.dispatch(messages)?;
        info!("After, the followers respond.");
        let messages = scenario.peers.get_mut(&2).unwrap().read_messages();
        scenario.dispatch(messages)?;
        let messages = scenario.peers.get_mut(&3).unwrap().read_messages();
        scenario.dispatch(messages)?;

        info!("Advancing leader, now in joint");
        scenario.expect_and_apply_transition_entry(&[1], ConfChangeType::BeginSetNodes);
        
        // At this point, the leader has committed the Begin log.
        // Now the leader will inform the followers of the commit.
        info!("Leader distributes confirmation of the commit. Finalize as well, since they're immediately after one another in the log.");
        let messages = scenario.peers.get_mut(&1).unwrap().read_messages();
        scenario.dispatch(messages)?;
        
        info!("Advancing old follower configuration, now in joint");
        scenario.expect_and_apply_transition_entry(&[2, 3], ConfChangeType::BeginSetNodes);
        
        // Here we validate that the membership list is for the OLD is correct.
        //
        // With the leader, it will already be exiting the transition, so we only check followers.
        for peer in 2..=3 {
            let mut voter_set = scenario.peers[&peer].prs().voter_ids().iter().cloned().collect::<Vec<_>>();
            voter_set.sort();
            assert_eq!(&[1,2,3,4], voter_set.as_slice());
            assert!(scenario.peers[&peer].prs().is_in_transition(), "{} should be in transition", peer);
        }

        // Now that the new peers are part of the ProgressSet, they need to be caught up.
        // TODO: This should also send finalize.
        // debug!("Allowing leader to dispatch messages. Sending finalize to OLD peers, NEW peers must catch up. (This may require them to reach a quorum)");
        // let messages = network.peers.get_mut(&1).unwrap().read_messages();
        // network.dispatch(messages);

        // New follower gets initialized.
        info!("Allowing NEW peers to catch up");
        // We are essentially "isolating" these two to make it easier.
        for _ in 1..4 {
            let messages = scenario.peers.get_mut(&1).unwrap().read_messages();
            scenario.dispatch(messages)?;
            let messages = scenario.peers.get_mut(&4).unwrap().read_messages();
            scenario.dispatch(messages)?;
        }
        
        info!("Advancing NEW configuration, now in joint");
        scenario.expect_and_apply_transition_entry(&[4], ConfChangeType::BeginSetNodes);

        // At this point the leader will have commited the Begin log,
        // and every peer must transition.
        // The leader will have also appended the Finalize log.
        // 
        // By now all followers should be able to commit the Finalize.
        // Check in with the OLD configuration and get any responses they have.
        info!("Finishing up peer communications.");
        for _ in 1..=2 {
            let messages = scenario.peers.get_mut(&4).unwrap().read_messages();
            scenario.dispatch(messages)?;
            let messages = scenario.peers.get_mut(&1).unwrap().read_messages();
            scenario.dispatch(messages)?;
        }
        let messages = scenario.peers.get_mut(&3).unwrap().read_messages();
        scenario.dispatch(messages)?;
        let messages = scenario.peers.get_mut(&2).unwrap().read_messages();
        scenario.dispatch(messages)?;
        // Leader informs OLD of commit. NEW still catching up.
        let messages = scenario.peers.get_mut(&1).unwrap().read_messages();
        scenario.dispatch(messages)?;
        // NEW catches up.
        
        info!("Advancing all nodes, now leaving the joint");
        scenario.expect_and_apply_transition_entry(&[1, 2, 3, 4], ConfChangeType::CommitSetNodes);

        info!("Verifying existing peers are not transitioning.");
        for peer in 1..=4 {
            assert!(!scenario.peers[&peer].prs().is_in_transition(), "Peer {} is in transition. Should not be.", peer);
        }

        Ok(())
    }
}

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
    fn new(leader: u64, old_configuration: impl Into<Configuration>, new_configuration: impl Into<Configuration>) -> Result<Scenario> {
        let old_configuration = old_configuration.into();
        let new_configuration = new_configuration.into();
        let starting_peers = old_configuration.voters.iter().map(|&id|
            Some(Raft::new(&Config {
                id: id,
                peers: old_configuration.voters.iter().cloned().collect(),
                learners: old_configuration.learners.iter().cloned().collect(),
                ..Default::default()
            }, MemStorage::new()).unwrap().into())
        ).collect();
        Ok(Scenario {
            leader,
            old_configuration,
            new_configuration,
            network: Network::new(starting_peers),
        })

    }
    fn assert_not_in_transition<'a>(&self, peers: impl IntoIterator<Item= &'a u64>) {
        for peer in peers.into_iter().map(|id| self.network.peers.get(id).unwrap()) {
            assert!(!peer.is_in_transition(), "Peer {} should not have been in transition.");
        }
    }
    fn assert_in_transition<'a>(&self, peers: impl IntoIterator<Item= &'a u64>) {
        for peer in peers.into_iter().map(|id| self.network.peers.get(id).unwrap()) {
            assert!(peer.is_in_transition(), "Peer {} should have been in transition.");
        }
    }

    fn expect_and_apply_transition_entry<'a>(&mut self, peers: impl IntoIterator<Item = &'a u64>, entry_type: ConfChangeType) -> Result<()> {
        for peer in peers.into_iter() {
            debug!("Advancing peer {}, expecting a {:?} entry.", peer, entry_type);
            let peer = self.network.peers.get_mut(peer).unwrap();
            if let Some(entries) = peer.raft_log.next_entries() {
                peer.mut_store().wl().append(&entries).unwrap();
                let mut found = false;
                for entry in &entries {
                    if entry.get_entry_type() == EntryType::EntryConfChange {
                        let conf_change = protobuf::parse_from_bytes::<ConfChange>(entry.get_data())?;
                        if conf_change.get_change_type() == entry_type {
                            found = true;
                            if entry_type == ConfChangeType::BeginSetNodes {
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
                    }
                }
                assert!(found, "{:?} message not found for peer {}. Got: {:?}", entry_type, peer.id, entries);
                found = false;
            } else { panic!("Didn't have any entries {}", peer.id); }
        }
        Ok(())
    }
}

//mod single_to_cluster {
//    use super::*;
//    #[test]
//    fn stable() -> Result<()> {
//        setup_for_test();
//        let leader = 1;
//        let start = (&[1], &[]);
//        let end = (&[1, 2, 3], &[4]);
//        let new = &[2, 3, 4];
//        let mut scenario = Scenario::initialize(
//            leader, start.0, start.1
//        )?;
//        scenario.send_set_nodes(end.0, end.1)?;
//        let messages = scenario.initialize_new_peers(new)?;
//        scenario.replicate_begin_set_nodes(messages)?;
//        scenario.drive_to(Phase::Finalized)?;
//        Ok(())
//    }

//    #[test]
//    fn minority_follower_failure() -> Result<()> {
//        setup_for_test();
//        let leader = 1;
//        let start = (&[1], &[]);
//        let end = (&[1, 2, 3], &[4]);
//        let new = &[2, 3, 4];
//        let failed = &[2];
//        let mut scenario = Scenario::initialize(
//            leader, start.0, start.1
//        )?;
//        scenario.send_set_nodes(end.0, end.1)?;
//        let messages = scenario.initialize_new_peers(new)?;
//        failed.iter().for_each(|&failure| { scenario.isolate(failure); });
//        scenario.replicate_begin_set_nodes(messages)?;
//        assert!(scenario.transitioning(true, &[1]));
//        scenario.drive_to(Phase::LeaderHasBeginSetNodesResponses);
//        assert!(scenario.transitioning(true, &[3, 4]));
//        assert!(scenario.transitioning(false, &[1]));
//        scenario.drive_to(Phase::LeaderHasCommitSetNodesResponses);
//        assert!(scenario.transitioning(false, &[1, 3, 4]));
//        scenario.drive_to(Phase::Finalized);
//        Ok(())
//    }

//    #[test]
//    fn majority_follower_failure() -> Result<()> {
//        setup_for_test();
//        let leader = 1;
//        let start = (&[1], &[]);
//        let end = (&[1, 2, 3], &[4]);
//        let new = &[2, 3, 4];
//        let failed = &[2, 3];
//        let mut scenario = Scenario::initialize(
//            leader, start.0, start.1
//        )?;
//        scenario.send_set_nodes(end.0, end.1)?;
//        let messages = scenario.initialize_new_peers(new)?;
//        failed.iter().for_each(|&failure| { scenario.isolate(failure); });
//        scenario.replicate_begin_set_nodes(messages)?;
//        assert!(scenario.transitioning(true, &[1]));
//        // Since the majority is down, the cluster can't progress.
//        scenario.drive_to(Phase::LeaderHasBeginSetNodesResponses);
//        assert!(scenario.transitioning(true, &[1, 4]));
//        assert!(scenario.transitioning(false, &[2, 3]));
//        scenario.drive_to(Phase::LeaderHasCommitSetNodesResponses);
//        assert!(scenario.transitioning(true, &[1, 4]));
//        assert!(scenario.transitioning(false, &[2, 3]));
//        scenario.drive_to(Phase::Finalized);
//        Ok(())
//    }

//    #[test]
//    fn majority_follower_failure_with_recovery() -> Result<()> {
//        setup_for_test();
//        let leader = 1;
//        let start = (&[1], &[]);
//        let end = (&[1, 2, 3], &[4]);
//        let new = &[2, 3, 4];
//        let failed = &[2, 3];
//        let mut scenario = Scenario::initialize(
//            leader, start.0, start.1
//        )?;
//        scenario.send_set_nodes(end.0, end.1)?;
//        let messages = scenario.initialize_new_peers(new)?;
//        failed.iter().for_each(|&failure| { scenario.isolate(failure); });
//        scenario.replicate_begin_set_nodes(messages)?;
//        // Since the majority is down, the cluster can't progress.
//        assert!(scenario.transitioning(true, &[1, 4]));
//        assert!(scenario.transitioning(false, &[2, 3]));
//        // Let it try again.
//        scenario.phase(Phase::NewPeersInitialized);
//        warn!("Recovered");
//        scenario.recover();
//        scenario.heartbeat_timeout_and_exchange(&[1]);
//        scenario.drive_to(Phase::PeersHaveBeginSetNodes)?;
//        assert!(scenario.transitioning(true, &[1, 2, 3, 4]));
//        scenario.drive_to(Phase::LeaderHasCommitSetNodesResponses)?;
//        assert!(scenario.transitioning(false, &[1, 2, 3, 4]));
//        scenario.drive_to(Phase::Finalized);
//        Ok(())
//    }
//}

//#[test]
//fn test_cluster_to_single_leader() -> Result<()> {
//    setup_for_test();
//    let leader = 1;
//    let start = (&[1, 2, 3], &[]);
//    let end = (&[1], &[]);
//    let new = &[];
//    let mut scenario = Scenario::initialize(
//        leader, start.0, start.1
//    )?;
//    scenario.send_set_nodes(end.0, end.1)?;
//    let messages = scenario.initialize_new_peers(new)?;
//    scenario.replicate_begin_set_nodes(messages)?;
//    scenario.drive_to(Phase::Finalized)?;
//    Ok(())
//}

//#[test]
//fn test_replace_node() -> Result<()> {
//    setup_for_test();
//    let leader = 1;
//    let start = (&[1, 2, 3], &[]);
//    let end = (&[1, 2, 4], &[]);
//    let new = &[4];
//    let mut scenario = Scenario::initialize(
//        leader, start.0, start.1
//    )?;
//    scenario.send_set_nodes(end.0, end.1)?;
//    let messages = scenario.initialize_new_peers(new)?;
//    scenario.replicate_begin_set_nodes(messages)?;
//    scenario.drive_to(Phase::Finalized)?;
//    Ok(())
//}

//#[test]
//fn test_promote_learner() -> Result<()> {
//    setup_for_test();
//    let leader = 1;
//    let start = (&[1, 2, 3], &[4]);
//    let end = (&[1, 2, 3, 4], &[]);
//    let new = &[];
//    let mut scenario = Scenario::initialize(
//        leader, start.0, start.1
//    )?;
//    scenario.send_set_nodes(end.0, end.1)?;
//    let messages = scenario.initialize_new_peers(new)?;
//    scenario.replicate_begin_set_nodes(messages)?;
//    scenario.drive_to(Phase::Finalized)?;
//    Ok(())
//}

//#[test]
//fn test_add_learner() -> Result<()> {
//    setup_for_test();
//    let leader = 1;
//    let start = (&[1, 2, 3], &[]);
//    let end = (&[1, 2, 3], &[4]);
//    let new = &[4];
//    let mut scenario = Scenario::initialize(
//        leader, start.0, start.1
//    )?;
//    scenario.send_set_nodes(end.0, end.1)?;
//    let messages = scenario.initialize_new_peers(new)?;
//    scenario.replicate_begin_set_nodes(messages)?;
//    scenario.drive_to(Phase::Finalized)?;
//    Ok(())
//}

//#[test]
//fn test_add_voter() -> Result<()> {
//    setup_for_test();
//    let leader = 1;
//    let start = (&[1, 2, 3], &[]);
//    let end = (&[1, 2, 3, 4], &[]);
//    let new = &[4];
//    let mut scenario = Scenario::initialize(
//        leader, start.0, start.1
//    )?;
//    scenario.send_set_nodes(end.0, end.1)?;
//    let messages = scenario.initialize_new_peers(new)?;
//    scenario.replicate_begin_set_nodes(messages)?;
//    scenario.drive_to(Phase::Finalized)?;
//    Ok(())
//}

//#[test]
//fn test_disjoint_with_remaining_leader() -> Result<()> {
//    setup_for_test();
//    let leader = 1;
//    let start = (&[1, 2, 3], &[4, 5]);
//    let end = (&[1, 6, 7, 8], &[9, 10]);
//    let new = &[6, 7, 8, 9, 10];
//    let mut scenario = Scenario::initialize(
//        leader, start.0, start.1
//    )?;
//    scenario.send_set_nodes(end.0, end.1)?;
//    let messages = scenario.initialize_new_peers(new)?;
//    scenario.replicate_begin_set_nodes(messages)?;
//    scenario.drive_to(Phase::Finalized)?;
//    Ok(())
//}

//#[test]
//fn test_disjoint_with_departing_leader() -> Result<()> {
//    setup_for_test();
//    let leader = 1;
//    let start = (&[1, 2, 3], &[4, 5]);
//    let end = (&[1, 6, 7, 8], &[9, 10]);
//    let new = &[6, 7, 8, 9, 10];
//    let mut scenario = Scenario::initialize(
//        leader, start.0, start.1
//    )?;
//    scenario.send_set_nodes(end.0, end.1)?;
//    let messages = scenario.initialize_new_peers(new)?;
//    scenario.replicate_begin_set_nodes(messages)?;
//    scenario.drive_to(Phase::Finalized)?;
//    Ok(())
//}

//use std::ops::{Deref, DerefMut};
//#[derive(Clone, Copy, PartialEq, Eq, Debug, PartialOrd, Ord)]
//enum Phase {
//    Initialized,
//    StartClusterConnected,
//    ReceivedSetNodes,
//    NewPeersInitialized,
//    PeersHaveBeginSetNodes,
//    LeaderHasBeginSetNodesResponses,
//    PeersHaveCommitSetNodes,
//    LeaderHasCommitSetNodesResponses,
//    Finalized,
//}

///// A harness around a `Network` which has a bit of common context, and some driver functions to
///// progress along the workflow.
/////
///// Since `Scenario` implements `Deref` to a `Network` you can call all network functions on it directly.
/////
///// ```rust
///// let network = Network::initialize(1, vec[1, 2, 3], FxHash::default());;
///// network.isolate(1);
///// ```
//struct Scenario {
//    leader: u64,
//    network: Network,
//    phase: Phase,
//}
//impl Deref for Scenario {
//    type Target = Network;
//    fn deref(&self) -> &Network {
//        &self.network
//    }
//}
//impl DerefMut for Scenario {
//    fn deref_mut(&mut self) -> &mut Network {
//        &mut self.network
//    }
//}

//impl Scenario {
//    /// Wrap a given network with a specified leader, and inform the Scenario that the network is starting from a specific point.
//    fn network_at_phase(leader: u64, network: Network, phase: Phase) -> Scenario {
//        Scenario {
//            leader,
//            network,
//            phase
//        }
//    }

//    /// Drive the scenario to the specified Phase.
//    ///
//    /// This must be called on after the cluster has been allowed to initialize any new peers.
//    fn drive_to(&mut self, to: Phase) -> Result<()> {
//        assert!(self.phase < to, "Already in {:?} phase", to);
//        assert!(self.phase >= Phase::NewPeersInitialized, "Cluster must have at least received a `set_nodes` call and initialized new peers before it can be driven.");
//        if self.phase < Phase::PeersHaveBeginSetNodes && to >= Phase::PeersHaveBeginSetNodes {
//            self.replicate_begin_set_nodes(vec![])?;
//        }
//        if self.phase < Phase::LeaderHasBeginSetNodesResponses && to >= Phase::LeaderHasBeginSetNodesResponses  {
//            self.receive_begin_set_nodes_responses()?;
//        }
//        if self.phase < Phase::PeersHaveCommitSetNodes && to >= Phase::PeersHaveCommitSetNodes {
//            self.replicate_commit_set_nodes()?;
//        }
//        if self.phase < Phase::LeaderHasCommitSetNodesResponses && to >= Phase::LeaderHasCommitSetNodesResponses {
//            self.receive_commit_set_nodes_responses()?;
//        }
//        if self.phase < Phase::Finalized && to >= Phase::Finalized {
//            self.finalize()?;
//        }
//        Ok(())
//    }

//    /// Set the phase.
//    fn phase(&mut self, phase: Phase) {
//        self.phase = phase;
//    }

//    /// Set up a new cluster ready for a Set Nodes test scenario.
//    ///
//    /// Unlike `Network::new()` this will explicitly not make the peers aware of one another.
//    fn initialize<'a>(
//        leader: u64,
//        voters: impl IntoIterator<Item = &'a u64>,
//        learners: impl IntoIterator<Item = &'a u64>,
//    ) -> Result<Self> {
//        let voter_ids = voters.into_iter().map(|v| *v).collect::<Vec<_>>();
//        let learner_ids = learners.into_iter().map(|v| *v).collect::<Vec<_>>();
//        info!("Begin initialize phase, voters: {:?}, learners: {:?}.", voter_ids, learner_ids);
//        let voters = voter_ids.iter().map(|v|
//            Some(Interface::new(Raft::new(&Config {
//                id: *v,
//                tag: format!("{}", *v),
//                peers: voter_ids.clone(),
//                learners: learner_ids.clone(),
//                ..Default::default()
//            }, MemStorage::new()).unwrap()))
//        ).collect::<Vec<_>>();
//        let learners = learner_ids.iter().map(|v|
//            Some(Interface::new(Raft::new(&Config {
//                id: *v,
//                tag: format!("{}", *v),
//                peers: voter_ids.clone(),
//                learners: learner_ids.clone(),
//                ..Default::default()
//            }, MemStorage::new()).unwrap()))
//        ).collect::<Vec<_>>();

//        // Build up a network.
//        let mut network = Network::new(
//            voters.into_iter().chain(learners.into_iter()).collect(),
//        );
//        network.peers.get_mut(&leader).unwrap().become_candidate();
//        network.peers.get_mut(&leader).unwrap().become_leader();
//        let scenario = Scenario::network_at_phase(leader, network, Phase::Initialized);
//        info!("Initialized.");
//        Ok(scenario)
//    }

//    ///// Connects the given peers together in the network.
//    /////
//    ///// Typically this is called to connect together the starting peers in the network.
//    fn connect<'a>(
//        &mut self,
//        voters: impl IntoIterator<Item = &'a u64>,
//        learners: impl IntoIterator<Item = &'a u64>,
//    ) -> Result<()> {
//        assert_eq!(self.phase, Phase::Initialized);
//        let voters = voters.into_iter().map(|v| *v).collect::<FxHashSet<_>>();
//        let learners = learners.into_iter().map(|v| *v).collect::<FxHashSet<_>>();
//        info!("Transitioning from {:?} to {:?}.", self.phase, Phase::StartClusterConnected);

//        connect_peers(self, &voters, &learners);
//        self.phase(Phase::StartClusterConnected);
//        debug!("Now in {:?} phase.", self.phase);
//        Ok(())
//    }

//    fn send_set_nodes<'a> (&mut self,
//        voters: impl IntoIterator<Item = &'a u64>,
//        learners: impl IntoIterator<Item = &'a u64>,
//    ) -> Result<()> {
//        assert_eq!(self.phase, Phase::Initialized);
//        info!("Transitioning from {:?} to {:?}.", self.phase, Phase::ReceivedSetNodes);
//        let mut configuration = ConfState::new();
//        configuration.set_nodes(voters.into_iter().cloned().collect());
//        configuration.set_learners(learners.into_iter().cloned().collect());
//        // if let Some(ref mut peer) = self.network.peers.get_mut(&self.leader) {
//        //     peer.set_nodes(&configuration)?;
//        //     assert!(peer.prs().is_in_transition());
//        // } else { unreachable!(); }
//        self.phase(Phase::ReceivedSetNodes);
//        debug!("Now in {:?} phase.", self.phase);
//        Ok(())
//    }

//    /// The leader is going to send any new peers an initial request, which they will reject,
//    /// and then it will send messages to catch it up.
//    /// Ultimately, we want to make sure all the nodes are at the point where the leader is
//    /// sending off the BeginSetNodes.
//    ///
//    /// This is not *necessary* (the process still works), but further tests phases will make
//    /// assertions about the messages they process, so those may fail if the cluster is not at
//    /// the right state.
//    fn initialize_new_peers<'a>(&mut self, peers: impl IntoIterator<Item = &'a u64>) -> Result<Vec<Message>> {
//        assert_eq!(self.phase, Phase::ReceivedSetNodes);
//        info!("Transitioning from {:?} to {:?}.", self.phase, Phase::NewPeersInitialized);
//        let peers = peers.into_iter().map(|v| *v).collect::<FxHashSet<_>>();
//        let leader = self.leader;

//        // Currently the network is not populated with the declared new peers.
//        // So we must initialize them.
//        let leader = self.leader;
//        let voters = self.peers.get_mut(&leader).unwrap().prs().voter_ids().into_iter().collect::<Vec<_>>();
//        let learners = self.peers.get_mut(&leader).unwrap().prs().learner_ids().into_iter().collect::<Vec<_>>();
//        for peer in peers {
//            self.network.peers.insert(peer, Interface::new(Raft::new(&Config {
//                id: peer,
//                peers: voters.clone(),
//                learners: learners.clone(),
//                tag: format!("{}", peer),
//                ..Default::default()
//            }, MemStorage::new())?));
//        }

//        let mut pending_set_nodes = vec![];
//        let mut pending_initialization_messages = vec![];
//        // When new peer are first contacted they initialize entries to their log.
//        // Existing peers, however, are unlikely to need to do this.
//        //
//        // So split them and send those rounds first.
//        let mut messages = self.network.peers.get_mut(&leader).unwrap()
//            .read_messages();
//        while !pending_initialization_messages.is_empty() || !messages.is_empty() {
//            info!("Pending: {} uncategorized messages, {} initialization_messages, {} SetNodes", messages.len(), pending_initialization_messages.len(), pending_set_nodes.len());
//            for message in messages.drain(..) {
//                let is_set_nodes = {
//                    if let Some(entry) = message.get_entries().iter().next() {
//                        protobuf::parse_from_bytes::<ConfChange>(entry.get_data()).unwrap().get_change_type() == ConfChangeType::BeginSetNodes
//                    } else { false }
//                };
//                if is_set_nodes {
//                    pending_set_nodes.push(message);
//                } else {
//                    pending_initialization_messages.push(message);
//                }
//            }
//            for message in pending_initialization_messages.drain(..) {
//                let to = message.get_to();
//                self.network.dispatch(vec![message])?;
//                let responses = self.network.peers.get_mut(&to).unwrap().read_messages();
//                self.network.dispatch(responses)?;
//            }
//            messages.append(&mut self.network.peers.get_mut(&leader).unwrap()
//                .read_messages());
//        }
//        self.phase(Phase::NewPeersInitialized);
//        debug!("Now in {:?} phase.", self.phase);
//        Ok(pending_set_nodes)
//    }
//    fn replicate_begin_set_nodes(&mut self, pending_set_nodes: impl Into<Option<Vec<Message>>>) -> Result<()> {
//        assert_eq!(self.phase, Phase::NewPeersInitialized);
//        info!("Transitioning from {:?} to {:?}.", self.phase, Phase::PeersHaveBeginSetNodes);

//        let messages = self.network.read_messages().into_iter().chain(pending_set_nodes.into().unwrap_or(vec![])).collect::<Vec<_>>();
//        for message in messages.iter() {
//            assert_eq!(message.get_msg_type(), MessageType::MsgAppend);
//            if let Some(entry) = message.get_entries().iter().next() {
//                let data = protobuf::parse_from_bytes::<ConfChange>(entry.get_data())?;
//                assert_eq!(data.get_change_type(), ConfChangeType::BeginSetNodes, "Peer ID: {:?}", data.get_node_id());
//            }
//        }
//        self.network.dispatch(messages)?;
//        self.phase(Phase::PeersHaveBeginSetNodes);
//        debug!("Now in {:?} phase.", self.phase);
//        Ok(())
//    }
//    ///
//    fn receive_begin_set_nodes_responses(&mut self) -> Result<()> {
//        assert_eq!(self.phase, Phase::PeersHaveBeginSetNodes);
//        info!("Transitioning from {:?} to {:?}.", self.phase, Phase::LeaderHasBeginSetNodesResponses);
//        let messages = self.network
//            .read_messages();
//        let mut responses_to_collect = vec![];
//        for message in messages.iter() {
//            assert_eq!(message.get_msg_type(), MessageType::MsgAppendResponse);
//            assert_eq!(message.get_reject(), false);
//            assert!(message.get_entries().iter().next().is_none());
//            responses_to_collect.push(message.get_to());
//        }
//        self.network.dispatch(messages)?;
//        // for respondee in responses_to_collect {
//        //     let response = self.network.peers.get_mut(&respondee).unwrap().read_messages();
//        //     self.network.dispatch(response);
//        // }
//        self.phase(Phase::LeaderHasBeginSetNodesResponses);
//        debug!("Now in {:?} phase.", self.phase);
//        Ok(())
//    }
//    fn replicate_commit_set_nodes(&mut self) -> Result<()> {
//        assert_eq!(self.phase, Phase::LeaderHasBeginSetNodesResponses);
//        info!("Transitioning from {:?} to {:?}.", self.phase, Phase::PeersHaveCommitSetNodes);
//        let messages = self.network
//            .peers.get_mut(&self.leader).unwrap().read_messages();
//        let messages = self.network.filter(messages);
//        for message in messages.iter() {
//            assert_eq!(message.get_msg_type(), MessageType::MsgAppend);
//            if let Some(entry) = message.get_entries().iter().next() {
//                let data = protobuf::parse_from_bytes::<ConfChange>(entry.get_data())?;
//                assert_eq!(data.get_change_type(), ConfChangeType::CommitSetNodes);
//            }
//        }
//        self.network.dispatch(messages)?;
//        self.phase(Phase::PeersHaveCommitSetNodes);
//        debug!("Now in {:?} phase.", self.phase);
//        Ok(())
//    }
//    fn receive_commit_set_nodes_responses(&mut self) -> Result<()> {
//        assert_eq!(self.phase, Phase::PeersHaveCommitSetNodes);
//        info!("Transitioning from {:?} to {:?}.", self.phase, Phase::LeaderHasCommitSetNodesResponses);
//        let messages = self.network
//            .read_messages();
//        for message in messages.iter() {
//            assert_eq!(message.get_msg_type(), MessageType::MsgAppendResponse);
//            assert_eq!(message.get_reject(), false);
//            assert!(message.get_entries().iter().next().is_none());
//        }
//        self.network.dispatch(messages)?;
//        self.phase(Phase::LeaderHasCommitSetNodesResponses);
//        debug!("Now in {:?} phase.", self.phase);
//        Ok(())
//    }
//    fn finalize(&mut self) -> Result<()> {
//        assert_eq!(self.phase, Phase::LeaderHasCommitSetNodesResponses);
//        info!("Transitioning from {:?} to {:?}.", self.phase, Phase::Finalized);
//        // for (id, _) in self.network.peers.get(&self.leader).unwrap().prs().iter() {
//        //    assert!(!self.network.peers.get(&id).unwrap().prs().is_in_transition(),
//        //            "Peer {:?} should have left transition.", id);
//        // }
//        debug!("Now in {:?} phase.", self.phase);
//        self.phase(Phase::Finalized);
//        Ok(())
//    }
//    fn transitioning<'a>(&self, expected: bool, peers: impl IntoIterator<Item = &'a u64>) -> bool {
//        peers.into_iter().all(|id| {
//            let is_transitioning = self.network.peers[id].prs().is_in_transition();
//            if is_transitioning != expected {
//                debug!("{} transition state: {}, should be {}", id, is_transitioning, expected);
//            }
//            is_transitioning == expected
//        })
//    }
//    fn heartbeat_timeout_and_exchange<'a>(&mut self, peers: impl IntoIterator<Item = &'a u64>) {
//        let heartbeats = peers.into_iter().flat_map(|id| {
//            let peer = self.network.peers.get_mut(id).unwrap();
//            let timeout = peer.get_heartbeat_timeout();
//            (0..timeout).for_each(|_| { peer.tick(); });
//            peer.read_messages()
//        }).collect::<Vec<_>>();

//        self.network.dispatch(heartbeats.clone());
//        let responses = heartbeats.iter()
//            .map(|v| v.get_to())
//            .flat_map(|id| self.network.peers.get_mut(&id).unwrap().read_messages())
//            .collect::<Vec<_>>();
//        self.network.dispatch(responses);
//    }
//}

//fn connect_peers<'a>(
//    network: &mut Network,
//    voters: impl IntoIterator<Item = &'a u64>,
//    learners: impl IntoIterator<Item = &'a u64>,
//) {
//    let voters = voters.into_iter().cloned().collect::<FxHashSet<u64>>();
//    let learners = learners.into_iter().cloned().collect::<FxHashSet<u64>>();
//    for voter in voters.clone() {
//        for &other_peer in voters.iter().chain(&learners) {
//            if voter == other_peer {
//                continue;
//            } else {
//                network
//                    .peers
//                    .get_mut(&other_peer)
//                    .expect(&format!("Expected node {} to exist.", other_peer))
//                    .add_node(voter);
//            }
//        }
//    }
//    for learner in learners.clone() {
//        for &other_peer in voters.iter().chain(&learners) {
//            if learner == other_peer {
//                continue;
//            } else {
//                network
//                    .peers
//                    .get_mut(&other_peer)
//                    .expect(&format!("Expected node {} to exist.", other_peer))
//                    .add_learner(learner);
//            }
//        }
//    }
//    let messages = voters.iter().chain(&learners).flat_map(|peer| {
//        let peer = network.peers.get_mut(peer).unwrap();
//        peer.tick();
//        peer.read_messages()
//    }).collect();
//    network.send(messages);
//    // Ensure the node has the intended initial configuration
//    voters.iter().chain(learners.iter()).for_each(|id| {
//        assert_membership(
//            &voters,
//            &learners,
//            network
//                .peers
//                .get_mut(id)
//                .expect(&format!("Expected peer {} to be created.", id))
//                .prs(),
//        )
//    });
//}

//fn process(network: &mut Network, messages: Vec<Message>) -> Result<Vec<Message>> {
//    let mut responses = vec![];
//    for message in messages {
//        let id = message.get_to();
//        // Append
//        let peer = network.peers.get_mut(&id).unwrap();
//        peer.step(message)?;
//        responses.append(&mut peer.read_messages());
//    }
//    Ok(responses)
//}

//fn assert_membership<'a>(
//    assert_voters: impl IntoIterator<Item = &'a u64>,
//    assert_learners: impl IntoIterator<Item = &'a u64>,
//    progress_set: &ProgressSet,
//) {
//    let assert_voters = assert_voters.into_iter().cloned().collect::<FxHashSet<_>>();
//    let assert_learners = assert_learners
//        .into_iter()
//        .cloned()
//        .collect::<FxHashSet<_>>();
//    let voters = progress_set
//        .voter_ids()
//        .iter()
//        .cloned()
//        .collect::<FxHashSet<_>>();
//    let learners = progress_set
//        .learner_ids()
//        .iter()
//        .cloned()
//        .collect::<FxHashSet<_>>();
//    assert_eq!(voters, assert_voters);
//    assert_eq!(learners, assert_learners);
//}
