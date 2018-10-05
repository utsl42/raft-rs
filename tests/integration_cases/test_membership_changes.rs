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
    Config, ProgressSet, Raft, Result,
};
use protobuf;
use test_util::{setup_for_test, Interface, Network};

fn new_unconnected_network<'a>(
    initial_leader: u64,
    voters: impl IntoIterator<Item = &'a u64>,
    learners: impl IntoIterator<Item = &'a u64>,
) -> Result<Network> {
    let mut voters = voters.into_iter().cloned().collect::<FxHashSet<_>>();
    voters.remove(&initial_leader);

    let mut network = Network::new(vec![Some(Interface::new(Raft::new(
        &Config {
            id: initial_leader,
            peers: vec![initial_leader],
            ..Default::default()
        },
        MemStorage::new(),
    )?))]);

    for &id in voters.iter() {
        let config = Config {
            id,
            peers: vec![id],
            ..Default::default()
        };
        network
            .peers
            .insert(id, Interface::new(Raft::new(&config, MemStorage::new())?));
    }

    for id in learners.into_iter().cloned() {
        let config = Config {
            id,
            learners: vec![id],
            ..Default::default()
        };
        network
            .peers
            .insert(id, Interface::new(Raft::new(&config, MemStorage::new())?));
    }
    network
        .peers
        .get_mut(&initial_leader)
        .unwrap()
        .become_candidate();
    network
        .peers
        .get_mut(&initial_leader)
        .unwrap()
        .become_leader();

    Ok(network)
}

fn connect_peers<'a>(
    network: &mut Network,
    voters: impl IntoIterator<Item = &'a u64>,
    learners: impl IntoIterator<Item = &'a u64>,
) {
    let voters = voters.into_iter().cloned().collect::<FxHashSet<u64>>();
    let learners = learners.into_iter().cloned().collect::<FxHashSet<u64>>();
    for voter in voters.clone() {
        for &other_peer in voters.iter().chain(&learners) {
            if voter == other_peer {
                continue;
            } else {
                network
                    .peers
                    .get_mut(&other_peer)
                    .expect(&format!("Expected node {} to exist.", other_peer))
                    .add_node(voter);
            }
        }
    }
    for learner in learners.clone() {
        for &other_peer in voters.iter().chain(&learners) {
            if learner == other_peer {
                continue;
            } else {
                network
                    .peers
                    .get_mut(&other_peer)
                    .expect(&format!("Expected node {} to exist.", other_peer))
                    .add_learner(learner);
            }
        }
    }
    let messages = voters.iter().chain(&learners).flat_map(|peer| {
        let peer = network.peers.get_mut(peer).unwrap();
        peer.tick();
        peer.read_messages()
    }).collect();
    network.send(messages);
    // Ensure the node has the intended initial configuration
    voters.iter().chain(learners.iter()).for_each(|id| {
        assert_membership(
            &voters,
            &learners,
            network
                .peers
                .get_mut(id)
                .expect(&format!("Expected peer {} to be created.", id))
                .prs(),
        )
    });
}

fn process(network: &mut Network, messages: Vec<Message>) -> Result<Vec<Message>> {
    let mut responses = vec![];
    for message in messages {
        let id = message.get_to();
        // Append
        let peer = network.peers.get_mut(&id).unwrap();
        peer.step(message)?;
        responses.append(&mut peer.read_messages());
    }
    Ok(responses)
}

fn assert_membership<'a>(
    assert_voters: impl IntoIterator<Item = &'a u64>,
    assert_learners: impl IntoIterator<Item = &'a u64>,
    progress_set: &ProgressSet,
) {
    let assert_voters = assert_voters.into_iter().cloned().collect::<FxHashSet<_>>();
    let assert_learners = assert_learners
        .into_iter()
        .cloned()
        .collect::<FxHashSet<_>>();
    let voters = progress_set
        .voter_ids()
        .iter()
        .cloned()
        .collect::<FxHashSet<_>>();
    let learners = progress_set
        .learner_ids()
        .iter()
        .cloned()
        .collect::<FxHashSet<_>>();
    assert_eq!(voters, assert_voters);
    assert_eq!(learners, assert_learners);
}


#[test]
fn test_single_to_cluster() -> Result<()> {
    setup_for_test();
    let leader = 1;
    let start = (&[1], &[]);
    let end = (&[1, 2, 3], &[4]);
    let all = (end.0.into_iter().chain(start.0.into_iter()), end.1.into_iter().chain(start.1.into_iter()));
    let mut scenario = Scenario::initialize(
        leader, all.0, all.1
    )?;
    scenario.connect(start.0, start.1)?;
    scenario.send_set_nodes(end.0, end.1)?;
    // scenario.initialize_peers(&[4]);
    scenario.drive_to(Phase::Finalized);
    Ok(())
}

#[test]
fn test_cluster_to_single_leader() -> Result<()> {
    setup_for_test();
    let leader = 1;
    let start = (&[1, 2, 3], &[]);
    let end = (&[1], &[]);
    let all = (end.0.into_iter().chain(start.0.into_iter()), end.1.into_iter().chain(start.1.into_iter()));
    let mut scenario = Scenario::initialize(
        leader, all.0, all.1
    )?;
    scenario.connect(start.0, start.1)?;
    scenario.send_set_nodes(end.0, end.1)?;
    // scenario.initialize_peers(&[]);
    scenario.drive_to(Phase::Finalized);
    Ok(())
}

#[test]
fn test_replace_node() -> Result<()> {
    setup_for_test();
    let leader = 1;
    let start = (&[1, 2, 3], &[]);
    let end = (&[1, 2, 4], &[]);
    let all = (end.0.into_iter().chain(start.0.into_iter()), end.1.into_iter().chain(start.1.into_iter()));
    let mut scenario = Scenario::initialize(
        leader, all.0, all.1
    )?;
    scenario.connect(start.0, start.1)?;
    scenario.send_set_nodes(end.0, end.1)?;
    // scenario.initialize_peers(&[4]);
    scenario.drive_to(Phase::Finalized);
    Ok(())
}

#[test]
fn test_promote_learner() -> Result<()> {
    setup_for_test();
    let leader = 1;
    let start = (&[1, 2, 3], &[4]);
    let end = (&[1, 2, 3, 4], &[]);
    let all = (end.0.into_iter().chain(start.0.into_iter()), end.1.into_iter().chain(start.1.into_iter()));
    let mut scenario = Scenario::initialize(
        leader, all.0, all.1
    )?;
    scenario.connect(start.0, start.1)?;
    scenario.send_set_nodes(end.0, end.1)?;
    // scenario.initialize_peers(&[]);
    scenario.drive_to(Phase::Finalized);
    Ok(())
}

#[test]
fn test_add_learner() -> Result<()> {
    setup_for_test();
    let leader = 1;
    let start = (&[1, 2, 3], &[]);
    let end = (&[1, 2, 3], &[4]);
    let all = (end.0.into_iter().chain(start.0.into_iter()), end.1.into_iter().chain(start.1.into_iter()));
    let mut scenario = Scenario::initialize(
        leader, all.0, all.1
    )?;
    scenario.connect(start.0, start.1)?;
    scenario.send_set_nodes(end.0, end.1)?;
    // scenario.initialize_peers(&[4]);
    scenario.drive_to(Phase::Finalized);
    Ok(())
}

#[test]
fn test_add_voter() -> Result<()> {
    setup_for_test();
    let leader = 1;
    let start = (&[1, 2, 3], &[]);
    let end = (&[1, 2, 3, 4], &[]);
    let all = (end.0.into_iter().chain(start.0.into_iter()), end.1.into_iter().chain(start.1.into_iter()));
    let mut scenario = Scenario::initialize(
        leader, all.0, all.1
    )?;
    scenario.connect(start.0, start.1)?;
    scenario.send_set_nodes(end.0, end.1)?;
    // scenario.initialize_peers(&[4]);
    scenario.drive_to(Phase::Finalized);
    Ok(())
}

#[test]
fn test_disjoint_with_remaining_leader() -> Result<()> {
    setup_for_test();
    let leader = 1;
    let start = (&[1, 2, 3], &[4, 5]);
    let end = (&[1, 6, 7, 8], &[9, 10]);
    let all = (end.0.into_iter().chain(start.0.into_iter()), end.1.into_iter().chain(start.1.into_iter()));
    let mut scenario = Scenario::initialize(
        leader, all.0, all.1
    )?;
    scenario.connect(start.0, start.1)?;
    scenario.send_set_nodes(end.0, end.1)?;
    // scenario.initialize_peers(&[6, 7, 8, 9, 10]);
    scenario.drive_to(Phase::Finalized);
    Ok(())
}

#[test]
fn test_disjoint_with_departing_leader() -> Result<()> {
    setup_for_test();
    let leader = 1;
    let start = (&[1, 2, 3], &[4, 5]);
    let end = (&[6, 7, 8], &[9, 10]);
    let all = (end.0.into_iter().chain(start.0.into_iter()), end.1.into_iter().chain(start.1.into_iter()));
    let mut scenario = Scenario::initialize(
        leader, all.0, all.1
    )?;
    scenario.connect(start.0, start.1)?;
    scenario.send_set_nodes(end.0, end.1)?;
    // scenario.initialize_peers(&[6, 7, 8, 9, 10]);
    scenario.drive_to(Phase::Finalized);
    Ok(())
}

use std::ops::{Deref, DerefMut};
#[derive(Clone, Copy, PartialEq, Eq, Debug, PartialOrd, Ord)]
enum Phase {
    Initialized,
    StartClusterConnected,
    ReceivedSetNodes,
    // PeersInitialized,
    PeersHaveBeginSetNodes,
    LeaderHasBeginSetNodesResponses,
    PeersHaveCommitSetNodes,
    LeaderHasCommitSetNodesResponses,
    Finalized,
}
struct Scenario {
    leader: u64,
    network: Network,
    phase: Phase,
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
    fn network_at_phase(leader: u64, network: Network, phase: Phase) -> Scenario {
        Scenario {
            leader,
            network,
            phase
        }
    }

    fn drive_to(&mut self, to: Phase) -> Result<()> {
        assert!(self.phase < to);
        assert!(self.phase >= Phase::ReceivedSetNodes, "Cluster must have at least received a `set_nodes` call before it can be driven.");
        if self.phase < Phase::PeersHaveBeginSetNodes {
            self.replicate_begin_set_nodes();
        }
        if self.phase < Phase::LeaderHasBeginSetNodesResponses {
            self.receive_begin_set_nodes_responses();
        }
        if self.phase < Phase::PeersHaveCommitSetNodes {
            self.replicate_commit_set_nodes();
        }
        if self.phase < Phase::LeaderHasCommitSetNodesResponses {
            self.receive_commit_set_nodes_responses();
        }
        if self.phase < Phase::Finalized {
            self.finalize();
        }
        Ok(())
    }

    fn phase(&mut self, phase: Phase) {
        self.phase = phase;
    }

    fn initialize<'a>(
        leader: u64,
        voters: impl IntoIterator<Item = &'a u64>,
        learners: impl IntoIterator<Item = &'a u64>,
    ) -> Result<Self> {
        debug!("Begin initialize phase.");
        let voters = voters.into_iter().map(|v| *v).collect::<FxHashSet<_>>();
        let learners = learners.into_iter().map(|v| *v).collect::<FxHashSet<_>>();

        // Build up a network.
        let mut network = new_unconnected_network(
            leader,
            &voters,
            &learners,
        )?;
        // Connect the starting peers.
        let scenario = Scenario::network_at_phase(leader, network, Phase::Initialized);
        Ok(scenario)
    }

    fn connect<'a>(
        &mut self,
        voters: impl IntoIterator<Item = &'a u64>,
        learners: impl IntoIterator<Item = &'a u64>,
    ) -> Result<()> {
        assert_eq!(self.phase, Phase::Initialized);
        let voters = voters.into_iter().map(|v| *v).collect::<FxHashSet<_>>();
        let learners = learners.into_iter().map(|v| *v).collect::<FxHashSet<_>>();
        debug!("Proceeding through {:?} phase.", self.phase);

        connect_peers(self, &voters, &learners);
        debug!("Finished {:?} phase.", self.phase);
        self.phase(Phase::StartClusterConnected);
        Ok(())
    }

    fn send_set_nodes<'a> (&mut self,
        voters: impl IntoIterator<Item = &'a u64>,
        learners: impl IntoIterator<Item = &'a u64>,
    ) -> Result<()> {
        assert_eq!(self.phase, Phase::StartClusterConnected);
        debug!("Proceeding through {:?} phase.", self.phase);
        let mut configuration = ConfState::new();
        configuration.set_nodes(voters.into_iter().cloned().collect());
        configuration.set_learners(learners.into_iter().cloned().collect());
        if let Some(ref mut peer) = self.network.peers.get_mut(&self.leader) {
            peer.set_nodes(&configuration);
            assert!(peer.prs().is_in_transition());
        }
        debug!("Finished {:?} phase.", self.phase);
        self.phase(Phase::ReceivedSetNodes);
        Ok(())
    }
    // fn initialize_peers<'a>(&mut self, peers: impl IntoIterator<Item = &'a u64>) -> Result<()> {
    //     assert_eq!(self.phase, Phase::ReceivedSetNodes);
    //     debug!("Proceeding through {:?} phase.", self.phase);
    //     let peers = peers.into_iter().map(|v| *v).collect::<FxHashSet<_>>();
    //     let leader = self.leader;
    //     let messages = self.network.peers.get_mut(&leader).unwrap()
    //         .read_messages();
    //     let messages = messages.into_iter().filter(|message| peers.contains(&message.get_to())).collect::<Vec<_>>();
    //     // One round trip will add the initializer entry to the node and have it respond to the leader.
    //     for message in messages.iter() {
    //         assert_eq!(message.get_msg_type(), MessageType::MsgAppend);
    //         let entry = message.get_entries().iter().next().unwrap();
    //         assert_eq!(entry.get_entry_type(), EntryType::EntryNormal);
    //         assert_eq!(entry.get_index(), 1);

    //     }
    //     self.network.dispatch(messages)?;

    //     // Next, the peers will respond.
    //     let messages = self.network.peers.iter_mut().filter(|(id, _)| peers.contains(id)).flat_map(|(_, p)| p.read_messages()).collect::<Vec<_>>();
    //     for message in messages.iter() {
    //         assert_eq!(message.get_msg_type(), MessageType::MsgAppendResponse);
    //         assert_eq!(message.get_reject(), false);
    //         assert!(message.get_entries().iter().next().is_none());
    //     }
    //     self.network.dispatch(messages)?;
    //     debug!("Finished {:?} phase.", self.phase);
    //     self.phase(Phase::PeersInitialized);
    //     Ok(())
    // }
    fn replicate_begin_set_nodes(&mut self) -> Result<()> {
        assert_eq!(self.phase, Phase::ReceivedSetNodes);
        debug!("Proceeding through {:?} phase.", self.phase);
        let messages = self.network
            .read_messages();
        let (initializers, config_changes) = messages.into_iter().fold((vec![], vec![]), |(mut inits, mut configs), message| {
                // If it has an entry it's a ConfChange.
                assert!(message.get_msg_type() == MessageType::MsgAppend);
                let is_set_nodes = {
                    let entry = message.get_entries().iter().next().unwrap();
                    protobuf::parse_from_bytes::<ConfChange>(entry.get_data()).unwrap().get_change_type() == ConfChangeType::BeginSetNodes
                };
                if is_set_nodes {
                    configs.push(message);
                } else {
                    inits.push(message);
                }
                (inits, configs)
        });
        let pending_responses = initializers.iter().map(|v| v.get_to()).collect::<Vec<_>>();
        self.network.dispatch(initializers)?;
        for pending in pending_responses {
            let messages = self.network.peers.get_mut(&pending).unwrap().read_messages();
            self.network.dispatch(messages);
        }

        for message in self.network.read_messages().iter().chain(config_changes.iter()) {
            assert_eq!(message.get_msg_type(), MessageType::MsgAppend);
            let entry = message.get_entries().iter().next().unwrap();
            let data = protobuf::parse_from_bytes::<ConfChange>(entry.get_data())?;
            assert_eq!(data.get_change_type(), ConfChangeType::BeginSetNodes, "Peer ID: {:?}", data.get_node_id());
        }
        self.network.dispatch(config_changes)?;
        debug!("Finished {:?} phase.", self.phase);
        self.phase(Phase::PeersHaveBeginSetNodes);
        Ok(())
    }
    fn receive_begin_set_nodes_responses(&mut self) -> Result<()> {
        assert_eq!(self.phase, Phase::PeersHaveBeginSetNodes);
        debug!("Proceeding through {:?} phase.", self.phase);
        let messages = self.network
            .read_messages();
        for message in messages.iter() {
            assert_eq!(message.get_msg_type(), MessageType::MsgAppendResponse);
            assert_eq!(message.get_reject(), false);
            assert!(message.get_entries().iter().next().is_none());
        }
        self.network.dispatch(messages)?;
        debug!("Finished {:?} phase.", self.phase);
        self.phase(Phase::LeaderHasBeginSetNodesResponses);
        Ok(())
    }
    fn replicate_commit_set_nodes(&mut self) -> Result<()> {
        assert_eq!(self.phase, Phase::LeaderHasBeginSetNodesResponses);
        debug!("Proceeding through {:?} phase.", self.phase);
        let messages = self.network
            .read_messages();
        for message in messages.iter() {
            let entry = message.get_entries().iter().next().unwrap();
            assert_eq!(message.get_msg_type(), MessageType::MsgAppend);
            let data = protobuf::parse_from_bytes::<ConfChange>(entry.get_data())?;
            assert_eq!(data.get_change_type(), ConfChangeType::CommitSetNodes);
        }
        self.network.dispatch(messages)?;
        debug!("Finished {:?} phase.", self.phase);
        self.phase(Phase::PeersHaveCommitSetNodes);
        Ok(())
    }
    fn receive_commit_set_nodes_responses(&mut self) -> Result<()> {
        assert_eq!(self.phase, Phase::PeersHaveCommitSetNodes);
        debug!("Proceeding through {:?} phase.", self.phase);
        let messages = self.network
            .read_messages();
        for message in messages.iter() {
            assert_eq!(message.get_msg_type(), MessageType::MsgAppendResponse);
            assert_eq!(message.get_reject(), false);
            assert!(message.get_entries().iter().next().is_none());
        }
        self.network.dispatch(messages)?;
        debug!("Finished {:?} phase.", self.phase);
        self.phase(Phase::LeaderHasCommitSetNodesResponses);
        Ok(())
    }
    fn finalize(&mut self) -> Result<()> {
        assert_eq!(self.phase, Phase::LeaderHasCommitSetNodesResponses);
        debug!("Proceeding through {:?} phase.", self.phase);
        for (id, _) in self.network.peers.get(&self.leader).unwrap().prs().iter() {
           assert!(!self.network.peers.get(&id).unwrap().prs().is_in_transition(),
                   "Peer {:?} should have left transition.", id);
        }
        debug!("Finished {:?} phase.", self.phase);
        self.phase(Phase::Finalized);
        Ok(())
    }
}

// This is more a test of the Senario harness than membership changes
#[test]
fn test_scenarios_can_be_driven() -> Result<()> {
    setup_for_test();
    let leader = 1;
    let start = (&[1], &[]);
    let end = (&[1, 2, 3], &[4]);
    let all = (end.0.into_iter().chain(start.0.into_iter()), end.1.into_iter().chain(start.1.into_iter()));
    let mut scenario = Scenario::initialize(
        leader, all.0, all.1
    )?;
    scenario.connect(start.0, start.1)?;
    scenario.send_set_nodes(end.0, end.1)?;
    // scenario.initialize_peers(&[2, 3, 4])?;
    scenario.replicate_begin_set_nodes()?;
    scenario.receive_begin_set_nodes_responses()?;
    scenario.replicate_commit_set_nodes()?;
    scenario.receive_commit_set_nodes_responses()?;
    scenario.finalize()?;
    Ok(())
}
