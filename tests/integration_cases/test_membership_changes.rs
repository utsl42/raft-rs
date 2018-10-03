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
    Config, ProgressSet, Raft, Result,
};
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

fn process(network: &mut Network, messages: Vec<Message>) -> Vec<Message> {
    let mut responses = vec![];
    for message in messages {
        let id = message.get_to();
        // Append
        let peer = network.peers.get_mut(&id).unwrap();
        peer.step(message);
        responses.append(&mut peer.read_messages());
    }
    responses
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
    let mut voters = progress_set
        .voter_ids()
        .iter()
        .cloned()
        .collect::<FxHashSet<_>>();
    let mut learners = progress_set
        .learner_ids()
        .iter()
        .cloned()
        .collect::<FxHashSet<_>>();

    assert_eq!(voters, assert_voters);
    assert_eq!(learners, assert_learners);
}

#[test]
fn test_one_voter_to_4_peer_cluster() -> Result<()> {
    setup_for_test();
    let initial_leader = 1;
    let start_voters = vec![1];
    let start_learners: Vec<u64> = vec![];
    let expected_voters = vec![1, 2, 3];
    let expected_learners = vec![4];
    exhaustive_phase_by_phase_test(
        initial_leader,
        start_voters,
        start_learners,
        expected_voters,
        expected_learners,
    )
}

#[test]
fn test_4_peer_cluster_to_1_voter() -> Result<()> {
    setup_for_test();
    let initial_leader = 1;
    let start_voters = vec![1];
    let start_learners: Vec<u64> = vec![];
    let expected_voters = vec![1, 2, 3];
    let expected_learners = vec![4];
    exhaustive_phase_by_phase_test(
        initial_leader,
        start_voters,
        start_learners,
        expected_voters,
        expected_learners,
    )
}

#[test]
fn test_3_peer_cluster_replace_node() -> Result<()> {
    setup_for_test();
    let initial_leader = 1;
    let start_voters = vec![1, 2, 3];
    let start_learners: Vec<u64> = vec![];
    let expected_voters = vec![1, 2, 4];
    let expected_learners: Vec<u64> = vec![];
    exhaustive_phase_by_phase_test(
        initial_leader,
        start_voters,
        start_learners,
        expected_voters,
        expected_learners,
    )
}

#[test]
fn test_3_peer_cluster_add_learner() -> Result<()> {
    setup_for_test();
    let initial_leader = 1;
    let start_voters = vec![1, 2, 3];
    let start_learners: Vec<u64> = vec![];
    let expected_voters = vec![1, 2, 3];
    let expected_learners: Vec<u64> = vec![4];
    exhaustive_phase_by_phase_test(
        initial_leader,
        start_voters,
        start_learners,
        expected_voters,
        expected_learners,
    )
}
#[test]
fn test_3_peer_cluster_add_voter() -> Result<()> {
    setup_for_test();
    let initial_leader = 1;
    let start_voters = vec![1, 2, 3];
    let start_learners: Vec<u64> = vec![];
    let expected_voters = vec![1, 2, 3, 4];
    let expected_learners: Vec<u64> = vec![];
    exhaustive_phase_by_phase_test(
        initial_leader,
        start_voters,
        start_learners,
        expected_voters,
        expected_learners,
    )
}

fn exhaustive_phase_by_phase_test<'a>(
    initial_leader: u64,
    start_voters: impl IntoIterator<Item = u64>,
    start_learners: impl IntoIterator<Item = u64>,
    expected_voters: impl IntoIterator<Item = u64>,
    expected_learners: impl IntoIterator<Item = u64>,
) -> Result<()> {
    let start_voters = start_voters.into_iter().collect::<FxHashSet<_>>();
    let start_learners = start_learners.into_iter().collect::<FxHashSet<_>>();
    let expected_voters = expected_voters.into_iter().collect::<FxHashSet<_>>();
    let expected_learners = expected_learners.into_iter().collect::<FxHashSet<_>>();

    let initial_leader = 1;
    let num_messages_start = start_voters.len() + start_learners.len() - 1;
    let num_messages_expected = expected_voters.len() + expected_learners.len() - 1;
    let mut network = new_unconnected_network(
        initial_leader,
        expected_voters.union(&start_voters),
        expected_learners.union(&start_learners),
    )?;

    // Ensure the node has the intended initial configuration
    expected_voters.iter().for_each(|id| {
        assert_membership(
            &vec![*id],
            &vec![],
            network
                .peers
                .get_mut(id)
                .expect(&format!("Expected peer {} to be created.", id))
                .prs(),
        )
    });
    expected_learners.iter().for_each(|id| {
        assert_membership(
            &vec![],
            &vec![*id],
            network
                .peers
                .get_mut(id)
                .expect(&format!("Expected peer {} to be created.", id))
                .prs(),
        )
    });

    let messages = network
        .peers
        .get_mut(&initial_leader)
        .unwrap()
        .read_messages();
    assert!(messages.is_empty());
    network.send(messages);

    // Send the message to start the joint.
    let mut configuration = ConfState::new();
    configuration.set_nodes(expected_voters.iter().cloned().collect());
    configuration.set_learners(expected_learners.iter().cloned().collect());
    network
        .peers
        .get_mut(&initial_leader)
        .unwrap()
        .set_nodes(&configuration)?;

    assert_eq!(
        network
            .peers
            .get(&initial_leader)
            .unwrap()
            .prs()
            .is_in_transition(),
        true,
        "Peer {} should be in transition.",
        initial_leader
    );
    assert_membership(
        &expected_voters,
        &expected_learners,
        network.peers[&initial_leader].prs(),
    );

    // Replicate the configuration change.
    let messages = network
        .peers
        .get_mut(&initial_leader)
        .unwrap()
        .read_messages();

    warn!("Initialize peers phase.");
    assert!(
        messages
            .iter()
            .all(|m| m.get_msg_type() == MessageType::MsgAppend)
    );
    assert_eq!(
        messages.len(),
        num_messages_expected,
        "Should have {} Append messages to send",
        num_messages_expected
    );
    let messages = process(&mut network, messages);

    warn!("Initializer append responses phase.");
    assert!(
        messages
            .iter()
            .all(|m| m.get_msg_type() == MessageType::MsgAppendResponse)
    );
    assert_eq!(
        messages.len(),
        num_messages_expected,
        "Should have {} AppendResponse messages to send",
        num_messages_expected
    );
    let messages = process(&mut network, messages);

    warn!("Append for BeginSetNodes phase.");
    assert!(
        messages
            .iter()
            .all(|m| m.get_msg_type() == MessageType::MsgAppend)
    );
    assert_eq!(
        messages.len(),
        num_messages_expected,
        "Should have {} Append messages to send",
        num_messages_expected
    );
    let messages = process(&mut network, messages);

    for id in expected_learners.iter().chain(expected_voters.iter()) {
        assert_eq!(
            network.peers.get(&id).unwrap().prs().is_in_transition(),
            true,
            "Peer {} should be in transition.",
            id
        );
        assert_eq!(network.peers.get(&id).unwrap().raft_log.committed, 1);
    }

    warn!("AppendResponse for BeginSetNodes phase.");
    assert!(
        messages
            .iter()
            .all(|m| m.get_msg_type() == MessageType::MsgAppendResponse)
    );
    assert_eq!(
        messages.len(),
        num_messages_expected,
        "Should have {} AppendResponse messages to send",
        num_messages_expected
    );
    let messages = process(&mut network, messages);

    assert_eq!(network.peers.get(&1).unwrap().raft_log.committed, 2);

    warn!("Append for CommitSetNodes phase.");
    assert!(
        messages
            .iter()
            .all(|m| m.get_msg_type() == MessageType::MsgAppend)
    );
    assert_eq!(
        messages.len(),
        num_messages_expected,
        "Should have {} Append messages to send",
        num_messages_expected
    );
    let messages = process(&mut network, messages);

    for id in expected_learners.iter().chain(expected_voters.iter()) {
        assert_eq!(network.peers.get(&id).unwrap().raft_log.committed, 2);
        assert_eq!(
            network.peers.get(&id).unwrap().prs().is_in_transition(),
            false,
            "Peer {} should have proceeded through transition.",
            id
        );
    }

    Ok(())
}
