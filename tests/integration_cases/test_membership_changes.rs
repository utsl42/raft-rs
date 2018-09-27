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
use protobuf::{self, RepeatedField};
use raft::{
    eraftpb::{ConfChange, ConfChangeType, ConfState, Entry, EntryType, Message, MessageType},
    storage::MemStorage,
    Config, Raft, Result, ProgressSet,
};
use test_util::{setup_for_test, Interface, Network};

fn new_unconnected_network(voters: impl IntoIterator<Item=u64>, learners: impl IntoIterator<Item=u64>) -> Network {
    let mut voters = voters.into_iter();
    let init = voters.next().unwrap();
    let voters = voters.collect::<Vec<_>>();

    let mut network = Network::new(vec![Some(Interface::new(Raft::new(&Config {
        id: init,
        peers: vec![init],
        ..Default::default()
    }, MemStorage::new())))]);

    for &id in voters.iter() {
        let config = Config {
            id,
            peers: vec![id],
            ..Default::default()
        };
        network.peers.insert(id, Interface::new(Raft::new(&config, MemStorage::new())));
    }

    for id in learners.into_iter() {
        let config = Config {
            id,
            learners: vec![id],
            ..Default::default()
        };
        network.peers.insert(id, Interface::new(Raft::new(&config, MemStorage::new())));
    }

    network
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

fn assert_membership(assert_voters: &Vec<u64>, assert_learners: &Vec<u64>, progress_set: &ProgressSet) {
    let mut voters = progress_set.voter_ids()
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    voters.sort();
    let mut learners = progress_set.learner_ids()
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    learners.sort();
    assert_eq!(&voters, assert_voters);
    assert_eq!(&learners, assert_learners);
}

#[test]
fn test_one_node_to_cluster() -> Result<()> {
    setup_for_test();
    let expected_voters = vec![1, 2, 3];
    let expected_learners = vec![4];
    let mut network = new_unconnected_network(vec![1, 2, 3], vec![4]);
    network.peers.get_mut(&1).unwrap().become_candidate();
    network.peers.get_mut(&1).unwrap().become_leader();

    // Ensure the node has the intended initial configuration
    (1..=3).for_each(|id| assert_membership(&vec![id], &vec![], network.peers[&id].prs()));
    assert_membership(&vec![], &vec![4], network.peers[&4].prs());

    let messages = network.peers.get_mut(&1).unwrap().read_messages();
    assert!(messages.is_empty());
    network.send(messages);

    // Send the message to start the joint.
    let mut configuration = ConfState::new();
    configuration.set_nodes(expected_voters.clone());
    configuration.set_learners(expected_learners.clone());
    network.peers.get_mut(&1).unwrap().set_nodes(&configuration);

    assert_eq!(network.peers.get(&1).unwrap().prs().is_in_transition(), true, "Peer 1 should be in transition.");
    assert_membership(&expected_voters, &expected_learners, network.peers[&1].prs());

    // Replicate the configuration change.
    let messages = network.peers.get_mut(&1).unwrap().read_messages();

    warn!("Initialize peers phase.");
    assert!(messages.iter().all(|m| m.get_msg_type() == MessageType::MsgAppend));
    assert_eq!(messages.len(), 3, "Should have 3 Append messages to send");
    let messages = process(&mut network, messages);

    warn!("Initializer append responses phase.");
    assert!(messages.iter().all(|m| m.get_msg_type() == MessageType::MsgAppendResponse));
    assert_eq!(messages.len(), 3, "Should have 3 AppendResponse messages to send");
    let messages = process(&mut network, messages);

    warn!("Append for BeginSetNodes phase.");
    assert!(messages.iter().all(|m| m.get_msg_type() == MessageType::MsgAppend));
    assert_eq!(messages.len(), 3, "Should have 3 Append messages to send");
    let messages = process(&mut network, messages);

    for id in 1..=4 {
        assert_eq!(network.peers.get(&id).unwrap().prs().is_in_transition(), true, "Peer {} should be in transition.", id);
        assert_eq!(network.peers.get(&id).unwrap().raft_log.committed, 1);
    }

    warn!("AppendResponse for BeginSetNodes phase.");
    assert!(messages.iter().all(|m| m.get_msg_type() == MessageType::MsgAppendResponse));
    assert_eq!(messages.len(), 3, "Should have 3 AppendResponse messages to send");
    let messages = process(&mut network, messages);

    assert_eq!(network.peers.get(&1).unwrap().raft_log.committed, 2);

    warn!("Append for CommitSetNodes phase.");
    assert!(messages.iter().all(|m| m.get_msg_type() == MessageType::MsgAppend));
    assert_eq!(messages.len(), 3, "Should have 3 Append messages to send");
    let messages = process(&mut network, messages);

    for id in 1..=4 {
        assert_eq!(network.peers.get(&id).unwrap().raft_log.committed, 2);
        assert_eq!(network.peers.get(&id).unwrap().prs().is_in_transition(), false, "Peer {} should have proceeded through transition.", id);
    }

    Ok(())
}
