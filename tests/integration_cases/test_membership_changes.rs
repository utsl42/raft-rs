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

fn new_unconnected_network(peers: impl IntoIterator<Item=u64>) -> Network {
    let mut peers = peers.into_iter();
    let init = peers.next().unwrap();
    let peers = peers.collect::<Vec<_>>();

    let mut network = Network::new(vec![Some(Interface::new(Raft::new(&Config {
        id: init,
        peers: vec![init],
        ..Default::default()
    }, MemStorage::new())))]);

    for &id in peers.iter() {
        let config = Config {
            id,
            peers: vec![id],
            ..Default::default()
        };
        network.peers.insert(id, Interface::new(Raft::new(&config, MemStorage::new())));
    }
    network
}

fn new_conf_change_message(voters: &Vec<u64>, learners: &Vec<u64>) -> Message {
    let mut configuration = ConfState::new();
    configuration.set_nodes(voters.clone());
    configuration.set_learners(learners.clone());
    let mut conf_change = ConfChange::new();
    conf_change.set_change_type(ConfChangeType::BeginSetNodes);
    conf_change.set_configuration(configuration);
    let data = protobuf::Message::write_to_bytes(&conf_change).unwrap();
    let mut entry = Entry::new();
    entry.set_entry_type(EntryType::EntryConfChange);
    entry.set_data(data);
    entry.set_context(vec![]);
    let mut message = Message::new();
    message.set_msg_type(MessageType::MsgPropose);
    message.set_entries(RepeatedField::from_vec(vec![entry]));
    message
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
    let mut network = new_unconnected_network(vec![1, 2, 3, 4]);
    network.peers.get_mut(&1).unwrap().become_candidate();
    network.peers.get_mut(&1).unwrap().become_leader();

    // Ensure the node has the intended initial configuration
    assert_membership(&vec![1], &vec![], network.peers[&1].prs());

    // Send the message to start the joint.
    let message = new_conf_change_message(&expected_voters, &expected_learners);
    network.peers.get_mut(&1).unwrap().step(message)?;

    assert_eq!(network.peers.get(&1).unwrap().prs().is_in_transition(), true, "Peer 1 should be in transition.");
    assert_membership(&expected_voters, &expected_learners, network.peers[&1].prs());

    // Replicate the configuration change.
    let messages = network.peers.get_mut(&1).unwrap().read_messages();
    // This will carry out all related responses as well.
    network.send(messages);

    for id in 1..=4 {
        assert_eq!(network.peers.get(&id).unwrap().prs().is_in_transition(), true, "Peer {} should be in transition.", id);
    }

    // Now the leader should be prepared to commit.
    let messages = network.peers.get_mut(&1).unwrap().read_messages();
    println!("Should have commit {:?}", messages);

    assert_eq!(network.peers.get(&1).unwrap().prs().is_in_transition(), false, "Should have proceeded through transition.");

    Ok(())
}
