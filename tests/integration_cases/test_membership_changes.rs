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
    Config, Raft, Result,
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

#[test]
fn test_one_node_to_cluster() -> Result<()> {
    setup_for_test();
    let expected_voters = vec![1, 2, 3];
    let expected_learners = vec![4];
    let mut network = new_unconnected_network(vec![1, 2, 3, 4]);
    network.peers.get_mut(&1).unwrap().become_candidate();
    network.peers.get_mut(&1).unwrap().become_leader();

    // Ensure the node has the intended initial configuration
    assert_eq!(network.peers[&1]
        .prs()
        .voter_ids()
        .len(), 1);
    assert_eq!(network.peers[&1]
        .prs()
        .learner_ids()
        .len(), 0);

    // Send the message to start the joint.
    network.peers.get_mut(&1).unwrap().set_nodes(expected_voters.clone(), expected_learners.clone())?;

    let mut voters = network.peers[&1]
        .prs()
        .voter_ids()
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    voters.sort();
    let mut learners = network.peers[&1]
        .prs()
        .learner_ids()
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    learners.sort();
    assert_eq!(&voters, &expected_voters);
    assert_eq!(&learners, &expected_learners);
    assert_eq!(network.peers.get(&1).unwrap().prs().is_in_transition(), true, "Should be in transition.");

    // Replicate the configuration change.
    let messages = network.peers.get_mut(&1).unwrap().read_messages();
    println!("Should have begin {:?}", messages);
    // This will carry out all related responses as well.
    network.send(messages);

    // Now the leader should be prepared to commit.
    let messages = network.peers.get_mut(&1).unwrap().read_messages();
    println!("Should have commit {:?}", messages);

    assert_eq!(network.peers.get(&1).unwrap().prs().is_in_transition(), false, "Should have proceeded through transition.");

    Ok(())
}
