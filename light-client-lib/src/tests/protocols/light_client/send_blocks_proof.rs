use ckb_network::{CKBProtocolHandler, PeerIndex, SupportProtocols};
use ckb_types::{
    core::BlockNumber, h256, packed, prelude::*,
    utilities::merkle_mountain_range::VerifiableHeader, U256,
};
use std::sync::{Arc, RwLock};

use crate::{
    protocols::{LastState, PendingTxs, ProveRequest, ProveState, StatusCode},
    service::{FetchStatus, HistoricalHeaderProofStatus, LightClientChainService},
    storage::{BatchWriter, Key, LightClientStorage, StorageBackend, StorageWithChainData},
    tests::{
        prelude::*,
        utils::{MockChain, MockNetworkContext},
    },
};

#[tokio::test]
async fn peer_state_is_not_found() {
    let chain = MockChain::new_with_dummy_pow("test-light-client");
    let nc = MockNetworkContext::new(SupportProtocols::LightClient);

    let peers = chain.create_peers();
    let mut protocol = chain.create_light_client_protocol(peers);

    let data = {
        let content = packed::SendBlocksProof::new_builder().build();
        packed::LightClientMessage::new_builder()
            .set(content)
            .build()
    }
    .as_bytes();

    let peer_index = PeerIndex::new(1);
    protocol.received(nc.context(), peer_index, data).await;

    assert!(nc.banned_since(peer_index, StatusCode::PeerIsNotFound));
}

#[tokio::test]
async fn no_matched_request() {
    let chain = MockChain::new_with_dummy_pow("test-light-client");
    let nc = MockNetworkContext::new(SupportProtocols::LightClient);

    let peer_index = PeerIndex::new(1);
    let peers = {
        let peers = chain.create_peers();
        peers.add_peer(peer_index);
        peers.request_last_state(peer_index).unwrap();
        peers
    };
    let mut protocol = chain.create_light_client_protocol(peers);

    let data = {
        let content = packed::SendBlocksProof::new_builder().build();
        packed::LightClientMessage::new_builder()
            .set(content)
            .build()
    }
    .as_bytes();

    protocol.received(nc.context(), peer_index, data).await;

    assert!(nc.banned_since(peer_index, StatusCode::PeerIsNotOnProcess));
}

#[tokio::test(flavor = "multi_thread")]
async fn last_state_is_changed() {
    let chain = MockChain::new_with_dummy_pow("test-light-client").start();
    let nc = MockNetworkContext::new(SupportProtocols::LightClient);

    let peer_index = PeerIndex::new(1);
    let peers = {
        let peers = chain.create_peers();
        peers.add_peer(peer_index);
        peers.request_last_state(peer_index).unwrap();
        peers
    };
    let mut protocol = chain.create_light_client_protocol(peers);

    let mut num = 12;
    chain.mine_to(12 + 1);

    let snapshot = chain.shared().snapshot();

    let block_numbers = vec![3, 5, 8];

    // Setup the test fixture.
    {
        let peer_state = protocol
            .get_peer_state(&peer_index)
            .expect("has peer state");
        let prove_request = {
            let last_header: VerifiableHeader = snapshot
                .get_verifiable_header_by_number(num)
                .expect("block stored")
                .into();
            let content = protocol
                .build_prove_request_content(&peer_state, &last_header)
                .await
                .expect("build prove request content");
            let last_state = LastState::new(last_header);
            ProveRequest::new(last_state, content)
        };
        let last_state = LastState::new(prove_request.get_last_header().to_owned());
        let prove_state = {
            let last_n_blocks_start_number = if num > protocol.last_n_blocks() + 1 {
                num - protocol.last_n_blocks()
            } else {
                1
            };
            let last_n_headers = (last_n_blocks_start_number..num)
                .map(|num| snapshot.get_header_by_number(num).expect("block stored"))
                .collect::<Vec<_>>();
            ProveState::new_from_request(prove_request.clone(), Vec::new(), last_n_headers)
        };
        let content = chain.build_blocks_proof_content(num, &block_numbers, &[]);
        protocol
            .peers()
            .update_last_state(peer_index, last_state)
            .unwrap();
        protocol
            .peers()
            .update_prove_request(peer_index, prove_request)
            .unwrap();
        protocol
            .commit_prove_state(peer_index, prove_state)
            .await
            .unwrap();
        protocol
            .peers()
            .update_blocks_proof_request(peer_index, Some(content), true);
    }

    num += 1;

    // Run the test.
    {
        let last_header = snapshot
            .get_verifiable_header_by_number(num)
            .expect("block stored");
        let data = {
            let content = packed::SendBlocksProof::new_builder()
                .last_header(last_header.clone())
                .build();
            packed::LightClientMessage::new_builder()
                .set(content)
                .build()
        }
        .as_bytes();

        protocol.received(nc.context(), peer_index, data).await;

        assert!(nc.not_banned(peer_index));

        let peer = protocol.get_peer(&peer_index).expect("has peer");
        assert!(peer.get_blocks_proof_request().is_none());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn unexpected_response() {
    let chain = MockChain::new_with_dummy_pow("test-light-client").start();
    let nc = MockNetworkContext::new(SupportProtocols::LightClient);

    let peer_index = PeerIndex::new(1);
    let peers = {
        let peers = chain.create_peers();
        peers.add_peer(peer_index);
        peers.request_last_state(peer_index).unwrap();
        peers
    };
    let mut protocol = chain.create_light_client_protocol(peers);

    let num = 20;
    chain.mine_to(20);

    let snapshot = chain.shared().snapshot();

    let block_numbers = vec![3, 5, 8, 11, 16, 18];
    let bad_block_numbers = vec![3, 5, 7, 11, 16, 18];

    // Setup the test fixture.
    {
        let peer_state = protocol
            .get_peer_state(&peer_index)
            .expect("has peer state");
        let prove_request = {
            let last_header: VerifiableHeader = snapshot
                .get_verifiable_header_by_number(num)
                .expect("block stored")
                .into();
            let content = protocol
                .build_prove_request_content(&peer_state, &last_header)
                .await
                .expect("build prove request content");
            let last_state = LastState::new(last_header);
            ProveRequest::new(last_state, content)
        };
        let last_state = LastState::new(prove_request.get_last_header().to_owned());
        let prove_state = {
            let last_n_blocks_start_number = if num > protocol.last_n_blocks() + 1 {
                num - protocol.last_n_blocks()
            } else {
                1
            };
            let last_n_headers = (last_n_blocks_start_number..num)
                .map(|num| snapshot.get_header_by_number(num).expect("block stored"))
                .collect::<Vec<_>>();
            ProveState::new_from_request(prove_request.clone(), Vec::new(), last_n_headers)
        };
        let content = chain.build_blocks_proof_content(num, &block_numbers, &[]);
        protocol
            .peers()
            .update_last_state(peer_index, last_state)
            .unwrap();
        protocol
            .peers()
            .update_prove_request(peer_index, prove_request)
            .unwrap();
        protocol
            .commit_prove_state(peer_index, prove_state)
            .await
            .unwrap();
        protocol
            .peers()
            .update_blocks_proof_request(peer_index, Some(content), true);
    }

    // Run the test.
    {
        let last_header = snapshot
            .get_verifiable_header_by_number(num)
            .expect("block stored");
        let data = {
            let headers = bad_block_numbers
                .iter()
                .map(|n| *n as BlockNumber)
                .map(|n| {
                    snapshot
                        .get_header_by_number(n)
                        .expect("block stored")
                        .data()
                })
                .collect::<Vec<_>>();
            let last_number: BlockNumber = last_header.header().raw().number().unpack();
            let proof = chain.build_proof_by_numbers(last_number, &bad_block_numbers);
            let content = packed::SendBlocksProof::new_builder()
                .last_header(last_header)
                .proof(proof)
                .headers(headers.pack())
                .build();
            packed::LightClientMessage::new_builder()
                .set(content)
                .build()
        }
        .as_bytes();

        assert!(nc.sent_messages().borrow().is_empty());

        protocol.received(nc.context(), peer_index, data).await;

        assert!(nc.banned_since(peer_index, StatusCode::UnexpectedResponse));
        assert!(nc.sent_messages().borrow().is_empty());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn get_blocks_with_chunks() {
    let chain = MockChain::new_with_dummy_pow("test-light-client").start();
    let nc = MockNetworkContext::new(SupportProtocols::LightClient);

    let peer_index = PeerIndex::new(1);
    let peers = {
        let peers = chain.create_peers();
        peers.add_peer(peer_index);
        peers.request_last_state(peer_index).unwrap();
        peers
    };
    let mut protocol = chain.create_light_client_protocol(peers);
    let chunk_size = 3;
    protocol.set_init_blocks_in_transit_per_peer(chunk_size);

    let num = 20;
    chain.mine_to(20);

    let snapshot = chain.shared().snapshot();

    let block_numbers = vec![3, 5, 8, 11, 13, 16, 18];

    // Setup the test fixture.
    {
        let peer_state = protocol
            .get_peer_state(&peer_index)
            .expect("has peer state");
        let prove_request = {
            let last_header: VerifiableHeader = snapshot
                .get_verifiable_header_by_number(num)
                .expect("block stored")
                .into();
            let content = protocol
                .build_prove_request_content(&peer_state, &last_header)
                .await
                .expect("build prove request content");
            let last_state = LastState::new(last_header);
            ProveRequest::new(last_state, content)
        };
        let last_state = LastState::new(prove_request.get_last_header().to_owned());
        let prove_state = {
            let last_n_blocks_start_number = if num > protocol.last_n_blocks() + 1 {
                num - protocol.last_n_blocks()
            } else {
                1
            };
            let last_n_headers = (last_n_blocks_start_number..num)
                .map(|num| snapshot.get_header_by_number(num).expect("block stored"))
                .collect::<Vec<_>>();
            ProveState::new_from_request(prove_request.clone(), Vec::new(), last_n_headers)
        };
        let content = chain.build_blocks_proof_content(num, &block_numbers, &[]);
        protocol
            .peers()
            .update_last_state(peer_index, last_state)
            .unwrap();
        protocol
            .peers()
            .update_prove_request(peer_index, prove_request)
            .unwrap();
        protocol
            .commit_prove_state(peer_index, prove_state)
            .await
            .unwrap();
        protocol
            .peers()
            .update_blocks_proof_request(peer_index, Some(content), true);
    }

    // Run the test.
    {
        let last_header = snapshot
            .get_verifiable_header_by_number(num)
            .expect("block stored");
        let headers = block_numbers
            .iter()
            .map(|n| *n as BlockNumber)
            .map(|n| snapshot.get_header_by_number(n).expect("block stored"))
            .collect::<Vec<_>>();
        let block_hashes = headers.iter().map(|h| h.hash()).collect::<Vec<_>>();
        let data = {
            let headers = headers.iter().map(|h| h.data()).collect::<Vec<_>>();
            let last_number: BlockNumber = last_header.header().raw().number().unpack();
            let proof = chain.build_proof_by_numbers(last_number, &block_numbers);
            let uncles_hashes = headers
                .iter()
                .map(|h| {
                    snapshot
                        .get_block_by_number(h.raw().number().unpack())
                        .expect("block stored")
                        .calc_uncles_hash()
                })
                .collect::<Vec<_>>();
            let extensions = headers
                .iter()
                .map(|h| {
                    packed::BytesOpt::new_builder()
                        .set(
                            snapshot
                                .get_block_by_number(h.raw().number().unpack())
                                .expect("block stored")
                                .extension(),
                        )
                        .build()
                })
                .collect::<Vec<_>>();
            let content = packed::SendBlocksProofV1::new_builder()
                .last_header(last_header)
                .proof(proof)
                .headers(headers.pack())
                .blocks_uncles_hash(uncles_hashes.pack())
                .blocks_extension(extensions)
                .build();
            packed::LightClientMessage::new_builder()
                .set(content)
                .build()
        }
        .as_bytes();

        assert!(nc.sent_messages().borrow().is_empty());

        protocol.received(nc.context(), peer_index, data).await;

        assert!(nc.not_banned(peer_index));

        let msg_count = if block_numbers.len() % chunk_size == 0 {
            0
        } else {
            1
        } + block_numbers.len() / chunk_size;
        assert_eq!(nc.sent_messages().borrow().len(), msg_count);

        let actual_block_hashes = nc
            .sent_messages()
            .borrow()
            .iter()
            .enumerate()
            .flat_map(|(idx, msg)| {
                let data = &msg.2;
                let message = packed::SyncMessageReader::new_unchecked(data);
                let hashes =
                    if let packed::SyncMessageUnionReader::GetBlocks(content) = message.to_enum() {
                        content.block_hashes().to_entity().into_iter()
                    } else {
                        panic!("unexpected message");
                    };
                if idx < msg_count - 1 {
                    assert_eq!(hashes.len(), chunk_size);
                } else {
                    assert_eq!(hashes.len(), block_numbers.len() % chunk_size);
                }
                hashes
            })
            .collect::<Vec<_>>();
        assert_eq!(actual_block_hashes.as_slice(), block_hashes.as_slice());

        let peer = protocol.get_peer(&peer_index).expect("has peer");
        assert!(peer.get_blocks_proof_request().is_none());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn valid_proof() {
    let last_block_number = 20;
    let block_numbers = vec![3, 5, 8, 11, 16, 18];
    let param = TestParameter {
        last_block_number,
        block_numbers: block_numbers.clone(),
        proved_block_numbers: block_numbers.clone(),
        returned_headers: block_numbers,
        ..Default::default()
    };
    test_send_blocks_proof(param).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn historical_header_request_proves_and_persists_old_cached_candidate() {
    let block_number = 3;
    let param = TestParameter {
        last_block_number: 200,
        block_numbers: vec![block_number],
        proved_block_numbers: vec![block_number],
        returned_headers: vec![block_number],
        historical_lookup: Some(HistoricalHeaderLookup {
            block_number,
            cache_candidate_without_height_mapping: true,
            ..Default::default()
        }),
        ..Default::default()
    };
    test_send_blocks_proof(param).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn historical_header_request_does_not_verify_a_wrong_candidate_hash() {
    let wrong_hash = h256!("0xdead");
    let param = TestParameter {
        last_block_number: 200,
        block_numbers: vec![3],
        proved_block_numbers: vec![3],
        returned_headers: vec![3],
        expected_status: Some(StatusCode::UnexpectedResponse),
        historical_lookup: Some(HistoricalHeaderLookup {
            block_number: 3,
            candidate_hash: Some(wrong_hash),
            ..Default::default()
        }),
        ..Default::default()
    };
    test_send_blocks_proof(param).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn historical_header_request_does_not_verify_a_candidate_at_another_height() {
    let requested_height = 3;
    let candidate_height = 4;
    let param = TestParameter {
        last_block_number: 200,
        block_numbers: vec![candidate_height],
        proved_block_numbers: vec![candidate_height],
        returned_headers: vec![candidate_height],
        historical_lookup: Some(HistoricalHeaderLookup {
            block_number: requested_height,
            candidate_height: Some(candidate_height),
            ..Default::default()
        }),
        ..Default::default()
    };
    test_send_blocks_proof(param).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn historical_header_request_remains_unverified_when_proof_is_invalid() {
    let block_number = 3;
    let param = TestParameter {
        last_block_number: 200,
        block_numbers: vec![block_number],
        returned_headers: vec![block_number],
        expected_status: Some(StatusCode::InvalidProof),
        historical_lookup: Some(HistoricalHeaderLookup {
            block_number,
            ..Default::default()
        }),
        ..Default::default()
    };
    test_send_blocks_proof(param).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn historical_header_request_rejects_a_fork_header_with_invalid_mmr_membership() {
    let block_number = 3;
    let fork_header = packed::Header::new_builder()
        .raw(
            packed::RawHeader::new_builder()
                .number(block_number)
                .parent_hash(h256!("0xf0").pack())
                .build(),
        )
        .build();
    let param = TestParameter {
        last_block_number: 200,
        block_numbers: vec![block_number],
        proved_block_numbers: vec![block_number],
        returned_headers: vec![block_number],
        expected_status: Some(StatusCode::InvalidProof),
        historical_lookup: Some(HistoricalHeaderLookup {
            block_number,
            ..Default::default()
        }),
        replacement_header: Some(fork_header),
        ..Default::default()
    };
    test_send_blocks_proof(param).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn historical_header_request_reports_a_missing_candidate() {
    let param = TestParameter {
        last_block_number: 200,
        historical_lookup: Some(HistoricalHeaderLookup {
            block_number: 3,
            peer_reports_missing: true,
            ..Default::default()
        }),
        ..Default::default()
    };
    test_send_blocks_proof(param).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn historical_header_request_reports_when_its_anchor_changes() {
    let block_number = 3;
    let param = TestParameter {
        last_block_number: 200,
        block_numbers: vec![block_number],
        proved_block_numbers: vec![block_number],
        returned_headers: vec![block_number],
        historical_lookup: Some(HistoricalHeaderLookup {
            block_number,
            change_tip_before_response: true,
            dispatch_with_scheduler: true,
            ..Default::default()
        }),
        ..Default::default()
    };
    test_send_blocks_proof(param).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn historical_header_request_public_api_schedules_and_verifies() {
    let block_number = 3;
    let param = TestParameter {
        last_block_number: 200,
        block_numbers: vec![block_number],
        proved_block_numbers: vec![block_number],
        returned_headers: vec![block_number],
        historical_lookup: Some(HistoricalHeaderLookup {
            block_number,
            dispatch_with_scheduler: true,
            ordinary_fetch_candidate: true,
            ..Default::default()
        }),
        ..Default::default()
    };
    test_send_blocks_proof(param).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn historical_only_missing_does_not_pollute_fetch_header() {
    let param = TestParameter {
        last_block_number: 200,
        historical_lookup: Some(HistoricalHeaderLookup {
            block_number: 3,
            dispatch_with_scheduler: true,
            peer_reports_missing: true,
            ..Default::default()
        }),
        ..Default::default()
    };
    test_send_blocks_proof(param).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn mixed_ordinary_and_historical_missing_keeps_lifecycles_independent() {
    let param = TestParameter {
        last_block_number: 200,
        historical_lookup: Some(HistoricalHeaderLookup {
            block_number: 3,
            dispatch_with_scheduler: true,
            ordinary_fetch_candidate: true,
            peer_reports_missing: true,
            ..Default::default()
        }),
        ..Default::default()
    };
    test_send_blocks_proof(param).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn historical_header_request_retries_expires_and_is_process_local() {
    let chain = MockChain::new_with_dummy_pow("test-light-client").start();
    chain.mine_to(200);
    let snapshot = chain.shared().snapshot();
    let tip = snapshot.get_header_by_number(200).unwrap().data();
    let tip_hash = tip.calc_header_hash();
    chain
        .client_storage()
        .update_last_state(&U256::one(), &tip, &[]);

    let peers = chain.create_peers();
    let peer_index = PeerIndex::new(1);
    peers.add_peer(peer_index);
    let service = LightClientChainService::new(
        StorageWithChainData::new(
            chain.client_storage().clone(),
            Arc::clone(&peers),
            Arc::new(RwLock::new(PendingTxs::default())),
        ),
        Arc::new(chain.consensus().clone()),
    );
    let candidate_hash: ckb_types::H256 = snapshot.get_header_by_number(3).unwrap().hash().unpack();
    let request = service
        .request_historical_header_proof(3.into(), candidate_hash.clone())
        .unwrap();
    let content = packed::GetBlocksProof::new_builder()
        .last_hash(tip_hash.clone())
        .block_hashes(vec![candidate_hash.pack()].pack())
        .build();
    peers.update_blocks_proof_request_with_historical_targets(
        peer_index,
        Some(content),
        false,
        Vec::new(),
        vec![request.id()],
    );
    assert!(matches!(
        service.fetch_header(&candidate_hash),
        FetchStatus::Added { .. }
    ));
    let generic_fetch_hash = candidate_hash.pack();
    peers.fetching_idle_headers(
        std::slice::from_ref(&generic_fetch_hash),
        ckb_systemtime::unix_time_as_millis(),
    );
    assert!(!peers.get_headers_to_fetch().contains(&generic_fetch_hash));
    assert!(matches!(
        service.poll_historical_header_proof(&request),
        Ok(HistoricalHeaderProofStatus::Fetching { .. })
    ));

    peers.remove_peer(peer_index).await;
    assert!(!peers.get_headers_to_fetch().contains(&generic_fetch_hash));
    assert!(matches!(
        service.poll_historical_header_proof(&request),
        Ok(HistoricalHeaderProofStatus::Added { .. })
    ));

    let abandoned = service
        .request_historical_header_proof(3.into(), candidate_hash.clone())
        .unwrap();
    let expiry_time = ckb_systemtime::unix_time_as_millis() + 3_600_001;
    peers.cleanup_historical_header_proofs(&tip_hash, expiry_time);
    assert_eq!(
        service.poll_historical_header_proof(&request).unwrap(),
        HistoricalHeaderProofStatus::Expired
    );
    assert_eq!(
        service.poll_historical_header_proof(&abandoned).unwrap(),
        HistoricalHeaderProofStatus::Expired
    );
    peers.cleanup_historical_header_proofs(&tip_hash, expiry_time + 300_001);
    assert_eq!(
        service.poll_historical_header_proof(&request).unwrap(),
        HistoricalHeaderProofStatus::Expired
    );

    let retry = service
        .request_historical_header_proof(3.into(), candidate_hash.clone())
        .unwrap();
    assert_ne!(request.id(), retry.id());
    assert!(matches!(
        service.poll_historical_header_proof(&retry),
        Ok(HistoricalHeaderProofStatus::Added { .. })
    ));
    let retry_peer = PeerIndex::new(2);
    peers.add_peer(retry_peer);
    let retry_content = packed::GetBlocksProof::new_builder()
        .last_hash(tip_hash.clone())
        .block_hashes(vec![candidate_hash.pack()].pack())
        .build();
    peers.update_blocks_proof_request_with_historical_targets(
        retry_peer,
        Some(retry_content),
        false,
        Vec::new(),
        vec![retry.id()],
    );
    assert!(matches!(
        service.poll_historical_header_proof(&retry),
        Ok(HistoricalHeaderProofStatus::Fetching { .. })
    ));

    let no_response = service
        .request_historical_header_proof(3.into(), candidate_hash.clone())
        .unwrap();
    peers.remove_fetching_header(&generic_fetch_hash);
    let unresponsive_peer = PeerIndex::new(3);
    peers.add_peer(unresponsive_peer);
    let no_response_content = packed::GetBlocksProof::new_builder()
        .last_hash(tip_hash.clone())
        .block_hashes(vec![candidate_hash.pack()].pack())
        .build();
    peers.update_blocks_proof_request_with_historical_targets(
        unresponsive_peer,
        Some(no_response_content),
        false,
        Vec::new(),
        vec![no_response.id()],
    );
    assert!(matches!(
        service.fetch_header(&candidate_hash),
        FetchStatus::Added { .. }
    ));
    peers.fetching_idle_headers(
        std::slice::from_ref(&generic_fetch_hash),
        ckb_systemtime::unix_time_as_millis(),
    );
    assert!(!peers.get_headers_to_fetch().contains(&generic_fetch_hash));
    peers.mark_fetching_headers_timeout(unresponsive_peer);
    assert!(!peers.get_headers_to_fetch().contains(&generic_fetch_hash));
    assert!(matches!(
        service.poll_historical_header_proof(&no_response),
        Ok(HistoricalHeaderProofStatus::Added { .. })
    ));
    let no_response_expiry = ckb_systemtime::unix_time_as_millis() + 3_600_001;
    peers.cleanup_historical_header_proofs(&tip_hash, no_response_expiry);
    assert_eq!(
        service.poll_historical_header_proof(&no_response).unwrap(),
        HistoricalHeaderProofStatus::Expired
    );

    let restarted_peers = chain.create_peers();
    let restarted_service = LightClientChainService::new(
        StorageWithChainData::new(
            chain.client_storage().clone(),
            restarted_peers,
            Arc::new(RwLock::new(PendingTxs::default())),
        ),
        Arc::new(chain.consensus().clone()),
    );
    let after_restart = restarted_service
        .request_historical_header_proof(3.into(), candidate_hash)
        .unwrap();
    assert_ne!(retry.id(), after_restart.id());
    assert_eq!(
        restarted_service
            .poll_historical_header_proof(&retry)
            .unwrap(),
        HistoricalHeaderProofStatus::Expired
    );
    assert!(matches!(
        restarted_service.poll_historical_header_proof(&after_restart),
        Ok(HistoricalHeaderProofStatus::Added { .. })
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn valid_proof_without_any_proof_items() {
    let last_block_number = 20;
    let block_numbers = (0..last_block_number).collect::<Vec<_>>();
    let param = TestParameter {
        last_block_number,
        block_numbers: block_numbers.clone(),
        proved_block_numbers: block_numbers.clone(),
        returned_headers: block_numbers,
        ..Default::default()
    };
    test_send_blocks_proof(param).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_proof_since_all_blocks_are_missing() {
    let last_block_number = 20;
    let block_numbers = vec![];
    let missing_block_hashes = vec![h256!("0x1").pack(), h256!("0x2").pack()];
    let param = TestParameter {
        last_block_number,
        block_numbers: block_numbers.clone(),
        proved_block_numbers: block_numbers.clone(),
        returned_headers: block_numbers,
        missing_block_hashes: missing_block_hashes.clone(),
        returned_missing_block_hashes: missing_block_hashes,
        ..Default::default()
    };
    test_send_blocks_proof(param).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn legacy_proof_with_extension_block_is_rejected() {
    // Every block mined by the mock chain carries an extension, so a legacy
    // (v0) message which withholds the V1 fields must be rejected.
    let last_block_number = 20;
    let block_numbers = vec![3, 5, 8, 11, 16, 18];
    let param = TestParameter {
        last_block_number,
        block_numbers: block_numbers.clone(),
        proved_block_numbers: block_numbers.clone(),
        returned_headers: block_numbers,
        use_legacy_message: true,
        expected_status: Some(StatusCode::InvalidProof),
        ..Default::default()
    };
    test_send_blocks_proof(param).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn v1_proof_with_incorrect_extension_is_rejected() {
    let last_block_number = 20;
    let block_numbers = vec![3, 5, 8, 11, 16, 18];
    let returned_uncles_hashes = block_numbers
        .iter()
        .map(|_| packed::Byte32::zero())
        .collect::<Vec<_>>();
    // The headers commit to the real block extensions, but the message
    // carries different extension bytes, so the V1 extra-hash verification
    // must reject it.
    let incorrect_extension = packed::Bytes::new_builder().push(2u8).build();
    let returned_extensions = block_numbers
        .iter()
        .map(|_| {
            packed::BytesOpt::new_builder()
                .set(Some(incorrect_extension.clone()))
                .build()
        })
        .collect::<Vec<_>>();
    let param = TestParameter {
        last_block_number,
        block_numbers: block_numbers.clone(),
        proved_block_numbers: block_numbers.clone(),
        returned_headers: block_numbers,
        returned_uncles_hashes: Some(returned_uncles_hashes),
        returned_extensions: Some(returned_extensions),
        expected_status: Some(StatusCode::InvalidProof),
        ..Default::default()
    };
    test_send_blocks_proof(param).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_v1_fields_when_all_blocks_are_missing() {
    let missing_block_hashes = vec![h256!("0x1").pack(), h256!("0x2").pack()];
    let param = TestParameter {
        last_block_number: 20,
        missing_block_hashes: missing_block_hashes.clone(),
        returned_missing_block_hashes: missing_block_hashes,
        returned_uncles_hashes: Some(vec![packed::Byte32::default()]),
        expected_status: Some(StatusCode::MalformedProtocolMessage),
        ..Default::default()
    };
    test_send_blocks_proof(param).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn nonempty_proof_since_all_blocks_are_missing() {
    let last_block_number = 20;
    let block_numbers = vec![];
    let returned_headers = vec![9];
    let missing_block_hashes = vec![h256!("0x1").pack(), h256!("0x2").pack()];
    let param = TestParameter {
        last_block_number,
        block_numbers: block_numbers.clone(),
        proved_block_numbers: block_numbers.clone(),
        returned_headers,
        missing_block_hashes: missing_block_hashes.clone(),
        returned_missing_block_hashes: missing_block_hashes,
        ..Default::default()
    };
    test_send_blocks_proof(param).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn valid_proof_with_missing_block_hashes() {
    let last_block_number = 20;
    let block_numbers = vec![3, 5, 8, 11, 16, 18];
    let missing_block_hashes = vec![h256!("0x1").pack(), h256!("0x2").pack()];
    let param = TestParameter {
        last_block_number,
        block_numbers: block_numbers.clone(),
        proved_block_numbers: block_numbers.clone(),
        returned_headers: block_numbers,
        missing_block_hashes: missing_block_hashes.clone(),
        returned_missing_block_hashes: missing_block_hashes,
        ..Default::default()
    };
    test_send_blocks_proof(param).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_proof_with_insufficient_missing_block_hashes() {
    let last_block_number = 20;
    let block_numbers = vec![3, 5, 8, 11, 16, 18];
    let missing_block_hashes = vec![h256!("0x1").pack(), h256!("0x2").pack()];
    let returned_missing_block_hashes = vec![h256!("0x1").pack()];
    let param = TestParameter {
        last_block_number,
        block_numbers: block_numbers.clone(),
        proved_block_numbers: block_numbers.clone(),
        returned_headers: block_numbers,
        missing_block_hashes,
        returned_missing_block_hashes,
        ..Default::default()
    };
    test_send_blocks_proof(param).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_proof_with_redundant_missing_block_hashes() {
    let last_block_number = 20;
    let block_numbers = vec![3, 5, 8, 11, 16, 18];
    let missing_block_hashes = vec![h256!("0x1").pack()];
    let returned_missing_block_hashes = vec![h256!("0x1").pack(), h256!("0x2").pack()];
    let param = TestParameter {
        last_block_number,
        block_numbers: block_numbers.clone(),
        proved_block_numbers: block_numbers.clone(),
        returned_headers: block_numbers,
        missing_block_hashes,
        returned_missing_block_hashes,
        ..Default::default()
    };
    test_send_blocks_proof(param).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_proof_with_duplicate_missing_block_hashes() {
    let last_block_number = 20;
    let block_numbers = vec![3, 5, 8, 11, 16, 18];
    let missing_block_hashes = vec![h256!("0x1").pack()];
    let returned_missing_block_hashes = vec![h256!("0x1").pack(), h256!("0x1").pack()];
    let param = TestParameter {
        last_block_number,
        block_numbers: block_numbers.clone(),
        proved_block_numbers: block_numbers.clone(),
        returned_headers: block_numbers,
        missing_block_hashes,
        returned_missing_block_hashes,
        ..Default::default()
    };
    test_send_blocks_proof(param).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_proof_with_insufficient_proved_blocks() {
    let last_block_number = 20;
    let block_numbers = vec![3, 5, 8, 11, 16, 18];
    let proved_block_numbers = vec![3, 5, 11, 16, 18];
    let param = TestParameter {
        last_block_number,
        block_numbers: block_numbers.clone(),
        proved_block_numbers,
        returned_headers: block_numbers,
        ..Default::default()
    };
    test_send_blocks_proof(param).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_proof_with_redundant_proved_blocks() {
    let last_block_number = 20;
    let block_numbers = vec![3, 5, 8, 11, 16, 18];
    let proved_block_numbers = vec![3, 5, 7, 8, 11, 16, 18];
    let param = TestParameter {
        last_block_number,
        block_numbers: block_numbers.clone(),
        proved_block_numbers,
        returned_headers: block_numbers,
        ..Default::default()
    };
    test_send_blocks_proof(param).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_proof_with_insufficient_returned_headers() {
    let last_block_number = 20;
    let block_numbers = vec![3, 5, 8, 11, 16, 18];
    let returned_headers = vec![3, 5, 11, 16, 18];
    let param = TestParameter {
        last_block_number,
        block_numbers: block_numbers.clone(),
        proved_block_numbers: block_numbers,
        returned_headers,
        ..Default::default()
    };
    test_send_blocks_proof(param).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_proof_with_redundant_returned_headers() {
    let last_block_number = 20;
    let block_numbers = vec![3, 5, 8, 11, 16, 18];
    let returned_headers = vec![3, 5, 7, 8, 11, 16, 18];
    let param = TestParameter {
        last_block_number,
        block_numbers: block_numbers.clone(),
        proved_block_numbers: block_numbers,
        returned_headers,
        ..Default::default()
    };
    test_send_blocks_proof(param).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_proof_with_duplicate_returned_headers() {
    let last_block_number = 20;
    let block_numbers = vec![3, 5, 8, 11, 16, 18];
    let returned_headers = vec![3, 5, 8, 11, 16, 18, 8];
    let param = TestParameter {
        last_block_number,
        block_numbers: block_numbers.clone(),
        proved_block_numbers: block_numbers,
        returned_headers,
        ..Default::default()
    };
    test_send_blocks_proof(param).await;
}

#[derive(Default)]
struct TestParameter {
    last_block_number: BlockNumber,
    block_numbers: Vec<BlockNumber>,
    proved_block_numbers: Vec<BlockNumber>,
    returned_headers: Vec<BlockNumber>,
    missing_block_hashes: Vec<packed::Byte32>,
    returned_missing_block_hashes: Vec<packed::Byte32>,
    returned_uncles_hashes: Option<Vec<packed::Byte32>>,
    returned_extensions: Option<Vec<packed::BytesOpt>>,
    use_legacy_message: bool,
    expected_status: Option<StatusCode>,
    historical_lookup: Option<HistoricalHeaderLookup>,
    replacement_header: Option<packed::Header>,
}

#[derive(Default)]
struct HistoricalHeaderLookup {
    block_number: BlockNumber,
    candidate_height: Option<BlockNumber>,
    candidate_hash: Option<ckb_types::H256>,
    cache_candidate_without_height_mapping: bool,
    peer_reports_missing: bool,
    change_tip_before_response: bool,
    dispatch_with_scheduler: bool,
    ordinary_fetch_candidate: bool,
}

async fn test_send_blocks_proof(param: TestParameter) {
    let chain = MockChain::new_with_dummy_pow("test-light-client").start();
    let nc = MockNetworkContext::new(SupportProtocols::LightClient);

    let peer_index = PeerIndex::new(1);
    let peers = chain.create_peers();
    peers.add_peer(peer_index);
    peers.request_last_state(peer_index).unwrap();
    let mut protocol = chain.create_light_client_protocol(Arc::clone(&peers));

    let num = param.last_block_number;
    chain.mine_to(num);

    let snapshot = chain.shared().snapshot();
    let mut missing_block_hashes = param.missing_block_hashes.clone();
    let mut returned_missing_block_hashes = param.returned_missing_block_hashes.clone();
    let mut historical_candidate_hash = None;
    let mut historical_request = None;

    if let Some(lookup) = &param.historical_lookup {
        let candidate_height = lookup.candidate_height.unwrap_or(lookup.block_number);
        let candidate_hash = lookup.candidate_hash.clone().unwrap_or_else(|| {
            param
                .replacement_header
                .as_ref()
                .map(|header| header.calc_header_hash().unpack())
                .unwrap_or_else(|| {
                    snapshot
                        .get_header_by_number(candidate_height)
                        .expect("candidate header exists")
                        .hash()
                        .unpack()
                })
        });

        if lookup.cache_candidate_without_height_mapping {
            let header = snapshot
                .get_header_by_number(candidate_height)
                .expect("cached candidate header exists")
                .data();
            let mut batch = chain.client_storage().batch();
            batch.put(
                &Key::BlockHash(&candidate_hash.pack()).into_vec(),
                header.as_slice(),
            );
            batch.commit().unwrap();
            assert!(chain
                .client_storage()
                .get_block_hash(lookup.block_number)
                .is_none());
        }

        if lookup.peer_reports_missing {
            missing_block_hashes = vec![candidate_hash.pack()];
            returned_missing_block_hashes = missing_block_hashes.clone();
        }
        historical_candidate_hash = Some(candidate_hash);
    }

    // Setup the test fixture.
    {
        let peer_state = protocol
            .get_peer_state(&peer_index)
            .expect("has peer state");
        let prove_request = {
            let last_header: VerifiableHeader = snapshot
                .get_verifiable_header_by_number(num)
                .expect("block stored")
                .into();
            let content = protocol
                .build_prove_request_content(&peer_state, &last_header)
                .await
                .expect("build prove request content");
            let last_state = LastState::new(last_header);
            ProveRequest::new(last_state, content)
        };
        let last_state = LastState::new(prove_request.get_last_header().to_owned());
        let prove_state = {
            let last_n_blocks_start_number = if num > protocol.last_n_blocks() + 1 {
                num - protocol.last_n_blocks()
            } else {
                1
            };
            let last_n_headers = (last_n_blocks_start_number..num)
                .map(|num| snapshot.get_header_by_number(num).expect("block stored"))
                .collect::<Vec<_>>();
            ProveState::new_from_request(prove_request.clone(), Vec::new(), last_n_headers)
        };
        let proof_anchor_height = num;
        let content = if let Some(candidate_hash) = param
            .replacement_header
            .as_ref()
            .map(|header| header.calc_header_hash().unpack())
            .or_else(|| {
                param
                    .historical_lookup
                    .as_ref()
                    .and_then(|lookup| lookup.candidate_hash.clone())
            }) {
            let anchor_hash = snapshot
                .get_header_by_number(proof_anchor_height)
                .expect("proof anchor header exists")
                .hash();
            packed::GetBlocksProof::new_builder()
                .last_hash(anchor_hash)
                .block_hashes(vec![candidate_hash.pack()].pack())
                .build()
        } else {
            chain.build_blocks_proof_content(
                proof_anchor_height,
                &param.block_numbers,
                &missing_block_hashes,
            )
        };
        protocol
            .peers()
            .update_last_state(peer_index, last_state)
            .unwrap();
        protocol
            .peers()
            .update_prove_request(peer_index, prove_request)
            .unwrap();
        protocol
            .commit_prove_state(peer_index, prove_state)
            .await
            .unwrap();

        if let (Some(lookup), Some(candidate_hash)) =
            (&param.historical_lookup, historical_candidate_hash.as_ref())
        {
            let accepted_tip = snapshot
                .get_header_by_number(num)
                .expect("accepted test tip exists")
                .data();
            chain
                .client_storage()
                .update_last_state(&U256::one(), &accepted_tip, &[]);
            let service = LightClientChainService::new(
                StorageWithChainData::new(
                    chain.client_storage().clone(),
                    Arc::clone(&peers),
                    Arc::new(RwLock::new(PendingTxs::default())),
                ),
                Arc::new(chain.consensus().clone()),
            );
            if lookup.ordinary_fetch_candidate {
                assert!(matches!(
                    service.fetch_header(candidate_hash),
                    FetchStatus::Added { .. }
                ));
            }
            let request = service
                .request_historical_header_proof(lookup.block_number.into(), candidate_hash.clone())
                .expect("request queues the candidate for proof verification");
            if lookup.cache_candidate_without_height_mapping {
                assert!(matches!(
                    service.fetch_header(candidate_hash),
                    FetchStatus::Fetched { .. }
                ));
            }
            assert!(matches!(
                service.poll_historical_header_proof(&request),
                Ok(HistoricalHeaderProofStatus::Added { .. })
            ));
            historical_request = Some((service, request));
        }

        if let Some((_, request)) = &historical_request {
            let lookup = param.historical_lookup.as_ref().unwrap();
            if lookup.dispatch_with_scheduler {
                protocol
                    .notify(
                        nc.context(),
                        crate::protocols::light_client::constant::FETCH_HEADER_TX_TOKEN,
                    )
                    .await;
            } else {
                protocol
                    .peers()
                    .update_blocks_proof_request_with_historical_targets(
                        peer_index,
                        Some(content),
                        true,
                        Vec::new(),
                        vec![request.id()],
                    );
            }
            let peer = protocol.get_peer(&peer_index).unwrap();
            let attached = peer.get_blocks_proof_request().unwrap();
            assert!(attached
                .block_hashes()
                .contains(historical_candidate_hash.as_ref().unwrap()));
            let expected_normal_fetch_hashes = if lookup.ordinary_fetch_candidate {
                vec![historical_candidate_hash.as_ref().unwrap().pack()]
            } else {
                Vec::new()
            };
            assert_eq!(
                attached.normal_fetch_hashes(),
                expected_normal_fetch_hashes.as_slice()
            );
            assert_eq!(attached.historical_targets().len(), 1);
            assert_eq!(attached.historical_targets()[0].id, request.id());
            assert_eq!(
                attached.historical_targets()[0].block_number,
                param.historical_lookup.as_ref().unwrap().block_number
            );
            assert_eq!(
                attached.historical_targets()[0].candidate_hash,
                historical_candidate_hash.as_ref().unwrap().pack()
            );
            assert_eq!(
                attached.historical_targets()[0].anchor_hash,
                snapshot.get_header_by_number(num).unwrap().hash()
            );
            if lookup.dispatch_with_scheduler {
                let sent_messages = nc.sent_messages().borrow();
                assert_eq!(sent_messages.len(), 1);
                assert_eq!(sent_messages[0].1, peer_index);
                let wire_message =
                    packed::LightClientMessageReader::new_unchecked(&sent_messages[0].2);
                let wire_request = match wire_message.to_enum() {
                    packed::LightClientMessageUnionReader::GetBlocksProof(request) => request,
                    _ => panic!("scheduler sent an unexpected message"),
                };
                assert!(wire_request
                    .block_hashes()
                    .to_entity()
                    .into_iter()
                    .any(|hash| hash == historical_candidate_hash.as_ref().unwrap().pack()));
                drop(sent_messages);
                nc.sent_messages().borrow_mut().clear();

                let fetch_info = protocol
                    .peers()
                    .get_header_fetch_info(&historical_candidate_hash.as_ref().unwrap().pack());
                assert_eq!(fetch_info.is_some(), lookup.ordinary_fetch_candidate);
            }
        } else {
            protocol
                .peers()
                .update_blocks_proof_request(peer_index, Some(content), true);
        }
    }

    let changed_tip = if param
        .historical_lookup
        .as_ref()
        .is_some_and(|lookup| lookup.change_tip_before_response)
    {
        let changed_tip = snapshot
            .get_header_by_number(num - 1)
            .expect("changed test tip exists")
            .data();
        chain
            .client_storage()
            .update_last_state(&U256::from(1u8), &changed_tip, &[]);
        Some(changed_tip)
    } else {
        None
    };

    // Run the test.
    {
        let proof_anchor_height = num;
        let last_header = snapshot
            .get_verifiable_header_by_number(proof_anchor_height)
            .expect("block stored");
        let mut headers = param
            .returned_headers
            .iter()
            .map(|n| snapshot.get_header_by_number(*n).expect("block stored"))
            .collect::<Vec<_>>();
        if let Some(replacement_header) = &param.replacement_header {
            let lookup = param
                .historical_lookup
                .as_ref()
                .expect("historical lookup parameters exist");
            let candidate_height = lookup.candidate_height.unwrap_or(lookup.block_number);
            let index = param
                .returned_headers
                .iter()
                .position(|height| *height == candidate_height)
                .expect("replacement header height is returned");
            headers[index] = replacement_header.clone().into_view();
        }
        let block_hashes = headers.iter().map(|h| h.hash()).collect::<Vec<_>>().pack();
        let data = {
            let headers = headers.iter().map(|h| h.data()).collect::<Vec<_>>();
            let last_number: BlockNumber = last_header.header().raw().number().unpack();
            let proof = chain.build_proof_by_numbers(last_number, &param.proved_block_numbers);
            let all_block_numbers = (0..last_number).collect::<Vec<_>>();
            if param.proved_block_numbers == all_block_numbers {
                assert!(proof.is_empty());
            }
            let content = if param.use_legacy_message {
                // A legacy (v0) message which withholds the V1 fields. Only
                // valid for blocks committing to no uncles/extensions.
                let content = packed::SendBlocksProof::new_builder()
                    .last_header(last_header)
                    .proof(proof)
                    .headers(headers.pack())
                    .missing_block_hashes(returned_missing_block_hashes.clone().pack())
                    .build();
                packed::LightClientMessage::new_builder()
                    .set(content)
                    .build()
            } else if let Some(uncles_hashes) = &param.returned_uncles_hashes {
                // A V1 message with explicitly crafted uncles/extensions.
                let mut builder = packed::SendBlocksProofV1::new_builder()
                    .last_header(last_header)
                    .proof(proof)
                    .headers(headers.pack())
                    .missing_block_hashes(returned_missing_block_hashes.clone().pack())
                    .blocks_uncles_hash(uncles_hashes.to_owned().pack());
                if let Some(extensions) = &param.returned_extensions {
                    let extensions = packed::BytesOptVec::new_builder()
                        .set(extensions.clone())
                        .build();
                    builder = builder.blocks_extension(extensions);
                }
                let content = builder.build();
                packed::LightClientMessage::new_builder()
                    .set(content)
                    .build()
            } else {
                // A V1 message carrying the real uncles hashes and extensions
                // from the snapshot, like an honest server would send.
                let uncles_hashes = headers
                    .iter()
                    .map(|h| {
                        snapshot
                            .get_block_by_number(h.raw().number().unpack())
                            .expect("block stored")
                            .calc_uncles_hash()
                    })
                    .collect::<Vec<_>>();
                let extensions = headers
                    .iter()
                    .map(|h| {
                        packed::BytesOpt::new_builder()
                            .set(
                                snapshot
                                    .get_block_by_number(h.raw().number().unpack())
                                    .expect("block stored")
                                    .extension(),
                            )
                            .build()
                    })
                    .collect::<Vec<_>>();
                let content = packed::SendBlocksProofV1::new_builder()
                    .last_header(last_header)
                    .proof(proof)
                    .headers(headers.pack())
                    .missing_block_hashes(returned_missing_block_hashes.clone().pack())
                    .blocks_uncles_hash(uncles_hashes.pack())
                    .blocks_extension(extensions)
                    .build();
                packed::LightClientMessage::new_builder()
                    .set(content)
                    .build()
            };
            content
        }
        .as_bytes();

        assert!(nc.sent_messages().borrow().is_empty());

        protocol.received(nc.context(), peer_index, data).await;

        if let Some(expected_status) = param.expected_status {
            assert!(nc.banned_since(peer_index, expected_status));
            assert!(nc.sent_messages().borrow().is_empty());
            if let Some((service, request)) = &historical_request {
                let candidate_hash = historical_candidate_hash.as_ref().unwrap();
                assert!(!matches!(
                    service.poll_historical_header_proof(request),
                    Ok(HistoricalHeaderProofStatus::Verified { .. })
                ));
                let requested_height: u64 = param
                    .historical_lookup
                    .as_ref()
                    .unwrap()
                    .block_number
                    .into();
                assert_ne!(
                    chain.client_storage().get_block_hash(requested_height),
                    Some(candidate_hash.pack())
                );
            }
        } else if param.block_numbers == param.proved_block_numbers
            && param.block_numbers == param.returned_headers
            && missing_block_hashes == returned_missing_block_hashes
        {
            assert!(nc.not_banned(peer_index));

            if param.block_numbers.is_empty()
                || param
                    .historical_lookup
                    .as_ref()
                    .is_some_and(|lookup| lookup.dispatch_with_scheduler)
            {
                assert!(nc.sent_messages().borrow().is_empty());
            } else {
                assert_eq!(nc.sent_messages().borrow().len(), 1);

                let data = &nc.sent_messages().borrow()[0].2;
                let message = packed::SyncMessageReader::new_unchecked(data);
                let content =
                    if let packed::SyncMessageUnionReader::GetBlocks(content) = message.to_enum() {
                        content
                    } else {
                        panic!("unexpected message");
                    };
                assert_eq!(content.block_hashes().as_slice(), block_hashes.as_slice());
            }

            let peer = protocol.get_peer(&peer_index).expect("has peer");
            assert!(peer.get_blocks_proof_request().is_none());

            if let Some((service, request)) = historical_request {
                let candidate_hash = historical_candidate_hash.as_ref().unwrap();
                let requested_height: u64 = param
                    .historical_lookup
                    .as_ref()
                    .expect("historical lookup parameters exist")
                    .block_number
                    .into();
                if param
                    .historical_lookup
                    .as_ref()
                    .expect("historical lookup parameters exist")
                    .change_tip_before_response
                {
                    let changed_tip = changed_tip.expect("test changed its accepted tip");
                    assert_eq!(
                        service.poll_historical_header_proof(&request).unwrap(),
                        HistoricalHeaderProofStatus::StaleAnchor {
                            requested_anchor: snapshot
                                .get_header_by_number(num)
                                .unwrap()
                                .hash()
                                .unpack(),
                            current_anchor: changed_tip.calc_header_hash().unpack(),
                        }
                    );
                    assert_eq!(
                        chain.client_storage().get_block_hash(requested_height),
                        Some(candidate_hash.pack())
                    );

                    let second = service
                        .request_historical_header_proof(
                            requested_height.into(),
                            candidate_hash.clone(),
                        )
                        .expect("a new anchor gets an independent operation");
                    assert_ne!(request.id(), second.id());
                    assert!(matches!(
                        service.poll_historical_header_proof(&second),
                        Ok(HistoricalHeaderProofStatus::Added { .. })
                    ));

                    let new_anchor = changed_tip.calc_header_hash();
                    let content = packed::GetBlocksProof::new_builder()
                        .last_hash(new_anchor.clone())
                        .block_hashes(vec![candidate_hash.pack()].pack())
                        .build();
                    let _ = content;
                    protocol
                        .notify(
                            nc.context(),
                            crate::protocols::light_client::constant::FETCH_HEADER_TX_TOKEN,
                        )
                        .await;
                    let second_peer = protocol.get_peer(&peer_index).unwrap();
                    let second_proof_request = second_peer.get_blocks_proof_request().unwrap();
                    assert_eq!(second_proof_request.last_hash(), new_anchor);
                    assert_eq!(second_proof_request.historical_targets().len(), 1);
                    assert_eq!(second_proof_request.historical_targets()[0].id, second.id());
                    assert!(second_proof_request.normal_fetch_hashes().is_empty());
                    assert_eq!(nc.sent_messages().borrow().len(), 1);
                    nc.sent_messages().borrow_mut().clear();
                    let last_header = snapshot
                        .get_verifiable_header_by_number(num - 1)
                        .expect("new accepted anchor exists");
                    let candidate = snapshot
                        .get_header_by_number(requested_height)
                        .expect("candidate header exists");
                    let candidate_block = snapshot
                        .get_block_by_number(requested_height)
                        .expect("candidate block exists");
                    let proof = chain.build_proof_by_numbers(num - 1, &[requested_height]);
                    let message = packed::SendBlocksProofV1::new_builder()
                        .last_header(last_header)
                        .proof(proof)
                        .headers(vec![candidate.data()].pack())
                        .missing_block_hashes(Vec::<packed::Byte32>::new().pack())
                        .blocks_uncles_hash(vec![candidate_block.calc_uncles_hash()].pack())
                        .blocks_extension(
                            packed::BytesOptVec::new_builder()
                                .set(vec![packed::BytesOpt::new_builder()
                                    .set(candidate_block.extension())
                                    .build()])
                                .build(),
                        )
                        .build();
                    let message = packed::LightClientMessage::new_builder()
                        .set(message)
                        .build()
                        .as_bytes();
                    protocol.received(nc.context(), peer_index, message).await;
                    assert_eq!(
                        service.poll_historical_header_proof(&second).unwrap(),
                        HistoricalHeaderProofStatus::Verified {
                            block_number: requested_height.into(),
                            block_hash: candidate_hash.clone(),
                            anchor_hash: new_anchor.unpack(),
                        }
                    );
                } else if param
                    .historical_lookup
                    .as_ref()
                    .expect("historical lookup parameters exist")
                    .peer_reports_missing
                {
                    let status = service.poll_historical_header_proof(&request);
                    assert_eq!(status.unwrap(), HistoricalHeaderProofStatus::Unavailable);
                    assert_ne!(
                        chain.client_storage().get_block_hash(requested_height),
                        Some(candidate_hash.pack())
                    );
                    let lookup = param.historical_lookup.as_ref().unwrap();
                    if lookup.dispatch_with_scheduler {
                        let fetch_info = protocol
                            .peers()
                            .get_header_fetch_info(&candidate_hash.pack());
                        if lookup.ordinary_fetch_candidate {
                            assert!(matches!(fetch_info, Some((_, _, true))));
                            assert!(matches!(
                                service.fetch_header(candidate_hash),
                                FetchStatus::NotFound
                            ));
                        } else {
                            assert!(fetch_info.is_none());
                            assert!(matches!(
                                service.fetch_header(candidate_hash),
                                FetchStatus::Added { .. }
                            ));
                            assert!(protocol
                                .peers()
                                .get_header_fetch_info(&candidate_hash.pack())
                                .is_some());
                        }
                    }
                } else if param
                    .historical_lookup
                    .as_ref()
                    .and_then(|lookup| lookup.candidate_height)
                    .is_some_and(|height| height != requested_height)
                {
                    let status = service.poll_historical_header_proof(&request);
                    assert_eq!(status.unwrap(), HistoricalHeaderProofStatus::Unavailable);
                    assert_ne!(
                        chain.client_storage().get_block_hash(requested_height),
                        Some(candidate_hash.pack())
                    );
                } else {
                    let second = service
                        .request_historical_header_proof(
                            requested_height.into(),
                            candidate_hash.clone(),
                        )
                        .expect("each caller receives an independent operation handle");
                    assert_ne!(request.id(), second.id());
                    assert!(matches!(
                        service.poll_historical_header_proof(&second),
                        Ok(HistoricalHeaderProofStatus::Added { .. })
                    ));
                    let status = service.poll_historical_header_proof(&request);
                    assert_eq!(
                        status.unwrap(),
                        HistoricalHeaderProofStatus::Verified {
                            block_number: requested_height.into(),
                            block_hash: candidate_hash.clone(),
                            anchor_hash: snapshot
                                .get_header_by_number(num)
                                .unwrap()
                                .hash()
                                .unpack(),
                        }
                    );
                    assert_eq!(
                        chain.client_storage().get_block_hash(requested_height),
                        Some(candidate_hash.pack())
                    );
                    if param
                        .historical_lookup
                        .as_ref()
                        .is_some_and(|lookup| lookup.dispatch_with_scheduler)
                    {
                        assert!(protocol
                            .peers()
                            .get_header_fetch_info(&candidate_hash.pack())
                            .is_none());
                    }
                }
            }
        } else {
            if missing_block_hashes != returned_missing_block_hashes
                || param.block_numbers != param.returned_headers
            {
                assert!(nc.banned_since(peer_index, StatusCode::UnexpectedResponse));
            } else if param.block_numbers != param.proved_block_numbers {
                assert!(nc.banned_since(peer_index, StatusCode::InvalidProof));
            } else {
                panic!("unhandled failed tests");
            }

            assert!(nc.sent_messages().borrow().is_empty());
        }
    }
}
