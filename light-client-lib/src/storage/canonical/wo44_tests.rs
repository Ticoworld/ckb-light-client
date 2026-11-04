use super::*;
use crate::storage::canonical::coverage;

#[test]
fn wo44_rechecked_inflight_headers_are_local_staleness_not_bad_proof() {
    let (_directory, storage, blocks) = positional_fixture();
    let revision = coverage::scripts_revision(&storage).unwrap();
    coverage::save_filters(&storage, 1, 1, vec![(1, blocks[0].hash())], revision).unwrap();
    assert!(coverage::check_headers(&storage, &[blocks[0].header()]).unwrap());
    coverage::recheck(&storage, 1).unwrap();
    let before = snapshot_target_rows(&storage);
    for _ in 0..3 {
        assert!(!coverage::check_headers(&storage, &[blocks[0].header()]).unwrap());
        coverage::prepare(&storage).unwrap();
        assert_eq!(
            snapshot_target_rows(&storage),
            before,
            "stale response/timer rewound twice"
        );
    }
}

#[test]
fn wo44_current_position_mismatch_still_rejects_and_refilters() {
    let (_directory, storage, blocks) = positional_fixture();
    let revision = coverage::scripts_revision(&storage).unwrap();
    coverage::save_filters(&storage, 1, 1, vec![(1, blocks[0].hash())], revision).unwrap();
    assert!(coverage::check_headers(&storage, &[blocks[1].header()]).is_err());
    assert!(
        !coverage::cached_matches(&storage, &storage.get_earliest_matched_blocks().unwrap())
            .unwrap()
            .is_some()
    );
}

#[test]
fn wo44_pending_anchor_survives_150_linear_acceptances_reopen_but_not_fork() {
    let (directory, storage, blocks) = positional_fixture();
    let revision = coverage::scripts_revision(&storage).unwrap();
    coverage::save_filters(&storage, 1, 1, vec![(1, blocks[0].hash())], revision).unwrap();
    let original_size = storage.get(coverage::key(1)).unwrap().unwrap().len();
    let mut tip = storage.get_tip_header().into_view();
    let mut window = Vec::new();
    for number in 7..=156 {
        window.push(tip.clone());
        if window.len() > 100 {
            window.remove(0);
        }
        tip = core::HeaderBuilder::default()
            .number(number)
            .parent_hash(tip.hash())
            .epoch(core::EpochNumberWithFraction::new(0, number, 1000))
            .build();
        update_state(
            &storage,
            &U256::from(number + 1),
            &tip.data(),
            &window,
            None,
        )
        .unwrap();
    }
    assert_eq!(
        storage.get(coverage::key(1)).unwrap().unwrap().len(),
        original_size
    );
    drop(storage);
    let storage = Storage::new(directory.path());
    let range = storage.get_earliest_matched_blocks().unwrap();
    assert!(coverage::cached_matches(&storage, &range)
        .unwrap()
        .is_some());
    assert!(!coverage::prepare(&storage).unwrap());
    let competing = tip.as_advanced_builder().nonce(1234u128).build();
    update_state(
        &storage,
        &U256::from(1000u64),
        &competing.data(),
        &[],
        Some(0),
    )
    .unwrap();
    assert!(coverage::cached_matches(&storage, &range)
        .unwrap()
        .is_none());
}

#[test]
fn wo44_forward_pending_work_preserves_certified_prefix_but_recheck_fences_it() {
    let (_directory, storage, blocks) = positional_fixture();
    replay_target_page(&storage, 1, 1, &blocks[..1]);
    assert_eq!(positional_capacity(&storage).unwrap(), 100);
    let revision = coverage::scripts_revision(&storage).unwrap();
    coverage::save_filters(&storage, 2, 1, vec![(2, blocks[1].hash())], revision).unwrap();
    assert_eq!(positional_capacity(&storage).unwrap(), 100);
    coverage::recheck(&storage, 2).unwrap();
    assert!(positional_capacity(&storage).is_err());
}

#[test]
fn wo44_legacy_cursor_is_not_a_birth_or_complete_membership_certificate() {
    let (directory, storage, blocks) = positional_fixture();
    replay_target_page(&storage, 1, 1, &blocks[..1]);
    let primary = storage
        .get(Key::TxHash(&blocks[0].transactions()[0].hash()).into_vec())
        .unwrap();
    storage.delete(&meta(RECOVERY_FORMAT)).unwrap();
    storage.delete(&meta(INDEX_FORMAT)).unwrap();
    drop(storage);
    let storage = Storage::new(directory.path());
    storage.init_genesis_block(ConsensusBuilder::default().build().genesis_block().data());
    assert_eq!(storage.get_filter_scripts()[0].block_number, 0);
    assert_eq!(
        storage
            .get(Key::TxHash(&blocks[0].transactions()[0].hash()).into_vec())
            .unwrap(),
        primary
    );
    assert!(
        positional_capacity(&storage).is_err(),
        "a legacy cursor must not certify unproved history"
    );
}

#[derive(Clone)]
struct Reads {
    storage: Storage,
    reads: Arc<std::sync::atomic::AtomicUsize>,
}
impl StorageBackend for Reads {
    type Batch = <Storage as StorageBackend>::Batch;
    fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.storage.get(key)
    }
    fn get_many(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>> {
        self.reads.fetch_add(keys.len(), Ordering::Relaxed);
        self.storage.get_many(keys)
    }
    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.storage.put(key, value)
    }
    fn delete(&self, key: &[u8]) -> Result<()> {
        self.storage.delete(key)
    }
    fn batch(&self) -> Self::Batch {
        self.storage.batch()
    }
    fn collect_iterator(
        &self,
        start: IteratorStart,
        direction: IteratorDirection,
        take: TakeWhileFn,
        filter: FilterMapFn,
        limit: usize,
    ) -> Vec<KVPair> {
        self.storage
            .collect_iterator(start, direction, take, filter, limit)
    }
    fn try_collect_iterator(
        &self,
        start: IteratorStart,
        direction: IteratorDirection,
        take: TakeWhileFn,
        filter: FilterMapFn,
        limit: usize,
    ) -> Result<Vec<KVPair>> {
        self.storage
            .try_collect_iterator(start, direction, take, filter, limit)
    }
}
impl LightClientStorage for Reads {}

#[test]
fn wo44_history_page_does_not_validate_2000_irrelevant_tail_transactions() {
    use crate::service::{LightClientService, Order, SearchKey};
    let (_directory, storage, blocks) = positional_fixture();
    replay_target_page(&storage, 1, 1, &blocks[..1]);
    let script = storage.get_filter_scripts().remove(0).script;
    for number in 3..2003 {
        storage
            .put(
                Key::TxLockScript(&script, number, 1, 0, crate::storage::CellType::Output)
                    .into_vec(),
                vec![0; 32],
            )
            .unwrap();
    }
    let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let service = LightClientService::new(Arc::new(Reads {
        storage,
        reads: reads.clone(),
    }));
    let key = || SearchKey {
        script: script.clone().into(),
        ..Default::default()
    };
    for grouped in [false, true] {
        reads.store(0, Ordering::Relaxed);
        let mut search = key();
        search.group_by_transaction = Some(grouped);
        let result = service.get_transactions(search, Order::Asc, 1u32.into(), None);
        eprintln!(
            "WO44 history grouped={grouped}: key reads={}",
            reads.load(Ordering::Relaxed)
        );
        assert_eq!(result.unwrap().objects.len(), 1);
        assert!(
            reads.load(Ordering::Relaxed) < 80,
            "small page validated the tail"
        );
        assert!(
            service
                .get_transactions(key(), Order::Desc, 1u32.into(), None)
                .is_err(),
            "invalid returned row must still fail"
        );
    }
}
