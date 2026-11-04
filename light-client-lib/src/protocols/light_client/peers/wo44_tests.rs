use super::*;

async fn hint_deadline(kind: Kind) {
    let clock = faketime();
    clock.set_faketime(START);
    let mut f = Fixture::new();
    let hash = f.hash(kind);
    f.arm(kind, &hash).await;
    let owner = Captured::get(&f, kind, f.peer);
    let hint = changed_hint(&f, 441);
    f.receive(f.negative(kind, hint.clone(), vec![])).await;
    owner.assert_same(&Captured::get(&f, kind, f.peer));
    clock.set_faketime(START + MESSAGE_TIMEOUT);
    // An independently live last-state channel must not be disconnected merely
    // because an uncorrelated proof hint could not consume its request owner.
    f.peers
        .update_last_state(f.peer, LastState::new(VerifiableHeader::from(hint.clone())))
        .unwrap();
    f.protocol
        .notify(f.context.context(), REFRESH_PEERS_TOKEN)
        .await;
    assert!(f.context.take_disconnects().is_empty());
    owner.assert_same(&Captured::get(&f, kind, f.peer));
    clock.set_faketime(START + MESSAGE_TIMEOUT + 1);
    f.peers
        .update_last_state(f.peer, LastState::new(VerifiableHeader::from(hint)))
        .unwrap();
    f.protocol
        .notify(f.context.context(), REFRESH_PEERS_TOKEN)
        .await;
    assert!(
        f.context.take_disconnects().is_empty(),
        "hinted proof deadline punished a live peer"
    );
    assert!(slot_is_empty(&f, kind, f.peer));
    assert_eligible(&f, kind, true);
    assert!(!owner.finish(&f, f.peer, true));
    assert!(f.context.not_banned(f.peer));
}

#[tokio::test]
async fn wo44_block_hint_expires_without_punishing_a_live_peer() {
    hint_deadline(Kind::Header).await;
}
#[tokio::test]
async fn wo44_transaction_hint_expires_without_punishing_a_live_peer() {
    hint_deadline(Kind::Transaction).await;
}
