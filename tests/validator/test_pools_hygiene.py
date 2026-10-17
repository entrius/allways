from types import SimpleNamespace

from solders.keypair import Keypair

from allways.solana import pdas
from allways.validator.actuator.actuator import Actuator
from allways.validator.actuator.planners import HygienePlanner, PoolPlanner, derive_weight_vector
from allways.validator.app import Config, build
from allways.validator.chains.subtensor_view import Head
from allways.validator.runtime.bus import Latest
from allways.validator.world.snapshot import World

NOW = 1_000_000
ME = Keypair().pubkey()
MINER = Keypair().pubkey()


def _config(min_collateral=100, tao_min_collateral=0, last_weights_update=0, validators=()):
    return SimpleNamespace(
        min_collateral=min_collateral,
        tao_min_collateral=tao_min_collateral,
        last_weights_update=last_weights_update,
        validators=list(validators),
    )


def _world(config=None, pools=(), miners=(), attestations=None, attribution=None):
    return World(
        taken_at=NOW,
        slot=1,
        complete=True,
        config=config or _config(),
        accounts={
            'Pool': {f'pool{i}': p for i, p in enumerate(pools)},
            'MinerState': {f'ms{i}': m for i, m in enumerate(miners)},
            'BondAttestation': attestations or {},
        },
        attribution=attribution or {},
    )


def _pool(opened_at=1, closes_at=NOW - 1, requests=('r',)):
    return SimpleNamespace(
        opened_at=opened_at, closes_at=closes_at, requests=list(requests), miner=MINER, collateral_chain='sol'
    )


def test_only_closed_non_empty_pools_are_resolved():
    pools = [_pool(), _pool(opened_at=0), _pool(closes_at=NOW + 5), _pool(requests=())]
    intents = PoolPlanner().plan(_world(pools=pools), NOW)
    assert [i.key for i in intents] == [('resolve_pool', 'pool0')]


def _miner(collateral=50, active_backings=pdas.BACKING_BIT_SOL, busy_until=(0, 0), swaps=0):
    return SimpleNamespace(
        miner=bytes(MINER),
        active=True,
        active_backings=active_backings,
        collateral=collateral,
        busy_until=list(busy_until),
        active_swap_backings=swaps,
    )


def test_a_miner_under_its_hubs_floor_is_voted_out_once_the_hub_is_idle():
    hygiene = HygienePlanner(Latest())
    (vote,) = hygiene.plan(_world(miners=[_miner(collateral=50)]), NOW)
    assert vote.key == ('vote_deactivate', (str(MINER), 'sol'))
    assert hygiene.plan(_world(miners=[_miner(collateral=150)]), NOW) == []  # above the floor
    assert hygiene.plan(_world(miners=[_miner(busy_until=(NOW + 60, 0))]), NOW) == []  # hub busy: later
    assert hygiene.plan(_world(miners=[_miner(swaps=pdas.BACKING_BIT_SOL)]), NOW) == []


def test_a_vault_hub_is_judged_by_its_own_attested_bond_and_never_on_an_unreadable_one():
    both = pdas.BACKING_BIT_SOL | pdas.BACKING_BIT_TAO
    config = _config(min_collateral=10, tao_min_collateral=1000)
    att_pda = str(pdas.bond_attestation_pda(str(MINER), 'tao'))
    low = {att_pda: SimpleNamespace(locked=True, effective_balance=5)}
    (vote,) = HygienePlanner(Latest()).plan(
        _world(config, miners=[_miner(collateral=50, active_backings=both)], attestations=low), NOW
    )
    assert vote.key == ('vote_deactivate', (str(MINER), 'tao'))
    unlocked = {att_pda: SimpleNamespace(locked=False, effective_balance=5)}
    assert (
        HygienePlanner(Latest()).plan(
            _world(config, miners=[_miner(collateral=50, active_backings=both)], attestations=unlocked), NOW
        )
        == []
    )


def _head(stake):
    metagraph = SimpleNamespace(hotkeys=['hk-me'], alpha_stake=[stake])
    head = Latest()
    head.set(Head(block=7200, block_read_at=NOW, metagraph=metagraph, metagraph_read_at=NOW))
    return head


def test_the_weights_vote_fires_once_per_boundary_and_only_when_the_vector_changed():
    validators = [SimpleNamespace(key=bytes(ME), weight=1)]
    world = _world(_config(validators=validators), attribution={str(ME): 'hk-me'})
    (vote,) = HygienePlanner(_head(stake=350_000), me=ME).plan(world, NOW)
    assert vote.key[0] == 'vote_set_weights' and vote.payload[0] == [10]
    assert HygienePlanner(_head(stake=35_000), me=ME).plan(world, NOW) == []  # vector unchanged ([1])
    landed = _world(_config(validators=validators, last_weights_update=NOW), attribution={str(ME): 'hk-me'})
    assert HygienePlanner(_head(stake=350_000), me=ME).plan(landed, NOW) == []  # already landed this boundary
    stranger = HygienePlanner(_head(stake=350_000), me=Keypair().pubkey())
    assert stranger.plan(world, NOW) == []  # not whitelisted


class _Planner:
    name = 'p'

    def __init__(self, intents):
        self.intents = intents

    def plan(self, world, now):
        return list(self.intents)


def test_a_send_that_went_nowhere_rests_instead_of_repeating_every_tick():
    sends = []
    world = Latest()
    world.set(_world())
    pool_intent = PoolPlanner().plan(_world(pools=[_pool()]), NOW)
    act = Actuator(
        world,
        [_Planner(pool_intent)],
        {'resolve_pool': lambda c, i: sends.append(1)},
        lambda: SimpleNamespace(rpc=None),
    )
    act.setup()
    for t in (0, 1, 1.5):  # a lost race returns no signature
        act.tick(NOW + t)
    assert len(sends) == 1  # rested for resolve_pool's 2s
    act.tick(NOW + 2.5)
    assert len(sends) == 2


def test_the_app_builds_with_no_relay_without_a_vault_and_serving_off(tmp_path):
    cfg = Config(solana_rpc_url='http://unused', program_id=None, keypair=Keypair(), state_dir=str(tmp_path), netuid=19)
    app = build(cfg, assets={}, subtensor_factory=lambda: None)
    names = {c.name for c in app.components}
    assert {'world', 'actuator', 'feed', 'scoring', 'weights', 'subtensor', 'usage'} <= names
    assert not any(n.startswith('relay') or n == 'serving' for n in names)


def test_a_configured_vault_adds_its_relay_which_blocks_swaps_for_a_miner_owing_a_slash(tmp_path):
    cfg = Config(
        solana_rpc_url='http://unused',
        program_id=None,
        keypair=Keypair(),
        state_dir=str(tmp_path),
        netuid=19,
        vaults={'tao': lambda: None},
    )
    app = build(cfg, assets={}, subtensor_factory=lambda: None)
    relay = next(c for c in app.components if c.name == 'relay-tao')
    relay.ledger.record_relay_slash('aa', str(MINER), 'tao', 1, 1, 'payee', NOW)
    swaps = next(p for p in next(c for c in app.components if c.name == 'actuator').planners if p.name == 'swaps')
    assert swaps.owes_settlement(SimpleNamespace(miner=MINER, collateral_chain='tao'))
    assert not swaps.owes_settlement(SimpleNamespace(miner=ME, collateral_chain='tao'))


def _metagraph(hotkeys, stakes):
    return SimpleNamespace(hotkeys=list(hotkeys), alpha_stake=list(stakes))


def test_draw_weights_bucket_stake_floor_a_bound_validator_at_one_and_zero_the_rest():
    keys = [Keypair().pubkey() for _ in range(4)]
    validators = [SimpleNamespace(key=bytes(k), weight=1) for k in keys]
    attribution = {str(keys[0]): 'hkA', str(keys[1]): 'hkB', str(keys[2]): 'hkC'}  # keys[3] unbound
    mg = _metagraph(['hkA', 'hkB', 'hkC'], [178_000.0, 34_999.0, 35_000.0])
    assert derive_weight_vector(validators, attribution, mg) == [5, 1, 1, 0]
    assert derive_weight_vector(validators[:1], {str(keys[0]): 'gone'}, mg) == [0]  # off the metagraph
