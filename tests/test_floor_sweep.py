"""CollateralFloorSweep: the vote_deactivate driver for the min_collateral-raise
edge case. The properties under test: steady state costs no RPC, the miner scan
runs only on boot / floor raise, busy miners are retried instead of dropped, and
a cast vote stays pending until quorum actually flips the miner inactive."""

from types import SimpleNamespace

from solders.pubkey import Pubkey

from allways.solana import pdas
from allways.validator.floor_sweep import CollateralFloorSweep

FLOOR = 1_000_000_000  # 1 SOL

MINER_A = Pubkey.new_unique()
MINER_B = Pubkey.new_unique()


def miner_state(miner, collateral, active=True, has_active_swap=False, busy_until=0):
    return SimpleNamespace(
        miner=bytes(miner),
        collateral=collateral,
        active=active,
        has_active_swap=has_active_swap,
        busy_until=busy_until,
    )


class FakeClient:
    def __init__(self, states):
        # {pubkey_str: miner_state}; mutate `states` to simulate chain changes.
        self.states = states
        self.keypair = SimpleNamespace(pubkey=lambda: Pubkey.new_unique())
        self.voted = set()
        self.calls = {'get_all': 0, 'get_miner_state': 0, 'vote_deactivate': 0}
        self.fail_get_all = 0  # raise on this many get_all calls before serving
        self.fail_state_for = set()  # miner keys whose reads raise

    def get_all(self, name):
        assert name == 'MinerState'
        self.calls['get_all'] += 1
        if self.fail_get_all > 0:
            self.fail_get_all -= 1
            raise RuntimeError('rpc down')
        return [(k, v) for k, v in self.states.items()]

    def get_miner_state(self, miner):
        self.calls['get_miner_state'] += 1
        if str(miner) in self.fail_state_for:
            raise RuntimeError('rpc down')
        return self.states.get(str(miner))

    def has_voted(self, req_type, miner, voter):
        assert req_type == pdas.REQ_DEACTIVATE
        return str(miner) in self.voted

    def vote_deactivate(self, miner):
        self.calls['vote_deactivate'] += 1
        self.voted.add(str(miner))
        return 'sig'


class Clock:
    def __init__(self, now=1000.0):
        self.now = now

    def __call__(self):
        return self.now


class TestSteadyState:
    def test_first_step_scans_once_then_unchanged_floor_is_free(self):
        client = FakeClient({str(MINER_A): miner_state(MINER_A, FLOOR)})
        sweep = CollateralFloorSweep(client, clock=Clock())
        sweep.step(FLOOR)
        assert client.calls['get_all'] == 1  # boot scan covers a raise-while-offline
        for _ in range(50):
            sweep.step(FLOOR)
        assert client.calls['get_all'] == 1
        assert client.calls['get_miner_state'] == 0
        assert client.calls['vote_deactivate'] == 0

    def test_floor_decrease_does_not_rescan(self):
        client = FakeClient({})
        sweep = CollateralFloorSweep(client, clock=Clock())
        sweep.step(FLOOR)
        sweep.step(FLOOR // 2)
        assert client.calls['get_all'] == 1


class TestRaise:
    def test_raise_kicks_underfloor_active_idle_miner(self):
        client = FakeClient(
            {
                str(MINER_A): miner_state(MINER_A, 150_000_000),  # under new floor
                str(MINER_B): miner_state(MINER_B, 2 * FLOOR),  # comfortably above
            }
        )
        sweep = CollateralFloorSweep(client, clock=Clock())
        sweep.step(100_000_000)  # boot at the old floor: everyone compliant
        assert client.calls['vote_deactivate'] == 0
        sweep.step(FLOOR)  # the raise
        assert client.calls['get_all'] == 2
        assert client.voted == {str(MINER_A)}

    def test_inactive_and_topped_up_miners_are_ignored(self):
        client = FakeClient(
            {
                str(MINER_A): miner_state(MINER_A, 100, active=False),
                str(MINER_B): miner_state(MINER_B, FLOOR),
            }
        )
        sweep = CollateralFloorSweep(client, clock=Clock())
        sweep.step(FLOOR)
        assert client.calls['vote_deactivate'] == 0


class TestPending:
    def test_busy_miner_retried_after_interval_not_before(self):
        clock = Clock()
        state = miner_state(MINER_A, 100, has_active_swap=True)
        client = FakeClient({str(MINER_A): state})
        sweep = CollateralFloorSweep(client, clock=clock)
        sweep.step(FLOOR)
        assert client.calls['vote_deactivate'] == 0  # busy: contract would reject

        sweep.step(FLOOR)  # inside retry interval — no reads
        assert client.calls['get_miner_state'] == 0

        state.has_active_swap = False  # swap resolved on-chain
        clock.now += CollateralFloorSweep.RETRY_SECS
        sweep.step(FLOOR)
        assert client.calls['get_miner_state'] == 1
        assert client.voted == {str(MINER_A)}

    def test_voted_miner_stays_pending_until_quorum_flips_inactive(self):
        clock = Clock()
        state = miner_state(MINER_A, 100)
        client = FakeClient({str(MINER_A): state})
        sweep = CollateralFloorSweep(client, clock=clock)
        sweep.step(FLOOR)
        assert client.calls['vote_deactivate'] == 1

        # Quorum not reached yet: recheck must not double-vote (has_voted gate)
        # but must keep the miner pending.
        clock.now += CollateralFloorSweep.RETRY_SECS
        sweep.step(FLOOR)
        assert client.calls['vote_deactivate'] == 1

        state.active = False  # peers reached quorum
        clock.now += CollateralFloorSweep.RETRY_SECS
        sweep.step(FLOOR)
        clock.now += CollateralFloorSweep.RETRY_SECS
        sweep.step(FLOOR)  # pending drained: no further reads
        assert client.calls['get_miner_state'] == 2

    def test_miner_topping_up_while_pending_is_released(self):
        clock = Clock()
        state = miner_state(MINER_A, 100, has_active_swap=True)
        client = FakeClient({str(MINER_A): state})
        sweep = CollateralFloorSweep(client, clock=clock)
        sweep.step(FLOOR)

        state.has_active_swap = False
        state.collateral = FLOOR  # posted collateral to comply instead
        clock.now += CollateralFloorSweep.RETRY_SECS
        sweep.step(FLOOR)
        assert client.calls['vote_deactivate'] == 0
        clock.now += CollateralFloorSweep.RETRY_SECS
        sweep.step(FLOOR)
        assert client.calls['get_miner_state'] == 1  # released, no more rechecks


class TestRpcFailure:
    def test_failed_arm_scan_is_retried_not_lost(self):
        """A raise whose scan RPC fails must stay armed: the floor is only
        committed after a successful scan, so the raise re-fires on the retry
        cadence instead of being swallowed forever."""
        clock = Clock()
        client = FakeClient({str(MINER_A): miner_state(MINER_A, 100)})
        client.fail_get_all = 1
        sweep = CollateralFloorSweep(client, clock=clock)
        try:
            sweep.step(FLOOR)  # production wraps this in maybe_sweep_floor
        except RuntimeError:
            pass
        assert client.voted == set()

        sweep.step(FLOOR)  # inside back-off: no immediate re-scan hammering
        assert client.calls['get_all'] == 1

        clock.now += CollateralFloorSweep.RETRY_SECS
        sweep.step(FLOOR)
        assert client.calls['get_all'] == 2
        assert client.voted == {str(MINER_A)}

    def test_one_unreadable_pending_miner_does_not_block_the_rest(self):
        clock = Clock()
        client = FakeClient(
            {
                str(MINER_A): miner_state(MINER_A, 100, has_active_swap=True),
                str(MINER_B): miner_state(MINER_B, 100, has_active_swap=True),
            }
        )
        sweep = CollateralFloorSweep(client, clock=clock)
        sweep.step(FLOOR)  # both pending (busy)

        for ms in client.states.values():
            ms.has_active_swap = False
        client.fail_state_for = {str(MINER_A)}
        clock.now += CollateralFloorSweep.RETRY_SECS
        sweep.step(FLOOR)
        assert client.voted == {str(MINER_B)}  # B kicked despite A's read failing

        client.fail_state_for = set()
        clock.now += CollateralFloorSweep.RETRY_SECS
        sweep.step(FLOOR)  # next cadence, not next step
        assert client.voted == {str(MINER_A), str(MINER_B)}


class TestStaleVoteRound:
    def test_stale_round_vote_does_not_gate_a_revote(self):
        """The contract reopens a round older than VOTE_ROUND_TTL_SECS and lets
        prior voters vote again; the client's has_voted must mirror that, or a
        first pass that missed quorum leaves every prior voter skipping forever
        and the miner stranded (review finding on #616)."""
        from solders.keypair import Keypair

        from allways.constants import VOTE_ROUND_TTL_SECS
        from allways.solana.client import AllwaysSolanaClient

        voter = Keypair().pubkey()
        client = AllwaysSolanaClient('http://127.0.0.1:1', program_id=Pubkey.new_unique(), keypair=Keypair())
        round_ = SimpleNamespace(voters=[voter], created_at=10_000)
        client.get_vote_round = lambda req_type, target: round_

        live = 10_000 + VOTE_ROUND_TTL_SECS  # exactly at the TTL: still live
        assert client.has_voted(pdas.REQ_DEACTIVATE, MINER_A, voter, now=live) is True
        stale = 10_000 + VOTE_ROUND_TTL_SECS + 1
        assert client.has_voted(pdas.REQ_DEACTIVATE, MINER_A, voter, now=stale) is False

        # An empty round is closed/reset — never a gate.
        client.get_vote_round = lambda req_type, target: SimpleNamespace(voters=[], created_at=0)
        assert client.has_voted(pdas.REQ_DEACTIVATE, MINER_A, voter, now=live) is False

    def test_already_voted_race_is_benign_and_miner_stays_pending(self):
        """If the vote lands AlreadyVoted (the has_voted read raced the send),
        the sweep treats it as an on-chain no-op — and the miner stays pending
        until quorum flips it inactive."""
        clock = Clock()
        state = miner_state(MINER_A, 100)
        client = FakeClient({str(MINER_A): state})

        def racing_vote(miner):
            client.calls['vote_deactivate'] += 1
            raise RuntimeError('custom program error. Error Message: AlreadyVoted.')

        client.vote_deactivate = racing_vote
        sweep = CollateralFloorSweep(client, clock=clock)
        sweep.step(FLOOR)
        assert client.calls['vote_deactivate'] == 1

        state.active = False  # quorum reached via peers
        clock.now += CollateralFloorSweep.RETRY_SECS
        sweep.step(FLOOR)  # still pending → rechecked → released
        assert client.calls['get_miner_state'] == 1


class TestReadOnly:
    def test_watch_mode_never_votes_but_keeps_watching(self):
        clock = Clock()
        client = FakeClient({str(MINER_A): miner_state(MINER_A, 100)})
        sweep = CollateralFloorSweep(client, read_only=True, clock=clock)
        sweep.step(FLOOR)
        clock.now += CollateralFloorSweep.RETRY_SECS
        sweep.step(FLOOR)
        assert client.calls['vote_deactivate'] == 0
        assert client.calls['get_miner_state'] == 1  # still tracked as pending
