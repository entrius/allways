"""Program/RPC failures reach the operator as one actionable line — never a traceback or the raw RPC dict."""

from allways.cli.swap_commands.helpers import solana_failure_message
from allways.solana.client import PROGRAM_ERRORS, program_error_code
from allways.solana.rpc import SolanaRpcError

PREFLIGHT = (
    "sendTransaction: {'code': -32002, 'message': 'Transaction simulation failed: Error processing Instruction 0: "
    "custom program error: 0x1772', 'data': {'err': {'InstructionError': [0, {'Custom': 6002}]}}}"
)
LANDED = "tx SIG failed: {'InstructionError': [0, {'Custom': 6015}]}"


def test_program_error_code_reads_preflight_landed_and_hex_forms():
    assert program_error_code(Exception(PREFLIGHT)) == 6002
    assert program_error_code(Exception(LANDED)) == 6015
    assert program_error_code(Exception('custom program error: 0x1772')) == 6002
    assert program_error_code(Exception("tx SIG failed: {'InstructionError': [0, 'InvalidAccountData']}")) is None


def test_program_errors_come_from_the_packaged_idl():
    assert PROGRAM_ERRORS[6002] == ('InsufficientCollateral', 'Insufficient collateral for this withdrawal')


def test_failure_message_hints_or_falls_back_to_idl_message():
    assert solana_failure_message(SolanaRpcError(PREFLIGHT)) == (
        'Deposit collateral first: alw collateral deposit (InsufficientCollateral 6002)'
    )
    assert solana_failure_message(SolanaRpcError("tx SIG failed: {'InstructionError': [0, {'Custom': 6018}]}")) == (
        'System is halted (SystemHalted 6018)'
    )


def test_failure_message_keeps_only_the_rpc_message_line():
    err = SolanaRpcError(
        "sendTransaction: {'code': -32002, 'message': 'Transaction simulation failed: Blockhash not found', 'data': {'logs': []}}"
    )
    assert solana_failure_message(err) == 'Transaction simulation failed: Blockhash not found'
    assert (
        solana_failure_message(SolanaRpcError('tx SIG not confirmed within 30.0s'))
        == 'tx SIG not confirmed within 30.0s'
    )
