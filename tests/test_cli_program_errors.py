"""Program error codes are read from every failure form (preflight, landed, hex) against the packaged IDL."""

from allways.solana.client import PROGRAM_ERRORS, program_error_code

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
