"""Stub out heavy external deps so unit tests run without the full chain."""

import sys
from types import ModuleType
from unittest.mock import MagicMock


def _mock_module(name: str, **attrs) -> ModuleType:
    mod = ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


def _stub_bittensor() -> None:
    bt = MagicMock()
    bt.__name__ = 'bittensor'
    sys.modules['bittensor'] = bt
    sys.modules['bittensor.utils'] = MagicMock()


def _stub_substrate() -> None:
    """async_substrate_interface and websockets pull in native extensions."""
    _mock_module('scalecodec')
    _mock_module('async_substrate_interface')
    errors_mod = _mock_module('async_substrate_interface.errors')
    errors_mod.ExtrinsicNotFound = Exception
    _mock_module('websockets')
    _mock_module('websockets.exceptions', ConnectionClosed=Exception)


_stub_bittensor()
_stub_substrate()
