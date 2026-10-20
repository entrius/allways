import tempfile

import pytest


@pytest.fixture(autouse=True, scope='session')
def contain_tempfiles(tmp_path_factory):
    # Route bare mkdtemp() calls into pytest's basetemp, which pytest prunes to the last 3 runs.
    tempfile.tempdir = str(tmp_path_factory.getbasetemp())
