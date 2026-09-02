"""The bond-vault ABI must work from a pip install, where smart-contracts/ does not exist —
the packaged copy under allways/metadata/ is the fallback DEFAULT_METADATA resolves to.
"""

from allways.vault import codec
from allways.vault.client import _PACKAGED_METADATA, _REPO_METADATA, DEFAULT_METADATA


def test_packaged_metadata_ships_inside_the_package():
    assert _PACKAGED_METADATA.exists(), 'allways/metadata/allways_bond_vault.json missing from the package'
    assert 'smart-contracts' not in _PACKAGED_METADATA.parts


def test_packaged_metadata_is_valid_vault_metadata():
    meta = codec.VaultMetadata.from_path(_PACKAGED_METADATA)
    assert meta.call('post_collateral')


def test_packaged_metadata_matches_build_artifact():
    # The vault is frozen (D7); the copies only diverge by mistake.
    if not _REPO_METADATA.exists():
        return
    assert _PACKAGED_METADATA.read_bytes() == _REPO_METADATA.read_bytes()


def test_default_resolves_to_an_existing_file():
    assert DEFAULT_METADATA.exists()
