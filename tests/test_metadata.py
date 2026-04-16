"""Tests for allways.metadata — package constants."""

from pathlib import Path

from allways.metadata import METADATA_DIR


def test_metadata_dir_is_path():
    assert isinstance(METADATA_DIR, Path)


def test_metadata_dir_is_the_metadata_package():
    assert METADATA_DIR.name == 'metadata'
    assert METADATA_DIR.is_dir()
