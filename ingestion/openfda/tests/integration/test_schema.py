import pytest

def test_download_json_keys(download_json):
    """Basic schema validation of download.json from openFDA API"""
    assert "results" in download_json
    assert "drug" in download_json['results']
    assert "event" in download_json['results']['drug']