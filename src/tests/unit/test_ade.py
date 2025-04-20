import os
import pytest

from scripts.ade import ADE
from utilities.helper import read_json_file

@pytest.fixture
def load_mock_json():
    return read_json_file('./src/tests/mocks/data/drug-event-0001-of-0005.json')

def test_ADE_extractJSON(load_mock_json):
    ade = ADE()
    ade.extractJSON(load_mock_json)
    
    # Assert attributes
    # Check type
    assert isinstance(ade.patients_list,list) 
    assert isinstance(ade.drugs_list,list) 
    assert isinstance(ade.reactions_list,list) 

    # Check content
    assert ade.patients_list != [] # Non-empty
    assert ade.drugs_list != [] # Non-empty
    assert ade.reactions_list != [] # Non-empty

def test_ADE_save_as_parquet(tmp_path,load_mock_json):
    ade = ADE()
    ade.extractJSON(load_mock_json)
    os.chdir(tmp_path)

    ade.save_as_parquet(fname="testfile",dir='year')

    expected_files = [
        tmp_path / "pq" / "patient" / "year" / "testfile.parquet",
        tmp_path / "pq" / "drug" / "year" / "testfile.parquet",
        tmp_path / "pq" / "reaction" / "year" / "testfile.parquet",
    ]

    for f in expected_files:
        assert f.exists(), f"{f} not found"