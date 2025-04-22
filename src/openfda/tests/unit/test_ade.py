import os
from utilities import ADE

def test_extractJSON_valid(load_mock_json):
    ade = ADE()
    ade.extractJSON(load_mock_json)
    
    # Assert attributes
    # Check type (Doesnt make sense since it is initialized to be a list?)
    assert isinstance(ade.patients_list,list) 
    assert isinstance(ade.drugs_list,list) 
    assert isinstance(ade.reactions_list,list) 

    # Check content
    assert ade.patients_list != [] # Non-empty
    assert ade.drugs_list != [] # Non-empty
    assert ade.reactions_list != [] # Non-empty

def test_extractJSON_empty():
    ade = ADE()
    ade.extractJSON({'results': []})

    assert ade.patients_list == []
    assert ade.drugs_list == []
    assert ade.reactions_list == []

def test_dataframe_structure(load_mock_json):
    ade = ADE()
    ade.extractJSON(load_mock_json)
    df_pat, df_drug, df_reac = ade._to_dataframe()

    # Basic column checks
    assert list(df_pat.columns) == ADE.patient_header
    assert list(df_drug.columns) == ADE.drug_header
    assert list(df_reac.columns) == ADE.reaction_header

    # Optional: sanity check row counts
    assert len(df_pat) > 0
    assert len(df_drug) > 0
    assert len(df_reac) > 0

def test_save_as_parquet(tmp_path,load_mock_json):
    ade = ADE()
    ade.extractJSON(load_mock_json)

    original_cwd = os.getcwd()
    try:
        os.chdir(tmp_path)
        ade.save_as_parquet(fname="testfile",dir='year')
        expected_files = [
            tmp_path / "pq" / "patient" / "year" / "testfile.parquet",
            tmp_path / "pq" / "drug" / "year" / "testfile.parquet",
            tmp_path / "pq" / "reaction" / "year" / "testfile.parquet",
        ]

        for f in expected_files:
            assert f.exists(), f"{f} not found"

    finally:
        os.chdir(original_cwd)