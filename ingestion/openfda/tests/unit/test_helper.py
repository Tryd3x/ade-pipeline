import tempfile
import pytest
from utilities.helper import *

# Keeping it short: one positive case, one negative case, null case

@pytest.mark.parametrize(
        "input,expected",
        [
            pytest.param(
                {'display_name':'2023 Q1'},
                '2023',
                id="Valid Key",
            ),
            pytest.param(
                {'name':'2023 Q1'},
                '',
                id="Invalid Key",
            ),
            pytest.param(
                {},
                '',
                id="Empty",
            ),
        ],
)
def test_get_partition_id(input,expected):
    # Assert various input format
    assert partition_id_by_year(input) == expected

    # Assert return type
    assert isinstance(partition_id_by_year(input),str), f"Expected result to be a string, but got {type(partition_id_by_year(input))}"


@pytest.mark.parametrize(
        "input,expected",
        [   
            pytest.param(
                {},
                -1,
                id="Empty"
            ),
            pytest.param(
                {'size_mb':'51.2'},
                51.2,
                id="Valid Key"
            ),
            pytest.param(
                {'size':'51.5'},
                -1,
                id="Invalid Key"
            ),
            pytest.param(
                {'size_mb':100.24},
                100.24,
                id="Floating Value"
            ),
        ],
)
def test_part_size_mb(input,expected):
    assert part_size_mb(input) == expected

    # Assert return type
    assert isinstance(part_size_mb(input),(float,int)), f"Expected result to be a float, but got {type(part_size_mb(input))}"


def test_read_json_file_valid():
    """Assert Valid JSON file"""
    expected_data = {"key":"value"}
    with tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8') as temp_file:
        json.dump(expected_data, temp_file)
        path = temp_file.name
    
    assert read_json_file(path) == expected_data

def test_read_file_nonexistent():
    """Assert Non-existent JSON file"""
    with pytest.raises(FileNotFoundError):
        read_json_file("non_existent.json")

    # Above code translates to below
    # try:
    #     read_json_file("non_existent.json")
    #     assert False, "Expected FileNotFoundError, but none was raised"
    # except FileNotFoundError:
    #     pass  # ✅ Test passes

def test_read_json_file_invalid_json():
    with tempfile.NamedTemporaryFile(delete=False, mode='w',encoding='utf-8') as temp_file:
        temp_file.write('random data not JSON structure')
        path = temp_file.name

    with pytest.raises(json.JSONDecodeError):
        read_json_file(path)



