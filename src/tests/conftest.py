""" Fixtures, preparing test suites, cleaning test suites, external data, API calls etc"""
import pytest
import requests

@pytest.fixture
def setup_test_suite():
    print("Setup phase")
    yield
    print("Cleanup phase")
    pass

@pytest.fixture
def clean_test_suite():
    pass

@pytest.fixture
def get_openfda_api_data():
    """Simulate a real HTTP request to openFDA API to fetch download.json. Used for Integration testing."""
    url = "https://api.fda.gov/download.json"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        pytest.fail(f"Failed to fetch data: {response.status_code}")
