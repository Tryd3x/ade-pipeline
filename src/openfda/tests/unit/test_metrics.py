from utilities import Metrics, ADE

# Test update for:
# - Empty JSON
# - Valid JSON
def test_update_valid(load_mock_json):
    ade = ADE()
    ade.extractJSON(load_mock_json)

    metric = Metrics()
    metric.update(ade)

    # Test total_records
    for s in metric.schema:
        assert metric.total_records[s] != 0, "Total records must not be 0"

    # Test null_count
    for s in metric.schema:
        for k in metric.null_count[s].keys():
            assert isinstance(metric.null_count[s][k], int)
            assert metric.null_count[s][k] >= 0, "Null count cannot be negative"
    
    metric._null_ratio()

    # Test null_ratio
    for s in metric.schema:
        for k in metric.null_ratio[s].keys():
            assert isinstance(metric.null_ratio[s][k], float)
            assert metric.null_ratio[s][k] >= 0 and metric.null_ratio[s][k] <= 1 , "Null ratio must be between 0 and 1"

def test_update_empty():
    ade = ADE()
    ade.extractJSON({"results" : []})

    metric = Metrics()
    metric.update(ade)

    # Test total_records
    for s in metric.schema:
        assert metric.total_records[s] == 0, "Total records must be 0"

    # Test null_count
    for s in metric.schema:
        for k in metric.null_count[s].keys():
            assert isinstance(metric.null_count[s][k], int)
            assert metric.null_count[s][k] >= 0, "Null count cannot be negative"
    
    metric._null_ratio()

    # Test null_ratio
    for s in metric.schema:
        for k in metric.null_ratio[s].keys():
            assert isinstance(metric.null_ratio[s][k], float)
            assert metric.null_ratio[s][k] >= 0 and metric.null_ratio[s][k] <= 1 , "Null ratio must be between 0 and 1"

