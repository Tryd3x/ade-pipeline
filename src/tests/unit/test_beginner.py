import pytest

# Define the function and the test
def add(num):
    sum = 0
    for i in num:
        sum+=i
    return sum

def multiply(num):
    mul = 1
    for i in num:
        mul*=i
    return mul

@pytest.fixture(autouse=True)
def setup():
    print("Executing test case")

@pytest.mark.parametrize(
        "input,expected",
        [
            pytest.param([1,1],2, id="Two sum"),
            pytest.param([1,2,3],6, id="Three sum"),
            pytest.param([10,-10],0, id="Mixed Integer sum"),
        ]
)
def test_add(input, expected):
    assert add(input) == expected

@pytest.mark.parametrize(
        "input,expected",
        [
            ([1,1],1),
            ([1,2,3],6),
            ([10,-10],-100),
        ],
        ids=[
            "Two Multiply",
            "Three Multiply",
            "Mixed Integer Multiply",
        ]
)

def test_multiply(input, expected):
    assert multiply(input) == expected