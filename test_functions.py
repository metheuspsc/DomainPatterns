import pytest
import pandas as pd
from functions import Domain, Pattern
from pandas.testing import assert_frame_equal


@pytest.fixture
def domain():
    return Domain(r'.\Domains\Golden Files\org.txt')


@pytest.fixture
def pattern():
    return Pattern(r'.\Patterns\LLLL.csv')


def test_clean_domain_df(domain):

    domain.clean_domain_df()

    golden_file = pd.read_csv(
        r'.\Domains\Golden Files\Cleaned_gold_org.txt', usecols=[0], dtype='category')

    output = golden_file = pd.read_csv(
        r'.\Domains\Golden Files\Cleaned_org.txt', usecols=[0], dtype='category')

    assert_frame_equal(output,
                       golden_file, check_column_type=False)


def test_pattern_valid(pattern):
    assert pattern.path_isvalid() == True
