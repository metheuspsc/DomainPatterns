import pandas as pd
from functions import clean_domain_df


@pytest.fixture
def domain():
    return Domain(r'.\Domains\Golden Files\org.txt')


def test_clean_domain_df(domain):
    cleaned_domain = pd.read_csv(r'.\Domains\Golden Files\Cleaned_org.txt')
    assert clean_domain_df(domain) == clean_domain_df
