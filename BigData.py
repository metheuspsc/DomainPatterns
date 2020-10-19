import os
import logging
import json
from functions import *

logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M',
                    level=logging.DEBUG,
                    handlers=[logging.FileHandler('BigData.log'),
                              logging.StreamHandler()])

org_file = r'.\Domains\Org\org.txt'
net_file = r'.\Domains\Net\net.txt'
com_file = r'.\Domains\Com\com.txt'

domain_list = [org_file]

pattern_folder = r'.\Patterns'

if __name__ == "__main__":

    for domain_path in domain_list:

        domain = Domain(domain_path)

        domain.df = domain.clean_df()

        for pattern in os.listdir(pattern_folder):

            pattern = Pattern(os.path.join(pattern_folder, pattern))

            if pattern.path_isvalid():

                pattern_df = get_non_matches(
                    domain, pattern)

                create_json(domain, pattern)

        domain.merge()
