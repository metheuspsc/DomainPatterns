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

length = 10

if __name__ == "__main__":

    for domain_file_path in domain_list:

        domain_folder, domain_file_basename = break_domain_path(
            domain_file_path)

        domain_df = clean_domain_df(
            domain_file_path, domain_file_basename, domain_folder)

        for pattern_file in os.listdir(pattern_folder):

            if pattern_file_isvalid(pattern_file):

                pattern_df = get_non_matches(
                    domain_df, os.path.join(pattern_folder, pattern_file))

                create_json(domain_folder, pattern_file, pattern_df)

        merge_json(domain_folder, domain_file_basename)
