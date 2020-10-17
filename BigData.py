import os
import logging
import json
import pandas as pd

# These are the Domain File Paths

org_file = r'.\Domains\Org\org.txt'
net_file = r'.\Domains\Net\net.txt'
com_file = r'.\Domains\Com\com.txt'

# This is a list with the Domain Files to be processed, if you want to process less than the three of them, just erase one or more from the list.

domain_list = [org_file]

# This is the folder holding the 12 Patters CSV files.

pattern_folder = r'.\Patterns'

# This is the lenght of the strings filtered on the Domain file since our largest pattern has 5 characthers the length is 5 + .org.(Or net or com) so it adds up to 10
length = 10


def chunk_processing(chunk):

    # Processes chunks of the Domain file so as to reduce memory usage

    chunk.drop_duplicates(inplace=True)

    # Erases lines longer than the length we want
    chunk = chunk[chunk.str.len() <= length]

    # Erases the extension of the domain(.com.;.net.;.org.)
    chunk = chunk.str[:-5]

    return chunk


def clean_domain_df(domain_file, domain_file_basename, domain_folder):

    # This function cleans the domain file, saving a CSV and returning a dataframe.

    domain_df = pd.DataFrame()

    for chunk in pd.read_csv(domain_file, sep='\t', usecols=[0], dtype='category', chunksize=200000, squeeze=True):

        chunk = chunk_processing(chunk)

        domain_df = pd.concat([domain_df, chunk], ignore_index=True, axis=0)

    domain_df.drop_duplicates(inplace=True)

    # Saves results to CSV
    domain_df.to_csv(f'{domain_folder}\Cleaned_{domain_file_basename}',
                     index=False)

    return domain_df


def get_non_matches(domain_df, pattern_file):

    # This Function returns a Dataframes with all unmatched elements in the Pattern file

    pattern_df = pd.DataFrame()

    # Reads the Pattern file in chunks, and them get non matches against the domain file
    for chunk in pd.read_csv(pattern_file, header=None, usecols=[0], dtype='category', chunksize=200000):

        chunk.iloc[:, 0] = chunk.iloc[:, 0].str.lower()

        chunk = chunk.merge(domain_df, how='left', indicator=True)

        chunk = chunk[chunk['_merge'] == 'left_only']

        chunk.drop('_merge', axis=1, inplace=True)

        pattern_df = pd.concat([pattern_df, chunk], ignore_index=True, axis=0)

    return pattern_df


def merge_json(domain_file, domain_folder, domain_file_basename):

    # Merges all json files on the Domain Folder

    json_path = os.path.join(
        domain_folder, domain_file_basename[:-4] + '.json')

    if os.path.exists(json_path):

        os.remove(json_path)

    for f in os.listdir(domain_folder):

        if f.endswith('.json') and f != os.path.basename(json_path):

            with open(os.path.join(domain_folder, f), 'r') as infile,\
                    open(json_path, 'a') as outfile:

                logging.debug(f'Writing {f} into merged json')

                for line in infile:

                    try:

                        result = json.loads(line)
                        json.dump(result, outfile)

                    except:

                        pass

                infile.close()

                os.remove(os.path.join(domain_folder, f))


def valid_pattern_file(pattern_file):
    return pattern_file.endswith('.csv')


if __name__ == "__main__":

    # Creates log file and shows logs on console as well
    logging.basicConfig(format='%(asctime)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M',
                        level=logging.DEBUG,
                        handlers=[logging.FileHandler('BigData.log'),
                                  logging.StreamHandler()])

    # Iterates through all files on the Domain File list
    for domain_file in domain_list:

        domain_file_basename = os.path.basename(domain_file)
        domain_folder = os.path.dirname(domain_file)

        logging.debug(f'Cleaning {domain_file_basename} ...')

        domain_df = clean_domain_df(
            domain_file, domain_file_basename, domain_folder)

        logging.debug(f'{domain_file_basename} Cleaned!!!')

        # Lists all files on the Pattern Folder and iterates through them
        for pattern_file in os.listdir(pattern_folder):

            # Checks is file is a CSV
            if valid_pattern_file(pattern_file):

                pattern = os.path.basename(pattern_file)[:-4]

                logging.debug(f'Looking for non matches in {pattern} file...')

                pattern_df = get_non_matches(
                    domain_df, os.path.join(pattern_folder, pattern_file))

                pattern_df.columns = [f'{pattern}']

                logging.debug(f'Matching for {pattern} file finished')

                with open(f'{domain_folder}\{pattern_file[:-4]}.json', 'w') as f:
                    json.dump(pattern_df.to_dict(orient='list'), f)

            else:

                logging.debug(f'File {pattern_file} is not a CSV!!')

        merge_json(domain_file)
