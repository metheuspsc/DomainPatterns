
import os
import logging
import json
import pandas as pd


def chunk_processing(chunk):

    chunk.drop_duplicates(inplace=True)

    chunk = chunk[chunk.str.len() <= length]

    chunk = chunk.str[:-5]

    return chunk


def chunk_match(chunk):

    chunk.iloc[:, 0] = chunk.iloc[:, 0].str.lower()

    chunk = chunk.merge(domain_df, how='left', indicator=True)

    chunk = chunk[chunk['_merge'] == 'left_only']

    chunk.drop('_merge', axis=1, inplace=True)

    return chunk


def clean_domain_df(domain_path, domain_basename, domain_folder):
    '''This function cleans the domain file, saving a CSV and returning a dataframe.'''

    logging.debug(f'Cleaning {domain_basename} ...')

    domain_df = pd.DataFrame()

    for chunk in pd.read_csv(domain_path, sep='\t', usecols=[0], dtype='category', chunksize=200000, squeeze=True):

        chunk = chunk_processing(chunk)

        domain_df = pd.concat([domain_df, chunk], ignore_index=True, axis=0)

    domain_df.drop_duplicates(inplace=True)

    domain_df.to_csv(f'{domain_folder}\Cleaned_{domain_basename}',
                     index=False)

    logging.debug(f'{domain_basename} Cleaned!!!')

    return domain_df


def get_non_matches(domain_df, pattern_file):
    '''This Function returns a Dataframes with all unmatched elements in the Pattern file'''

    pattern = os.path.basename(pattern_file)[:-4]

    logging.debug(f'Looking for non matches in {pattern} file...')

    pattern_df = pd.DataFrame()

    for chunk in pd.read_csv(pattern_file, header=None, usecols=[0], dtype='category', chunksize=200000):

        chunk = chunk_match(chunk)

        pattern_df = pd.concat([pattern_df, chunk], ignore_index=True, axis=0)

    pattern_df.columns = [f'{pattern}']

    logging.debug(f'Matching for {pattern} file finished')

    return pattern_df


def create_json(domain_folder, pattern_file, pattern_df):
    with open(f'{domain_folder}\{pattern_file[:-4]}.json', 'w') as f:
        json.dump(pattern_df.to_dict(orient='list'), f)


def merge_json(domain_folder, domain_basename):
    '''Merges all json files on the Domain Folder'''
    final_json = os.path.join(
        domain_folder, domain_basename[:-4] + '.json')

    if os.path.exists(final_json):

        os.remove(final_json)

    for pattern_json in os.listdir(domain_folder):

        if pattern_json.endswith('.json') and pattern_json != os.path.basename(final_json):

            with open(os.path.join(domain_folder, pattern_json), 'r') as infile,\
                    open(final_json, 'a') as outfile:

                logging.debug(f'Writing {pattern_json} into merged json')

                for line in infile:

                    try:

                        result = json.loads(line)
                        json.dump(result, outfile)

                    except:

                        pass

                infile.close()

                os.remove(os.path.join(domain_folder, pattern_json))


def break_domain_path(domain_path):
    return os.path.dirname(domain_path), os.path.basename(domain_path)


def pattern_file_isvalid(pattern_file):
    '''Checks is file is a CSV'''
    valid = pattern_file.endswith('.csv')
    if not valid:
        logging.debug(f'File {pattern_file} is not a CSV!!')
    return valid
