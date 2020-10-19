
import os
import logging
import json
import pandas as pd


class Domain:
    def __init__(self, path):
        self.path = path
        self.folder = os.path.dirname(self.path)
        self.basename = os.path.basename(self.path)
        self.json = os.path.join(domain.folder, domain.basename[:-4] + '.json')
        self.df = pd.DataFrame()

    def clean_domain_df(self):
        '''This function cleans the domain file, saving a CSV and returning a dataframe.'''

        logging.debug(f'Cleaning {self.basename} ...')

        for chunk in pd.read_csv(self.path, sep='\t', usecols=[0], dtype='category', chunksize=200000, squeeze=True):

            chunk = chunk_processing(chunk)

            self.df = pd.concat([self.df, chunk],
                                ignore_index=True, axis=0)

        self.df.drop_duplicates(inplace=True)

        self.df.to_csv(f'{self.folder}\Cleaned_{self.basename}',
                       index=False)

        logging.debug(f'{self.basename} Cleaned!!!')

        return domain_df


class Pattern:
    def __init__(self, path):
        self.path = path
        self.name = os.path.basename(self.path)[:-4]
        self.df = pd.DataFrame()

    def path_isvalid(self):
        '''Checks is file is a CSV'''
        valid = self.path.endswith('.csv')
        if not valid:
            logging.debug(f'File {self.path} is not a CSV!!')
        return valid


def chunk_processing(chunk):

    chunk.drop_duplicates(inplace=True)

    chunk = chunk[chunk.str.len() <= 10]

    chunk = chunk.str[:-5]

    return chunk


def chunk_match(chunk):

    chunk.iloc[:, 0] = chunk.iloc[:, 0].str.lower()

    chunk = chunk.merge(domain_df, how='left', indicator=True)

    chunk = chunk[chunk['_merge'] == 'left_only']

    chunk.drop('_merge', axis=1, inplace=True)

    return chunk


def create_json(domain, pattern):
    with open(f'{domain.folder}\{pattern.path[:-4]}.json', 'w') as f:
        json.dump(pattern.df.to_dict(orient='list'), f)


def merge_json(domain):
    '''Merges all json files on the Domain Folder'''

    if os.path.exists(domain.json):

        os.remove(domain.json)

    for pattern_json in os.listdir(domain.folder):

        if pattern_json.endswith('.json') and pattern_json != os.path.basename(domain.json):

            with open(os.path.join(domain.folder, pattern_json), 'r') as infile,\
                    open(domain.json, 'a') as outfile:

                logging.debug(f'Writing {pattern_json} into merged json')

                for line in infile:

                    try:

                        result = json.loads(line)
                        json.dump(result, outfile)

                    except:

                        pass

                infile.close()

                os.remove(os.path.join(domain.folder, pattern_json))


def get_non_matches(domain, pattern):
    '''This Function returns a Dataframes with all unmatched elements in the Pattern file'''

    logging.debug(f'Looking for non matches in {pattern.name} file...')

    for chunk in pd.read_csv(pattern.path, header=None, usecols=[0], dtype='category', chunksize=200000):

        chunk = chunk_match(chunk)

        pattern.df = pd.concat([pattern.df, chunk], ignore_index=True, axis=0)

    pattern.df.columns = [f'{pattern.name}']

    logging.debug(f'Matching for {pattern.name} file finished')

    return pattern.df
