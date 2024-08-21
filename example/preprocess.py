import os
import sys
import logging
import pandas as pd
import re
from spellpy import spell

logging.basicConfig(level=logging.WARNING,
                    format='[%(asctime)s][%(levelname)s]: %(message)s')
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))

def deeplog_df_transfer(df, event_id_map):
    # Convert 'datetime' column to datetime type
    df['datetime'] = pd.to_datetime(df['datetime'], format='%d/%b/%Y:%H:%M:%S %z')
    
    # Map the 'Request' column to event IDs
    df['EventId'] = df['Request'].apply(lambda r: event_id_map.get(r, -1))
    
    # Keep only relevant columns
    df = df[['datetime', 'EventId']]
    
    # Resample the data to 1-minute intervals and apply custom resampling
    deeplog_df = df.set_index('datetime').resample('1min').apply(_custom_resampler).reset_index()
    return deeplog_df



def _custom_resampler(array_like):
    return list(array_like)


def deeplog_file_generator(filename, df):
    with open(filename, 'w') as f:
        for event_id_list in df['EventId']:
            for event_id in event_id_list:
                f.write(str(event_id) + ' ')
            f.write('\n')


if __name__ == '__main__':
    ##########
    # Parser #
    ##########
    input_dir = './data/OpenStack/'
    output_dir = './Result/'
    log_format = '<IP> - - [<Date>:<Time> <Timezone>] "<Request>" <Status> <Size> "<Referrer>" "<UserAgent>"'
    log_main = 'web_server'
    tau = 0.5

    parser = spell.LogParser(
        indir=input_dir,
        outdir=output_dir,
        log_format=log_format,
        logmain=log_main,
        tau=tau,
    )

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for log_name in ['acessnorm1.log', 'acessnorm2.log', 'oacessabnorm.log']:
        parser.parse(log_name)

    ##################
    # Transformation #
    ##################
    df = process_log_file(f'{output_dir}/acessnormal1.log')
    df_normal = process_log_file(f'{output_dir}/acessnorm2.log')
    df_abnormal = process_log_file(f'{output_dir}/acessabnorm.log')

    event_id_map = dict()
    for i, event_id in enumerate(pd.concat([df, df_normal, df_abnormal])['Request'].unique(), 1):
        event_id_map[event_id] = i

    logger.info(f'length of event_id_map: {len(event_id_map)}')

    #########
    # Train #
    #########
    deeplog_train = deeplog_df_transfer(df, event_id_map)
    deeplog_file_generator('train', deeplog_train)

    ###############
    # Test Normal #
    ###############
    deeplog_test_normal = deeplog_df_transfer(df_normal, event_id_map)
    deeplog_file_generator('test_normal', deeplog_test_normal)

    #################
    # Test Abnormal #
    #################
    deeplog_test_abnormal = deeplog_df_transfer(df_abnormal, event_id_map)
    deeplog_file_generator('test_abnormal', deeplog_test_abnormal)
