import glob
import pandas as pd
from datetime import datetime


def pretty_text(text):
    text = text.replace('<', '')
    text = text.replace('>', '')
    text = text.replace('-', ' ')
    text = text.title()
    return text


# data_path = 'file:///home/s2118947/full_result/'
data_path = '../../final_loc/'

df_month = pd.DataFrame(columns=['language', 'subject', 'popularity_measure',
                                 'year', 'month', 'post_type_id', 'country',
                                 'value'])
df_year = pd.DataFrame(columns=['language', 'subject', 'popularity_measure',
                                'year', 'post_type_id', 'country', 'value'])

subject_list = glob.glob(data_path + '*')
for sub in subject_list:
    year_data_list = glob.glob(sub + '/*_year_with_loc')
    month_data_list = glob.glob(sub + '/*_month_with_loc')

    for file in month_data_list:
        csv_file = glob.glob(file + '/*.csv')[0]
        csv_df = pd.read_csv(csv_file, names=['year', 'month', 'post_type_id',
                                              'country', 'value'])
        csv_df['language'] = pretty_text(file.split('/')[-1].split('_')[0])
        csv_df['subject'] = pretty_text(sub.split('/')[-1])
        csv_df['popularity_measure'] = pretty_text(
            file.split('/')[-1].split('_')[1])

        df_month = df_month.append(
            csv_df, ignore_index=True, sort=False)

    for file in year_data_list:
        csv_file = glob.glob(file + '/*.csv')[0]
        csv_df = pd.read_csv(csv_file, names=['year', 'post_type_id',
                                              'country', 'value'])
        csv_df['language'] = pretty_text(file.split('/')[-1].split('_')[0])
        csv_df['subject'] = pretty_text(sub.split('/')[-1])
        csv_df['popularity_measure'] = pretty_text(
            file.split('/')[-1].split('_')[1])

        df_year = df_year.append(
            csv_df, ignore_index=True, sort=False)

df_month['date'] = df_month.apply(lambda row: datetime(year=row['year'],
                                                       month=row['month'],
                                                       day=1),
                                  axis=1)
df_year['date'] = df_year.apply(lambda row: datetime(year=row['year'],
                                                     month=1,
                                                     day=1),
                                axis=1)

# sort data on date, with other columns too
df_month = df_month.sort_values(['subject', 'language',
                                 'popularity_measure', 'date'])
df_year = df_year.sort_values(['subject', 'language',
                               'popularity_measure', 'date'])

# drop november and december of last year
df_month = df_month.drop(df_month[df_month['date'] == datetime(year=2018,
                                                               month=12,
                                                               day=1)].index)
df_month = df_month.drop(df_month[df_month['date'] == datetime(year=2018,
                                                               month=11,
                                                               day=1)].index)
df_month.to_csv('resultant_loc.csv', index=False)
