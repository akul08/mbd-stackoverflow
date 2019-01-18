import glob
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.backends.backend_pdf import PdfPages

plt.rcParams["figure.figsize"] = (14, 7)
plt.rcParams.update({'font.size': 15})


def pretty_text(text):
    text = text.replace('<', '')
    text = text.replace('>', '')
    text = text.replace('-', ' ')
    text = text.title()
    if text == 'Ques':
        text = 'Questions'
    return text


# data_path = 'file:///home/s2118947/full_result/'
data_path = '../../new/'

df_month = pd.DataFrame(columns=['language', 'subject', 'popularity_measure',
                                 'year', 'month', 'value'])
df_year = pd.DataFrame(columns=['language', 'subject', 'popularity_measure',
                                'year', 'value'])

subject_list = glob.glob(data_path + '*')
for sub in subject_list:
    year_data_list = glob.glob(sub + '/*_year')
    month_data_list = glob.glob(sub + '/*_month_with_location')

    for file in month_data_list:
        csv_file = glob.glob(file + '/*.csv')[0]
        csv_df = pd.read_csv(csv_file, names=['year', 'month', 'post_type_id',
                                              'country', 'value',
                                              'popularity_measure', 'language',
                                              'subject'])
        df_month = df_month.append(csv_df, ignore_index=True, sort=False)

    for file in year_data_list:
        csv_file = glob.glob(file + '/*.csv')[0]
        csv_df = pd.read_csv(csv_file, names=['year', 'post_type_id',
                                              'country', 'value',
                                              'popularity_measure', 'language',
                                              'subject'])
        df_year = df_year.append(csv_df, ignore_index=True, sort=False)

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

group_cols = ['date', 'language', 'subject', 'popularity_measure']

# drop countries for graph and combine values for same dates and lang, subject
df_month = df_month.drop('country', axis=1)
df_month = df_month.drop('post_type_id', axis=1)

df_agg = df_month.groupby(group_cols)['value'].sum()
df_month = df_month.drop('value', axis=1)
df_month.drop_duplicates(subset=group_cols, keep='last', inplace=True)
df_month = df_month.merge(right=df_agg.to_frame(), right_index=True,
                          left_on=group_cols, how='right')

df_year = df_year.drop('country', axis=1)
df_year = df_year.drop('post_type_id', axis=1)

df_agg = df_year.groupby(group_cols)['value'].sum()
df_year = df_year.drop('value', axis=1)
df_year.drop_duplicates(subset=group_cols, keep='last', inplace=True)
df_year = df_year.merge(right=df_agg.to_frame(), right_index=True,
                        left_on=group_cols, how='right')


df_month['language'] = df_month['language'].apply(lambda x: pretty_text(x))
df_year['language'] = df_year['language'].apply(lambda x: pretty_text(x))

df_month['popularity_measure'] = df_month['popularity_measure'].apply(
    lambda x: pretty_text(x))
df_year['popularity_measure'] = df_year['popularity_measure'].apply(
    lambda x: pretty_text(x))

df_month['subject'] = df_month['subject'].apply(lambda x: pretty_text(x))
df_year['subject'] = df_year['subject'].apply(lambda x: pretty_text(x))

df_month.to_csv('resultant.csv', index=False)


def plot_pdf(output_file, output_df):
    # The PDF document
    pdf_pages = PdfPages(output_file)

    subject_list = output_df['subject'].unique()
    language_list = output_df['language'].unique()
    pop_meas_list = output_df['popularity_measure'].unique()

    for graph_subj in subject_list:
        df = output_df[output_df['subject'] == graph_subj]

        for y_axis_pop_meas in pop_meas_list:
            df_popmeas = df[df['popularity_measure'] == y_axis_pop_meas]
            fig, ax = plt.subplots()

            for line_lang in language_list:
                df_subset = df_popmeas[df_popmeas['language'] == line_lang]
                ax.plot(df_subset['date'], df_subset['value'],
                        marker='.', label=line_lang)

            plt.xlabel('Time')
            plt.ylabel(y_axis_pop_meas)
            plt.title(graph_subj)

            # changing x axis labels and ticks
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %y"))
            ax.xaxis.set_major_locator(mdates.MonthLocator([1]))
            ax.xaxis.set_minor_locator(mdates.MonthLocator([7]))
            _ = plt.xticks(rotation=45)

            plt.legend()
            plt.grid(linestyle="--", color='lightgrey')
            # plt.show()
            pdf_pages.savefig(fig, bbox_inches='tight')

    # Write the PDF document to the disk
    pdf_pages.close()


plot_pdf('output_month.pdf', df_month)
plot_pdf('output_year.pdf', df_year)
