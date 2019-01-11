import glob
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages


def pretty_text(text):
    text = text.replace('<', '')
    text = text.replace('>', '')
    text = text.replace('-', ' ')
    text = text.title()
    return text


# data_path = 'file:///home/s2118947/full_result/'
data_path = '../../full_result/'

df_month = pd.DataFrame(columns=['language', 'subject', 'popularity_measure',
                                 'year', 'month', 'value'])
df_year = pd.DataFrame(columns=['language', 'subject', 'popularity_measure',
                                'year', 'value'])

subject_list = glob.glob(data_path + '*')
for sub in subject_list:
    year_data_list = glob.glob(sub + '/*_year')
    month_data_list = glob.glob(sub + '/*_month')

    for file in month_data_list:
        csv_file = glob.glob(file + '/*.csv')[0]
        csv_df = pd.read_csv(csv_file, names=['year', 'month', 'value'])
        csv_df['language'] = pretty_text(file.split('/')[-1].split('_')[0])
        csv_df['subject'] = pretty_text(sub.split('/')[-1])
        csv_df['popularity_measure'] = pretty_text(
            file.split('/')[-1].split('_')[1])

        df_month = df_month.append(
            csv_df, ignore_index=True, sort=False)

    for file in year_data_list:
        csv_file = glob.glob(file + '/*.csv')[0]
        csv_df = pd.read_csv(csv_file, names=['year', 'value'])
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
            fig = plt.figure(dpi=100)

            for line_lang in language_list:
                df_subset = df_popmeas[df_popmeas['language'] == line_lang]
                plt.plot(df_subset['date'], df_subset['value'],
                         marker='.', label=line_lang)

            plt.xlabel('Time')
            plt.ylabel(y_axis_pop_meas)
            plt.title(graph_subj)
            plt.legend()
            # plt.show()
            plt.tight_layout()
            pdf_pages.savefig(fig)

    # Write the PDF document to the disk
    pdf_pages.close()


plot_pdf('output_month.pdf', df_month)
plot_pdf('output_year.pdf', df_year)
