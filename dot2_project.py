#Since I cannot find data on each variants hospitalization and deaths of Washington State by day, week, or month,
#I will retrieve data on the number of confirmed cases of each variants, and get the weekly hospitalized and death counts in Washington States
import pandas as pd
import slate3k as sl
from re import search
from dateutil.parser import parse
import requests as rq

def process_pdf():
    """
    This function extract, process and clean a pdf file on covid variants in Washington State
    :return: a dataframe with data on Covid variants, their lineages, the first date and last date each variant are detected in Washington State
    """
    file_name = 'WASequencingAndVariantsReport.pdf'

    #create an empty dataframe for later
    df = pd.DataFrame()

    #extract and process the pdf file
    with open(file_name, "rb") as fileObject:
        pdfDoc = sl.PDF(fileObject)

        # create a list to store data rows
        data_list = []

        # loop through the pages in pdfDoc
        for page in pdfDoc:
            # create a list of data for each page
            pageList = page.split("\n")
            # append each page to the data list
            data_list.append(pageList)

        # I only need data in page 4 to know when the variants started emerging and last time in was recorded
        # Therefore, I only retrieve data on page 4
        data = data_list[3]

        # loop through the list to remove all extra blank character in each item and adjust some data in the list
        for i in range(len(data)):
            # Special cases: These variants has their names separated
            # merge their names into 1 data cell
            # the logic is based on the result of the data list
            if data[i].strip() == 'Delta':
                data[i] = data[i] + data[i + 1]
                # update the data list
                for j in range(i + 1, len(data) - 2, 1):
                    data[j] = data[j + 1]
            if data[i].strip() == 'Epsilon (B.1.427 /':
                data[i] = data[i] + data[i + 2]
                # update the data list
                for j in range(i + 1, len(data) - 2, 1):
                    data[j] = data[j + 2]
            data[i] = data[i].strip()

            data[len(data) - 1] = ''
            data[len(data) - 2] = ''

        # create a empty dict to create a dataframe later
        data_dict = {}

        # create empty lists to store values of each field for the dataframe
        variant = []
        first_occur = []
        last_detect = []

        # the following logic is based on the result of the list data

        # there are 2 tables that I need data on this page
        # the first table only has one data row
        startIdx = 34  # Starting index in the list 'data' for the cell in the first table
        endIdx = 61  # Ending index for the the list 'data' for the cell in the first table

        # append data of the first table to corresponding list based on the result of the list 'data'
        first_occur.append(data[endIdx - 2])
        last_detect.append(data[endIdx])

        # loop through the data in page of interest to retrieve data on variant names
        # the last 10 items in the lists are the notes, so I exclude them
        while startIdx < len(data) - 10:
            if data[startIdx].find('(') != -1:
                variant.append(data[startIdx])
            startIdx += 1

        # append data of the second table to corresponding list based on the result of the list 'data'
        # loop through the data in a range to retrieve data on the first occurences of variants on the second table
        for i in range(139, 168, 4):
            first_occur.append(data[i])
        # loop through the data in a range to retrieve data on the last detected case of variants on the second table
        for i in range(141, 170, 4):
            last_detect.append(data[i])

        # create empty lists to store the variants and their lineage
        lineage = []
        variant_type = []
        for var in variant:
            variant_type.append(var.split('(')[0].strip())
            lineage.append('(' + var.split('(')[1])

        # Header for fields of interest
        headers = ['Variant', 'Lineage', 'first_detection', 'last_detection']

        # add the header and corresponding list to the dictionary
        data_dict[headers[0]] = variant_type
        data_dict[headers[1]] = lineage
        data_dict[headers[2]] = first_occur
        data_dict[headers[3]] = last_detect

        df1 = pd.DataFrame(data_dict)

        # sort the variant to match other dataframe
        df1 = df1.sort_values('Variant')

        # change the first detection and last detection date to datetime format in Python
        df1['first_detection'] = df1['first_detection'].apply(lambda x: parse(x).date())
        df1['last_detection'] = df1['last_detection'].apply(lambda x: parse(x).date())

        # Transpose the dataframe to merge data later
        df1 = df1.T

        # get rid of the extra header because of transposing
        new_header = df1.iloc[0]  # grab the first row for the header
        df1 = df1[1:]  # take the data less the header row
        df1.columns = new_header  # set the header row as the df header

        # Add column which show the source
        df1['ds_source_for_lineage_first_and_last_detection'] = 'Washington State Department of Health'

        #Assign the processed dataframe to the iinitial dataframe
        df =df1
    return df

def process_excel():
    """
    This function extract, process and clean a excel file about each covid variant in Washington State
    Since the excel file is updated bi-weekly on data of two previous weeks, I can only retrieve data until Nov 13
    (start date at Nov 6) at the point of doing the project (Dec 9-10)
    :return: a dataframe with data on the confirmed cases of each variant in Washington State from 03/01/2020 to 11/13/2021
    """

    filename = 'SARS-CoV-2 Sequencing Data.xlsx'
    data = pd.read_excel(filename, 'Weekly data')

    # Fill the missing data in the Variant columns
    data['Variant'].fillna('Other variants', inplace=True)

    # For this dataset to be coherent with other datasets
    # If the start date is on or later than Dec 25, the year would belong to the end date, so I subtract the year by 1 in this case
    # Otherwise,get only the start date of the week by concatenating the weekly start date with the year
    data['Week'] = data['Week'].apply(lambda x: x.split('-')[0] + ' ' + str(int(x.split('-')[1].split(',')[1]) - 1)
    if x.split('-')[0].split(' ')[0] == 'Dec' and int(x.split('-')[0].split(' ')[1]) >= 25
    else x.split('-')[0] + x.split('-')[1].split(',')[1])

    # change the start date to datetime format in Python
    data['Week'] = data['Week'].apply(lambda x: parse(x).date())

    data = data.sort_values('Week') #sort data by week

    data = data[data['Week'] > pd.to_datetime('2020-02-29').date()]  #get only data from March 2020


    # rename the columns
    data.rename(columns={'Week': 'Weekly start date'}, inplace=True)

    #restructure the dataframe to merge with other dataframes later
    data = data.pivot(index='Weekly start date', columns='Variant', values='Sequence Count')

    # Add column which show the source
    data['ds_source_for_cases_of_each_variant'] = 'Washington State Department of Health'
    #Fill missing values with 0 because there was no record of the variant on the data with missing data
    data.fillna(0, inplace = True)

    return data

def process_csv_1():
    """
    This function extract, process and clean a csv file on all Covid variants
    :return: a dataframe with data on 7-day average hospitalizations of all Covid variants before 7/19/2020
    """

    content = pd.read_csv('COVID-19_Reported_Patient_Impact_and_Hospital_Capacity_by_State_Timeseries.csv')

    # only get the data on Washing State
    data = content[content['state'] == 'WA']

    # create a datafrane that only contains data on date and daily hospitalized cases
    df = data[['date', 'total_adult_patients_hospitalized_confirmed_covid',
               'total_pediatric_patients_hospitalized_confirmed_covid']]
    df.reset_index(drop=True, inplace=True)
    df = df.sort_values('date')

    # this dataset has missing data for the number of hospitalized patients before 7/14/2020
    # Another csv file has the daily data from 04/04/2020 to 03/07/2021, which I will merge later

    # drop the rows with missing data
    df.dropna(axis=0, how='any', inplace=True)

    # hospitalization data is relative since some people might be released from hospital while others are hospitalized
    # I can only find daily data, not weekly data of hospitalized patients ,so I will calculate the mean daily hospitalization data within a week
    df['daily_hospitalization'] = df['total_adult_patients_hospitalized_confirmed_covid'] + df[
        'total_pediatric_patients_hospitalized_confirmed_covid']

    df.reset_index(inplace=True, drop =True)

    # drop the unnecessary columns
    df.drop(columns=['total_adult_patients_hospitalized_confirmed_covid',
                     'total_pediatric_patients_hospitalized_confirmed_covid'], inplace=True)

    # since the first day of the record is Wednesday, and my start day of the week is Sunday, I will drop the first 4 records, whose data is included in the other csv file
    df = df.drop(df.index[:4])

    # set all the dates in weeks to the date of the starting day of that week
    # set all the dates whose index ranging from i to i+6 (using iloc[i:i+7] with i+7 exclusive) in a loop to the date at index i
    for i in range(0, len(df), 7):
        df.iloc[i:i + 7, 0] = df.iloc[i, 0]

    # aggregate the dataset by the start date of the week
    # the hospitalization data is relative since some people are released while other are hospitalized
    # I cannot find the data that has the actual numbers people hospitalized weekly, so I will calculate the mean of people hospitalize in the 7-day period
    df = round(df.groupby('date').mean())


    # Add column which show the source
    df['ds_source_for_7_day_avarage_hospitalization_all_variants'] = 'U.S. Department of Health & Human Services'

    df.reset_index(inplace=True)
    # rename the columns to be at weekly level
    df.rename(columns={'date': 'Weekly start date','daily_hospitalization': '7-day avarage hospitalization (All covid variants)'}, inplace=True)

    # change the weekly start date to datetime format in Python
    df['Weekly start date'] = df['Weekly start date'].apply(lambda x: parse(x).date())
    df.reset_index(inplace=True, drop=True)
    return df

def process_csv_2():
    """
     This function extract, process and clean a csv file on all Covid variants
     I cannot find hospitalization data before 04/04/2020 as those data are missing in all of the datasets I can find

     :return: a dataframe with data on 7-day average hospitalizations of all Covid variants from 04/05/2020 to 02/18/2020
     """

    data = pd.read_csv('washington-history.csv')

    # create a datafrane that only contains data on date and daily hospitalized cases
    df = data[['date', 'hospitalizedCurrently']]
    df.reset_index(drop=True, inplace=True)
    df = df.sort_values('date')

    # This csv file has the daily data from 04/04/2020 to 03/07/2021
    # Later, I will merge this dataset with another dataset that has dataframe for the number of hospitalized patients after 7/18/2020

    # drop the rows with missing data
    df.dropna(axis=0, how='any', inplace=True)

    # get only data before 7/19/2020 since the other dataset contains data after this date
    df = df[df['date'] < '2020-07-19']

    # since 04/04/2020 - the first date that has data on daily hospitalization- is Saturday, and my start day of the week is Sunday,
    # I need to drop this row as there are not enough data to aggregate the week ending on 04/04/2020
    df = df.drop(df.index[0])

    # set all the dates in weeks to the date of the starting day of that week
    # set all the dates whose index ranging from i to i+6 (using iloc[i:i+7] with i+7 exclusive) in a loop to the date at index i
    for i in range(0, len(df), 7):
        df.iloc[i:i + 7, 0] = df.iloc[i, 0]

    # aggregate the dataset by the start date of the week
    # the hospitalization data is relative since some people are released while other are hospitalized
    # I cannot find the data that has the actual numbers people hospitalized weekly, so I will calculate the mean of people hospitalize in the 7-day period
    df = round(df.groupby('date').mean())

    df.reset_index(inplace=True)

    # rename the columns to be at weekly level
    df.rename(columns={'date': 'Weekly start date',
                       'hospitalizedCurrently': '7-day avarage hospitalization (All covid variants)'}, inplace=True)

    # Add column which show the source
    df['ds_source_for_7_day_avarage_hospitalization_all_variants'] = 'The COVID Tracking Project'
    df.reset_index(inplace=True, drop = True)
    return df

def process_API():
    """
     This function extract, process and clean data retrieved from MongoDB API
     :return: a dataframe with data on weekly deaths of all Covid variants from 03/01/2020 to 09/12/2021
     """

    # base URL for the API
    base_url = 'https://webhooks.mongodb-stitch.com/api/client/v2.0/app/covid-19-qppza/service/REST-API/incoming_webhook/us_only'

    # the minimum and maximum date to retrieve data
    min_date = '2020-03-01T00:00:00.000Z'
    max_date = '2021-12-09T00:00:00.000Z'

    # this variable store fields to be omitted in the json outcome
    field_to_hide = '_id,country,country_code,country_iso2,country_iso3,loc,combined_name,fips,county,uid,population,state,confirmed,deaths'

    # query the data
    query = f'min_date={min_date}&max_date={max_date}&state=Washington&hide_fields={field_to_hide}'

    data = rq.get(f'{base_url}?{query}').json()
    # print(data)

    # create lists to store the attributes that would create a dataframe later
    date = []
    confirmed_daily = []
    deaths_daily = []

    for i in range(len(data)):
        date.append(data[i]['date'])
        confirmed_daily.append(data[i]['confirmed_daily'])
        deaths_daily.append(data[i]['deaths_daily'])

    data_dict = {'date': date,
                 'confirmed_daily': confirmed_daily,
                 'deaths_daily': deaths_daily}
    df = pd.DataFrame(data_dict)

    df = df.groupby('date').sum()
    df.reset_index(inplace=True)

    # remove time and get only the date in the date column of the dataframe
    df['date'] = df['date'].apply(lambda x: parse(x).date())

    # set all the dates in weeks to the date of the starting day of that week
    # set all the dates whose index ranging from i to i+6 (using iloc[i:i+7] with i+7 exclusive) in a loop to the date at index i
    for i in range(0, len(df), 7):
        df.iloc[i:i + 7, 0] = df.iloc[i, 0]

    # aggregate the dataset by the start date of the week
    df = df.groupby('date').sum()  # get the sum of each day because this dataset contains data on every county, not the whole state

    df.reset_index(inplace=True)

    # rename the columns to be at weekly level
    df.rename(columns={'date': 'Weekly start date', 'confirmed_daily': 'Weekly confirmed case(All covid variants)',
                       'deaths_daily': 'Weekly deaths (All covid variants)'}, inplace=True)

    # Add column which show the source
    df['ds_source_for_all_variants_weekly_deaths'] = 'MongoDB'

    return df

def main():
    """
    this function calls each of the functions in a sequential manner and reports its progress to the console
    this function also process the merged dataset and create a csv file
    :return: a csv file merged from 5 datasets
    """
    df1 = process_pdf()
    print('Extracted, processed, cleaned the pdf file \'WASequencingAndVariantsReport.pdf\'!')

    df2 = process_excel()
    print('Extracted, processed, cleaned the excel file \'SARS-CoV-2 Sequencing Data.xlsx\'!')

    df3 = process_csv_1()
    print('Extracted, processed, cleaned the csv file \'COVID-19_Reported_Patient_Impact_and_Hospital_Capacity_by_State_Timeseries.csv\'!')

    df4 = process_csv_2()
    print('Extracted, processed, cleaned the csv file \'washington-history.csv\'!')

    df5 = process_API()
    print('Extracted, processed, cleaned from the MongoDB API!')

    #merge 2 cleaned dataframes generated from 2 csv files
    print('First, merge 2 cleaned dataframes generated from 2 csv files and return a dataframe with data on 7-day average hospitalizations')
    df_csv = pd.concat([df4, df3], axis=0)
    print('Done merging 2 csv files!')

    print('Then merge 2 cleaned dataframes generated from the two csv files and the MongoDB API and return a dataframe '
          'with data on weekly COVID cases counts and death counts')
    df_general = pd.merge(df_csv,df5, how = 'right', on = 'Weekly start date')
    df_general.to_csv('general_covid.csv')
    print('Done merging 2 csv files with data from the MongoDB API!')

    print('Then merge data extracted from the pdf file and data from the excel file and return a dataframe with data on '
          'the confirmed cases of each variant and additional data including each variant\'s lineage, the first detected '
          'case and last detected case' )
    df = pd.concat([df1,df2], axis = 0)
    df.reset_index(inplace = True)
    df.rename(columns = {'index':'Weekly start date'}, inplace = True)
    print('Done merging data from pdf file and data from excel file')

    print('Finally, merge all file together and process the merged dataset')
    df = pd.merge(df,df_general,how = 'outer', on = 'Weekly start date')

    for col in list(df.columns):
        #since the excel file is updated bi-weekly, I can only retrieve data until Nov 13 (start date at Nov 6) at the point of doing the project (Dec 9-10)
        #leave all the missing numeric values empty as there is no feasible data to fill in those missing values
        if col not in list(df2.columns)[:len(df2.columns)-1] and 'All covid variants' not in col:
            #fill the missing data sources and other  with N/A because the data for that attribute in that column is missing
            df[col].fillna('N/A', inplace = True)
    #reorder the columns
    new_columns = ['Weekly start date', 'Alpha', 'Beta', 'Delta', 'Epsilon', 'Eta',
       'Gamma', 'Iota', 'Kappa', 'Mu', 'Other variants',
       'Zeta','7-day avarage hospitalization (All covid variants)',
       'Weekly confirmed case(All covid variants)',
       'Weekly deaths (All covid variants)','ds_source_for_lineage_first_and_last_detection',
        'ds_source_for_cases_of_each_variant','ds_source_for_7_day_avarage_hospitalization_all_variants',
       'ds_source_for_all_variants_weekly_deaths']
    df = df.reindex(columns=new_columns)

    df.to_csv('WA_COVID_variants.csv',index = False)
    #print(df.columns)
    print('Complete. Created the \'WA_COVID_variants.csv\' file for Washington State!!!')


if __name__ == '__main__':
    main()

