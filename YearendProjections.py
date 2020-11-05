import sys

import pandas as pd

import datetime

from YearendAnalysis import analysis_controller

pd.set_option('display.max_columns', None)

pd.set_option('display.max_columns', None)

def CountProvidersYearend(providers_list):
    def helper(ctrl_list,year,output):

        #print([ctrl.start_year for ctrl in ctrl_list])

        ctrl_list = list(filter(lambda ctrl: ctrl.start_year <= year,ctrl_list))

        data_list = [ctrl.read_data(ctrl.read_from_dir + "/{} {}.xls".format(year, ctrl.provider),
                                    sheets={
                                        "Yearend": None
                                    },
                                    converters={
                                        'Specialty Id': str,
                                        'Res1Sp Id': str,
                                        'Res2Sp Id': str,
                                        'Res3Sp Id': str,
                                        'School Id': str,
                                        'City Pop': int
                                    }
                                    )[0]['Yearend'] for ctrl in ctrl_list]



        for ctrl,data in zip(ctrl_list,data_list):

            output.loc[year, ctrl.provider] = data.shape[0]

        return output
    ctrl_list = [analysis_controller(
        start_year=None,
        end_year=None,
        provider=prov,
        validate='False',
        balance='False',
        summarize='False',
        adhoc='False',
        meded='False',
        project='False',
        index_name='Hcp Id'
    ) for prov in providers_list]

    [ctrl.check_directory() for ctrl in ctrl_list]

    output = pd.DataFrame(columns=[ctrl.provider for ctrl in ctrl_list],
                          index=range(min([ctrl.start_year for ctrl in ctrl_list]), ctrl_list[0].end_year + 1))

    # cities = ctrl_list[0].reference_data_extract('City Population')

    # cities = cities['Cities'][['City']]

    for year in range(min([ctrl.start_year for ctrl in ctrl_list]), ctrl_list[0].end_year + 1):
        output = helper(ctrl_list, year, output)

    ctrl_list[0].output_results(
        custom_data=output,
        custom=True,
        custom_name='Count of {}'.format(' '.join(providers_list))
    )

def ProviderAttritionAnalysis(prov):
    ctrl = analysis_controller(
        start_year=1977,
        end_year=2019,
        provider=prov,
        validate='False',
        balance='False',
        summarize='False',
        adhoc='False',
        meded='False',
        project='False',
        index_name=None
    )

    ctrl.check_directory()

    output = pd.DataFrame()

    for y in range(ctrl.start_year,ctrl.end_year + 1):

        data = ctrl.read_data(
            "{} {}.xls".format(y,prov),
            sheets={"DEL": None},
            converters={
                "Role Abbr": str,
            })[0]['DEL']

        dt = data.groupby('Status Id')[['Hcp Id']].count().T

        dt['Year'] = y

        output = output.append(dt)

    output = output.set_index('Year').fillna(int(0)).astype('int64')

    print(output)
    # output = nmw[[
    #     'First Name',
    #     'Middle Name',
    #     'Last Name',
    #     'Name Suffix',
    #     'Degree Id',
    #     'Email',
    #     'Worksite Name',
    #     'Address1',
    #     'Address2',
    #     'City',
    #     'State',
    #     'County Name',
    #     'Zip',
    #     'Phone',
    #     'Fax'
    # ]]

    ctrl.output_results(
        custom_data=output,
        custom=True,
        custom_name='{}.{}.{} Yearend Attrition'.format(datetime.datetime.now().month,
                                                       datetime.datetime.now().day,
                                                       datetime.datetime.now().year)
    )
    # print(output.columns)

def Communities_with_Pa_Apn_Phy_Time_Series(providers_list):

    def helper(ctrl_list,year,output):

        #print([ctrl.start_year for ctrl in ctrl_list])

        ctrl_list = list(filter(lambda ctrl: ctrl.start_year <= year,ctrl_list))

        data_list = [ctrl.read_data(ctrl.read_from_dir + "/{} {}.xls".format(year, ctrl.provider),
                                    sheets={
                                        "Yearend": None
                                    },
                                    converters={
                                        'Specialty Id': str,
                                        'Res1Sp Id': str,
                                        'Res2Sp Id': str,
                                        'Res3Sp Id': str,
                                        'School Id': str,
                                        'City Pop': int
                                    }
                                    )[0]['Yearend'] for ctrl in ctrl_list]

        prov_counts = [data.groupby('City')['City'].count() for data in data_list]

        for ctrl,data in zip(ctrl_list,prov_counts):

            output.loc[year, ctrl.provider] = data.shape[0]

        return output

    ctrl_list = [analysis_controller(
        start_year=None,
        end_year=None,
        provider=prov,
        validate='False',
        balance='False',
        summarize='False',
        adhoc='False',
        meded='False',
        project='False',
        index_name='Hcp Id'
    ) for prov in providers_list]

    [ctrl.check_directory() for ctrl in ctrl_list]

    output = pd.DataFrame(columns=[ctrl.provider for ctrl in ctrl_list],
                          index=range(min([ctrl.start_year for ctrl in ctrl_list]), ctrl_list[0].end_year + 1))

    for year in range(min([ctrl.start_year for ctrl in ctrl_list]), ctrl_list[0].end_year + 1):
        output = helper(ctrl_list, year, output)

    ctrl_list[0].output_results(
        custom_data=output,
        custom=True,
        custom_name='Communities with providers {}'.format(' '.join(providers_list))
    )

def Family_medicine_pc_minus_admin_hosp(end_year):
    output_tb = pd.DataFrame(
        columns=['000', '000 Admin Hosp Only', '000 without Admin Hosp',
                 '001-006', '001-006 Admin Hosp Only', '001-006 without Admin Hosp',
                 '010w/FMRes', '010w/FMRes Admin Hosp Only', '010w/FMRes without Admin Hosp',
                 '100', '100 Admin Hosp Only', '100 without Admin Hosp',
                 '400', '400 Admin Hosp Only', '400 without Admin Hosp',
                 ],
        index=[str(y) for y in range(end_year - 10, end_year + 1)]
    )

    phy_ctrl = analysis_controller(
        start_year=2009,
        end_year=2019,
        provider='PHY',
        validate='False',
        balance='False',
        summarize='False',
        adhoc='False',
        meded='False',
        project='True',
        index_name='Hcp Id'
    )

    phy_ctrl.check_directory()

    for year in range(end_year - 10, end_year + 1):
        '''Get Data'''
        data, success = phy_ctrl.read_data(
            "{} PHY.xls".format(year),
            sheets={"Yearend": None},
            converters={
                'Specialty Id': str,
                'Res1Sp Id': str,
                'Res2Sp Id': str,
                'Res3Sp Id': str,
                'School Id': str,
                'City Pop': int,
                'Act Id': str
            }
        )

        phy_ctrl.build_cubic_space(data, load_default_queries=True)

        '''withdraw results'''

        row = [("Yearend", "-Sp=000"),
               ("Yearend", "-Sp=000 -Activity=1,3,13,14,15,16"),
               ("Yearend", "-Sp=000 -Activity!=1,3,13,14,15,16"),

               ("Yearend", "-Sp=001-006"),
               ("Yearend", "-Sp=001-006 -Activity=1,3,13,14,15,16"),
               ("Yearend", "-Sp=001-006 -Activity!=1,3,13,14,15,16"),

               ("Yearend", "-Sp=010 -Res=000"),
               ("Yearend", "-Sp=010 -Res=000 -Activity=1,3,13,14,15,16"),
               ("Yearend", "-Sp=010 -Res=000 -Activity!=1,3,13,14,15,16"),

               ("Yearend", '-Sp=100'),
               ("Yearend", "-Sp=100 -Activity=1,3,13,14,15,16"),
               ("Yearend", "-Sp=100 -Activity!=1,3,13,14,15,16"),

               ("Yearend", '-Sp=400'),
               ("Yearend", "-Sp=400 -Activity=1,3,13,14,15,16"),
               ("Yearend", "-Sp=400 -Activity!=1,3,13,14,15,16")
               ]

        row = [r for r in map(phy_ctrl.get_result, row)]

        output_tb.loc[str(year), :] = row

    phy_ctrl.output_results(custom_data=output_tb, custom=True,
                            custom_name='Family Medicine PC Minus Admin Hosp {}'.format(end_year))

def Deletes_Unknown_Status_Id():
    phy_ctrl = analysis_controller(
        start_year=1977,
        end_year=2019,
        provider='PHY',
        validate='False',
        balance='False',
        summarize='False',
        adhoc='False',
        meded='False',
        project='False',
        index_name='Hcp Id'
    )

    pa_ctrl = analysis_controller(
        start_year=1995,
        end_year=2019,
        provider='PA',
        validate='False',
        balance='False',
        summarize='False',
        adhoc='False',
        meded='False',
        project='False',
        index_name='Hcp Id'
    )

    phy_ctrl.environment_check()
    pa_ctrl.environment_check()
    phy_ctrl.check_directory()
    pa_ctrl.check_directory()

    phy_output = pd.DataFrame(columns=['Year', 'Hcp Id', 'First Name', 'Last Name', 'Status Id'])
    pa_output = pd.DataFrame(columns=['Year', 'Hcp Id', 'First Name', 'Last Name', 'Status Id'])

    for y in range(phy_ctrl.start_year, phy_ctrl.end_year + 1):
        phy_data, success = phy_ctrl.read_data(
            "{} {}.xls".format(y, phy_ctrl.provider),
            sheets={"DEL": None},
            converters={"Hcp Id": str,
                        "Status Id": str},
            header=0
        )

        phy_data = phy_data['DEL']

        phy_data['Year'] = y

        phy_output = phy_output.append(phy_data.loc[(phy_data['Status Id'].str.lower() == 'un')][
                                           ['Year', 'Hcp Id', 'First Name', 'Last Name', 'Status Id']])

    for y in range(pa_ctrl.start_year, pa_ctrl.end_year + 1):
        pa_data, success = pa_ctrl.read_data(
            "{} {}.xls".format(y, pa_ctrl.provider),
            sheets={"DEL": None},
            converters={"Hcp Id": str,
                        "Status Id": str},
            header=0
        )

        pa_data = pa_data['DEL']

        pa_data['Year'] = y

        pa_output = pa_output.append(pa_data.loc[(pa_data['Status Id'].str.lower() == 'un')][
                                         ['Year', 'Hcp Id', 'First Name', 'Last Name', 'Status Id']])

    phy_ctrl.output_results(custom_name='PHY Deletes with Unknown Status.xlsx',
                            custom_data=phy_output,
                            custom=True)

    pa_ctrl.output_results(custom_name='PA Deletes with Unknown Status.xlsx',
                           custom_data=pa_output,
                           custom=True)

    return

def Provider_Community_Summary_Master(mode=None, write_file=True):
    '''
    :param mode: The type of report. if set to None, will run on normal mode. If set to "In State", population and counts will be adjusted by combining clustered in state cities. If set to "Out State", population and counts will be adjusted by combining clustered cities including nearby out of state cities.
    :param write_file: If True, output data to outfile. Otherwise, just return dataframe of results.
    :return: Returns a dataframe of results.
    '''

    def grouping_helper(ctrl, city_comb_tb=None, city_pop_tb=None):
        data = ctrl.read_data(
            "{} {}.xls".format(2019, ctrl.provider),
            sheets={"Yearend": None},
            converters={
                'Specialty Id': str,
                'Res1Sp Id': str,
                'Res2Sp Id': str,
                'Res3Sp Id': str,
                'School Id': str,
                'City Pop': int,
                'Act Id': str
            })[0]['Yearend']

        ## get count of provider by community

        # print(comb_rl)
        # print(city_pop_tb.loc[(city_pop_tb['City'] == 'Des Moines')])

        city_pop_tb['{} Count'.format(ctrl.provider)] = 0

        '''Get the counts of the provider grouped by city '''
        data_group_by_city = data.groupby('City')['Hcp Id'].count().reset_index()

        '''check that hpinv cities can all be found in census data'''

        # print("*****************" , data_group_by_city.loc[(~data_group_by_city['City'].str.lower().isin(city_pop_tb['City'].str.lower())),'City'])

        exclude = []

        if city_comb_tb is not None:

            for city, metro in zip(city_comb_tb.loc[:, 'City'], city_comb_tb.loc[:, 'Nearest Metropolis']):

                provider_count = data_group_by_city.loc[
                    (data_group_by_city['City'].str.strip().str.lower() == city.strip().lower()), 'Hcp Id']

                if provider_count.shape[0] == 1:
                    provider_count = provider_count.item()

                    city_pop_tb.loc[
                        (city_pop_tb['City'].str.lower().str.lower() == metro.strip().lower()), '{} Count'.format(
                            ctrl.provider)] += provider_count

                exclude.append(metro.strip().lower())

        for _, row in data_group_by_city.iterrows():

            if row['City'].strip().lower() not in exclude:
                city_pop_tb.loc[
                    (city_pop_tb['City'].str.lower().str.strip() == row['City'].lower().strip()), '{} Count'.format(
                        ctrl.provider)] += row['Hcp Id']

        return city_pop_tb

    ctrl_list = [analysis_controller(
        start_year=1999,
        end_year=2019,
        provider=x,
        validate='False',
        balance='False',
        summarize='False',
        adhoc='False',
        meded='False',
        project='False',
        index_name='Hcp Id'
    ) for x in ['PHY', 'PA', 'APN', 'PHA', 'DDS']]  # ,'PA','APN','PHA','DDS'

    ''' check environments'''

    [ctrl.environment_check() for ctrl in ctrl_list]

    '''check directories'''

    [ctrl.check_directory() for ctrl in ctrl_list]

    '''get reference data'''

    city_pop_tb = ctrl_list[0].reference_data_extract("City Population")["Cities"].loc[:, ['City', 'Census', 'State']]

    '''combine populations'''

    if mode != None:

        city_comb_tb = ctrl_list[0].reference_data_extract("City Combinations")

        if mode == 'In State':
            city_comb_tb = city_comb_tb['In State Combination']

            city_pop_tb = city_pop_tb.loc[
                (city_pop_tb['State'] == 'IA')
            ]
        elif mode == 'Out State':
            city_comb_tb = city_comb_tb['Out State Combination']

        city_pop_tb['Combined Population'] = 0

        # print(city_pop_tb)
        for _, row in city_comb_tb.iterrows():
            ''' Increment combined city populations'''
            city_pop_tb.loc[
                (city_pop_tb['City'].str.lower() == row['Nearest Metropolis'].lower()), 'Combined Population'] += \
                city_pop_tb.loc[(city_pop_tb['City'].str.lower() == row['City'].lower()), 'Census'].item()

        city_pop_tb = city_pop_tb.loc[~(
                (city_pop_tb['City'].str.lower().str.strip().isin(city_comb_tb['City'].str.lower().str.strip()))
                &
                ~(city_pop_tb['City'].str.lower().str.strip().isin(
                    city_comb_tb['Nearest Metropolis'].str.lower().str.strip()))
        )]

        # print(city_pop_tb.loc[(city_pop_tb['City'] == "Waterloo")])

    else:
        city_comb_tb = None
        city_pop_tb['Combined Population'] = 0
    for prov_ctrl in ctrl_list:
        city_pop_tb = grouping_helper(prov_ctrl, city_comb_tb=city_comb_tb, city_pop_tb=city_pop_tb)

    output = city_pop_tb.loc[
        (city_pop_tb[['{} Count'.format(x.provider) for x in ctrl_list]] != [0, 0, 0, 0, 0]).any(1)]

    if write_file:
        ctrl_list[0].output_results(
            custom_data=output,
            custom=True,
            custom_name='Health Professionals by Community Population Master Sheet mode={}'.format(mode)
        )

    return output

def Provider_Community_Summary_By_Pop_Bins(bins=[(0, 5000), (5000, 15000), (15000, 50000), (50000, sys.maxsize)],
                                           mode=None):
    '''
    Counts how many of each provider falls into each bin defined by the user.

    The bins must be in the following format:

    bins = [(0,5000),(5000,15000),(15000,50000),(50000,sys.maxsize)]

    where sys.maxsize is the maximum integer upper bound the system can handle
    :param bins:
    :param mode:
    :return:
    '''
    tb = Provider_Community_Summary_Master(mode=mode, write_file=False)
    '''
    tb = Provider_Community_Summary_Master(mode=mode, write_file=False)

    #print(tb)

    new_tb = pd.DataFrame(columns=tb.columns)

    #print(new_tb)

    '''

    new_tb = pd.DataFrame(columns=tb.columns,
                          index=[str(x)[1:-1].replace(",", "-<") for x in bins] + ["Total"]).iloc[:, 4:]

    for bin in bins:
        bin_min = bin[0]
        bin_max = bin[1]

        index_name = str(bin)[1:-1].replace(",", "-<")

        bin_sum = tb.loc[(
                (tb['Census'].astype('int64') >= bin_min)
                &
                (tb['Census'].astype('int64') < bin_max)
        )].sum(0).transpose()

        new_tb.loc[index_name, :] = bin_sum[4:]

    new_tb.loc['Total', :] = new_tb.sum(0)

    new_tb['Bin Total'] = new_tb.sum(1)

    ctrl = analysis_controller(
        start_year=2009,
        end_year=2019,
        provider='PHY',
        validate='False',
        balance='False',
        summarize='False',
        adhoc='False',
        meded='False',
        project='True',
        index_name='Hcp Id'
    )

    ctrl.check_directory()

    for prov in new_tb.columns:
        new_tb[prov.replace(" Count", "") + " %"] = new_tb[[prov]] / new_tb[prov]['Total']

    output = new_tb[['PHY Count', 'PHY %',
                     'APN Count', 'APN %',
                     'PA Count', 'PA %',
                     'PHA Count', 'PHA %',
                     'DDS Count', 'DDS %',
                     'Bin Total', 'Bin Total %'
                     ]]

    output.rename(index={'50000-< {}'.format(sys.maxsize): '50000 +'}, inplace=True)

    ctrl.output_results(
        custom_data=output,
        custom=True,
        custom_name='Provider Community Summary By Pop Bins mode={}'.format(mode)
    )

def familyMedicineMasterSummary():
    """
    Group by + count: gender, age, rural/urban, ft/pt, certified/no

    count: ui educated, ui affiliated, retired yes/no, deliveries?

    :return:
    """

    def helper(df, col_key):
        try:
            return df[col_key]
        except KeyError:
            return 0



    phy_ctrl = analysis_controller(
        start_year=1977,
        end_year=2020,
        provider='PHY',
        validate='False',
        balance='False',
        summarize='False',
        adhoc='False',
        meded='False',
        project='False',
        index_name='Hcp Id'
    )

    phy_ctrl.environment_check()
    phy_ctrl.check_directory()

    deliveries = phy_ctrl.reference_data_extract('Phy Delivery Data')['data'][['Hcp Id', 'Delivery Count', 'Year']]
    urban_rural = phy_ctrl.reference_data_extract('County by Regions')['County by Regions']
    affiliated = phy_ctrl.reference_data_extract('UI Afilliated Worksite Id', is_file=False)

    default = ['Count 000 + 005',
     'Count 000',
     'Count 000 Admin', 'Count 000 Hospitalists',
     'Count 001',
     'Count 001 Admin', 'Count 001 Hospitalists',
     'Count 002',
     'Count 002 Admin', 'Count 002 Hospitalists',
     'Count 003',
     'Count 003 Admin', 'Count 003 Hospitalists',
     'Count 004',
     'Count 004 Admin', 'Count 004 Hospitalists',
     'Count 005',
     'Count 005 Admin', 'Count 005 Hospitalists',
     'Count 006',
     'Count 006 Admin', 'Count 006 Hospitalists',
     'Count 009',
     'Count 009 Admin', 'Count 009 Hospitalists',
     'Count 010',
     'Count 010 Admin', 'Count 010 Hospitalists',
     'Count 010 w FM',
     'Count 010 w FM Admin', 'Count 010 w FM Hospitalists',
     'Count 100',
     'Count 100 Admin', 'Count 100 Hospitalists',
     'Count 400',
     'Count 400 Admin', 'Count 400 Hospitalists',
     'Count 200',
     'Count 200 Admin', 'Count 200 Hospitalists',

     'Count All PHY Delivering',
     'Count 000 delivering',
     'Count 200 delivering',

     'Count 000 Male',
     'Count 000 Female',
     'Avg 000 Age',
     'Count 000 55 +',
     '% of 000 55 +',
     'Count All PHY Urban',
     'Count All PHY Rural',
     'Count 000 PHY Urban',
     'Count 000 PHY Rural',
     'Count All PHY FT',
     'Count All PHY PT',
     'Count All PHY FT',
     'Count All PHY PT',
     'Count 000 Certified',
     'Count All PHY UI Affiliated',
     'Count All PHY UI Educated',
     'Count 000 PHY UI Affiliated',
     'Count 000 PHY UI Educated',
     'Count All PHY Retired',
     'Count 000 PHY Retired',
     'Count All Entering PHY',
     'Count 000 Entering PHY',
     'Count All Leaving PHY',
     'Count 000 Leaving PHY'
     ]

    output = pd.DataFrame(columns=default + [
         'Count 000 005 UI Med School Only (sub Afil)'
        ,'Count 000 005 Med Sch + Afil Res',
         'Count 000 005 Med Sch + Afil Fel',
         'Count 000 005 Med Sch + Afil Res & Fel',
         'Count 000 005 Afil Res and Fel',
         'Count 000 005 Afil Fel Only',
         'Count 000 005 Afil Res only',

         'Count 000 005 UI Med School Only (sub UI)',
         'Count 000 005 Med Sch + UI Res',
         'Conut 000 005 Med Sch + UI Fel',
         'Count 000 005 Med Sch + UI Res & Fel',
         'Count 000 005 UI Res and Fel',
         'Count 000 005 UI Fel',
         'Count 000 005 UI Res',

         'Count 000 005 010wFM UI Med School Only (sub Afil)',
         'Count 000 005 010wFM Med Sch + Afil Res',
         'Count 000 005 010wFM Med Sch + Afil Fel',
         'Count 000 005 010wFM Med Sch + Afil Res & Fel',
         'Count 000 005 010wFM Afil Res and Fel',
         'Count 000 005 010wFM Afil Fel Only',
         'Count 000 005 010wFM Afil Res only',

         'Count 000 005 010wFM UI Med School Only (sub UI)',
         'Count 000 005 010wFM Med Sch + UI Res',
         'Conut 000 005 010wFM Med Sch + UI Fel',
         'Count 000 005 010wFM Med Sch + UI Res & Fel',
         'Count 000 005 010wFM UI Res and Fel',
         'Count 000 005 010wFM UI Fel',
         'Count 000 005 010wFM UI Res'

    ])

    fm = ['000', '001', '002', '003', '004', '005', '006', '009', '010']

    for y in range(phy_ctrl.start_year, phy_ctrl.end_year+1):
        data = phy_ctrl.read_data(
            "{} {}.xls".format(y, phy_ctrl.provider),
            sheets={"Yearend": None,
                    "ADD": None,
                    "DEL": None},
            converters={
                'Hcp Id': str,
                'Specialty Id': str,
                'Res1Site Id':str,
                'Res2Site Id':str,
                'Res3Site Id':str,
                'Fel1Site Id':str,
                'Fel2Site Id':str,
                'Fel3Site Id':str,
                'Res1Sp Id': str,
                'Res2Sp Id': str,
                'Res3Sp Id': str,
                'School Id': str,
                'City Pop': int,
                'Act Id': str,
                'Speclty1Cert': int,
                'Speclty1Recert': int,
                'School Id': str,
                'Age': int,
                'Act Id': int
            })[0]

        yearend, add, delete = data['Yearend'], data['ADD'], data['DEL']

        yearend = pd.merge(
            left=yearend,
            right=urban_rural,
            left_on=['County Name'],
            right_on=['County'],
            how='left'
        )

        ## count fm specialties
        counts_of_fm_spec = yearend.loc[(yearend['Specialty Id'].isin(fm))].groupby('Specialty Id')['Hcp Id'].count().T

        ## count em specialties and em with family medicine training
        count_of_sp010 = yearend.loc[(yearend['Specialty Id'] == '010')]['Hcp Id'].count()

        count_of_sp100 = yearend.loc[(yearend['Specialty Id'] == '100')]['Hcp Id'].count()

        count_of_sp400 = yearend.loc[(yearend['Specialty Id'] == '400')]['Hcp Id'].count()

        count_of_sp200 = yearend.loc[(yearend['Specialty Id'] == '200')]['Hcp Id'].count()

        count_of_sp010_with_fm = yearend.loc[(
                (yearend['Specialty Id'] == '010')
                &
                ((yearend['Res1Sp Id'] == '000')
                 |
                 (yearend['Res2Sp Id'] == '000')
                 |
                 (yearend['Res3Sp Id'] == '000')
                 )
        )]['Hcp Id'].count()

        ## hosp admin splits
        count_000_hosp = yearend.loc[(
                (yearend['Specialty Id'].isin(['000', '005']))
                &
                (
                        (yearend['Act Id'] == 14)
                        |
                        (yearend['Act Id'] == 15)
                )
        )]['Hcp Id'].count()

        count_000_admin = yearend.loc[(
                (yearend['Specialty Id'].isin(['000', '005']))
                &
                (yearend['Act Id'] == 1)
        )]['Hcp Id'].count()

        count_001_hosp = yearend.loc[(
                (yearend['Specialty Id'] == '001')
                &
                (
                        (yearend['Act Id'] == 14)
                        |
                        (yearend['Act Id'] == 15)
                )
        )]['Hcp Id'].count()
        count_001_admin = yearend.loc[(
                (yearend['Specialty Id'] == '001')
                &
                (yearend['Act Id'] == 1)
        )]['Hcp Id'].count()

        "---"
        count_002_hosp = yearend.loc[(
                (yearend['Specialty Id'] == '002')
                &
                (
                        (yearend['Act Id'] == 14)
                        |
                        (yearend['Act Id'] == 15)
                )
        )]['Hcp Id'].count()
        count_002_admin = yearend.loc[(
                (yearend['Specialty Id'] == '002')
                &
                (yearend['Act Id'] == 1)
        )]['Hcp Id'].count()

        count_003_hosp = yearend.loc[(
                (yearend['Specialty Id'] == '003')
                &
                (
                        (yearend['Act Id'] == 14)
                        |
                        (yearend['Act Id'] == 15)
                )
        )]['Hcp Id'].count()
        count_003_admin = yearend.loc[(
                (yearend['Specialty Id'] == '003')
                &
                (yearend['Act Id'] == 1)
        )]['Hcp Id'].count()

        count_004_hosp = yearend.loc[(
                (yearend['Specialty Id'] == '004')
                &
                (
                        (yearend['Act Id'] == 14)
                        |
                        (yearend['Act Id'] == 15)
                )
        )]['Hcp Id'].count()
        count_004_admin = yearend.loc[(
                (yearend['Specialty Id'] == '004')
                &
                (yearend['Act Id'] == 1)
        )]['Hcp Id'].count()

        count_005_hosp = yearend.loc[(
                (yearend['Specialty Id'] == '005')
                &
                (
                        (yearend['Act Id'] == 14)
                        |
                        (yearend['Act Id'] == 15)
                )
        )]['Hcp Id'].count()
        count_005_admin = yearend.loc[(
                (yearend['Specialty Id'] == '005')
                &
                (yearend['Act Id'] == 1)
        )]['Hcp Id'].count()

        count_006_hosp = yearend.loc[(
                (yearend['Specialty Id'] == '006')
                &
                (
                        (yearend['Act Id'] == 14)
                        |
                        (yearend['Act Id'] == 15)
                )
        )]['Hcp Id'].count()
        count_006_admin = yearend.loc[(
                (yearend['Specialty Id'] == '006')
                &
                (yearend['Act Id'] == 1)
        )]['Hcp Id'].count()

        count_009_hosp = yearend.loc[(
                (yearend['Specialty Id'] == '006')
                &
                (
                        (yearend['Act Id'] == 14)
                        |
                        (yearend['Act Id'] == 15)
                )
        )]['Hcp Id'].count()
        count_009_admin = yearend.loc[(
                (yearend['Specialty Id'] == '006')
                &
                (yearend['Act Id'] == 1)
        )]['Hcp Id'].count()

        count_010_hosp = yearend.loc[(
                (yearend['Specialty Id'] == '010')
                &
                (
                        (yearend['Act Id'] == 14)
                        |
                        (yearend['Act Id'] == 15)
                )
        )]['Hcp Id'].count()

        count_010_admin = yearend.loc[(
                (yearend['Specialty Id'] == '010')
                &
                (yearend['Act Id'] == 1)
        )]['Hcp Id'].count()

        count_010_w_fm_hosp = yearend.loc[(
                (yearend['Specialty Id'] == '010')
                &
                (
                        (yearend['Res1Sp Id'] == '000')
                        |
                        (yearend['Res2Sp Id'] == '000')
                        |
                        (yearend['Res3Sp Id'] == '000')
                )
                &
                (
                        (yearend['Act Id'] == 14)
                        |
                        (yearend['Act Id'] == 15)
                )
        )]['Hcp Id'].count()

        count_010_w_fm_admin = yearend.loc[(
                (yearend['Specialty Id'] == '010')
                &
                (
                        (yearend['Res1Sp Id'] == '000')
                        |
                        (yearend['Res2Sp Id'] == '000')
                        |
                        (yearend['Res3Sp Id'] == '000')
                )
                &
                (yearend['Act Id'] == 1)
        )]['Hcp Id'].count()

        count_100_hosp = yearend.loc[(
                (yearend['Specialty Id'] == '100')
                &
                (
                        (yearend['Act Id'] == 14)
                        |
                        (yearend['Act Id'] == 15)
                )
        )]['Hcp Id'].count()

        count_100_admin = yearend.loc[(
                (yearend['Specialty Id'] == '100')
                &
                (yearend['Act Id'] == 1)
        )]['Hcp Id'].count()

        count_400_hosp = yearend.loc[(
                (yearend['Specialty Id'] == '400')
                &
                (
                        (yearend['Act Id'] == 14)
                        |
                        (yearend['Act Id'] == 15)
                )
        )]['Hcp Id'].count()

        count_400_admin = yearend.loc[(
                (yearend['Specialty Id'] == '400')
                &
                (yearend['Act Id'] == 1)
        )]['Hcp Id'].count()

        count_200_hosp = yearend.loc[(
                (yearend['Specialty Id'] == '200')
                &
                (
                        (yearend['Act Id'] == 14)
                        |
                        (yearend['Act Id'] == 15)
                )
        )]['Hcp Id'].count()

        count_200_admin = yearend.loc[(
                (yearend['Specialty Id'] == '200')
                &
                (yearend['Act Id'] == 1)
        )]['Hcp Id'].count()

        ## Total Deliver
        deliveries = deliveries.rename(columns={'Hcp Id':'Hcp'})
        cols_to_use = yearend.columns.difference(deliveries.columns)

        #print(cols_to_use)

        hcp_delivering = pd.merge(
            left=yearend[cols_to_use],
            left_on='Hcp Id',
            right=deliveries.loc[(deliveries['Year'] == y)],
            right_on='Hcp',
            how='left'
        )

        #print(hcp_delivering.columns)

        hcp_delivering = hcp_delivering.loc[(hcp_delivering['Delivery Count'].notnull())]

        count_all_phy_delivering = hcp_delivering['Hcp Id'].count()

        count_gen_fm_delivering = hcp_delivering.loc[(hcp_delivering['Specialty Id'].isin(['000', '005']))][
            'Hcp Id'].count()
        count_200_delivering = hcp_delivering.loc[(hcp_delivering['Specialty Id'] == '200')]['Hcp Id'].count()

        ## count of each gender in fm

        counts_fm_gender = yearend.loc[(yearend['Specialty Id'].isin(['000', '005']))].groupby('Gender')[
            'Hcp Id'].count().T

        ## average age and % over 55

        yearend['Age'].replace('', 0, inplace=True)

        avg_fm_age = yearend.loc[(yearend['Specialty Id'].isin(['000', '005']))]['Age'].mean()

        number_over_55 = yearend.loc[(
                (yearend['Specialty Id'].isin(['000', '005']))
                &
                (yearend['Age'].astype('int64') >= 55)
        )]['Hcp Id'].count()

        percent_over_55 = number_over_55 / (helper(counts_of_fm_spec, '000') + helper(counts_of_fm_spec, '005')) * 100

        ## urban rural
        counts_all_phy_urban_rural = yearend.groupby('Urban Rural')['Hcp Id'].count().T
        counts_gen_fm_urban_rural = yearend.loc[(yearend['Specialty Id'].isin(['000', '005']))].groupby('Urban Rural')[
            'Hcp Id'].count().T

        ## FT/PT
        counts_all_phy_fte = yearend.groupby('Fte')['Hcp Id'].count().T
        counts_gen_fm_fte = yearend.loc[(yearend['Specialty Id'].isin(['000', '005']))].groupby('Fte')[
            'Hcp Id'].count().T

        ## Certification Speclty1Cert	Speclty1Recert

        count_gen_fm_certified = yearend.loc[(
                (yearend['Specialty Id'].isin(['000', '005']))
                &
                (
                        (yearend['Speclty1Cert'] >= 2018)
                        |
                        (yearend['Speclty1Recert'] >= 2018)
                )

        )]['Hcp Id'].count()

        ## UI Affiliated

        ui_affiliated = yearend.loc[(
                (yearend['School Id'] == '1803')
                |
                (yearend['Res1Site Id'].isin(affiliated))
                |
                (yearend['Res2Site Id'].isin(affiliated))
                |
                (yearend['Res3Site Id'].isin(affiliated))
                |
                (yearend['Fel1Site Id'].isin(affiliated))
                |
                (yearend['Fel2Site Id'].isin(affiliated))
                |
                (yearend['Fel3Site Id'].isin(affiliated))
        )]

        count_ui_affiliated = ui_affiliated['Hcp Id'].count()

        count_gen_fm_ui_affiliated = ui_affiliated.loc[(ui_affiliated['Specialty Id'].isin(['000', '005']))][
            'Hcp Id'].count()

        ## Ui educated
        ui_educated = yearend.loc[(
                (yearend['School Id'] == '1803')
                |
                (yearend['Res1Site Id'] == '18010')
                |
                (yearend['Res2Site Id'] == '18010')
                |
                (yearend['Res3Site Id'] == '18010')
                |
                (yearend['Fel1Site Id'] == '18010')
                |
                (yearend['Fel2Site Id'] == '18010')
                |
                (yearend['Fel3Site Id'] == '18010')
        )]

        count_ui_educated = ui_educated['Hcp Id'].count()

        count_gen_fm_ui_educated = ui_educated.loc[(ui_educated['Specialty Id'].isin(['000', '005']))]['Hcp Id'].count()

        ## 005, 000 UI Affiliated/Educated

        raw_fam_minus_hosp = yearend.loc[(
                (yearend['Specialty Id'].isin(['000','005']))
                &
                ~(
                        (yearend['Act Id'] == 14)
                        |
                        (yearend['Act Id'] == 15)
                        |
                        (yearend['Act Id'] == 1)
                )
        )]

        count_fam_ui_med_school_sub_afil, count_fam_ui_sch_afil_res, count_fam_ui_sch_afil_fel, count_fam_ui_sch_afil_res_fel, \
        count_fam_afil_res_fel, count_fam_afil_fel, count_fam_afil_res, count_fam_ui_med_school_sub_ui, count_fam_ui_sch_ui_res, \
        count_fam_ui_sch_ui_fel, count_fam_ui_sch_ui_res_fel, count_fam_ui_res_fel, count_fam_ui_fel, count_fam_ui_res = \
            MedicalEducationBreakdown(raw_fam_minus_hosp,affiliated)

        ## 005, 000 UI Affiliated/Educated

        raw_fam_010_minus_hosp = yearend.loc[(
                (
                    (yearend['Specialty Id'].isin(['000', '005']))
                    |
                    (
                        (yearend['Specialty Id'] == '010')
                        &
                        (
                            (yearend['Res1Sp Id'] == '000')
                            |
                            (yearend['Res2Sp Id'] == '000')
                            |
                            (yearend['Res3Sp Id'] == '000')
                        )
                    )
                )
                &
                ~(
                        (yearend['Act Id'] == 14)
                        |
                        (yearend['Act Id'] == 15)
                        |
                        (yearend['Act Id'] == 1)
                )
        )]

        count_fam_010_ui_med_school_sub_afil, count_fam_010_ui_sch_afil_res, count_fam_010_ui_sch_afil_fel, count_fam_010_ui_sch_afil_res_fel, \
        count_fam_010_afil_res_fel, count_fam_010_afil_fel, count_fam_010_afil_res, count_fam_010_ui_med_school_sub_ui, count_fam_010_ui_sch_ui_res, \
        count_fam_010_ui_sch_ui_fel, count_fam_010_ui_sch_ui_res_fel, count_fam_010_ui_res_fel, count_fam_010_ui_fel, count_fam_010_ui_res = \
            MedicalEducationBreakdown(raw_fam_010_minus_hosp,affiliated)

        ## Retirement

        all_phy_rt = delete.loc[(delete['Status Id'].str.lower().str.strip() == 'rt')]

        count_all_phy_rt = all_phy_rt['Hcp Id'].count()

        count_gen_fm_rt = all_phy_rt.loc[(all_phy_rt['Specialty Id'].isin(['000', '005']))]['Hcp Id'].count()

        ## all adds all deletes

        count_all_add_phy = add['Hcp Id'].count()
        count_gen_fm_add = add.loc[(add['Specialty Id'].isin(['000', '005']))]['Hcp Id'].count()

        count_all_del_phy = delete['Hcp Id'].count()
        count_gen_fm_del = delete.loc[(delete['Specialty Id'].isin(['000', '005']))]['Hcp Id'].count()

        lst = [
            helper(counts_of_fm_spec, '000') + helper(counts_of_fm_spec, '005'),
            helper(counts_of_fm_spec, '000'),
            count_000_admin, count_000_hosp,
            helper(counts_of_fm_spec, '001'),
            count_001_admin, count_001_hosp,
            helper(counts_of_fm_spec, '002'),
            count_002_admin, count_002_hosp,
            helper(counts_of_fm_spec, '003'),
            count_003_admin, count_003_hosp,
            helper(counts_of_fm_spec, '004'),
            count_004_admin, count_004_hosp,
            helper(counts_of_fm_spec, '005'),
            count_005_admin, count_005_hosp,
            helper(counts_of_fm_spec, '006'),
            count_006_admin, count_006_hosp,
            helper(counts_of_fm_spec, '009'),
            count_009_admin, count_009_hosp,
            count_of_sp010,
            count_010_admin, count_010_hosp,
            count_of_sp010_with_fm,
            count_010_w_fm_admin, count_010_w_fm_hosp,
            count_of_sp100,
            count_100_admin, count_100_hosp,
            count_of_sp400,
            count_400_admin, count_400_hosp,
            count_of_sp200,
            count_200_admin, count_200_hosp,
            count_all_phy_delivering,
            count_gen_fm_delivering,
            count_200_delivering,
            helper(counts_fm_gender, 'M'),
            helper(counts_fm_gender, 'F'),
            avg_fm_age, number_over_55, percent_over_55,
            helper(counts_all_phy_urban_rural, 'Urban'),
            helper(counts_all_phy_urban_rural, 'Rural'),
            helper(counts_gen_fm_urban_rural, 'Urban'),
            helper(counts_gen_fm_urban_rural, 'Rural'),
            helper(counts_all_phy_fte, 'FT'),
            helper(counts_all_phy_fte, 'PT'),
            helper(counts_gen_fm_fte, 'FT'),
            helper(counts_gen_fm_fte, 'PT'),
            count_gen_fm_certified,
            count_ui_affiliated,
            count_ui_educated,
            count_gen_fm_ui_affiliated,
            count_gen_fm_ui_educated,
            count_all_phy_rt,
            count_gen_fm_rt,
            count_all_add_phy,
            count_gen_fm_add,
            count_all_del_phy,
            count_gen_fm_del,

            count_fam_ui_med_school_sub_afil,

            count_fam_ui_sch_afil_res,
            count_fam_ui_sch_afil_fel,
            count_fam_ui_sch_afil_res_fel,

            count_fam_afil_res_fel,
            count_fam_afil_fel,
            count_fam_afil_res,

            count_fam_ui_med_school_sub_ui,

            count_fam_ui_sch_ui_res,
            count_fam_ui_sch_ui_fel,
            count_fam_ui_sch_ui_res_fel,

            count_fam_ui_res_fel,
            count_fam_ui_fel,
            count_fam_ui_res,

            count_fam_010_ui_med_school_sub_afil,

            count_fam_010_ui_sch_afil_res,
            count_fam_010_ui_sch_afil_fel,
            count_fam_010_ui_sch_afil_res_fel,

            count_fam_010_afil_res_fel,
            count_fam_010_afil_fel,
            count_fam_010_afil_res,

            count_fam_010_ui_med_school_sub_ui,

            count_fam_010_ui_sch_ui_res,
            count_fam_010_ui_sch_ui_fel,
            count_fam_010_ui_sch_ui_res_fel,

            count_fam_010_ui_res_fel,
            count_fam_010_ui_fel,
            count_fam_010_ui_res

            ]

        output = output.append(pd.Series(lst, index=output.columns, name=y), ignore_index=False)

    phy_ctrl.output_results(
        custom_data=output,
        custom=True,
        custom_name="Family Medicine Master Summary"
    )

def Iowa_Health_Professionals_by_Community_Population(mode=None):
    master = Provider_Community_Summary_Master(mode=mode, write_file=False)

    print("obtained master")

    output = master[['Census', 'City', 'PHY Count',
                     'Census', 'City', 'APN Count',
                     'Census', 'City', 'PA Count',
                     'Census', 'City', 'PHA Count',
                     'Census', 'City', 'DDS Count',
                     ]]

    phy_ctrl = analysis_controller(
        start_year=1977,
        end_year=2019,
        provider='PHY',
        validate='False',
        balance='False',
        summarize='False',
        adhoc='False',
        meded='False',
        project='False',
        index_name='Hcp Id'
    )

    phy_ctrl.check_directory()

    phy_ctrl.output_results(
        custom_data=output,
        custom=True,
        custom_name='Iowa Health Professionals by Community Population mode={}'.format(mode)
    )

    print(master)

def Providers_By_Population_Main():
    Iowa_Health_Professionals_by_Community_Population(mode='Out State')
    Iowa_Health_Professionals_by_Community_Population(mode='In State')
    Iowa_Health_Professionals_by_Community_Population(mode=None)

    Provider_Community_Summary_By_Pop_Bins(mode='Out State')
    Provider_Community_Summary_By_Pop_Bins(mode='In State')
    Provider_Community_Summary_By_Pop_Bins(mode=None)

def PharmacyMedicalEducationBreakdown(mode="UI"):
    pha_ctrl = [analysis_controller(
        start_year=1996,
        end_year=2019,
        provider=x,
        validate='False',
        balance='False',
        summarize='False',
        adhoc='False',
        meded='False',
        project='False',
        index_name='Hcp Id'
    ) for x in ['PHA']][0]

    pha_ctrl.check_directory()
    pha_ctrl.environment_check()
    if mode == 'UI':
        afil = pha_ctrl.reference_data_extract('UI Afilliated PHA Residency', is_file=False)
        sch = '20'
    output = pd.DataFrame(
        columns=['UI Pharmacy School', 'Pharmacy School and Residency', 'Residency','No Affiliation', 'Summed Total', 'Actual Total'])

    for y in range(1996, 2019 + 1):
        data = pha_ctrl.read_data(pha_ctrl.read_from_dir + "/{} PHA.xls".format(y),
                                  sheets={
                                      "Yearend": None},
                                  converters={
                                      'Res1Site Id': str,
                                      'Res2Site Id': str,
                                      'Res3Site Id': str,
                                      'School Id': str,
                                  }
                                  )[0]['Yearend']

        pha_sch = data.loc[(
                data['School Id'].str.strip() == '20'
        )]

        pha_res = data.loc[(
                (data['Res1Site Id'].str.strip().isin(afil))
                |
                (data['Res2Site Id'].str.strip().isin(afil))
                |
                (data['Res3Site Id'].str.strip().isin(afil))
        )]

        pha_sch_res = pha_sch.loc[(
            pha_sch['Hcp Id'].isin(pha_res['Hcp Id'])
        )]

        res_only = pha_res.loc[(
            ~pha_res['Hcp Id'].isin(pha_sch['Hcp Id'])
        )]

        pha_sch = pha_sch.loc[(
            ~pha_sch['Hcp Id'].isin(pha_res['Hcp Id'])
        )]

        no_afil = data.loc[(
                (~data['Hcp Id'].isin(pha_sch['Hcp Id']))
                &
                (~data['Hcp Id'].isin(pha_res['Hcp Id']))
        )]

        rw = pd.Series(
            {'UI Pharmacy School': pha_sch['Hcp Id'].count()
                ,
             'Pharmacy School and Residency': pha_sch_res['Hcp Id'].count()
                ,
             'Residency': res_only['Hcp Id'].count()
                ,
             'No Affiliation': no_afil['Hcp Id'].count()
                ,
             'Summed Total':
                 pha_sch['Hcp Id'].count() + pha_sch_res['Hcp Id'].count() +
                 res_only['Hcp Id'].count() + no_afil['Hcp Id'].count()
                ,
             'Actual Total': data['Hcp Id'].count()
             },
            name=y)

        output = output.append(rw)

    pha_ctrl.output_results(
        custom_data=output, custom=True,
        custom_name='PHA Medical Education Breakdown mode={}'.format(mode)
    )

def mergeYearendDeliveries(y):
    phy_ctrl = analysis_controller(
        start_year=1977,
        end_year=2020,
        provider='PHY',
        validate='False',
        balance='False',
        summarize='False',
        adhoc='False',
        meded='False',
        project='False',
        index_name='Hcp Id'
    )

    phy_ctrl.environment_check()

    phy_ctrl.check_directory()

    deliveries = phy_ctrl.reference_data_extract('Phy Delivery Data')['data'][['Hcp Id', 'Delivery Count', 'Year']]

    deliveries = deliveries.loc[(deliveries['Year'] == y)]

    deliveries = deliveries.rename(columns={'Hcp Id': 'Hcp'})

    data = phy_ctrl.read_data(
        "{} {}.xls".format(y, phy_ctrl.provider),
        sheets={"Yearend": None},
        converters={
            'Hcp Id': str,
            'Specialty Id': str,
            'Res1Sp Id': str,
            'Res2Sp Id': str,
            'Res3Sp Id': str,
            'School Id': str,
            'City Pop': int,
            'Act Id': str,
            'Speclty1Cert': int,
            'Speclty1Recert': int,
            'School Id': str,
            'Age': int,
            'Act Id': int
        })[0]

    yearend = data['Yearend']

    try:
        yearend = yearend.drop(['Delivery', 'Year','Delivery Count'], axis=1)
    except KeyError:
        pass

    merged = pd.merge(
        left=yearend,
        left_on='Hcp Id',
        right=deliveries.loc[(deliveries['Year'] == y)],
        right_on='Hcp',
        how='left'
    )

    phy_ctrl.output_results(
        custom_data=merged, custom=True,
        custom_name='Yearend Joined with Deliveries {}'.format(y)
    )

def MedicalEducationBreakdown(data,affiliated):
        has_ui_med_school = data.loc[
            (data['School Id'] == '1803')
        ]

        has_afil_res = data.loc[
            (data['Res1Site Id'].isin(affiliated))
            |
            (data['Res2Site Id'].isin(affiliated))
            |
            (data['Res3Site Id'].isin(affiliated))
            ]

        has_afil_fel = data.loc[
            (data['Fel1Site Id'].isin(affiliated))
            |
            (data['Fel2Site Id'].isin(affiliated))
            |
            (data['Fel3Site Id'].isin(affiliated))
            ]

        count_ui_med_school_sub_afil = has_ui_med_school.loc[
            ~(
                    (has_ui_med_school['Hcp Id'].isin(has_afil_res['Hcp Id']))
                    |
                    (has_ui_med_school['Hcp Id'].isin(has_afil_fel['Hcp Id']))
            )
        ]['Hcp Id'].count()

        check = has_ui_med_school.loc[
            ~(
                    (has_ui_med_school['Hcp Id'].isin(has_afil_res['Hcp Id']))
                    |
                    (has_ui_med_school['Hcp Id'].isin(has_afil_fel['Hcp Id']))
            )
        ][['Res1Site Id', 'Res2Site Id', 'Res3Site Id', 'School Id']]

        count_ui_sch_afil_res = has_ui_med_school.loc[
            (has_ui_med_school['Hcp Id'].isin(has_afil_res['Hcp Id']))
            &
            (~has_ui_med_school['Hcp Id'].isin(has_afil_fel['Hcp Id']))
            ]['Hcp Id'].count()

        count_ui_sch_afil_fel = has_ui_med_school.loc[
            (~has_ui_med_school['Hcp Id'].isin(has_afil_res['Hcp Id']))
            &
            (has_ui_med_school['Hcp Id'].isin(has_afil_fel['Hcp Id']))
            ]['Hcp Id'].count()
        count_ui_sch_afil_res_fel = has_ui_med_school.loc[
            (has_ui_med_school['Hcp Id'].isin(has_afil_res['Hcp Id']))
            &
            (has_ui_med_school['Hcp Id'].isin(has_afil_fel['Hcp Id']))
            ]['Hcp Id'].count()

        count_afil_res_fel = has_afil_res.loc[
            (~has_afil_res['Hcp Id'].isin(has_ui_med_school['Hcp Id']))
            &
            (has_afil_res['Hcp Id'].isin(has_afil_fel['Hcp Id']))
            ]['Hcp Id'].count()

        count_afil_fel = has_afil_fel.loc[
            (~has_afil_fel['Hcp Id'].isin(has_ui_med_school['Hcp Id']))
            &
            (~has_afil_fel['Hcp Id'].isin(has_afil_res['Hcp Id']))
            ]['Hcp Id'].count()
        count_afil_res = has_afil_res.loc[
            (~has_afil_res['Hcp Id'].isin(has_ui_med_school['Hcp Id']))
            &
            (~has_afil_res['Hcp Id'].isin(has_afil_fel['Hcp Id']))
            ]['Hcp Id'].count()

        ## 005,000 Ui educated
        # has_ui_med_school = data.loc[
        #     (yearend['School Id'] == '1803')
        # ]
        has_ui_res = data.loc[
            (data['Res1Site Id'] == '18010')
            |
            (data['Res2Site Id'] == '18010')
            |
            (data['Res3Site Id'] == '18010')
            ]
        has_ui_fel = data.loc[
            (data['Fel1Site Id'] == '18010')
            |
            (data['Fel2Site Id'] == '18010')
            |
            (data['Fel3Site Id'] == '18010')
            ]

        count_ui_med_school_sub_ui = has_ui_med_school.loc[
            (~has_ui_med_school['Hcp Id'].isin(has_ui_res['Hcp Id']))
            &
            (~has_ui_med_school['Hcp Id'].isin(has_ui_fel['Hcp Id']))
            ]['Hcp Id'].count()

        count_ui_sch_ui_res = has_ui_med_school.loc[
            (has_ui_med_school['Hcp Id'].isin(has_ui_res['Hcp Id']))
            &
            (~has_ui_med_school['Hcp Id'].isin(has_ui_fel['Hcp Id']))
            ]['Hcp Id'].count()
        count_ui_sch_ui_fel = has_ui_med_school.loc[
            (~has_ui_med_school['Hcp Id'].isin(has_ui_res['Hcp Id']))
            &
            (has_ui_med_school['Hcp Id'].isin(has_ui_fel['Hcp Id']))
            ]['Hcp Id'].count()
        count_ui_sch_ui_res_fel = has_ui_med_school.loc[
            (has_ui_med_school['Hcp Id'].isin(has_ui_res['Hcp Id']))
            &
            (has_ui_med_school['Hcp Id'].isin(has_ui_fel['Hcp Id']))
            ]['Hcp Id'].count()

        count_ui_res_fel = has_ui_res.loc[
            (~has_ui_res['Hcp Id'].isin(has_ui_med_school['Hcp Id']))
            &
            (has_ui_res['Hcp Id'].isin(has_ui_fel['Hcp Id']))
            ]['Hcp Id'].count()

        count_ui_fel = has_ui_fel.loc[
            (~has_ui_fel['Hcp Id'].isin(has_ui_med_school['Hcp Id']))
            &
            (~has_ui_fel['Hcp Id'].isin(has_ui_res['Hcp Id']))
            ]['Hcp Id'].count()
        count_ui_res = has_ui_res.loc[
            (~has_ui_res['Hcp Id'].isin(has_ui_med_school['Hcp Id']))
            &
            (~has_ui_res['Hcp Id'].isin(has_ui_fel['Hcp Id']))
            ]['Hcp Id'].count()

        return count_ui_med_school_sub_afil,count_ui_sch_afil_res,count_ui_sch_afil_fel,count_ui_sch_afil_res_fel,\
               count_afil_res_fel,count_afil_fel,count_afil_res,count_ui_med_school_sub_ui,count_ui_sch_ui_res,\
               count_ui_sch_ui_fel,count_ui_sch_ui_res_fel,count_ui_res_fel,count_ui_fel,count_ui_res

def MedicalEducationinStateIowa(data):
    has_ia_med_school = data.loc[
        (data['Sch State'].str.strip().str.lower() == 'ia')
    ]

    has_ia_res = data.loc[
        (data['Res1State'].str.strip().str.lower() == 'ia')
        |
        (data['Res2State'].str.strip().str.lower() == 'ia')
        |
        (data['Res3State'].str.strip().str.lower() == 'ia')
        ]

    has_ia_fel = data.loc[
        (data['Fel1State'].str.strip().str.lower() == 'ia')
        |
        (data['Fel2State'].str.strip().str.lower() == 'ia')
        |
        (data['Fel3State'].str.strip().str.lower() == 'ia')
        ]

    count_ia_med_school_only = has_ia_med_school.loc[
        ~(
                (has_ia_med_school['Hcp Id'].isin(has_ia_res['Hcp Id']))
                |
                (has_ia_med_school['Hcp Id'].isin(has_ia_fel['Hcp Id']))
        )
    ]['Hcp Id'].count()

    count_ia_sch_ia_res = has_ia_med_school.loc[
        (has_ia_med_school['Hcp Id'].isin(has_ia_res['Hcp Id']))
        &
        (~has_ia_med_school['Hcp Id'].isin(has_ia_fel['Hcp Id']))
        ]['Hcp Id'].count()

    count_ia_sch_ia_fel = has_ia_med_school.loc[
        (~has_ia_med_school['Hcp Id'].isin(has_ia_res['Hcp Id']))
        &
        (has_ia_med_school['Hcp Id'].isin(has_ia_fel['Hcp Id']))
        ]['Hcp Id'].count()

    count_ia_sch_ia_res_fel = has_ia_med_school.loc[
        (has_ia_med_school['Hcp Id'].isin(has_ia_res['Hcp Id']))
        &
        (has_ia_med_school['Hcp Id'].isin(has_ia_fel['Hcp Id']))
        ]['Hcp Id'].count()

    count_ia_res_fel = has_ia_res.loc[
        (~has_ia_res['Hcp Id'].isin(has_ia_med_school['Hcp Id']))
        &
        (has_ia_res['Hcp Id'].isin(has_ia_fel['Hcp Id']))
        ]['Hcp Id'].count()

    count_afil_fel = has_ia_fel.loc[
        (~has_ia_fel['Hcp Id'].isin(has_ia_med_school['Hcp Id']))
        &
        (~has_ia_fel['Hcp Id'].isin(has_ia_res['Hcp Id']))
        ]['Hcp Id'].count()

    count_afil_res = has_ia_res.loc[
        (~has_ia_res['Hcp Id'].isin(has_ia_med_school['Hcp Id']))
        &
        (~has_ia_res['Hcp Id'].isin(has_ia_fel['Hcp Id']))
        ]['Hcp Id'].count()

    return count_ia_med_school_only,count_ia_sch_ia_res,count_ia_sch_ia_fel,count_ia_sch_ia_res_fel,\
           count_ia_res_fel,count_afil_fel,count_afil_res

def MedicalEducationinStateIowaUIDMUMedSchool(data):

    count_uidmu_med_school = data.loc[
        (data['School Id'].isin(['1875','1803']))
    ]

    has_ia_res = data.loc[
        (data['Res1State'].str.strip().str.lower() == 'ia')
        |
        (data['Res2State'].str.strip().str.lower() == 'ia')
        |
        (data['Res3State'].str.strip().str.lower() == 'ia')
        ]

    has_ia_fel = data.loc[
        (data['Fel1State'].str.strip().str.lower() == 'ia')
        |
        (data['Fel2State'].str.strip().str.lower() == 'ia')
        |
        (data['Fel3State'].str.strip().str.lower() == 'ia')
        ]

    count_uidmu_med_school_only = count_uidmu_med_school.loc[
        ~(
                (count_uidmu_med_school['Hcp Id'].isin(has_ia_res['Hcp Id']))
                |
                (count_uidmu_med_school['Hcp Id'].isin(has_ia_fel['Hcp Id']))
        )
    ]['Hcp Id'].count()

    count_ia_sch_ia_res = count_uidmu_med_school.loc[
        (count_uidmu_med_school['Hcp Id'].isin(has_ia_res['Hcp Id']))
        &
        (~count_uidmu_med_school['Hcp Id'].isin(has_ia_fel['Hcp Id']))
        ]['Hcp Id'].count()

    count_ia_sch_ia_fel = count_uidmu_med_school.loc[
        (~count_uidmu_med_school['Hcp Id'].isin(has_ia_res['Hcp Id']))
        &
        (count_uidmu_med_school['Hcp Id'].isin(has_ia_fel['Hcp Id']))
        ]['Hcp Id'].count()

    count_ia_sch_ia_res_fel = count_uidmu_med_school.loc[
        (count_uidmu_med_school['Hcp Id'].isin(has_ia_res['Hcp Id']))
        &
        (count_uidmu_med_school['Hcp Id'].isin(has_ia_fel['Hcp Id']))
        ]['Hcp Id'].count()

    count_ia_res_fel = has_ia_res.loc[
        (~has_ia_res['Hcp Id'].isin(count_uidmu_med_school['Hcp Id']))
        &
        (has_ia_res['Hcp Id'].isin(has_ia_fel['Hcp Id']))
        ]['Hcp Id'].count()

    count_afil_fel = has_ia_fel.loc[
        (~has_ia_fel['Hcp Id'].isin(count_uidmu_med_school['Hcp Id']))
        &
        (~has_ia_fel['Hcp Id'].isin(has_ia_res['Hcp Id']))
        ]['Hcp Id'].count()

    count_afil_res = has_ia_res.loc[
        (~has_ia_res['Hcp Id'].isin(count_uidmu_med_school['Hcp Id']))
        &
        (~has_ia_res['Hcp Id'].isin(has_ia_fel['Hcp Id']))
        ]['Hcp Id'].count()

    return count_uidmu_med_school_only, count_ia_sch_ia_res, count_ia_sch_ia_fel, count_ia_sch_ia_res_fel, \
           count_ia_res_fel, count_afil_fel, count_afil_res

def PhyMedicalEducationIAStateMain(y):
    phy_ctrl = analysis_controller(
        start_year=1977,
        end_year=2020,
        provider='PHY',
        validate='False',
        balance='False',
        summarize='False',
        adhoc='False',
        meded='False',
        project='False',
        index_name='Hcp Id'
    )



    phy_ctrl.environment_check()
    phy_ctrl.check_directory()

    deliveries = phy_ctrl.reference_data_extract('Phy Delivery Data')['data'][['Hcp Id', 'Delivery Count', 'Year']]
    urban_rural = phy_ctrl.reference_data_extract('County by Regions')['County by Regions']
    affiliated = phy_ctrl.reference_data_extract('UI Afilliated Worksite Id', is_file=False)

    data = phy_ctrl.read_data(
        "{} {}.xls".format(y, phy_ctrl.provider),
        sheets={"Yearend": None},
        converters={
            'Hcp Id': str,
            'Specialty Id': str,
            'Res1Site Id': str,
            'Res2Site Id': str,
            'Res3Site Id': str,
            'Fel1Site Id': str,
            'Fel2Site Id': str,
            'Fel3Site Id': str,
            'Res1Sp Id': str,
            'Res2Sp Id': str,
            'Res3Sp Id': str,
            'School Id': str,
            'Sch State': str,
            'City Pop': int,
            'Act Id': str,
            'Speclty1Cert': int,
            'Speclty1Recert': int,
            'School Id': str,
            'Age': int,
            'Act Id': int
        })[0]

    data = data['Yearend']

    output0 = pd.DataFrame(columns=[
         'Count UI Med School Only (sub Afil)',
         'Count Med Sch + Afil Res',
         'Count Med Sch + Afil Fel',
         'Count Med Sch + Afil Res & Fel',
         'Count Afil Res and Fel',
         'Count Afil Fel Only',
         'Count Afil Res only',

         'Count UI Med School Only (sub UI)',
         'Count Med Sch + UI Res',
         'Conut Med Sch + UI Fel',
         'Count Med Sch + UI Res & Fel',
         'Count UI Res and Fel',
         'Count UI Fel',
         'Count UI Res'

    ])

    output1 = pd.DataFrame(columns=[
        'Count IA Med School Only'
        , 'Count IA Med Sch + IA Res',
        'Count IA Med Sch + IA Fel',
        'Count IA Med Sch + IA Res & Fel',
        'Count IA Res and Fel',
        'Count IA Fel Only',
        'Count IA Res only'

    ])

    output2 = pd.DataFrame(columns=[
        'Count UI DMU Med School Only',
        'Count UI DMU Med Sch + IA Res',
        'Conut UI DMU Med Sch + IA Fel',
        'Count UI DMU Med Sch + IA Res & Fel',
        'Count UI DMU IA Res and Fel',
        'Count UI DMU IA Fel',
        'Count UI DMU IA Res'

    ])

    count_fam_ui_med_school_sub_afil, count_fam_ui_sch_afil_res, count_fam_ui_sch_afil_fel, count_fam_ui_sch_afil_res_fel, count_fam_afil_res_fel, count_fam_afil_fel, count_fam_afil_res, count_fam_ui_med_school_sub_ui, count_fam_ui_sch_ui_res, count_fam_ui_sch_ui_fel, count_fam_ui_sch_ui_res_fel, count_fam_ui_res_fel, count_fam_ui_fel, count_fam_ui_res = MedicalEducationBreakdown(data, affiliated)
    lst0 = [count_fam_ui_med_school_sub_afil, count_fam_ui_sch_afil_res, count_fam_ui_sch_afil_fel, count_fam_ui_sch_afil_res_fel, \
    count_fam_afil_res_fel, count_fam_afil_fel, count_fam_afil_res, count_fam_ui_med_school_sub_ui, count_fam_ui_sch_ui_res, \
    count_fam_ui_sch_ui_fel, count_fam_ui_sch_ui_res_fel, count_fam_ui_res_fel, count_fam_ui_fel, count_fam_ui_res]
    output0 = output0.append(pd.Series(lst0, index=output0.columns, name=y), ignore_index=False)

    count_ia_med_school_only, count_ia_sch_ia_res, count_ia_sch_ia_fel, count_ia_sch_ia_res_fel, \
    count_ia_res_fel, count_afil_fel, count_afil_res = MedicalEducationinStateIowa(data)
    lst1 = [count_ia_med_school_only, count_ia_sch_ia_res, count_ia_sch_ia_fel, count_ia_sch_ia_res_fel, \
    count_ia_res_fel, count_afil_fel, count_afil_res]
    output1 = output1.append(pd.Series(lst1, index=output1.columns, name=y), ignore_index=False)

    count_uidmu_med_school_only, count_ia_sch_ia_res, count_ia_sch_ia_fel, count_ia_sch_ia_res_fel, \
    count_ia_res_fel, count_afil_fel, count_afil_res = MedicalEducationinStateIowaUIDMUMedSchool(data)
    lst2 = [count_uidmu_med_school_only, count_ia_sch_ia_res, count_ia_sch_ia_fel, count_ia_sch_ia_res_fel, \
    count_ia_res_fel, count_afil_fel, count_afil_res]
    output2 = output2.append(pd.Series(lst2, index=output2.columns, name=y), ignore_index=False)

    phy_ctrl.output_results(
        custom_data=output0,
        custom=True,
        custom_name="Medical Education Breakdown UI and Affiliated"
    )

    phy_ctrl.output_results(
        custom_data=output1,
        custom=True,
        custom_name="Medical Education in State Iowa"
    )

    phy_ctrl.output_results(
        custom_data=output2,
        custom=True,
        custom_name="Medical Educationin State Iowa UI DMU Med School"
    )

def ruralUrbanBreakdown(provider_list,start_year,end_year):

    ctrl_lst = [analysis_controller(
        start_year=None,
        end_year=None,
        provider=prov,
        validate='False',
        balance='False',
        summarize='False',
        adhoc='False',
        meded='False',
        project='False',
        index_name=None
    ) for prov in provider_list]

    [ctrl.check_directory() for ctrl in ctrl_lst]

    output = dict(zip(provider_list,[pd.DataFrame() for prov in provider_list]))

    counties = ctrl_lst[0].reference_data_extract('County by Regions', is_file=True)['County by Regions']

    for year in range(start_year, end_year + 1):

        appending = pd.DataFrame()

        for ctrl in ctrl_lst:
            if year >= ctrl.start_year and year <= ctrl.end_year:
                data = ctrl.read_data(
                    "{} {}.xls".format(year, ctrl.provider),
                    sheets={"Yearend": None},
                    converters={
                        "Status Id": str,
                    }
                )[0]['Yearend']

                merged = pd.merge(
                    left=data,
                    left_on='County Name',
                    right=counties,
                    right_on='County',
                    how='left'
                )

                rural_urban = merged.groupby('Urban Rural')[['Hcp Id']].count().T

                rural_urban['Year'] = year

                output[ctrl.provider] = output[ctrl.provider].append(rural_urban)

    for prov,out in output.items():
        out = out.set_index('Year')[['Rural','Urban']]
        ctrl_lst[0].output_results(
            custom_data=out,
            custom=True,
            custom_name="{} Urban Rural Count {} to {}".format(prov,start_year,end_year)
        )

def PhysicianSupplyTrend(start_year,end_year,less_hosp = False):
    ctrl = analysis_controller(
        start_year=None,
        end_year=None,
        provider="PHY",
        validate='False',
        balance='False',
        summarize='False',
        adhoc='False',
        meded='False',
        project='False',
        index_name=None
    )

    ctrl.check_directory()

    output = pd.DataFrame()

    fm = ["00" + str(i) for i in range(0,7)] #[000,001,002 -- 006]

    for year in range(start_year, end_year + 1):

        if year >= ctrl.start_year and year <= ctrl.end_year:
            data = ctrl.read_data(
                "{} {}.xls".format(year, ctrl.provider),
                sheets={"Yearend": None},
                converters={
                    "Specialty Id": str,
                    "Res1Sp Id":str,
                    "Res2Sp Id":str,
                    "Res3Sp Id":str,
                    "Act Id":str
                }
            )[0]['Yearend']



            family_medicine = data.loc[
                (
                    (data['Specialty Id'].isin(fm))
                    |
                    (
                            (data['Specialty Id'] == '010')
                            &
                            (
                                (data['Res1Sp Id'].isin(fm))
                                |
                                (data['Res2Sp Id'].isin(fm))
                                |
                                (data['Res3Sp Id'].isin(fm))
                            )
                    )
                )

            ]

            internal_medicine = data.loc[
                (data['Specialty Id'] == "100")
            ]

            surgery = data.loc[
                (data['Specialty Id'] == "800")
            ]

            if less_hosp == True:
                family_medicine, internal_medicine, surgery = [frame.loc[
                                                                   (~data['Act Id'].isin(['1', '14', '15']))
                                                               ]['Hcp Id'].count() for frame in
                                                               [family_medicine, internal_medicine, surgery]]
            else:
                family_medicine, internal_medicine, surgery = [frame['Hcp Id'].count() for frame in
                                                               [family_medicine, internal_medicine, surgery]]



            output = output.append([[year,family_medicine,internal_medicine,surgery]])

    output = output.rename(columns={
                            0: "Year",
                            1: "Family Medicine",
                            2: "Internal Medicine",
                            3: "Surgery"
                            })

    ctrl.output_results(
        custom_data=output,
        custom=True,
        custom_name="{}-{} Physician Supply Trend {}.{}.{} -sub hosp = {}".format(
                                                          start_year,
                                                          end_year,
                                                          datetime.datetime.now().month,
                                                          datetime.datetime.now().day,
                                                          datetime.datetime.now().year,
                                                          less_hosp)
    )

def PhysicianIMGSupplyTrend(start_year,end_year):
    ctrl = analysis_controller(
        start_year=None,
        end_year=None,
        provider="PHY",
        validate='False',
        balance='False',
        summarize='False',
        adhoc='False',
        meded='False',
        project='False',
        index_name=None
    )

    ctrl.check_directory()

    output = pd.DataFrame()

    fm = ["00" + str(i) for i in range(0,7)]

    for year in range(start_year, end_year + 1):

        if year >= ctrl.start_year and year <= ctrl.end_year:
            data = ctrl.read_data(
                "{} {}.xls".format(year, ctrl.provider),
                sheets={"Yearend": None},
                converters={
                    "School Id":int
                }
            )[0]['Yearend']

            img = data.loc[
                (data['School Id'] >= 6000)
            ]['Hcp Id'].count()

            output = output.append([[year,img]])

    output = output.rename(columns={
                            0: "IMG Count",
                            })

    ctrl.output_results(
        custom_data=output,
        custom=True,
        custom_name="{}-{} IMG Supply Trend {}.{}.{}".format(
                                                          start_year,
                                                          end_year,
                                                          datetime.datetime.now().month,
                                                          datetime.datetime.now().day,
                                                          datetime.datetime.now().year,
                                                          )
    )

def Iowa_Community_Pharmacists(start_year,end_year):
    ctrl = analysis_controller(
        start_year=None,
        end_year=None,
        provider="PHA",
        validate='False',
        balance='False',
        summarize='False',
        adhoc='False',
        meded='False',
        project='False',
        index_name=None
    )

    ctrl.check_directory()

    output = pd.DataFrame()

    com = [1,2,3,4,5,6]

    for year in range(start_year, end_year + 1):

        if year >= ctrl.start_year and year <= ctrl.end_year:
            data = ctrl.read_data(
                "{} {}.xls".format(year, ctrl.provider),
                sheets={"Yearend": None},
                converters={
                    "Gender": str,
                    "Act Id": int
                }
            )[0]['Yearend']

            community = data.loc[data['Act Id'].isin(com)]

            by_gender = community.groupby('Gender')['Hcp Id'].count().T

            by_gender['Total'] = community['Hcp Id'].count()

            by_gender['Year'] = year

            output = output.append(by_gender)

    output = output.set_index("Year")

    ctrl.output_results(
        custom_data=output,
        custom=True,
        custom_name="{}-{} Iowa Community Pharmacists {}.{}.{}".format(
            start_year,
            end_year,
            datetime.datetime.now().month,
            datetime.datetime.now().day,
            datetime.datetime.now().year,
            )
    )

def Iowa_Independent_Pharmacists(start_year,end_year):
    ctrl = analysis_controller(
        start_year=None,
        end_year=None,
        provider="PHA",
        validate='False',
        balance='False',
        summarize='False',
        adhoc='False',
        meded='False',
        project='False',
        index_name=None
    )

    ctrl.check_directory()

    output = pd.DataFrame()



    for year in range(start_year, end_year + 1):

        if year >= ctrl.start_year and year <= ctrl.end_year:
            data = ctrl.read_data(
                "{} {}.xls".format(year, ctrl.provider),
                sheets={"Yearend": None},
                converters={
                    "Gender": str,
                    "Act Id": int
                }
            )[0]['Yearend']

            independent = data.loc[(data['Act Id'] == 1)]

            by_gender = independent.groupby('Gender')['Hcp Id'].count().T

            by_gender['Total'] = independent['Hcp Id'].count()

            by_gender['Year'] = year

            output = output.append(by_gender)

    output = output.set_index("Year")

    ctrl.output_results(
        custom_data=output,
        custom=True,
        custom_name="{}-{} Iowa Independent Pharmacists {}.{}.{}".format(
            start_year,
            end_year,
            datetime.datetime.now().month,
            datetime.datetime.now().day,
            datetime.datetime.now().year,
            )
    )

def Age_50plus_Pop_10kless_Pharmacists(start_year,end_year):
    ctrl = analysis_controller(
        start_year=None,
        end_year=None,
        provider="PHA",
        validate='False',
        balance='False',
        summarize='False',
        adhoc='False',
        meded='False',
        project='False',
        index_name=None
    )

    ctrl.check_directory()

    output = pd.DataFrame()

    for year in range(start_year, end_year + 1):

        if year >= ctrl.start_year and year <= ctrl.end_year:
            data = ctrl.read_data(
                "{} {}.xls".format(year, ctrl.provider),
                sheets={"Yearend": None},
                converters={
                    "Age": int,
                    "City Pop":int
                }
            )[0]['Yearend']

            queried = data.loc[
                (data['Age'] >= 55)
                &
                (data['City Pop'] <= 10000)
            ][['Hcp Id']].count()

            queried['Year'] = year

            queried = queried.T

            # print(queried)

            output = output.append(queried, ignore_index=True)

    output = output.set_index("Year")

    print(output)

    # ctrl.output_results(
    #     custom_data=output,
    #     custom=True,
    #     custom_name="{}-{} Iowa Independent Pharmacists {}.{}.{}".format(
    #         start_year,
    #         end_year,
    #         datetime.datetime.now().month,
    #         datetime.datetime.now().day,
    #         datetime.datetime.now().year,
    #         )
    # )

def Iowa_Pharmacist_Gender(start_year,end_year):
    ctrl = analysis_controller(
        start_year=None,
        end_year=None,
        provider="PHA",
        validate='False',
        balance='False',
        summarize='False',
        adhoc='False',
        meded='False',
        project='False',
        index_name=None
    )

    ctrl.check_directory()

    output = pd.DataFrame()

    for year in range(start_year, end_year + 1):

        if year >= ctrl.start_year and year <= ctrl.end_year:
            data = ctrl.read_data(
                "{} {}.xls".format(year, ctrl.provider),
                sheets={"Yearend": None},
                converters={
                    "Gender": str
                }
            )[0]['Yearend']

            by_gender = data.groupby('Gender')['Hcp Id'].count().T

            by_gender['Total'] = data['Hcp Id'].count()

            by_gender['Year'] = year

            output = output.append(by_gender)

    output = output.set_index("Year")

    ctrl.output_results(
        custom_data=output,
        custom=True,
        custom_name="{}-{} Iowa Pharmacists Gender {}.{}.{}".format(
            start_year,
            end_year,
            datetime.datetime.now().month,
            datetime.datetime.now().day,
            datetime.datetime.now().year,
        )
    )

def Iowa_Hospital_Pharmacist_Gender(start_year,end_year):
    ctrl = analysis_controller(
        start_year=None,
        end_year=None,
        provider="PHA",
        validate='False',
        balance='False',
        summarize='False',
        adhoc='False',
        meded='False',
        project='False',
        index_name=None
    )

    ctrl.check_directory()

    output = pd.DataFrame()

    for year in range(start_year, end_year + 1):

        if year >= ctrl.start_year and year <= ctrl.end_year:
            data = ctrl.read_data(
                "{} {}.xls".format(year, ctrl.provider),
                sheets={"Yearend": None},
                converters={
                    "Gender": str,
                    "Act Id": int
                }
            )[0]['Yearend']

            hospitalists = data.loc[(data['Act Id'] == 7)]

            by_gender = hospitalists.groupby('Gender')['Hcp Id'].count().T

            by_gender['Total'] = hospitalists['Hcp Id'].count()

            by_gender['Year'] = year

            output = output.append(by_gender)

    output = output.set_index("Year")

    ctrl.output_results(
        custom_data=output,
        custom=True,
        custom_name="{}-{} Iowa Hospital Pharmacists Gender {}.{}.{}".format(
            start_year,
            end_year,
            datetime.datetime.now().month,
            datetime.datetime.now().day,
            datetime.datetime.now().year,
        )
    )

def Communities_w_Independent_Pharmacist(start_year,end_year):
    ctrl = analysis_controller(
        start_year=None,
        end_year=None,
        provider="PHA",
        validate='False',
        balance='False',
        summarize='False',
        adhoc='False',
        meded='False',
        project='False',
        index_name=None
    )

    ctrl.check_directory()

    output = pd.DataFrame()

    for year in range(start_year, end_year + 1):

        if year >= ctrl.start_year and year <= ctrl.end_year:
            data = ctrl.read_data(
                "{} {}.xls".format(year, ctrl.provider),
                sheets={"Yearend": None},
                converters={
                    "Gender": str,
                    "Act Id": int
                }
            )[0]['Yearend']

            independent = data.loc[(data['Act Id'] == 1)]

            count_by_city = independent['City'].drop_duplicates().shape[0]

            print(count_by_city)

            output = output.append([[year,count_by_city]])

    output = output.rename(columns={
        0: "Year",
        1: "Number of Communities",
    })

    output = output.set_index("Year")

    ctrl.output_results(
        custom_data=output,
        custom=True,
        custom_name="{}-{} Count Communities With Independent Pharmacists {}.{}.{}".format(
            start_year,
            end_year,
            datetime.datetime.now().month,
            datetime.datetime.now().day,
            datetime.datetime.now().year,
        )
    )



Communities_w_Independent_Pharmacist(1996,2019)
