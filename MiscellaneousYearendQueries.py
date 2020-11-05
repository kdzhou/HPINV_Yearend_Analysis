from YearendAnalysis import analysis_controller
import os
import pandas as pd
import datetime

def rileyPhyscianMatching():

    riley_phy_ctrl = analysis_controller(
        start_year=None,
        end_year=None,
        provider='PHY',
        validate='False',
        balance='False',
        summarize='False',
        adhoc='False',
        meded='False',
        project='False',
        index_name=None
    )

    ctrl_list = [analysis_controller(
        start_year=None,
        end_year=None,
        provider=x,
        validate='False',
        balance='False',
        summarize='False',
        adhoc='False',
        meded='False',
        project='False',
        index_name='Hcp Id'
    ) for x in ['PHY','PA','APN']]

    [ctrl.check_directory() for ctrl in ctrl_list + [riley_phy_ctrl]]

    ## read in riley

    riley_phy_ctrl.setReadDirectory(os.getcwd() + "/Input")

    ## re-annotate
    riley_tb = riley_phy_ctrl.read_data(
        "Riley_physician_01Sept2020.xlsx",
        sheets={"physican_14Aug2020": None},
        converters={
        })[0]['physican_14Aug2020']

    riley_tb['ConcatName'] = riley_tb['Last_name'].str.lower().str.strip() + riley_tb['First_name'].str.lower().str.strip()



    riley_tb.loc[(riley_tb['Credentials'].str.strip().str.lower().isin(['md', 'do'])),'Type Id'] = 'PHY'
    riley_tb.loc[(riley_tb['Credentials'].str.strip().str.lower().isin(['pa', 'pa-c', 'pac'])),'Type Id'] = 'PA'

    riley_tb.loc[~(riley_tb['Type Id'].isin(['PHY','PA'])),'Type Id'] = 'APN'

    output = pd.DataFrame()

    matched = pd.DataFrame()

    for year in range(ctrl_list[0].end_year+1,min([ctrl.start_year-1 for ctrl in ctrl_list]),-1):
        ctrl_list = list(filter(lambda ctrl: ctrl.start_year <= year, ctrl_list))
        for ctrl in ctrl_list:
            #print(ctrl.read_from_dir)
            data = ctrl.read_data(
                "{} {}.xls".format(year,ctrl.provider),
                sheets={"Yearend": None},
                converters={
                    'Specialty Id': str,
                    'Res1Sp Id': str,
                    'Res2Sp Id': str,
                    'Res3Sp Id': str,
                    'School Id': str,
                    'City Pop': int,
                    'Act Id': str,
                    'Type Id':str
                }
            )[0]['Yearend']

            data['ConcatName'] = data['Last Name'].str.lower().str.strip() + data['First Name'].str.lower().str.strip()

            merge_result = pd.merge(
                left = data,
                left_on=['ConcatName','Type Id'],
                right = riley_tb,
                right_on = ['ConcatName','Type Id']
            )

            matched = matched.append(merge_result)

            riley_tb = riley_tb.loc[~(riley_tb['ConcatName'].isin(merge_result['ConcatName']))]

    matched.drop_duplicates()

    ctrl_list[0].output_results(
        custom_data=matched,
        custom=True,
        custom_name='Riley vs HPINV Matched'
    )

    ctrl_list[0].output_results(
        custom_data=riley_tb,
        custom=True,
        custom_name='Riley Only'
    )
def nmwMailingList09032020():
    apn = analysis_controller(
        start_year=None,
        end_year=None,
        provider='APN',
        validate='False',
        balance='False',
        summarize='False',
        adhoc='False',
        meded='False',
        project='False',
        index_name=None
    )

    apn.check_directory()

    data = apn.read_data(
        "2020 APN.xls",
        sheets={"Yearend": None},
        converters={
            "Role Abbr":str,
        })[0]['Yearend']

    nmw = data.loc[(data['Role Abbr'].str.strip().str.lower() == 'nmw')]

    output = nmw[[
        'First Name',
        'Middle Name',
        'Last Name',
        'Name Suffix',
        'Degree Id',
        'Email',
        'Worksite Name',
        'Address1',
        'Address2',
        'City',
        'State',
        'County Name',
        'Zip',
        'Phone',
        'Fax'
    ]]

    apn.output_results(
        custom_data=output,
        custom=True,
        custom_name='{}.{}.{} NMW Mailing List'.format(datetime.datetime.now().month,
                                                      datetime.datetime.now().day,
                                                      datetime.datetime.now().year)
    )
    print(output.columns)
def APNIowaStart2Years(reference_year):



    ctrl_lst = [analysis_controller(
                    start_year=reference_year - 2,
                    end_year=None,
                    provider=prov,
                    validate='False',
                    balance='False',
                    summarize='False',
                    adhoc='False',
                    meded='False',
                    project='False',
                    index_name=None
                ) for prov in ['APN','PA']]

    [ctrl.check_directory() for ctrl in ctrl_lst]

    output = {
        'APN':pd.DataFrame(),
        'PA':pd.DataFrame()
    }

    for year in range(reference_year - 2,reference_year+1):
        for ctrl in ctrl_lst:
            data = ctrl.read_data(
                "{} {}.xls".format(year,ctrl.provider),
                sheets={"ADD": None},
                converters={
                    "Status Id": str,
                }
                )[0]['ADD']
            if ctrl.provider == 'APN':
                data = data.loc[(data['Status Id'].str.lower().str.strip() == 'is')]
            if ctrl.provider == 'PA':
                data = data.loc[(data['Status Id'].str.lower().str.strip() == 'tn')]
            data['HPINV Year'] = year

            output[ctrl.provider] = output[ctrl.provider].append(data)

    output['APN'] = output['APN'][[

            'First Name',
            'Middle Name',
            'Last Name',
            'Role Abbr',
            'Effect Date',
            'Npi Number',
            'Address1',
            'Address2',
            'City',
            'State',
            'County Name',
            'Zip',
            'Worksite Name',
            'Phone',
            'Fax',
            'Email',
            'Sch Name',
            'Grad Year',
            'HPINV Year',
            'Status Id'
        ]]
    output['PA'] = output['PA'][[

            'First Name',
            'Middle Name',
            'Last Name',
            'Specialty Name',
            'Effect Date',
            'Npi Number',
            'Address1',
            'Address2',
            'City',
            'State',
            'County Name',
            'Zip',
            'Worksite Name',
            'Phone',
            'Fax',
            'Email',
            'Sch Name',
            'Grad Year',
            'HPINV Year',
            'Status Id'
        ]]

    ctrl_lst[0].output_results(
        custom_data=output['PA'],
        custom=True,
        custom_name='{}.{}.{} PA Iowa Start Prev 2 Years'.format(datetime.datetime.now().month,
                                                       datetime.datetime.now().day,
                                                       datetime.datetime.now().year)
    )

    ctrl_lst[0].output_results(
        custom_data=output['APN'],
        custom=True,
        custom_name='{}.{}.{} APN Iowa Start Prev 2 Years'.format(datetime.datetime.now().month,
                                                                 datetime.datetime.now().day,
                                                                 datetime.datetime.now().year)
    )
def queryByHcpId(hcp_list):
    phy = analysis_controller(
        start_year=None,
        end_year=None,
        provider='APN',
        validate='False',
        balance='False',
        summarize='False',
        adhoc='False',
        meded='False',
        project='False',
        index_name=None
    )

    phy.check_directory()

    output = pd.DataFrame()

    for y in range(1998,2020+1):
        data = phy.read_data(
            "{} APN.xls".format(y),
            sheets={"Yearend": None,
                    "ADD":None,
                    "DEL":None
            },
            converters={
            })[0]

        for sheet in data.keys():

            sht = data[sheet]


            found = sht.loc[(sht['Hcp Id'].isin(hcp_list))]

            output = output.append(found)

        output['Year'] = y



    phy.output_results(
        custom_data=output,
        custom=True,
        custom_name='{}.{}.{} APN matched with HPINV'.format(datetime.datetime.now().month,
                                                       datetime.datetime.now().day,
                                                       datetime.datetime.now().year)
    )
def psychologyAttrition(providers_list,start_year,end_year):
    """

    Role Abbr:

    PSNP
    ACNP
    CAP
    CCAP

    Specialty Id 500 - 529 + 009

    PHY PA APN

    deletes

    filter specialty and role

    groupby status id
    """

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

    output = pd.DataFrame()

    psych_sp = [str(sp) for sp in range(500,529 + 1)] + ['009']

    psych_role_abbr = ['PSNP','ACNP','CAP','CCAP']

    for year in range(start_year,end_year + 1):

        for ctrl in ctrl_list:
            if year >= ctrl.start_year and year <= ctrl.end_year:
                ## get deletes data
                data = ctrl.read_data(
                    "{} {}.xls".format(year, ctrl.provider),
                    sheets={"DEL": None},
                    converters={
                        "Specialty Id":str,
                        "Status Id":str,
                        "Type Id":str,
                        "Hcp Id":str
                    })[0]["DEL"]

                ## filter by specialties
                psych = data.loc[
                    (data['Specialty Id'].isin(psych_sp))
                    |
                    (data['Role Abbr'].isin(psych_role_abbr))
                ]
                ## groupby status id
                attrition = psych.groupby('Status Id')["Hcp Id"].count()



                attrition['Total Psychology Attrition'] = attrition.sum()
                attrition['Year'] = year
                attrition['Type Id'] = ctrl.provider

                ## append to output
                output = output.append(attrition)

    output = output.fillna(0)

    output = output.set_index('Year')

    ctrl_list[0].output_results(
        custom_data=output,
        custom=True,
        custom_name="{}-{} Psychology Attrition Summary {}.{}.{}".format(
            start_year,
            end_year,
            datetime.datetime.now().month,
            datetime.datetime.now().day,
            datetime.datetime.now().year)
    )
def fm_ob_delivery_master(start_year,end_year):


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

    counts_output = pd.DataFrame()
    tabular_output = pd.DataFrame()
    sp_output = pd.DataFrame()
    tabular_unmatched_output = pd.DataFrame()

    for y in range(start_year, end_year+1):
        if y >= phy_ctrl.start_year and y <= phy_ctrl.end_year:
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

            deliveries = deliveries.rename(columns={'Hcp Id': 'Hcp'})
            cols_to_use = yearend.columns.difference(deliveries.columns)

            this_year_deliv = deliveries.loc[(deliveries['Year'] == y)]

            hcp_delivering = pd.merge(
                left=yearend[cols_to_use],
                left_on='Hcp Id',
                right=this_year_deliv,
                right_on='Hcp',
                how='left'
            )

            hcp_delivering = hcp_delivering.loc[(hcp_delivering['Delivery Count'].notnull())]

            count_all_phy_delivering = hcp_delivering['Hcp Id'].count()

            count_gen_fm_delivering = hcp_delivering.loc[(hcp_delivering['Specialty Id'].isin(['000', '001','002','003','004','005','006']))][
                'Hcp Id'].count()
            count_200_delivering = hcp_delivering.loc[(hcp_delivering['Specialty Id'] == '200')]['Hcp Id'].count()

            count_other_delivering = hcp_delivering.loc[~(hcp_delivering['Specialty Id'].isin([
                '000', '001', '002', '003', '004', '005', '006',
                '200'
            ]))]['Hcp Id'].count()

            by_gender = hcp_delivering.groupby('Gender')['Hcp Id'].count()

            by_rural_urban = hcp_delivering.groupby('Urban Rural')['Hcp Id'].count()

            counts = pd.concat([by_gender,by_rural_urban])

            counts['Count 000 005 Delivering'] = count_gen_fm_delivering

            counts['Count OB Delivering'] = count_200_delivering

            counts['Other Sp Delivering'] = count_other_delivering

            counts['Total Delivering'] = count_all_phy_delivering

            counts['Year'] = y

            hcp_delivering['Year'] = y

            counts_output = counts_output.append(counts)

            tabular_output = tabular_output.append(hcp_delivering[[
                'Year',
                'Hcp Id',
                'First Name',
                'Last Name',
                'Gender',
                'Age',
                'Urban Rural',
                'City',
                'County',
                'Zip',
                'Specialty Id',
                'Delivery Count'
            ]])

            sp_output = sp_output.append(hcp_delivering.groupby('Specialty Id')['Hcp Id'].count())

            unmatched = this_year_deliv.loc[~(this_year_deliv['Hcp'].isin(hcp_delivering['Hcp Id']))]

            tabular_unmatched_output = tabular_unmatched_output.append(unmatched)

    counts_output = counts_output.set_index('Year')
    tabular_output = tabular_output.set_index('Year')
    sp_output = sp_output.sum(0)


    print(tabular_unmatched_output)
    print(tabular_output)
    phy_ctrl.output_results(
        custom_data=counts_output,
        custom=True,
        custom_name="{}-{} Delivery Overview - Counts {}.{}.{}".format(
            start_year,
            end_year,
            datetime.datetime.now().month,
            datetime.datetime.now().day,
            datetime.datetime.now().year)
    )

    phy_ctrl.output_results(
        custom_data=tabular_output,
        custom=True,
        custom_name="{}-{} Delivery Overview - Tabular {}.{}.{}".format(
            start_year,
            end_year,
            datetime.datetime.now().month,
            datetime.datetime.now().day,
            datetime.datetime.now().year)
    )

    phy_ctrl.output_results(
        custom_data=tabular_unmatched_output,
        custom=True,
        custom_name="{}-{} Delivery Overview - Unmatched Deliveries {}.{}.{}".format(
            start_year,
            end_year,
            datetime.datetime.now().month,
            datetime.datetime.now().day,
            datetime.datetime.now().year)
    )

    phy_ctrl.output_results(
        custom_data=sp_output,
        custom=True,
        custom_name="{}-{} Delivery Overview - Specialties {}.{}.{}".format(
            start_year,
            end_year,
            datetime.datetime.now().month,
            datetime.datetime.now().day,
            datetime.datetime.now().year)
    )
fm_ob_delivery_master(2015,2018)