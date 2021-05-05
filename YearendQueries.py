import numpy as np

class Queries(object):

    def __init__(self,adhoc=False):
        self.adhoc_queries = {}
        self.default_balancing_queries = {}
        self.default_summary_queries = {}
        self.default_project_queries = {}
        self.query_index_dict = None

    def apply_query(self, frame, grouping=None):

        '''
        This function does not apply all queries to all individual source data. It will apply all queries to a single
        individual source data frame.

        -This function sets a global list of query results specifically for balancing.
        -This function sets a dictionary of results associated with their query names.

        NOTE:*****************************************************************************************************
        to understand code, once done, move back to build_cubic_space
        **********************************************************************************************************

        :param grouping:
        :param frame: frame is a pandas DataFrame
        :return : returns a dictionary of results associated with their query names.
        '''

        '''
        A sample query:

        {"-Sp=010 -Res=000 -Title=MD": ## the name of the query. There is nothing enforcing this syntax. Standardized
                                       ## to make life easier
                np.asarray(## turns the results from a pandas series to a ndarray because the cubic space is made with
                           ## ndarray
                    frame.loc[## frame.loc will select rows based on a condition, which is in parenthesis as follows:

                       (                                      ## start of the condition
                        frame['Specialty Id'] == '010')       ## if the value in Specialty Id is the string 010
                        &                                     ## and
                        (                                     ## start of a nested condition
                                (frame['Res1Sp Id'] == '000')       ## if value in the Res1Sp Id column of the frame is
                                                                    ## equal to the string 000
                                |
                                (frame['Res2Sp Id'] == '000')       ## if value in the Res2Sp Id column of the frame is
                                                                    ## equal to the string 000
                                |
                                (frame['Res3Sp Id'] == '000')       ## if value in the Res3Sp Id column of the frame is
                                                                    ## equal to the string 000
                        )
                       &                                      ## and
                       (frame['Title'].str.upper() == 'MD')   ## if value in the Title column, when changed into
                                                              ## uppercase is equal to the string 'MD'


                       - When creating queries, data types much match as well as the exact spelling.




                    ]['Hcp Id']
                )
        }
        '''
        query_results = {}

        if 'adhoc' in grouping:

            self.adhoc_queries = {}

            query_results.update(self.adhoc_queries)

            query_index_dict = dict(zip(query_results.keys(), range(0, len(list(query_results.items())))))

        else:

            if 'balance' in grouping:

                self.default_balancing_queries = {
                    '-*':
                            np.asarray(frame['Hcp Id'])
                    ,
                    '-Sp=000-006,100,200,400':
                            np.asarray(frame.loc[

                                           (frame['Specialty Id'].isin(
                                               ['000', '001', '002', '003', '004', '005', '006', '100', '200', '400']))

                                       ]['Hcp Id'])
                    ,
                    '-Sp=000-006':
                            np.asarray(frame.loc[

                                           (frame['Specialty Id'].isin(
                                               ['000', '001', '002', '003', '004', '005', '006']))

                                       ]['Hcp Id'])
                    ,
                    "-Sp=000-006,010 -Res 010 and -Title MD":
                            np.asarray(frame.loc[

                                           (
                                                   (frame['Specialty Id'].isin(['000', '001', '002', '003', '004', '005', '006']))
                                                   |
                                                   (
                                                           (frame['Specialty Id'] == '010')
                                                           &
                                                           (
                                                                   (frame['Res1Sp Id'] == '000')
                                                                   |
                                                                   (frame['Res2Sp Id'] == '000')
                                                                   |
                                                                   (frame['Res3Sp Id'] == '000')
                                                           )
                                                   )
                                           )
                                           &
                                           (frame['Title'].str.upper() == 'MD')

                                           ]['Hcp Id'])
                    ,
                    "-Sp=000-006,010 -Res 010 and -Title DO":
                            np.asarray(frame.loc[

                                           (
                                                   (frame['Specialty Id'].isin(['000', '001', '002', '003', '004', '005', '006']))
                                                   |
                                                   (
                                                           (frame['Specialty Id'] == '010')
                                                           &
                                                           (
                                                                   (frame['Res1Sp Id'] == '000')
                                                                   |
                                                                   (frame['Res2Sp Id'] == '000')
                                                                   |
                                                                   (frame['Res3Sp Id'] == '000')
                                                           )
                                                   )
                                           )
                                           &
                                           (frame['Title'].str.upper() == 'DO')

                                           ]['Hcp Id'])
                    ,
                    "-Sp=000-006,010 -Res 010":
                            np.asarray(frame.loc[

                                           (
                                                   (frame['Specialty Id'].isin(['000', '001', '002', '003', '004', '005', '006']))
                                                   |
                                                   (
                                                           (frame['Specialty Id'] == '010')
                                                           &
                                                           (
                                                                   (frame['Res1Sp Id'] == '000')
                                                                   |
                                                                   (frame['Res2Sp Id'] == '000')
                                                                   |
                                                                   (frame['Res3Sp Id'] == '000')
                                                           )
                                                   )
                                           )
                                       ]['Hcp Id'])
                    ,
                    '-Sp=100':
                            np.asarray(frame.loc[

                                           (frame['Specialty Id'] == '100')

                                       ]['Hcp Id'])
                    ,
                    '-Sp=101-199':
                            np.asarray(frame.loc[

                                           (frame['Specialty Id'].isin(
                                               [str(x) for x in range(101, 200)]))

                                       ]['Hcp Id'])
                    ,
                    '-Sp=400':
                            np.asarray(frame.loc[

                                           (frame['Specialty Id'] == '400')

                                       ]['Hcp Id'])
                    ,
                    '-Sp=401-499':
                            np.asarray(frame.loc[

                                           (frame['Specialty Id'].isin(
                                               [str(x) for x in range(401, 500)]))

                                       ]['Hcp Id'])
                    ,
                    '-Sp=200':
                            np.asarray(frame.loc[

                                           (frame['Specialty Id'] == '200')

                                       ]['Hcp Id'])
                        ,
                    '-Sp=050':
                            np.asarray(frame.loc[

                                           (frame['Specialty Id'] == '050')

                                       ]['Hcp Id'])
                    ,
                    '-Sp=115':
                            np.asarray(frame.loc[

                                           (frame['Specialty Id'] == '115')

                                       ]['Hcp Id'])
                    ,
                    '-Sp=125':
                            np.asarray(frame.loc[

                                           (frame['Specialty Id'] == '125')

                                       ]['Hcp Id'])
                    ,
                    '-Sp=140,130,132':
                            np.asarray(frame.loc[

                                           (frame['Specialty Id'].isin(
                                               ['140', '130', '132']))

                                       ]['Hcp Id'])
                    ,
                    '-Sp=155':
                            np.asarray(frame.loc[

                                           (frame['Specialty Id'] == '155')

                                       ]['Hcp Id'])
                    ,
                    '-Sp=910':
                            np.asarray(frame.loc[

                                           (frame['Specialty Id'] == '910')

                                       ]['Hcp Id'])
                    ,
                    '-Sp=920':
                            np.asarray(frame.loc[

                                           (frame['Specialty Id'] == '920')

                                       ]['Hcp Id'])
                    ,
                        '-Sp=010':
                            np.asarray(frame.loc[

                                           (frame['Specialty Id'] == '010')

                                       ]['Hcp Id'])
                    ,
                        '-Sp=550':
                            np.asarray(frame.loc[

                                           (frame['Specialty Id'] == '550')

                                       ]['Hcp Id'])
                    ,
                        '-Sp=820':
                            np.asarray(frame.loc[

                                           (frame['Specialty Id'] == '820')

                                       ]['Hcp Id'])
                    ,
                        '-Sp=630':
                            np.asarray(frame.loc[

                                           (frame['Specialty Id'] == '630')

                                       ]['Hcp Id'])
                    ,
                        '-Sp=930':
                            np.asarray(frame.loc[

                                           (frame['Specialty Id'] == '930')

                                       ]['Hcp Id'])
                    ,
                        '-Sp=830':
                            np.asarray(frame.loc[

                                           (frame['Specialty Id'] == '830')

                                       ]['Hcp Id'])
                    ,
                    '-Sp=940':
                            np.asarray(frame.loc[

                                           (frame['Specialty Id'] == '940')

                                       ]['Hcp Id'])
                    ,
                    '-Sp=300':
                            np.asarray(frame.loc[

                                           (frame['Specialty Id'] == '300')

                                       ]['Hcp Id'])
                    ,
                    '-Sp=500,505,510,520':
                            np.asarray(frame.loc[

                                           (frame['Specialty Id'].isin(
                                               ['500', '505', '510', '515', '520']))

                                       ]['Hcp Id'])
                    ,
                    '-Sp=715':
                            np.asarray(frame.loc[

                                           (frame['Specialty Id'] == '715')

                                       ]['Hcp Id'])
                    ,
                    '-Sp=710':
                            np.asarray(frame.loc[

                                           (frame['Specialty Id'] == '710')

                                       ]['Hcp Id'])
                    ,
                    '-Sp=800':
                            np.asarray(frame.loc[

                                           (frame['Specialty Id'] == '800')

                                       ]['Hcp Id'])
                    ,
                    '-Sp=801,802,850,870':
                            np.asarray(frame.loc[

                                           (frame['Specialty Id'].isin(
                                               ['801', '802', '850', '870']))

                                       ]['Hcp Id'])
                    ,
                    '-Sp=860':
                            np.asarray(frame.loc[

                                           (frame['Specialty Id'] == '860')

                                       ]['Hcp Id'])
                    ,
                    '-Sp=960':
                            np.asarray(frame.loc[

                                           (frame['Specialty Id'] == '960')

                                       ]['Hcp Id'])
                    ,
                    '-School>=6000':
                            np.asarray(frame.loc[

                                (frame['School Id'].astype('int64') >= 6000)

                           ]['Hcp Id'])
                    ,
                    '-Sp=500,505,510,520,009 -RoleAbbr=PSNP,ACNP,CAP,CCAP':
                            np.asarray(frame.loc[

                                (frame['Specialty Id'].isin([str(x) for x in range(500,529+1)] + ['009']))
                                |
                                (frame['Role Abbr'].isin(['PSNP', 'ACNP', 'CAP', 'CCAP']))

                           ]['Hcp Id'])
        }

            if 'summary' in grouping:

                self.default_summary_queries = {

                '-Gender=M':
                    np.asarray(frame.loc[
                                   (frame['Gender'].str.upper() == 'M')
                               ]['Hcp Id'])
                ,
                '-Gender=F':
                    np.asarray(
                        frame.loc[
                            (frame['Gender'].str.upper() == 'F')
                        ]
                        ['Hcp Id'])
                ,
                "-Title=MD":
                    np.asarray(frame.loc[

                                   (frame['Title'].str.upper() == 'MD')
                               ]['Hcp Id'])
                ,
                "-Title=DO":
                    np.asarray(frame.loc[

                                   (frame['Title'].str.upper() == 'DO')
                               ]['Hcp Id'])
                ,
                '-Sp=000 -Title=MD':
                    np.asarray(frame.loc[

                                   (frame['Specialty Id'] == '000')
                                   &
                                   (frame['Title'].str.upper() == 'MD')

                                   ]['Hcp Id'])
                ,
                '-Sp=000 -Title=DO':
                    np.asarray(frame.loc[

                                   (frame['Specialty Id'] == '000')
                                   &
                                   (frame['Title'].str.upper() == 'DO')

                                   ]['Hcp Id'])
                ,
                '-Sp=000':
                    np.asarray(frame.loc[

                                   (frame['Specialty Id'] == '000')

                               ]['Hcp Id'])
                ,
                '-Sp=001-006 -Title=MD':
                    np.asarray(frame.loc[

                                   (frame['Specialty Id'].isin(['001', '002', '003', '004', '005', '006']))
                                   &
                                   (frame['Title'].str.upper() == 'MD')

                                   ]['Hcp Id'])
                ,
                '-Sp=001-006 -Title=DO':
                    np.asarray(frame.loc[

                                   (frame['Specialty Id'].isin(['001', '002', '003', '004', '005', '006']))
                                   &
                                   (frame['Title'].str.upper() == 'DO')

                                   ]['Hcp Id'])
                ,
                '-Sp=001-006':
                    np.asarray(frame.loc[

                                   (frame['Specialty Id'].isin(['001', '002', '003', '004', '005', '006']))

                               ]['Hcp Id'])
                ,
                '-Sp=000-006 -Title=MD':
                    np.asarray(frame.loc[

                                   (frame['Specialty Id'].isin(['000', '001', '002', '003', '004', '005', '006']))
                                   &
                                   (frame['Title'].str.upper() == 'MD')

                                   ]['Hcp Id'])
                ,
                '-Sp=000-006 -Title=DO':
                    np.asarray(frame.loc[

                                   (frame['Specialty Id'].isin(['000', '001', '002', '003', '004', '005', '006']))
                                   &
                                   (frame['Title'].str.upper() == 'DO')

                                   ]['Hcp Id'])
                ,

                ## Specialty Id == 010 and Res1SP Id == 000 and title = MD
                ## Specialty Id == 010 and Res2SP Id == 000 and title = MD
                ## Specialty Id == 010 and Res3SP Id == 000 and title = MD

                "-Sp=010 -Res=000 -Title=MD":
                    np.asarray(frame.loc[

                                   (frame['Specialty Id'] == '010')
                                   &
                                   (
                                           (frame['Res1Sp Id'] == '000')
                                           |
                                           (frame['Res2Sp Id'] == '000')
                                           |
                                           (frame['Res3Sp Id'] == '000')
                                   )
                                   &
                                   (frame['Title'].str.upper() == 'MD')

                                   ]['Hcp Id'])
                ,
                "-Sp=010 -Res=000 -Title=DO":
                    np.asarray(frame.loc[

                                   (frame['Specialty Id'] == '010')
                                   &
                                   (
                                           (frame['Res1Sp Id'] == '000')
                                           |
                                           (frame['Res2Sp Id'] == '000')
                                           |
                                           (frame['Res3Sp Id'] == '000')
                                   )
                                   &
                                   (frame['Title'].str.upper() == 'DO')

                                   ]['Hcp Id'])
                ,

                "-Sp=010 -Res=000":
                    np.asarray(frame.loc[

                                   (frame['Specialty Id'] == '010')
                                   &
                                   (
                                           (frame['Res1Sp Id'] == '000')
                                           |
                                           (frame['Res2Sp Id'] == '000')
                                           |
                                           (frame['Res3Sp Id'] == '000')
                                   )

                                   ]['Hcp Id'])

                ,
                '-Sp=000,005,100,400':
                    np.asarray(frame.loc[

                                   (frame['Specialty Id'].isin(
                                       ['000', '005', '100', '400']))

                               ]['Hcp Id'])
                ,
                '-Sp=000,005,100,400,200':
                    np.asarray(frame.loc[

                                   (frame['Specialty Id'].isin(
                                       ['000', '005', '100', '400', '200']))

                               ]['Hcp Id'])
                ,
                '-Sp=000-006,100,400':
                    np.asarray(frame.loc[

                                   (frame['Specialty Id'].isin(
                                       ['000', '001', '002', '003', '004', '005', '006', '100', '400']))

                               ]['Hcp Id'])
                ,
                '-Sp=100,199':
                    np.asarray(frame.loc[

                                   (frame['Specialty Id'].isin(
                                       ['100', '199']))

                               ]['Hcp Id'])
                ,




                '-Sp=199':
                    np.asarray(frame.loc[

                                   (frame['Specialty Id'] == '199')

                               ]['Hcp Id'])
                ,


                '-School=1803':
                    np.asarray(frame.loc[

                                   (frame['School Id'] == '1803')

                               ]['Hcp Id'])
                ,

                '-Sp=000 -City Pop<5000':
                    np.asarray(frame.loc[

                                   (frame['Specialty Id'] == '000')
                                   &
                                   (frame['City Pop'].astype('int64') < 5000)

                                   ]['Hcp Id'])
                ,
                '-Sp=005 -City Pop<5000':
                    np.asarray(frame.loc[

                                   (frame['Specialty Id'] == '005')
                                   &
                                   (frame['City Pop'].astype('int64') < 5000)

                                   ]['Hcp Id'])
                ,
                "-Sp=010 -Res=000 -City Pop<5000":
                    np.asarray(frame.loc[

                                   (frame['Specialty Id'] == '010')
                                   &
                                   (
                                           (frame['Res1Sp Id'] == '000')
                                           |
                                           (frame['Res2Sp Id'] == '000')
                                           |
                                           (frame['Res3Sp Id'] == '000')
                                   )
                                   &
                                   (frame['City Pop'].astype('int64') < 5000)
                                   ]['Hcp Id'])
                ,
                '-Sp=000 -City Pop=5000-15000':
                    np.asarray(frame.loc[

                                   (frame['Specialty Id'] == '000')
                                   &
                                   (frame['City Pop'].astype('int64') >= 5000)
                                   &
                                   (frame['City Pop'].astype('int64') < 15000)
                                   ]['Hcp Id'])
                ,
                '-Sp=005 -City Pop=5000-15000':
                    np.asarray(frame.loc[

                                   (frame['Specialty Id'] == '005')
                                   &
                                   (frame['City Pop'].astype('int64') >= 5000)
                                   &
                                   (frame['City Pop'].astype('int64') < 15000)

                                   ]['Hcp Id'])
                ,
                "-Sp=010 -Res=000 -City Pop=5000-15000":
                    np.asarray(frame.loc[

                                   (frame['Specialty Id'] == '010')
                                   &
                                   (
                                           (frame['Res1Sp Id'] == '000')
                                           |
                                           (frame['Res2Sp Id'] == '000')
                                           |
                                           (frame['Res3Sp Id'] == '000')
                                   )
                                   &
                                   (frame['City Pop'].astype('int64') >= 5000)
                                   &
                                   (frame['City Pop'].astype('int64') < 15000)
                                   ]['Hcp Id'])
                ,
                '-Sp=000 -City Pop=15000-50000':
                    np.asarray(frame.loc[

                                   (frame['Specialty Id'] == '000')
                                   &
                                   (frame['City Pop'].astype('int64') >= 15000)
                                   &
                                   (frame['City Pop'].astype('int64') < 50000)
                                   ]['Hcp Id'])
                ,
                '-Sp=005 -City Pop=15000-50000':
                    np.asarray(frame.loc[

                                   (frame['Specialty Id'] == '005')
                                   &
                                   (frame['City Pop'].astype('int64') >= 15000)
                                   &
                                   (frame['City Pop'].astype('int64') < 50000)

                                   ]['Hcp Id'])
                ,
                "-Sp=010 -Res=000 -City Pop=15000-50000":
                    np.asarray(frame.loc[

                                   (frame['Specialty Id'] == '010')
                                   &
                                   (
                                           (frame['Res1Sp Id'] == '000')
                                           |
                                           (frame['Res2Sp Id'] == '000')
                                           |
                                           (frame['Res3Sp Id'] == '000')
                                   )
                                   &
                                   (frame['City Pop'].astype('int64') >= 15000)
                                   &
                                   (frame['City Pop'].astype('int64') < 50000)
                                   ]['Hcp Id'])
                ,
                '-Sp=000 -City Pop>=50':
                    np.asarray(frame.loc[

                                   (frame['Specialty Id'] == '000')
                                   &
                                   (frame['City Pop'].astype('int64') >= 50000)

                                   ]['Hcp Id'])
                ,
                '-Sp=005 -City Pop>=50':
                    np.asarray(frame.loc[

                                   (frame['Specialty Id'] == '005')
                                   &
                                   (frame['City Pop'].astype('int64') >= 50000)

                                   ]['Hcp Id'])
                ,
                "-Sp=010 -Res=000 -City Pop>=50":
                    np.asarray(frame.loc[

                                   (frame['Specialty Id'] == '010')
                                   &
                                   (
                                           (frame['Res1Sp Id'] == '000')
                                           |
                                           (frame['Res2Sp Id'] == '000')
                                           |
                                           (frame['Res3Sp Id'] == '000')
                                   )
                                   &
                                   (frame['City Pop'].astype('int64') >= 50000)

                                   ]['Hcp Id'])
                ,
                "-Sp=000-006,010 -Res=000 -City Pop<5000":
                    np.asarray(frame.loc[

                                   (
                                           (frame['Specialty Id'].isin(['000', '001', '002', '003', '004', '005', '006']))
                                           |
                                           (
                                                   (frame['Specialty Id'] == '010')
                                                   &
                                                   (
                                                           (frame['Res1Sp Id'] == '000')
                                                           |
                                                           (frame['Res2Sp Id'] == '000')
                                                           |
                                                           (frame['Res3Sp Id'] == '000')
                                                   )
                                           )
                                   )
                                   &
                                   (
                                       (frame['City Pop'].astype('int64') < 5000)
                                   )
                                   ]['Hcp Id'])
                ,
                "-Sp=000-006,010 -Res=000 -City Pop=5000-15000":
                    np.asarray(frame.loc[

                                   (
                                           (frame['Specialty Id'].isin(['000', '001', '002', '003', '004', '005', '006']))
                                           |
                                           (
                                                   (frame['Specialty Id'] == '010')
                                                   &
                                                   (
                                                           (frame['Res1Sp Id'] == '000')
                                                           |
                                                           (frame['Res2Sp Id'] == '000')
                                                           |
                                                           (frame['Res3Sp Id'] == '000')
                                                   )
                                           )
                                   )
                                   &
                                   (
                                           (frame['City Pop'].astype('int64') >= 5000)
                                           &
                                           (frame['City Pop'].astype('int64') < 15000)
                                   )
                                   ]['Hcp Id'])
                ,
                "-Sp=000-006,010 -Res=000 -City Pop=15000-50000":
                    np.asarray(frame.loc[

                                   (
                                           (frame['Specialty Id'].isin(['000', '001', '002', '003', '004', '005', '006']))
                                           |
                                           (
                                                   (frame['Specialty Id'] == '010')
                                                   &
                                                   (
                                                           (frame['Res1Sp Id'] == '000')
                                                           |
                                                           (frame['Res2Sp Id'] == '000')
                                                           |
                                                           (frame['Res3Sp Id'] == '000')
                                                   )
                                           )
                                   )
                                   &
                                   (
                                           (frame['City Pop'].astype('int64') >= 15000)
                                           &
                                           (frame['City Pop'].astype('int64') < 50000)
                                   )
                                   ]['Hcp Id'])
                ,
                "-Sp=000-006,010 -Res=000 -City Pop>50000":
                    np.asarray(frame.loc[

                                   (
                                           (frame['Specialty Id'].isin(['000', '001', '002', '003', '004', '005', '006']))
                                           |
                                           (
                                                   (frame['Specialty Id'] == '010')
                                                   &
                                                   (
                                                           (frame['Res1Sp Id'] == '000')
                                                           |
                                                           (frame['Res2Sp Id'] == '000')
                                                           |
                                                           (frame['Res3Sp Id'] == '000')
                                                   )
                                           )
                                   )
                                   &
                                   (
                                       (frame['City Pop'].astype('int64') > 50000)
                                   )
                                   ]['Hcp Id'])
                ,











            }

            if 'project' in grouping:

                self.default_project_queries = {
                '-Sp=000 -Activity=1,3,13,14,15,16':
                    np.asarray(frame.loc[

                                   (frame['Specialty Id'] == '000')
                                   &
                                   (frame['Act Id'].isin(['1', '3', '13', '14', '15', '16']))

                                   ]['Hcp Id'])
                ,

                '-Sp=000 -Activity!=1,3,13,14,15,16':
                    np.asarray(frame.loc[

                                   (frame['Specialty Id'] == '000')
                                   &
                                   (~frame['Act Id'].isin(['1', '3', '13', '14', '15', '16']))

                                   ]['Hcp Id'])
                ,
                '-Sp=001-006 -Activity=1,3,13,14,15,16':
                    np.asarray(frame.loc[

                                   (frame['Specialty Id'].isin(['001', '002', '003', '004', '005', '006']))
                                   &
                                   (frame['Act Id'].isin(['1', '3', '13', '14', '15', '16']))

                                   ]['Hcp Id'])
                ,
                '-Sp=001-006 -Activity!=1,3,13,14,15,16':
                    np.asarray(frame.loc[

                                   (frame['Specialty Id'].isin(['001', '002', '003', '004', '005', '006']))
                                   &
                                   (~frame['Act Id'].isin(['1', '3', '13', '14', '15', '16']))

                                   ]['Hcp Id'])
                ,
                '-Sp=010 -Res=000 -Activity=1,3,13,14,15,16':
                    np.asarray(frame.loc[
                                   (frame['Specialty Id'] == '010')
                                   &
                                   (
                                           (frame['Res1Sp Id'] == '000')
                                           |
                                           (frame['Res2Sp Id'] == '000')
                                           |
                                           (frame['Res3Sp Id'] == '000')
                                   )
                                   &
                                   (frame['Act Id'].isin(['1', '3', '13', '14', '15', '16']))

                                   ]['Hcp Id'])
                ,
                '-Sp=010 -Res=000 -Activity!=1,3,13,14,15,16':
                    np.asarray(frame.loc[

                                   (frame['Specialty Id'] == '010')
                                   &
                                   (
                                           (frame['Res1Sp Id'] == '000')
                                           |
                                           (frame['Res2Sp Id'] == '000')
                                           |
                                           (frame['Res3Sp Id'] == '000')
                                   )
                                   &
                                   (~frame['Act Id'].isin(['1', '3', '13', '14', '15', '16']))

                                   ]['Hcp Id'])
                ,
                '-Sp=100 -Activity=1,3,13,14,15,16':
                    np.asarray(frame.loc[

                                   (frame['Specialty Id'] == '100')
                                   &
                                   (frame['Act Id'].isin(['1', '3', '13', '14', '15', '16']))

                                   ]['Hcp Id'])
                ,
                '-Sp=100 -Activity!=1,3,13,14,15,16':
                    np.asarray(frame.loc[

                                   (frame['Specialty Id'] == '100')
                                   &
                                   (~frame['Act Id'].isin(['1', '3', '13', '14', '15', '16']))

                                   ]['Hcp Id'])
                    ,
                    '-Sp=400 -Activity=1,3,13,14,15,16':
                        np.asarray(frame.loc[

                                       (frame['Specialty Id'] == '400')
                                       &
                                       (frame['Act Id'].isin(['1', '3', '13', '14', '15', '16']))

                                       ]['Hcp Id'])
                    ,
                    '-Sp=400 -Activity!=1,3,13,14,15,16':
                        np.asarray(frame.loc[

                                       (frame['Specialty Id'] == '400')
                                       &
                                       (~frame['Act Id'].isin(['1', '3', '13', '14', '15', '16']))

                                       ]['Hcp Id'])
                    ,
                    '-Sp=400 -Activity!=1,3,13,14,15,16':
                        np.asarray(frame.loc[

                                       (frame['Specialty Id'] == '400')
                                       &
                                       (~frame['Act Id'].isin(['1', '3', '13', '14', '15', '16']))

                                       ]['Hcp Id'])

                }

            query_results.update(self.default_balancing_queries)

            list(map(query_results.update,
                [self.default_balancing_queries, self.default_summary_queries, self.default_project_queries]))

        return query_results

    def apply_adhoc_query(self,frame,grouping=None):

        ##frame is a dataframe

        query_results = {
            "test query":
                np.asarray(frame.loc[

                               (frame['County'] == 'Adair')
                               &
                               (frame['April 1, 2010 - Census'] == 7682)

                               ]['County'])
        }

        query_results.update(self.adhoc_queries)

        query_index_dict = dict(zip(query_results.keys(), range(0, len(list(query_results.items())))))

        return query_results