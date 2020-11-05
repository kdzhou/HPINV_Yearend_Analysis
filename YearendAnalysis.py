import argparse
import copy
import datetime
import os
import sys
import time

import numpy as np
import pandas as pd
import xlrd

from YearendQueries import Queries

afilliated = ["18001",
              "18003",
              "18004",
              "18005",
              "18006",
              "18007",
              "18009",
              "18010",
              "18012",
              "18013",
              "18014"]

'''*****************************************************************************************************
0. To understand code, start with __name__ at the very bottom of file
*****************************************************************************************************'''

class analysis_controller(object):

    def __init__(self, provider, start_year, end_year, validate, balance,
                 summarize, project, adhoc, meded, index_name):
        '''
        3.
        A controller object has the groups of information defined below. Go to this function for more description.
        An object can be thought of as a real life object with properties that define what it looks like and
        what it does.


        -Basic running information that defines what scope of data the controller is working with.

        -Running information that defines which analyses to perform.

        -Directory information that defines where to read from, where to write to, and where reference materials are.

        -Error information that defines what errors were incurred while analyzing.

        -Important global containers that defines data structures that hold data for one or more analyses being
        performed

        NOTE: *****************************************************************************************************
            For understanding code, go to containers section of variables defined at the very end of this function.
            Read and gain a high level understanding of

            self.balancing_query_keys
            self.feature_space
            self.idict
            self.hcp_dct

            Then go to initiate_analysis_main.
            ******************************************************************************************************
        ----------------------------------------------------------------------------------------------------------------

        :return:
        '''


        '''
        Basic running information
        
        -Starting year: year to start analyzing with. The corresponding file must exist.
        -Ending year: year to end analyzing. The corresponding file must exist.
        -Provider: provider type to perform analysis on.
        '''
        self.provider = provider
        if start_year != None:
            self.start_year = start_year
        else:
            self.start_year = self.reference_data_extract('{} start'.format(self.provider),is_file=False)
        if end_year != None:
            self.end_year = end_year
        else:
            self.end_year = int(datetime.datetime.now().year) - 1

        self.axis_index_name = index_name

        '''
        Running options 
        
        -These are passed in via commandline, parsed and passed into this controller object
        -validation_error_override: if validation had been done on excel data before, 
                                    skip by setting cmd option to True
        -balance: ask the controller to perform balancing on the data
        -summarize: ask the controller to perform summarization analysis.
        -adhoc: ask the controller to perform a user defined analysis.
        -meded: medical education breakdowns(meded) are stored procedures already available in this code. Tell the
                controller to perform the necessary analysis.
        '''
        self.validation_error_override = validate
        self.balance = balance
        self.summarize = summarize
        self.adhoc = adhoc
        self.meded = meded
        self.project = project

        '''
        Directory Information
        
        -rt: refers to the root, or the explicit path YearendAnalysisDeprec.py is stored in. 
            Does not need to be manually set
            
        '''
        self.rt = os.getcwd()
        self.read_from_dir = None
        self.write_to_dir = None
        self.reference_dir = None
        '''
        Error Information
        
        directory_check_errors: errors incurred while checking directory. These checks must pass to ensure program runs
                                correctly.
        file_read_errors: errors incurred while reading file. These checks must pass to ensure program runs. Some errors
                          may stop program from running correctly.
        data_validation_errors: errors incurred while validating data. Errors marked essential will be warned about
                                since they may effect analysis results. They will not break the program from running.
        
        '''

        self.directory_check_errors = []
        self.file_read_errors = []
        self.data_validation_errors = {}

        '''
        Containers
        
        -self.temp_xxxxxx:
            Type: pandas.DataFrame object. 
            Description: Think of a typical excel table with headers and row indexes as the visual representation of
                         a pandas DataFrame object. These objects are built on top of numpy matrices(?) and
                         operations on DataFrames are performed using C, making querying much faster. 
            Usage: when a yearend file for a particular year is read by the program, its contents are stored here.
                   As the controller iterates through every year, the dataframes will change accordingly, hence the
                   "temp" prefix
                   
        -max_fifth_year:
            Type:integer
            Description: This is a specific parameter for a table in the LMT summaries, which requires information for
            every 5 years since 2000 and every year between the ending year and the closest year divisible by 5. 
            If the current end year is divisible by 5, then the max_fifth_year is the current
            year. Otherwise, we look for the closest year divisible by 5 and use that year for the max_fifth year. Then
            every year after the max_fifth_year will also be included. 
            
            Example:
            
            current year = 2019.
            
            max_fifth_year = 2015
            
            years required for this table:
            
            2000, 2005, 2010, 2015, 2016, 2017, 2018, 2019
            
        -summary_df:
            Type: a dictionary where each key is a table name string and each value is a pandas DataFrames.
            Description: after summary results are obtained, they are written to their corresponding result tables
                         stored in summary_df. These DataFrames are then written to excel in the output_results function
                         "Table(s)" will hitherto refer to a dataframe in summary_df or balance_df.
        -balance_df:
            Type: a dictionary where each key is a table name string and each value is a pandas DataFrames.
            Description: after balance results are obtained, they are written to their corresponding result tables
                         stored in balance_df. These DataFrames are then written to excel in the output_results function
                         
        -balancing_query_keys
            Type: a list of query name strings.
            Description: querying DataFrames is performed in apply_query function. In this function, querying
                         instructions are stored in a dictionary whose keys are query name strings spelled exactly like
                         those in balancing_query_keys. Their corresponding values are a set of querying instructions
                         that are executed as the compiler reads the dictionary. The results are returned as numpy
                         arrays of Hcp Id strings to facilitate the analyzing of the results.
                         
        -feature_space
            Type: a numpy.ndarray representing three axis matrix (think of a cube). numpy.ndarray is a powerful data
                  structure that resembles a list. It allows matrix operations to be performed (much faster for
                  computers to work with but possibly high memory requirements) in C (even faster language)
            Description: A cubic area (row,depth_column,height_column) that stores data in terms of which excel sheet
                         the source data came from, which query the results correspond to, and all unique hcp_ids across
                         all the source data for either the current year or the current and previous year.
                         The precise details of its composition can be visualized in the word guide and 
                         better explained by its creator function, build_cubic_space.
                         
        -idict:
            Type: a dictionary where each name is a query and each value is an integer.
            Description: this dictionary associates the query's name with an index on feature_space's query axis. This
                         ensures that all information is drawn correctly from the cubic space and stored to the correct
                         result dataframes.
                         
        -hcp_dict:
            Type: a dictionary where each name is an hcp id and each value is a integer.
            Description: this dictionary associates the hcp id with an index on feature_space's hcp axis. This ensures
                         that all information being associated with a given hcp id is stored in the correct place in the
                         cubic feature area.
            
        '''
        self.temp_previous = None
        self.temp_adds = None
        self.temp_deletes = None
        self.temp_current = None
        self.temp_transfers = None
        self.temp_deletes = None
        self.temp_changes = None


        self.max_fifth_year = 2015

        if self.summarize == 'True' or self.balance == 'True':
            [self.max_fifth_year + 5 for x in range(0, self.end_year) if self.max_fifth_year + 5 < self.end_year]

        self.summary_df = None
        self.balance_df = None

        self.balancing_query_keys = None
        self.feature_space = None
        self.idict = {}
        self.hcp_to_ind = {}
        self.ind_to_hcp = {}

        '''Plug in functions'''
        self.adhoc = None

        self.validate = self.validate_ya_data

        self.queries = Queries()

        self.apply_query = self.queries.apply_query

    def setReadDirectory(self,new_dir):
        self.read_from_dir = new_dir

    def build_cubic_space_original(self, year):

        '''
        Tacit inputs: self.start_year,self.temp_xxxxx
        Outputs : np.ndarray of the Cubic Space, dict of the query to index pairs

        go to function for precise explanation of algorithm

        :param year:
        :return:


        '''

        '''
        5. 
        
        -The cubic space is a 3-dimensional feature space made up of an add-delete-transfer axis, a query axis and
        a Hcp Id axis as follows:
                    ^
                    |
                    |
                    |
                    Q
                    u      
                    e        7     
                    r       /
                    i      /
                    e     /
                    s   hcp id axis
                    |   /
                    |  /
                    | /
                    o - - Previous - - Add - - Del - - Current - - Transfer - - Change - (ADT axis)->
        
        -This is achieved by the numpy.ndarray as follows:
        
                                            hcp Id axis
                    [ Previous : [ query:  [0 1 ... 0],
                                    query: [1 0 ... 1],
                                    query: [0 0 ...0],
                                ]
                      Add : [ query:       [0 1 ... 0],
                                    query: [0 1 ... 0],
                                    query: [0 1 ... 0],
                                ]
                      .
                      .
                      .        
                      Change : [ query:    [0 1 ... 0],
                                    query: [0 1 ... 0],
                                    query: [0 1 ... 0],
                                ]
                    ]
        
        -idict: a dictionary used to associate a query name with an index on the query axis
        
        -hcp_dict: a dictionary used to associate a hcp id with the index on the Hcp Id axis
        
        -each value in the cubic space has a value of 0 or 1. 
        
        -Example:
        
            If the a physician in the source data dataset "ADD" has specialty id 100.
            Lets say:
                - sheet DEL corresponds to 3 on the ADT axis
                - query "-SpId = 1000" corresponds to 5 on the query axis
                - Hcp Id of 1022939 is the 144th in the source data
            Then it's corresponding value at 
        
                ( <query axis index for 'DEL'> , <query axis index for query> , <query axis index for hcp id> )
                
                which translates to (3 , 5, 144) will have 1 stored as its value.
                
                because it is deleted, it cannot be in Current's list of Sp=100.
                Let's say Current corresponds to 4 on the ADT axis.
                So the value stored at position (4, 5, 144) will be 0.
        
        NOTE: *****************************************************************************************************
        To understand code, move to apply query before continuing with this function. Once done, move back to initiate
        analysis_main.
        *****************************************************************************************************
            
        '''



        '''
        Set the data view's scope.
        
        If the year we an analyzing is the start year, then we don't have data for it's relative "Previous" year, thus
        we only have 5 ADT sources being read in. 
        
        Otherwise we will need to read in the data for the relative "Previous" year, thus we have 6 ADT sources being
        read in.        
        '''
        if year == self.start_year:
            available_data = [self.temp_adds,
                              self.temp_deletes,
                              self.temp_current,
                              self.temp_transfers,
                              self.temp_changes]
            self.idict['sheets'] = {
                'Add': 0,
                'DEL': 1,
                'Yearend': 2,
                'Transfer': 3,
                'Change': 4
            }

        else:
            '''Here we set the scope'''
            available_data = [self.temp_previous,
                              self.temp_adds,
                              self.temp_deletes,
                              self.temp_current,
                              self.temp_transfers,
                              self.temp_changes]

            '''Here we set associate the data names to their corresponding indices on the ADT axis'''
            self.idict['sheets'] = {
                'Previous': 0,
                'Add': 1,
                'DEL': 2,
                'Yearend': 3,
                'Transfer': 4,
                'Change': 5
            }
            
        '''Apply queries to data and get the results in the form of list of ndarrays of hcp id's'''
        results_for_tables = [self.apply_query(df) for df in available_data]

        '''Within the scope of source data, we need to find all the unique hcp id's to construct an
        axis with them'''
        unique_hcp = np.unique(np.concatenate([np.asarray(x['Hcp Id']) for x in available_data]))

        '''Take measurements of source data, queries and unique hcp ids to establish the axis lengths for 
        the cubic space'''
        max_source_table_dim = len(available_data)
        max_query_dim = len(list(results_for_tables[0].values()))
        max_hcp_dim = unique_hcp.shape[0]

        '''Construct a nested matrix filled with zeros with the correct dimensions.
        
        As of 2019, there are near 5900 Hcp Id's, near 30 queries, and 6 source data sheets so the dimensions should be
        similar to (6, 30, 5900)
        
        '''
        result_space = np.zeros((max_source_table_dim, max_query_dim, max_hcp_dim))

        '''
        Associate the unique hcp id with the axis index
        '''
        hcp_ind = dict(zip(unique_hcp, range(0, unique_hcp.shape[0])))
        ind_hcp = dict(zip(range(0, unique_hcp.shape[0]),unique_hcp))
        for i in range(0,max_source_table_dim):

            for j in range(0,max_query_dim):

                hcp_dim_result = list(results_for_tables[i].values())[j]

                for k in range(0,hcp_dim_result.shape[0]):
                    '''By iterating through every axis, every query and every hcp, we populate the matrix 
                    with 1's where necessary.'''
                    fs_hcp_ind = hcp_ind[hcp_dim_result[k]]
                    result_space[i,j,fs_hcp_ind] = 1

        self.idict['hcp to ind'] = hcp_ind
        self.idict['ind to hcp'] = ind_hcp

        print(self.idict.keys())

        '''Return to intiate_analysis_main the resulting space and hcp dictionary'''
        return result_space

    def build_cubic_space(self, data, load_default_queries=False):

        '''
        Tacit inputs: self.start_year,self.temp_xxxxx
        Outputs : np.ndarray of the Cubic Space, dict of the query to index pairs

        go to function for precise explanation of algorithm

        :param year:
        :return:


        '''

        '''
        5. 

        -The cubic space is a 3-dimensional feature space made up of an add-delete-transfer axis, a query axis and
        a Hcp Id axis as follows:
                    ^
                    |
                    |
                    |
                    Q
                    u      
                    e        7     
                    r       /
                    i      /
                    e     /
                    s   hcp id axis
                    |   /
                    |  /
                    | /
                    o - - Previous - - Add - - Del - - Current - - Transfer - - Change - (ADT axis)->

        -This is achieved by the numpy.ndarray as follows:

                                            hcp Id axis
                    [ Previous : [ query:  [0 1 ... 0],
                                    query: [1 0 ... 1],
                                    query: [0 0 ...0],
                                ]
                      Add : [ query:       [0 1 ... 0],
                                    query: [0 1 ... 0],
                                    query: [0 1 ... 0],
                                ]
                      .
                      .
                      .        
                      Change : [ query:    [0 1 ... 0],
                                    query: [0 1 ... 0],
                                    query: [0 1 ... 0],
                                ]
                    ]

        -idict: a dictionary used to associate a query name with an index on the query axis

        -hcp_dict: a dictionary used to associate a hcp id with the index on the Hcp Id axis

        -each value in the cubic space has a value of 0 or 1. 

        -Example:

            If the a physician in the source data dataset "ADD" has specialty id 100.
            Lets say:
                - sheet DEL corresponds to 3 on the ADT axis
                - query "-SpId = 1000" corresponds to 5 on the query axis
                - Hcp Id of 1022939 is the 144th in the source data
            Then it's corresponding value at 

                ( <query axis index for 'DEL'> , <query axis index for query> , <query axis index for hcp id> )

                which translates to (3 , 5, 144) will have 1 stored as its value.

                because it is deleted, it cannot be in Current's list of Sp=100.
                Let's say Current corresponds to 4 on the ADT axis.
                So the value stored at position (4, 5, 144) will be 0.

        NOTE: *****************************************************************************************************
        To understand code, move to apply query before continuing with this function. Once done, move back to initiate
        analysis_main.
        *****************************************************************************************************

        '''

        '''
        Set the data view's scope.

        If the year we an analyzing is the start year, then we don't have data for it's relative "Previous" year, thus
        we only have 5 ADT sources being read in. 

        Otherwise we will need to read in the data for the relative "Previous" year, thus we have 6 ADT sources being
        read in.        
        '''

        third_axis_index_name = self.axis_index_name

        available_sheet_names = list(data.keys())

        # if available_sheet_names ==

        available_data = [data[x] for x in available_sheet_names]

        #print(self.queries.query_index_dict)



        #print(self.idict['sheets'])

        '''Apply queries to data and get the results in the form of list of ndarrays of hcp id's'''
        grouping = []

        if self.balance == 'True' or load_default_queries == True:
            grouping.append('balance')
        if self.summarize == 'True' or load_default_queries == True:
            grouping.append('summary')
        if self.adhoc == 'True':
            grouping.append('adhoc')
        if self.project == 'True' or load_default_queries == True:
            grouping.append('project')



        results_for_tables = [self.apply_query(df,grouping) for df in available_data]

        self.idict['queries'] = dict(zip(results_for_tables[0].keys(),range(0,len(list(results_for_tables[0].keys())))))
        self.idict['sheets'] = dict(zip(available_sheet_names, range(0, len(available_sheet_names))))

        self.balancing_query_keys = self.queries.default_balancing_queries.keys()

        '''Within the scope of source data, we need to find all the unique hcp id's to construct an
        axis with them'''
        unique_hcp = np.unique(np.concatenate([np.asarray(x[third_axis_index_name]) for x in available_data]))

        '''Take measurements of source data, queries and unique hcp ids to establish the axis lengths for 
        the cubic space'''
        max_source_table_dim = len(available_data)
        max_query_dim = len(list(results_for_tables[0].values()))
        max_hcp_dim = unique_hcp.shape[0]

        print(max_source_table_dim, max_query_dim, max_hcp_dim)

        '''Construct a nested matrix filled with zeros with the correct dimensions.

        As of 2019, there are near 5900 Hcp Id's, near 30 queries, and 6 source data sheets so the dimensions should be
        similar to (6, 30, 5900)

        '''
        result_space = np.zeros((max_source_table_dim, max_query_dim, max_hcp_dim))

        '''
        Associate the unique hcp id with the axis index
        '''
        hcp_ind = dict(zip(unique_hcp, range(0, unique_hcp.shape[0])))
        ind_hcp = dict(zip(range(0, unique_hcp.shape[0]), unique_hcp))

        for i in range(0, max_source_table_dim):

            for j in range(0, max_query_dim):

                hcp_dim_result = list(results_for_tables[i].values())[j]

                for k in range(0, hcp_dim_result.shape[0]):
                    '''By iterating through every axis, every query and every hcp, we populate the matrix 
                    with 1's where necessary.'''
                    fs_hcp_ind = hcp_ind[hcp_dim_result[k]]
                    result_space[i, j, fs_hcp_ind] = 1

        self.idict['ind to id'] = ind_hcp
        self.idict['id to ind'] = hcp_ind

        '''Return to intiate_analysis_main the resulting space and hcp dictionary'''

        self.feature_space = result_space

        return result_space

    def build_summary_df(self):

        '''
        When we build a new pandas dataframe, the following code:

        pd.DataFrame(
                    columns=['Entering MD', 'Entering DO', 'Leaving MD', 'Leaving DO'],
                    index=[str(y) for y in range(self.end_year - 10, self.end_year + 1)]
                )

        will result in:

                Entering MD | Entering DO | Leaving MD | Leaving DO <--columns
        2009
        2010
        2011
        2012
        2013
        2014
        2015
        2016
        2017
        2018
        2019
        ^
        |
        indexes (rows ) (this was written in 2020 and the year we were stopping at was 2019)

        :return:
        '''
        tables = {
            'tb1':
                pd.DataFrame(
                    columns=['Count'],
                    index=[str(self.end_year),
                           str(self.end_year - 1),
                           'Net gain/loss']
                )
            ,
            'tb2':
                pd.DataFrame(
                    columns=['Entering MD', 'Entering DO', 'Leaving MD', 'Leaving DO'],
                    index=[str(y) for y in range(self.end_year - 10, self.end_year + 1)]
                )
            ,
            'tb3 -current':
                pd.DataFrame(
                    columns=['Entered', 'Left', 'Net'],
                    index=['MD', 'DO', 'Total']
                )
            ,
            'tb3 -previous':
                pd.DataFrame(
                    columns=['Entered', 'Left', 'Net'],
                    index=['MD', 'DO', 'Total']
                )
            ,
            'tb4a':
                pd.DataFrame(
                    columns=['Entering 000-006', 'Leaving 000-006',
                             'Entering 100 + 199', 'Leaving 100 + 199',
                             'Entering 400', 'Leaving 400'
                             ],
                    index=[str(y) for y in range(self.end_year - 10, self.end_year + 1)]
                )
            ,
            'tb4b':
                pd.DataFrame(
                    columns=['EM Entering', 'EM Leaving',
                             'EM w/FP Entering', 'EM w/FP Leaving'],
                    index=[str(y) for y in range(self.end_year - 10, self.end_year + 1)]
                )
            ,
            'tb5':
                pd.DataFrame(
                    columns=['FP* MD Entering', 'FP* MD Leaving',
                             'FP* DO Entering', 'FP* DO Leaving'],
                    index=[str(y) for y in range(self.end_year - 10, self.end_year + 1)]
                )
            ,
            'tb6 -current -enter':
                pd.DataFrame(
                    columns=['000', '001-006', '010 -Res', 'NA1'],
                    index=['MD', 'DO', 'Total']
                )
            ,
            'tb6 -current -exit':
                pd.DataFrame(
                    columns=['000', '001-006', '010 -Res', 'NA1'],
                    index=['MD', 'DO', 'Total']
                )
            ,
            'tb6 -previous -enter':
                pd.DataFrame(
                    columns=['000', '001-006', '010 -Res', 'NA1'],
                    index=['MD', 'DO', 'Total']
                )
            ,
            'tb6 -previous -exit':
                pd.DataFrame(
                    columns=['000', '001-006', '010 -Res', 'NA1'],
                    index=['MD', 'DO', 'Total']
                )
            ,
            'tb7a':
                pd.DataFrame(
                    columns=['000 Entering',
                             '001-006 Entering',
                             '100 Entering',
                             '199 Entering',
                             '400 Entering',
                             '010 wFP Entering',

                             '000 Leaving',
                             '001-006 Leaving',
                             '100 Leaving',
                             '199 Leaving',
                             '400 Leaving',
                             '010 wFP Leaving'
                             ],
                    index=[str(y) for y in range(self.start_year, self.end_year + 1)]
                )
            ,
            'tb7b':
                pd.DataFrame(
                    columns=['001-006 MD Entering',
                             '010 wFP MD Entering',
                             'Total FP Entering',

                             '001-006 DO Entering',
                             '010 wFP DO Entering',
                             'Total DO Entering',

                             'Total 001-006 Entering',
                             'Total 010 wFP Entering',

                             '001-006 MD Leaving',
                             '010 wFP MD Leaving',
                             'Total FP Leaving',

                             '001-006 DO Leaving',
                             '010 wFP DO Leaving',
                             'Total DO Leaving',

                             'Total 001-006 Leaving',
                             'Total 010 wFP Leaving'
                             ],
                    index=[str(y) for y in range(self.start_year, self.end_year + 1)]
                )
            ,
            'tb8':
                pd.DataFrame(
                    columns=['Entered',
                             'Left',
                             'MD Entered',
                             'DO Entered',
                             'MD Leaving',
                             'DO Leaving',
                             'UI Grad Entering',
                             'UI Grad Leaving',
                             'UI Grad Total',
                             'IMG Grad Entering',
                             'IMG Grad Leaving',
                             'IMG Grad Total'
                             ],
                    index=[str(y) for y in range(self.start_year, self.end_year + 1)]
                )
            ,
            'tb9':
                pd.DataFrame(
                    columns=['Total',
                             'MD',
                             'DO',
                             '000',
                             '001-006',
                             '010 w/FP',
                             '100',
                             '400',
                             '200',
                             '000-006'
                             ],
                    index=[str(y) for y in range(self.start_year, self.end_year + 1)]
                )
            ,
            'tb10':
                pd.DataFrame(
                    columns=['FP w/010', 'PC', 'OB', 'PC + OB', 'All'],
                    index=[str(y) for y in list(range(2000, self.max_fifth_year, 5))
                           + list(range(self.max_fifth_year, self.end_year + 1))]
                )
            ,
            'tb11a':
                pd.DataFrame(
                    columns=['Total', '<5k', '5k-14999', '15k-49999', '>50k'],
                    index=[str(y) + name for y in range(self.end_year - 4, self.end_year + 1) for name in
                           [' 000', ' 005', ' 010W', ' Total']]
                )
            ,
            'tb11b':
                pd.DataFrame(
                    columns=['Total', '<5k', '5k-14999', '15k-49999', '>50k'],
                    index=[str(y) + name for y in range(self.end_year - 4, self.end_year + 1) for name in
                           [' 000', ' 005', ' 010W', ' Total']]
                )
            ,
            'tb12':
                pd.DataFrame(
                    columns=['Total'
                             ],
                    index=[str(y) for y in range(self.start_year, self.end_year + 1)]
                )
            ,
            'tb13a':
                pd.DataFrame(
                    columns=[str(y) for y in range(self.end_year - 4, self.end_year + 1)],
                    index=[' <5k', ' 5-15', ' 15-50', ' 50+']
                )
            ,
            'tb13b':
                pd.DataFrame(
                    columns=[str(y) for y in range(self.end_year - 4, self.end_year + 1)],
                    index=[' <5k', ' 5-15', ' 15-50', ' 50+']
                )
            ,
            'tb -gender':
                pd.DataFrame(
                    columns=['Male Yearend',
                             'Female Yearend',
                             'Male Adds',
                             'Female Adds',
                             'Male Delete',
                             'Female Delete'
                             ],
                    index=[str(y) for y in range(self.end_year - 10, self.end_year + 1)]
                )
        }
        return tables

    def build_balance_df(self):
        '''

        For every balance query, there will be two tables. One to record the raw and adjusted counts of adds and deletes
        and another to identify which providers made up for this difference.

        -count_tables: tables for holding counts
        -error_tables: tables for holding providers that may not have been in adds or deletes but because of
                        specialty changes, should be counted as an add or delete. Also finds specific adds
                        deletes that are actually errors.

        :return:
        '''
        count_tables = dict([(key,value) for key in self.balancing_query_keys for value in [pd.DataFrame(
                                columns=['In Previous', 'In Add', 'In Delete', 'In Current',
                                         'In Transfer', 'In Change', 'pAdd', 'pDelete', 'Is Error']
                            )]
        ])

        error_tables = dict([(key, value) for key in self.balancing_query_keys for value in [pd.DataFrame(
            columns=['Hcp Id','Year','In Previous', 'In Add', 'In Delete', 'In Current',
                     'In Transfer', 'In Change', 'pAdd', 'pDelete', 'Is Error']
        )]
                             ])

        tables = {'error':error_tables,
                  'count':count_tables}

        return tables

    def environment_check(self):
        queries = {
            'tb 1':
                '''
                SELECT COUNT(`Hcp Id`)
                '''
        }
        print("-->initiate environment check...")
        if np.version.version != '1.18.1':
            print("-->WARNING:np version suggested: 1.18.1, current: {}".format(np.version.version))
        if pd.__version__ != '1.0.3':
            print("-->WARNING:pd version suggested: 1.0.3, current: {}".format(pd.__version__))
        if sys.version.split()[0].strip() != '3.7.0':
            print("-->WARNING:Python version suggested: 3.7.0, current: {}".format(sys.version.split()[0]))
        print("-->environment check complete.")

    def check_directory(self, success=None):
        print("-->initiate directory check...")
        rt = self.rt
        errors = self.directory_check_errors
        provider = self.provider

        # checks specifically required of yearend analysis

        '''ensure provider "database" is in directory'''
        try:
            assert '{} Input'.format(provider) in os.listdir(rt)
            os.chdir(rt + '/{} Input/'.format(provider))

            files = os.listdir()

            for i in range(self.start_year, self.end_year + 1):
                try:
                    assert '{} {}.xls'.format(i, provider) in files
                except AssertionError:
                    success = False
                    errors.append("File not found: {} at directory: {}".format(
                        '{} {}.xls'.format(i, provider),
                        os.getcwd()
                    )
                    )

            self.read_from_dir = os.getcwd() + "/"
            os.chdir(rt)
        except AssertionError:
            errors.append("Folder not found: {} Input at directory: {}".format(provider, rt))

            success = False

        # checks specifically required of per request analysis

        '''ensure the Input folder is in the directory'''
        try:
            assert 'Input'.format(provider) in os.listdir(rt)
        except AssertionError:
            errors.append("Folder not found: Input at directory: {}".format(provider, rt))
            success = False

        # checks required of all analyses
        '''ensure the Output folder is in the directory'''
        try:
            assert 'Output' in os.listdir(rt)
            self.write_to_dir = self.rt + '/Output/'
            try:
                assert 'Balancing Output' in os.listdir(self.write_to_dir)
            except AssertionError:
                errors.append("Folder not found: Balancing Output at directory: {}".format(rt))
        except AssertionError:
            errors.append("Folder not found: Output at directory: {}".format(rt))
            success = False

        '''ensure Census Population and Reference Data folder is in the directory'''

        try:
            assert 'Census Population and Reference Data'.format(provider) in os.listdir(rt)
            try:
                assert '1990 Census Data.xlsx' in os.listdir(rt + '/Census Population and Reference Data')
            except AssertionError:

                errors.append("\tFile not found: '1990 Census Data.xlsx' | \n\tat directory: {}".format(provider,
                                                                                                        rt + '/Census Population and Reference Data'))

            try:
                assert 'Recent Census Data.xlsx' in os.listdir(rt + '/Census Population and Reference Data')
            except AssertionError:
                errors.append("\tFile not found: '2010 Census Data.xlsx' | \n\tat directory: {}".format(provider,
                                                                                                        rt + '/Census Population and Reference Data'))

            try:
                assert 'County by Regions.xls' in os.listdir(rt + '/Census Population and Reference Data')
            except AssertionError:
                errors.append("\tFile not found: 'County by Regions.xlsx' | \n\tat directory: {}".format(provider,
                                                                                                         rt + '/Census Population and Reference Data'))

            try:
                assert 'Physician Specialty Guide.xls' in os.listdir(rt + '/Census Population and Reference Data')
            except AssertionError:
                errors.append(
                    "\tFile not found: 'Physician Specialty Guide.xls' | \n\tat directory: {}".format(provider,
                                                                                                      rt + '/Census Population and Reference Data'))
            try:
                assert 'RUCC 2013.xls' in os.listdir(rt + '/Census Population and Reference Data')
            except AssertionError:
                errors.append("\tFile not found: 'RUCC 2013.xls' | \n\tat directory: {}".format(provider,
                                                                                                rt + '/Census Population and Reference Data'))
            self.reference_dir = rt + '/Census Population and Reference Data'
        except AssertionError:
            errors.append("Folder not found: Census and Population at directory: {}".format(provider,
                                                                                            rt + '/Census Population and Reference Data'))
            success = False

        # checks errors end. print out errors and return

        if not errors:
            success = True
            print("-->\t\t>Check success: {}<".format(success))
            print("-->\t\t>read from directory set to: {}<".format(self.read_from_dir))
            print("-->\t\t>Check success: {}<".format(self.write_to_dir))
        else:

            print("-->Directory Check: Failed. The following errors were found")
            for error in errors:
                print("-->\t\t>{}<".format(error))

        print("-->directory check complete")

        return success

    def yearend_analysis_main(self):

        '''
        *****************************************************************************************************
        4. Main program flow
        Input: None
        Output: None
        For each year, we carry out the following actions in order:

            1. Read in data
            2. Validate data (if validation parameter is set to True in command line settings
            3. Store data to temporary storage containers
                - the program read at most five sheets from the Yearend data per iteration. Previous year's yearend
                  is carried over
            4. Build a cubic space from the query results of source data within the scope( For balancing and
               summarizing, we need current year adds, deletes, yearend (transfer and change is included for
               convenience) in addition to previous year's yearend data.
               The scope will hitherto refer to previous, add, delete, current sheets for any particular year).

            5. Execute all analyses permitted by the command prompt settings the user defined by drawing from the
               results of queries stored in the cubic space and write results to the corresponding tables.

            6. Output tables if necessary.

        NOTE: *****************************************************************************************************
            For understanding code, go to the following functions in order:

            build_cubic_space
            build_summary_df
            summarize
            build_balancing_df
            balance
            ******************************************************************************************************

        :return:
        '''

        previous_year = None

        for year in range(self.start_year, self.end_year + 1):

            ''' Prepare a three axial feature space made up of sheets, queries and a user defined id along which data will
                    be aggregated'''

            ''' Construct the file name'''
            file_name = '{} {}.xls'.format(year, self.provider)

            ''' Read in data '''
            data, success = self.read_data(
                file_name,
                sheets={
                    "Yearend": None,
                    "ADD": None,
                    "DEL": None,
                    "TRF": None,
                    "CHG": None},
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

            if success == False or data == None:
                analysis_success = False
                return analysis_success

            ''' If validation flag = true, then perform the validation'''
            validation_success = True
            if self.validation_error_override == 'False':
                validation_success = self.validate_ya_data(data, year)

            ''' If validation failed and validation flag = true, then break, otherwise continue with analysis'''
            if self.validation_error_override == True and validation_success == False:
                return validation_success

            ''' Take the validated data and put it in temporary containers'''
            ''' Notice in the line above, temp_previous is not read again but rather its reference is redirected to
            that of the "previous" current year. A deep copy here is needed because dataframe is mutable and passed
            by reference. If we don't do deep copy, temp_current will be changed to point toward the new current and 
            temp_previous will follow. If we deep copy the previous current, then temp_previous will refer to a
            different object.'''

            if year != self.start_year:
                data['Previous'] = previous_year

            '''Build Cubic Space'''
            self.build_cubic_space(data)

            previous_year = data['Yearend']

            ''' Perform Analyses if permitted'''
            if self.summarize == 'True':
                if self.summary_df == None:
                    '''Summarize function call'''
                    self.summary_df = self.build_summary_df()
                t1 = time.time()

                self.summarize_data(year)
                t2 = time.time()
                print("-->elapsed summary time: {}".format(t2 - t1))

            if self.balance == 'True' and year in range(self.end_year-11,self.end_year+1):
                if self.balance_df == None:
                    '''Balancing function call'''
                    self.balance_df = self.build_balance_df()
                t1 = time.time()
                self.balance_data(year)
                t2 = time.time()
                print("-->elapsed balance time: {}".format(time.time() - t1))

            if self.meded != 'None':

                pass

        '''Write to output file if needed'''

        if self.balance == 'True' or self.summarize == 'True':

            self.output_results()

        if self.adhoc == 'True':

            self.adhoc()

    def read_data(self, file, sheets=None, converters=None, header=0):
        success = None
        print("-->reading data...")
        os.chdir(self.read_from_dir)

        for key in sheets.keys():

            try:
                xls = pd.ExcelFile(file)
                print("-->\t\treading file: {} sheet: {}".format(file, key))
                try:
                    sheet_data = pd.read_excel(xls,
                                               key,
                                               converters=converters,
                                               header=header,
                                               na_filter=False,
                                               engine='xlrd')

                    for name,tp in converters.items():
                        if tp == int:
                            sheet_data[name].replace("",0,inplace=True)

                    sheets[key] = sheet_data
                except xlrd.biffh.XLRDError:
                    self.file_read_errors.append("-->\t\t\t\tsheet: '{}' not found in file: {}".format(key, file))
                print("-->\t\tread file complete.".format(file))
            except PermissionError:
                self.file_read_errors.append(
                    "-->\t\tfile read failed. if file is open, close file and rerun. saving read history and exiting")
                success = False
        for error in self.file_read_errors:
            print('-->\t\t{}'.format(error))
        os.chdir(self.rt)

        return sheets, success

    def validate_ya_data(self, data, year):
        error_list = []
        error_prefix_str = '-->\t\t\t\tYear ' + str(
            year) + ' Sheet {} || row number {} || attribute >{}< with value >{}< has error >{}< type = {}'
        self.data_validation_errors[year] = []
        current_year_errors = self.data_validation_errors[year]
        for sheet_name, table in data.items():
            headings = [
                'Hcp Id', 'First Name', 'Middle Name', 'Last Name', 'Gender', 'Birth Year', 'Age',
                'School Id',
                'Specialty Id', 'Title', 'Degree Id', 'Npi Number',
                'City', 'Address1', 'Ruca', 'Zip', 'City Pop', 'County Name',
                'Type Id', 'Site Type Id', 'Worksite Id', 'Act Id',
            ]
            heading_all_present = True
            data_valid = None
            for heading in headings:
                if heading not in table.columns:
                    if heading == "Ruca" and sheet_name != 'Yearend':
                        pass
                    else:
                        current_year_errors.append("-->\t\t\t\theading: {} not found on sheet {} ".format(heading,
                                                                                                          sheet_name
                                                                                                          )
                                                   )
                    heading_all_present = False
            if heading_all_present == True:

                row_count, _ = table.shape
                for i in range(0, row_count):
                    for heading in headings:
                        value = table.at[i, heading]
                        if heading == "Hcp Id" and value == "":
                            current_year_errors.append(error_prefix_str.format(sheet_name,
                                                                               i + 1,
                                                                               heading,
                                                                               value,
                                                                               "missing",
                                                                               'essential'))
                        if heading == "First Name" and value == "":
                            current_year_errors.append(error_prefix_str.format(sheet_name,
                                                                               i + 1,
                                                                               heading,
                                                                               value,
                                                                               "missing",
                                                                               'essential'))
                        if heading == "Last Name" and value == "":
                            current_year_errors.append(error_prefix_str.format(sheet_name,
                                                                               i + 1,
                                                                               heading,
                                                                               value,
                                                                               "missing",
                                                                               'essential'))
                        if heading == "Gender" and (value == "" or value not in ['M', 'F', 'm', 'f']):
                            print(heading, value, value == "")
                            current_year_errors.append(error_prefix_str.format(sheet_name,
                                                                               i + 1,
                                                                               heading,
                                                                               value,
                                                                               "missing",
                                                                               'essential'))
                        if heading == "Birth Year":
                            if value != "" and np.issubdtype(value.dtype, np.number) == True:
                                difference = year - value
                                if self.provider == 'PHY' and difference < 28 or difference > 90:
                                    # current_year_errors.append(error_prefix_str.format(sheet_name,
                                    #                                                            i + 1,
                                    #                                                            heading,
                                    #                                                            difference,
                                    #                                                            "not in range 28 - 90"))
                                    pass
                                if self.provider == 'APN':
                                    # not implemented
                                    pass
                                if self.provider == 'PHA':
                                    # not implemented
                                    pass
                                if self.provider == 'PA':
                                    # not implemented
                                    pass
                                if self.provider == 'DDS':
                                    # not implemented
                                    pass
                            else:
                                current_year_errors.append(error_prefix_str.format(sheet_name,
                                                                                   i + 1,
                                                                                   heading,
                                                                                   value,
                                                                                   "missing",
                                                                                   'essential'))
                        if heading == "Age" :

                            if value == "":
                                current_year_errors.append(error_prefix_str.format(sheet_name,
                                                                                   i + 1,
                                                                                   heading,
                                                                                   value,
                                                                                   "missing",
                                                                                   'essential'))

                            elif int(value) < 28 or int(value) > 85:

                                current_year_errors.append(
                                    error_prefix_str.format(sheet_name,
                                                           i + 1,
                                                           heading,
                                                           value,
                                                           "not in range 28 - 85",
                                                           'essential')
                                )
                        if heading == "School Id" and value == "":
                            current_year_errors.append(error_prefix_str.format(sheet_name,
                                                                               i + 1,
                                                                               heading,
                                                                               value,
                                                                               "missing",
                                                                               ""))
                        if heading == "Specialty Id" and value == "":
                            current_year_errors.append(error_prefix_str.format(sheet_name,
                                                                               i + 1,
                                                                               heading,
                                                                               value,
                                                                               "missing",
                                                                               'essential'))
                        if heading == "Title" and value == "":
                            current_year_errors.append(error_prefix_str.format(sheet_name,
                                                                               i + 1,
                                                                               heading,
                                                                               value,
                                                                               "missing",
                                                                               'essential'))
                        if heading == "Degree Id" and value == "":
                            current_year_errors.append(error_prefix_str.format(sheet_name,
                                                                               i + 1,
                                                                               heading,
                                                                               value,
                                                                               "missing",
                                                                               ""))
                        if heading == "Npi Number":
                            '''
                            if value != "":
                                if len(value.strip()) != 10:
                                    current_year_errors.append(error_prefix_str.format(sheet_name,
                                                                                               i + 1,
                                                                                               heading,
                                                                                               value,
                                                                                               "wrong length"))
                            else:
                                current_year_errors.append(error_prefix_str.format(sheet_name,
                                                                                           i + 1,
                                                                                           heading,
                                                                                           value,
                                                                                           "missing"))'''
                        if heading == "City" and value == "":
                            current_year_errors.append(error_prefix_str.format(sheet_name,
                                                                               i + 1,
                                                                               heading,
                                                                               value,
                                                                               "missing",
                                                                               'essential'))
                        if heading == "Address1" and value == "":
                            current_year_errors.append(error_prefix_str.format(sheet_name,
                                                                               i + 1,
                                                                               heading,
                                                                               value,
                                                                               "missing",
                                                                               ""))
                        if heading == "Ruca" and value == "":
                            current_year_errors.append(error_prefix_str.format(sheet_name,
                                                                               i + 1,
                                                                               heading,
                                                                               value,
                                                                               "missing",
                                                                               ""))
                        if heading == "Zip":
                            if value != "":
                                is_num = False
                                try:
                                    is_num = np.issubdtype(value.dtype, np.number)
                                except AttributeError:
                                    is_num = isinstance(value, int)
                                if not is_num:
                                    current_year_errors.append(error_prefix_str.format(sheet_name,
                                                                                       i + 1,
                                                                                       heading,
                                                                                       value,
                                                                                       "Not a number, possibly read wrong",
                                                                                       ""))
                            else:
                                current_year_errors.append(error_prefix_str.format(sheet_name,
                                                                                   i + 1,
                                                                                   heading,
                                                                                   value,
                                                                                   "missing",
                                                                                   'essential'))
                        if heading == "City Pop" and value == "":
                            current_year_errors.append(error_prefix_str.format(sheet_name,
                                                                               i + 1,
                                                                               heading,
                                                                               value,
                                                                               "missing"))
                        if heading == "County Name" and value == "":
                            current_year_errors.append(error_prefix_str.format(sheet_name,
                                                                               i + 1,
                                                                               heading,
                                                                               value,
                                                                               "missing",
                                                                               'essential'))
                        if heading == "Type Id" and value == "":
                            current_year_errors.append(error_prefix_str.format(sheet_name,
                                                                               i + 1,
                                                                               heading,
                                                                               value,
                                                                               "missing",
                                                                               'essential'))
                        if heading == "Site Type Id" and value == "":
                            '''
                            current_year_errors.append(error_prefix_str.format(sheet_name,
                                                                                       i + 1,
                                                                                       heading,
                                                                                       value,
                                                                                       "missing",
                                                                                       ""))'''
                        if heading == "Worksite Id" and value == "":
                            current_year_errors.append(error_prefix_str.format(sheet_name,
                                                                               i + 1,
                                                                               heading,
                                                                               value,
                                                                               "missing",
                                                                               'essential',
                                                                               ""))
                        if heading == "Act Id" and value == "":
                            current_year_errors.append(error_prefix_str.format(sheet_name,
                                                                               i + 1,
                                                                               heading,
                                                                               value,
                                                                               "missing",
                                                                               ""))

        for error in current_year_errors:
            if 'essential' in error:
                print(error)

        return self.data_validation_errors[year] == []

    def get_result(self, tup, output='sum'):
        
        (sheet, q_name) = tup

        fs = self.feature_space
        idict = self.idict

        raw = fs[
            idict['sheets'][sheet],
            idict['queries'][q_name]
        ]

        if output == 'sum':
            return np.sum(raw)
        elif output == 'raw':
            return raw

    def summarize_data(self, year):

        print("-->updating tables with data...")

        '''assignments to simplify coding'''
        fs = self.feature_space

        idict = self.idict
        ''' Tables are organized by the years they require analysis to be performed upon. For specific guide to
        populating tables ,refer to tb6 or refer to the guide.'''

        if year == self.end_year - 1:

            ''' Tables can be turned off by assigning the boolean before it to False'''
            tb3p = True
            if tb3p:

                ''' Get the summary_df called tb3 -previous'''
                tb = self.summary_df['tb3 -previous']


                md_enter = np.sum(fs[
                    idict['sheets']['ADD'],
                    idict['queries']["-Title=MD"]
                ])

                md_exit = np.sum(fs[
                    idict['sheets']['DEL'],
                    idict['queries']["-Title=MD"]
                ])

                do_enter = np.sum(fs[
                    idict['sheets']['ADD'],
                    idict['queries']["-Title=DO"]
                ])
                do_exit = np.sum(fs[
                    idict['sheets']['DEL'],
                    idict['queries']["-Title=DO"]
                ])

                tb.loc['MD', 'Entered'] = md_enter
                tb.loc['MD', 'Left'] = md_exit

                tb.loc['DO', 'Entered'] = do_enter
                tb.loc['DO', 'Left'] = do_exit

                tb.loc['Total', 'Entered'] = md_enter + do_enter
                tb.loc['Total', 'Left'] = md_exit + do_exit

                tb.loc['MD', 'Net'] = md_enter - md_exit
                tb.loc['DO', 'Net'] = do_enter - do_exit

                tb.loc['Total', 'Net'] = tb.loc['Total', 'Entered'] - tb.loc['Total', 'Left']

            tb6_previous = True
            if tb6_previous:
                '''designate table writing to'''
                tb_enter = self.summary_df['tb6 -previous -enter']
                tb_exit = self.summary_df['tb6 -previous -exit']

                '''name the queries whose results we need'''
                r1_q = ["-Sp=000 -Title=MD", "-Sp=001-006 -Title=MD", "-Sp=010 -Res=000 -Title=MD"]
                r2_q = ["-Sp=000 -Title=DO", "-Sp=001-006 -Title=DO", "-Sp=010 -Res=000 -Title=DO"]

                '''[(sheet, q) for sheet in ["Add"] for q in r1_q]
                
                iterate over the query name lists from above and pair each term with the sheet name we want to use
                as the source data. Put both of these into a tuple. Returns a list of tuples'''

                r1_q_enter = [(sheet, q) for sheet in ['ADD'] for q in r1_q]
                r2_q_enter = [(sheet, q) for sheet in ['ADD'] for q in r2_q]
                r1_q_exit = [(sheet, q) for sheet in ['DEL'] for q in r1_q]
                r2_q_exit = [(sheet, q) for sheet in ['DEL'] for q in r2_q]

                '''
                map(self.__get_result__, r1_q_enter)
                iterate over the list of sheet name and query name pair tuples by passing each of these tuples into
                __get_result__ using map. Map is a python built in function that passes a list of inputs to a function
                
                r for r in
                this means "this list consists of all r that is in ..." this case in the iterable of results obtained
                after passing all the tuples into __get_result__. 
                '''
                r1_q_enter = [r for r in map(self.get_result, r1_q_enter)]
                r2_q_enter = [r for r in map(self.get_result, r2_q_enter)]
                r1_q_exit = [r for r in map(self.get_result, r1_q_exit)]
                r2_q_exit = [r for r in map(self.get_result, r2_q_exit)]

                '''perform calculations'''
                sum_enter = sum(r1_q_enter + r2_q_enter)
                sum_exit = sum(r1_q_exit + r2_q_exit)

                sum_fam_enter = sum(r1_q_enter[:-1] + r2_q_enter[:-1])
                sum_fam_exit = sum(r1_q_exit[:-1] + r2_q_exit[:-1])

                '''write to table
                
                Writing to tables is often done by the following:
                
                1. Designate where on the table we are writing to. 
                
                    this is done with something like 
                            
                            tb_enter.iloc[0, :]
                            
                    which means "in table tb_enter, use numeric indexes to designate the 0th row and all of the row's
                    columns. 
                    
                2. The equal sign will attempt to assign whatever is after it to the designated location. If the 
                container that is being assigned into the table does not fit the shape, there will be an error raised
                
                3. Build the row you want to insert. Lists and nparrays are all acceptable. a few examples follow
                
                    r1_q_enter + [""] we take a resulting row and add an extra empty cell to the row. 
                
                    ["", "", sum_fam_enter, sum_enter] we take two sums calculated and put them in a row with two empty
                    cells.
                                        
                
                '''

                tb_enter.iloc[0, :] = r1_q_enter + [""]
                tb_enter.iloc[1, :] = r2_q_enter + [""]
                tb_enter.iloc[2, :] = ["", "", sum_fam_enter, sum_enter]

                tb_exit.iloc[0, :] = r1_q_exit + [""]
                tb_exit.iloc[1, :] = r2_q_exit + [""]
                tb_exit.iloc[2, :] = ["", "", sum_fam_exit, sum_exit]

        if year == self.end_year:

            tb1 = True
            if tb1:
                '''designate table writing to'''
                tb = self.summary_df['tb1']

                '''withdraw list of hcp'''
                previous_md = self.get_result(('Previous', "-Title=MD"))

                previous_do = self.get_result(('Previous', "-Title=DO"))

                current_md = self.get_result(('Yearend', "-Title=MD"))

                current_do = self.get_result(('Yearend', "-Title=DO"))

                '''filter list of hcp'''

                current = current_md + current_do
                previous = previous_md + previous_do

                tb.loc[str(self.end_year - 1), 'Count'] = previous
                tb.loc[str(self.end_year), 'Count'] = current
                tb.loc['Net gain/loss', 'Count'] = current - previous

            tb3c = True
            if tb3c:
                '''designate table writing to'''
                tb = self.summary_df['tb3 -current']
                '''withdraw results'''
                md_enter = self.get_result(('ADD', "-Title=MD"))

                md_exit = self.get_result(('DEL', "-Title=MD"))

                do_enter = self.get_result(('ADD', "-Title=DO"))

                do_exit = self.get_result(('DEL', "-Title=DO"))

                '''perform calculations'''
                tb.loc['MD', 'Entered'] = md_enter
                tb.loc['MD', 'Left'] = md_exit

                tb.loc['DO', 'Entered'] = do_enter
                tb.loc['DO', 'Left'] = do_exit

                tb.loc['Total', 'Entered'] = md_enter + do_enter
                tb.loc['Total', 'Left'] = md_exit + do_exit

                tb.loc['MD', 'Net'] = md_enter - md_exit
                tb.loc['DO', 'Net'] = do_enter - do_exit

                tb.loc['Total', 'Net'] = tb.loc['Total', 'Entered'] - tb.loc['Total', 'Left']

            tb6_current = True
            if tb6_current:
                '''designate table writing to'''
                tb_enter = self.summary_df['tb6 -current -enter']
                tb_exit = self.summary_df['tb6 -current -exit']
                '''withdraw results'''
                r1_q = ["-Sp=000 -Title=MD", "-Sp=001-006 -Title=MD", "-Sp=010 -Res=000 -Title=MD"]
                r2_q = ["-Sp=000 -Title=DO", "-Sp=001-006 -Title=DO", "-Sp=010 -Res=000 -Title=DO"]

                r1_q_enter = [(sheet, q) for sheet in ['ADD'] for q in r1_q]
                r2_q_enter = [(sheet, q) for sheet in ['ADD'] for q in r2_q]
                r1_q_exit = [(sheet, q) for sheet in ['DEL'] for q in r1_q]
                r2_q_exit = [(sheet, q) for sheet in ['DEL'] for q in r2_q]

                r1_q_enter = [r for r in map(self.get_result, r1_q_enter)]
                r2_q_enter = [r for r in map(self.get_result, r2_q_enter)]
                r1_q_exit = [r for r in map(self.get_result, r1_q_exit)]
                r2_q_exit = [r for r in map(self.get_result, r2_q_exit)]

                '''perform calculations'''

                sum_enter = sum(r1_q_enter + r2_q_enter)
                sum_exit = sum(r1_q_exit + r2_q_exit)

                sum_fam_enter = sum(r1_q_enter[:-1] + r2_q_enter[:-1])
                sum_fam_exit = sum(r1_q_exit[:-1] + r2_q_exit[:-1])

                '''write to table'''

                tb_enter.iloc[0, :] = r1_q_enter + [""]
                tb_enter.iloc[1, :] = r2_q_enter + [""]
                tb_enter.iloc[2, :] = ["", "", sum_fam_enter, sum_enter]

                tb_exit.iloc[0, :] = r1_q_exit + [""]
                tb_exit.iloc[1, :] = r2_q_exit + [""]
                tb_exit.iloc[2, :] = ["", "", sum_fam_exit, sum_exit]

        if year >= self.end_year - 10 and year <= self.end_year:
            tb2 = True
            if tb2:
                '''designate table writing to'''
                tb = self.summary_df['tb2']

                '''withdraw results'''

                row = [('ADD', "-Title=MD"), ('ADD', "-Title=DO"),
                       ('DEL', "-Title=MD"), ('DEL', "-Title=DO"),
                       ]

                row = [r for r in map(self.get_result, row)]

                '''perform calculations'''
                '''write to table'''

                tb.loc[str(year), :] = row
            tb4a = True
            if tb4a:
                '''designate table writing to'''
                tb = self.summary_df['tb4a']

                '''withdraw results'''

                row = [('ADD', "-Sp=000-006"), ('DEL', "-Sp=000-006"),
                       ('ADD', "-Sp=100,199"), ('DEL', "-Sp=100,199"),
                       ('ADD', "-Sp=400"), ('DEL', "-Sp=400")
                       ]

                row = [r for r in map(self.get_result, row)]

                '''perform calculations'''
                '''write to table'''

                tb.loc[str(year), :] = row

            tb4b = True
            if tb4b:
                '''designate table writing to'''
                tb = self.summary_df['tb4b']

                '''withdraw results'''
                columns = ['EM Entering', 'EM Leaving',
                           'EM w/FP Entering', 'EM w/FP Leaving']
                row = [('ADD', "-Sp=010"), ('DEL', "-Sp=010"),
                       ('ADD', "-Sp=010 -Res=000"), ('DEL', "-Sp=010 -Res=000")
                       ]

                row = [r for r in map(self.get_result, row)]

                '''perform calculations'''
                '''write to table'''

                tb.loc[str(year), :] = row
            tb5 = True
            if tb5:
                '''designate table writing to'''
                tb = self.summary_df['tb5']

                '''withdraw results'''
                columns = ['FP* MD Entering',
                           'FP* MD Leaving',
                           'FP* DO Entering',
                           'FP* DO Leaving']

                row = [('ADD', "-Sp=000-006 -Title=MD"),
                       ('DEL', "-Sp=000-006 -Title=MD"),
                       ('ADD', "-Sp=000-006 -Title=DO"),
                       ('DEL', "-Sp=000-006 -Title=DO")
                       ]

                row = [r for r in map(self.get_result, row)]

                '''perform calculations'''
                '''write to table'''

                tb.loc[str(year), :] = row
            tb_gender = True
            if tb_gender:
                '''designate table writing to'''
                tb = self.summary_df['tb -gender']

                '''withdraw results'''
                columns = ['Male Yearend',
                           'Female Yearend',
                           'Male Adds',
                           'Female Adds',
                           'Male Delete',
                           'Female Delete'
                           ]

                row = [('Yearend', "-Gender=M"),
                       ('Yearend', "-Gender=F"),
                       ('ADD', "-Gender=M"),
                       ('ADD', "-Gender=F"),
                       ('DEL', "-Gender=M"),
                       ('DEL', "-Gender=F"),
                       ]

                row = [r for r in map(self.get_result, row)]

                '''perform calculations'''
                '''write to table'''

                tb.loc[str(year),:] = row

        if year >= self.start_year and year <= self.end_year:
            tb7a = True
            if tb7a:
                '''designate table writing to'''
                tb = self.summary_df['tb7a']

                '''withdraw results'''
                columns = ['000 Entering',
                           '001-006 Entering',
                           '100 Entering',
                           '199 Entering',
                           '400 Entering',
                           '010 wFP Entering',

                           '000 Leaving',
                           '001-006 Leaving',
                           '100 Leaving',
                           '199 Leaving',
                           '400 Leaving',
                           '010 wFP Leaving'
                           ]

                row = [('ADD', "-Sp=000"),
                       ('ADD', "-Sp=001-006"),
                       ('ADD', "-Sp=100"),
                       ('ADD', "-Sp=199"),
                       ('ADD', "-Sp=400"),
                       ('ADD', "-Sp=010 -Res=000"),

                       ('DEL', "-Sp=000"),
                       ('DEL', "-Sp=001-006"),
                       ('DEL', "-Sp=100"),
                       ('DEL', "-Sp=199"),
                       ('DEL', "-Sp=400"),
                       ('DEL', "-Sp=010 -Res=000"),
                       ]

                row = [r for r in map(self.get_result, row)]

                '''perform calculations'''
                '''write to table'''

                tb.loc[str(year), :] = row

            tb7b = True
            if tb7b:
                '''designate table writing to'''
                tb = self.summary_df['tb7b']

                '''withdraw results'''

                row = [('ADD', "-Sp=000-006 -Title=MD"),
                       ('ADD', "-Sp=010 -Res=000 -Title=MD"),
                       ('ADD', "-Sp=000-006 -Title=DO"),
                       ('ADD', "-Sp=010 -Res=000 -Title=DO"),
                       ('DEL', "-Sp=000-006 -Title=MD"),
                       ('DEL', "-Sp=010 -Res=000 -Title=MD"),
                       ('DEL', "-Sp=000-006 -Title=DO"),
                       ('DEL', "-Sp=010 -Res=000 -Title=DO"),
                       ]

                row = [r for r in map(self.get_result, row)]
                columns = [
                           '000-006 MD Entering',
                           '010 wFP MD Entering',
                           'Total FP Entering',

                           '000-006 DO Entering',
                           '010 wFP DO Entering',
                           'Total DO Entering',

                           'Total 001-006 Entering',
                           'Total 010 wFP Entering',

                           ]

                row = [
                    row[0],
                    row[1],
                    row[0] + row[1],
                    row[2],
                    row[3],
                    row[2] + row[3],
                    row[0] + row[2],
                    row[1] + row[3],

                    row[4],
                    row[5],
                    row[4] + row[5],
                    row[6],
                    row[7],
                    row[6] + row[7],
                    row[4] + row[6],
                    row[5] + row[7],
                ]
                '''perform calculations'''
                '''write to table'''

                tb.loc[str(year), :] = row

            tb8 = True
            if tb8:
                '''designate table writing to'''
                tb = self.summary_df['tb8']

                '''withdraw results'''
                columns = [
                           'Entered',
                           'Left',
                           'MD Entered',
                           'DO Entered',
                           'MD Leaving',
                           'DO Leaving',

                           'UI Grad Entering',
                           'UI Grad Leaving',
                           'UI Grad Total',
                           'IMG Grad Entering',
                           'IMG Grad Leaving',
                           'IMG Grad Total'
                           ]
                row = [('ADD', "-*"),
                       ('DEL', "-*"),
                       ('ADD', "-Title=MD"),
                       ('ADD', "-Title=DO"),
                       ('DEL', "-Title=MD"),
                       ('DEL', "-Title=DO"),
                       ('ADD', "-School=1803"),
                       ('DEL', "-School=1803"),
                       ('Yearend', "-School=1803"),
                       ('ADD', "-School>=6000"),
                       ('DEL', "-School>=6000"),
                       ('Yearend', "-School>=6000"),
                       ]

                row = [r for r in map(self.get_result, row)]

                '''perform calculations'''
                '''write to table'''

                tb.loc[str(year), :] = row

            tb9 = True
            if tb9:
                '''designate table writing to'''
                tb = self.summary_df['tb9']

                '''withdraw results'''
                columns = ['Total',
                           'MD',
                           'DO',
                           '000',
                           '001-006',
                           '010 w/FP',
                           '100',
                           '400',
                           '200',
                           '000-006'
                           ]
                row = [('Yearend', "-*"),
                       ('Yearend', "-Title=MD"),
                       ('Yearend', "-Title=DO"),
                       ('Yearend', "-Sp=000"),
                       ('Yearend', "-Sp=001-006"),
                       ('Yearend', "-Sp=010 -Res=000"),
                       ('Yearend', "-Sp=100"),
                       ('Yearend', "-Sp=400"),
                       ('Yearend', "-Sp=200"),
                       ('Yearend', "-Sp=000-006")
                       ]

                row = [r for r in map(self.get_result, row)]

                '''perform calculations'''
                '''write to table'''

                tb.loc[str(year), :] = row

            tb12 = True
            if tb12:
                '''designate table writing to'''
                tb = self.summary_df['tb12']

                '''withdraw results'''
                columns = ['Total'
                           ]
                row = [('Yearend', "-*")]

                row = [r for r in map(self.get_result, row)]

                '''perform calculations'''
                '''write to table'''

                tb.loc[str(year), :] = row

        if year in list(range(2000, self.max_fifth_year, 5)) + list(range(self.max_fifth_year, self.end_year + 1)):
            tb10 = True
            if tb10:
                '''designate table writing to'''
                tb = self.summary_df['tb10']

                '''withdraw results'''

                columns = ['FP w/010',
                           'PC',
                           'OB',
                           'PC + OB',
                           'All']

                row = [('Yearend', "-Sp=000-006,010 -Res 010"),
                       ('Yearend', '-Sp=000-006,100,400'),
                       ('Yearend', "-Sp=200"),
                       ('Yearend', "-*")
                       ]

                row = [r for r in map(self.get_result, row)]

                '''perform calculations'''

                row = row[:-1] + [row[1] + row[2]] + row[-1:]

                '''write to table'''

                tb.loc[str(year), :] = row

        if year in range(self.end_year - 4, self.end_year + 1):
            tb11 = True
            if tb11:
                '''designate table writing to'''

                tb1 = self.summary_df['tb11a']
                tb2 = self.summary_df['tb11b']

                '''withdraw results'''

                columns = ['Total', '<5k', '5k-14999', '15k-49999', '>50k']

                add_r1 = [('ADD', '-Sp=000 -City Pop<5000'),
                          ('ADD', '-Sp=000 -City Pop=5000-15000'),
                          ('ADD', '-Sp=000 -City Pop=15000-50000'),
                          ('ADD', '-Sp=000 -City Pop>=50')]

                add_r2 = [('ADD', '-Sp=005 -City Pop<5000'),
                          ('ADD', '-Sp=005 -City Pop=5000-15000'),
                          ('ADD', '-Sp=005 -City Pop=15000-50000'),
                          ('ADD', '-Sp=005 -City Pop>=50')]

                add_r3 = [
                    ('ADD', '-Sp=010 -Res=000 -City Pop<5000'),
                    ('ADD', '-Sp=010 -Res=000 -City Pop=5000-15000'),
                    ('ADD', '-Sp=010 -Res=000 -City Pop=15000-50000'),
                    ('ADD', '-Sp=010 -Res=000 -City Pop>=50'),
                ]

                delete_r1 = [('DEL', '-Sp=000 -City Pop<5000'),
                             ('DEL', '-Sp=000 -City Pop=5000-15000'),
                             ('DEL', '-Sp=000 -City Pop=15000-50000'),
                             ('DEL', '-Sp=000 -City Pop>=50')]

                delete_r2 = [('DEL', '-Sp=005 -City Pop<5000'),
                             ('DEL', '-Sp=005 -City Pop=5000-15000'),
                             ('DEL', '-Sp=005 -City Pop=15000-50000'),
                             ('DEL', '-Sp=005 -City Pop>=50')]

                delete_r3 = [
                    ('DEL', '-Sp=010 -Res=000 -City Pop<5000'),
                    ('DEL', '-Sp=010 -Res=000 -City Pop=5000-15000'),
                    ('DEL', '-Sp=010 -Res=000 -City Pop=15000-50000'),
                    ('DEL', '-Sp=010 -Res=000 -City Pop>=50'),
                ]

                '''perform calculations'''
                add_r1 = [r for r in map(self.get_result, add_r1)]
                add_r2 = [r for r in map(self.get_result, add_r2)]
                add_r3 = [r for r in map(self.get_result, add_r3)]
                add_r1 = [sum(add_r1)] + add_r1
                add_r2 = [sum(add_r2)] + add_r2
                add_r3 = [sum(add_r3)] + add_r3

                delete_r1 = [r for r in map(self.get_result, delete_r1)]
                delete_r2 = [r for r in map(self.get_result, delete_r2)]
                delete_r3 = [r for r in map(self.get_result, delete_r3)]
                delete_r1 = [sum(delete_r1)] + delete_r1
                delete_r2 = [sum(delete_r2)] + delete_r2
                delete_r3 = [sum(delete_r3)] + delete_r3

                add_sum = [sum(x) for x in zip(add_r1, add_r2, add_r3)]
                delete_sum = [sum(x) for x in zip(delete_r1, delete_r2, delete_r3)]

                '''perform calculations'''
                '''write to table'''
                index = ['2009 000', '2009 005', '2009 010W', '2009 Total']

                tb1.loc[str(year) + ' 000', :] = add_r1
                tb1.loc[str(year) + ' 005', :] = add_r2
                tb1.loc[str(year) + ' 010W', :] = add_r3
                tb1.loc[str(year) + ' Total', :] = add_sum

                tb2.loc[str(year) + ' 000', :] = delete_r1
                tb2.loc[str(year) + ' 005', :] = delete_r2
                tb2.loc[str(year) + ' 010W', :] = delete_r3
                tb2.loc[str(year) + ' Total', :] = delete_sum

            tb13 = True
            if tb13:
                '''designate table writing to'''

                tb1 = self.summary_df['tb13a']
                tb2 = self.summary_df['tb13b']

                '''withdraw results'''

                columns = ['Total', '<5k', '5k-14999', '15k-49999', '>50k']

                add_col = [('ADD', "-Sp=000-006,010 -Res=000 -City Pop<5000"),
                           ('ADD', "-Sp=000-006,010 -Res=000 -City Pop=5000-15000"),
                           ('ADD', "-Sp=000-006,010 -Res=000 -City Pop=15000-50000"),
                           ('ADD', "-Sp=000-006,010 -Res=000 -City Pop>50000")]

                delete_col = [('DEL', "-Sp=000-006,010 -Res=000 -City Pop<5000"),
                              ('DEL', "-Sp=000-006,010 -Res=000 -City Pop=5000-15000"),
                              ('DEL', "-Sp=000-006,010 -Res=000 -City Pop=15000-50000"),
                              ('DEL', "-Sp=000-006,010 -Res=000 -City Pop>50000")]

                '''perform calculations'''
                add_col = [r for r in map(self.get_result, add_col)]

                delete_col = [r for r in map(self.get_result, delete_col)]

                '''perform calculations'''
                '''write to table'''
                index = ['2009 000', '2009 005', '2009 010W', '2009 Total']

                tb1.loc[:, str(year)] = add_col

                tb2.loc[:, str(year)] = delete_col

        print("-->updating complete.")

    def balance_data(self, year):

        current_do = self.get_result(('TRF', "-*"))

        '''
        Conducts the balancing of balancing query results.
        :param year:
        :return:
        '''
        fs = self.feature_space
        
        idict = self.idict
        
        ## retrieve a list of all the result planes for each query in balancing queries
        
        fp_list = [(query_name,fs[:,idict["queries"][query_name],:]) for query_name in self.balancing_query_keys]
        
        for query_name,plane in fp_list:

            '''Create an expanded plane. Note that if the year being evaluated is the very first year, 
            there will not be a "Previous" column. Only the remaining 5 columns will exist'''

            new_plane = np.zeros((plane.shape[0] + 4,plane.shape[1]))
            new_plane[0,:] = year

            '''select the values of the new plane corresponding to the original plane and
            replace those values with those from the old plane.

            This creates a plane of the following format:
                                hcp, hcp, hcp, hcp, hcp .... hcp       ^
            <--------------------prepended row of years --------------->
            2019 2019 2019 2019 2019 2019 2019 2019 ...............2019
            <--------------------original plane below ----------------------->
            
            previous                                                   |
                                                                       |
            add                                                        |
                                                                       |
            del                                                        |
                                                                       |
            current                                                    |
                                                                       |
            <--------------------end original plane ---------------------->v
                                                                       ^
            pAdd (predicted adds)                                      |
                                                                       |
            pDel (predicted deletes)                                   |
                                                                       |
            isError (if transaction is an error)                       |
                                                                       |
            <--------------------added columns by importing ---------->v
                                 old plane to the new plane
            '''
            new_plane[1:plane.shape[0]+1,0:plane.shape[1]] = plane

            plane = new_plane

            '''Transpose switches the plane's axes into the following:

                year | Previous | Add | Del | Current | ... | pAdd | pDel | isError

            hcp  2019   0       1     0       1      ...     1      0      0
            .     .     .       .     .       .      ...     .      .      .
            .     .     .       .     .       .      ...     .      .      .
            .     .     .       .     .       .      ...     .      .      .
            hcp  2019   0       1     0       1      ...     1      0      0

            '''
            plane = plane.transpose()
            np.set_printoptions(threshold=np.inf)


            '''Get the set of error and count tables for this query so we can update them'''
            e_tb = self.balance_df['error'][query_name]
            c_tb = self.balance_df['count'][query_name]

            ''' If we are on the start year, there is no "previous" source data so there will only be 9 columns'''



            if plane.shape[1] == 9:
                '''hcp          current   add    del    trf    chg     previous  padd    pdel   num error'''
                ['12895020.0' '5938.0' '439.0' '452.0' '222.0' '8.0'            '434.0' '447.0' '0.0']

                ['12679320.0' '5947.0' '341.0' '332.0' '338.0' '15.0' '5938.0' '1.0' '5284.0' '5285.0']

                new_plane = np.zeros(plane.shape)

                new_plane[:, 0] = plane[:, 0]
                new_plane[:, 1] = plane[:, 2]
                new_plane[:, 2] = plane[:, 3]
                new_plane[:, 3] = plane[:, 1]
                new_plane[:, 4] = plane[:, 4]
                new_plane[:, 5] = plane[:, 5]
                new_plane[:, 6] = plane[:, 6]
                new_plane[:, 7] = plane[:, 7]
                new_plane[:, 8] = plane[:, 8]

                plane = new_plane

                ''' Refer to the 10-columns section for interpreting slicing the cubic space'''

                plane[[np.all(x) for x in (plane[:, 1:4] == [1, 0, 1])], 6:9] = [1, 0, 0]
                # plane[[np.all(x) for x in (plane[:, 0:4] == [1, 0, 0, 1])], 6:9] = [0, 0, 0]
                plane[[np.all(x) for x in (plane[:, 1:4] == [0, 1, 0])], 6:9] = [0, 1, 0]




                summary_line = np.sum(plane, axis=0)



                summary_line[0] = np.NaN



                c_tb.loc[year] = summary_line

            ''' If we are on any other year, there will be a previous source data so there will be 10 columns'''
            if plane.shape[1] == 10:

                '''
                Given the plane's previous set up,

                the following will look at the first 4 columns to decide whether the transaction is an add, delete or
                error.

                The following code:

                plane[[np.all(x) for x in (plane[:, 1:5] == [1, 0, 0, 1])], 7:10] = [0, 0, 0]

                1. (plane[:, 1:5]) will slice the plane by keeping all rows but only selecting columns 1 to 5. Note
                column 1 is actually the second the column, the first being column 0

                2. (plane[:, 1:5] == [1, 0, 0, 1]) will take the previous slice and check it against [1, 0, 0, 1] to see
                if corresponding columns have the same value. This is an element to element comparison, which means
                the result will be a list of booleans like [ True True False False ] that indicate whether or not
                the value at the designated position can be matched (true) or not (false)
                

                3. [np.all(x) for x in (plane[:, 1:5] == [1, 0, 0, 1])] will return a list of
                    booleans. It is shorthand for the following:

                    container = [] <--- the returned list. Empty at start and filled by the for loop
                    for x in (plane[:, 0:4] == [1, 0, 0, 1]) <--- for every list of
                                                                booleans resulting from ele to ele comparison from above
                        a = np.all(x) <--- np.all will evaluate all values in an iterable with the "and" operator
                                           to see if they evaluate to true (returns True). Other wise return (False).
                                           returns a single boolean for a list of booleans.

                        container.append(a) <--- appends the previous result boolean to the output list.

                4. plane[[np.all(x) for x in (plane[:, 1:5] == [1, 0, 0, 1])], 7:10]

                Because the previous step returns a list of booleans matching every single hcp id, we can use this list
                for numpy boolean indexing.

                    plane [ ... ] backets are operators used to to slice the plane.

                    [np.all(x) for x in (plane[:, 1:5] == [1, 0, 0, 1])] is from the previous step. This list is in the
                    slice position for rows (the first position in the slice operator [ O , ] )

                    7:10 is a slice of columns, namely columns 7 to 9 (pAdd, pDel, isError). This slice range is put
                    in the slice operator's second position [ , 0 ].

                    put together, this tells the slice index to choose all rows for which the condition (boolean list)
                    true and take those rows and select columns 7 to 9

                5. = [0 , 0 , 0] With the above selection, assign values [0,0,0] to every row.

                '''
                '''hcp          current   add    del    trf    chg     previous  padd    pdel   num error'''
                ['12895020.0' '5938.0' '439.0' '452.0' '222.0' '8.0'            '434.0' '447.0' '0.0']

                ['12679320.0' '5947.0' '341.0' '332.0' '338.0' '15.0' '5938.0' '1.0' '5284.0' '5285.0']

                new_plane = np.zeros(plane.shape)

                new_plane[:, 0] = plane[:, 0]
                new_plane[:, 1] = plane[:, 6]
                new_plane[:, 2] = plane[:, 2]
                new_plane[:, 3] = plane[:, 3]
                new_plane[:, 4] = plane[:, 1]
                new_plane[:, 5] = plane[:, 4]
                new_plane[:, 6] = plane[:, 5]
                new_plane[:, 7] = plane[:, 7]
                new_plane[:, 8] = plane[:, 8]
                new_plane[:, 9] = plane[:, 9]

                plane = new_plane

                plane[[np.all(x) for x in (plane[:, 1:5] == [0, 0, 0, 1])], 7:10] = [1, 0, 1]
                plane[[np.all(x) for x in (plane[:, 1:5] == [0, 1, 0, 1])], 7:10] = [1, 0, 0]
                plane[[np.all(x) for x in (plane[:, 1:5] == [1, 0, 0, 0])], 7:10] = [0, 1, 1]
                # plane[[np.all(x) for x in (plane[:, 0:4] == [1, 0, 0, 1])], 6:9] = [0, 0, 0]
                plane[[np.all(x) for x in (plane[:, 1:5] == [1, 0, 1, 0])], 7:10] = [0, 1, 0]

                '''Sum everything on the ADT axis (axis 0)'''
                summary_line = np.sum(plane,axis=0)

                '''Return a list of booleans where the rows are marked True if the ninth column where that column's 
                value is equal to 1.
                
                Selects all rows marked error and sets the corresponding place in the list to True
                '''
                errors_bool = (plane[:, 9] == 1)

                '''
                np.asarray([self.ind_to_hcp(x) for x in np.where(errors_bool)[0]])[:, np.newaxis]
                
                1. np.where(errors_bool)[0] take the first value of the returned container, which is list of booleans 
                from before and, using np.where, return their corresponding indices in that list if the value is True.
                
                2. [self.ind_to_hcp(x) for x in np.where(errors_bool)[0]] will
                    A. take the indices list given by np.where(errors_bool)[0]
                    B. use self.ind_to_hcp(x) to get the index's corresponding HCP id's
                    C. self.ind_to_hcp[x] get the hcp id corresponding to that index
                    D. [ ... ] brackets outside will place these hcp id's into a list
                    
                3. [:, np.newaxis] add a second axis to the one row matrix. This putss every value in the row
                into a value of a column. i.e:
                
                [ 1, 2, 3, 4, 5 ]
                
                to 
                
                [[1],
                [2],
                [3],
                [4],
                [5]]
                    
                '''
                error_hcp = np.asarray([self.idict['ind to id'][x] for x in np.where(errors_bool)[0]])[:, np.newaxis]
                '''
                plane[(plane[:, 9] == 1)]
                
                1. (plane[:,9] == 1) slice the matrix to consist of all rows and the 9th column, return a list of indices
                corresponding to the rows whose values are equal to 1.
                
                2. plane[(plane[:, 9] == 1)] select those rows in the plane using the indices returned above 
                '''
                error_val = plane[(plane[:, 9] == 1)]
                '''
                np.concatenate((error_hcp.astype(int),error_val.astype(int)),axis=1)
                concatenate based on axis 1:
                
                [
                [hcp id], -- concatenate to --> [ 0 , 1, 0 , 1 , 0 , 0, 1, 0, 0]
                [hcp id], -- concatenate to --> [ 0 , 1, 0 , 1 , 0 , 0, 1, 0, 0]
                [hcp id], -- concatenate to --> [ 0 , 1, 0 , 1 , 0 , 0, 1, 0, 0]
                [hcp id], -- concatenate to --> [ 0 , 1, 0 , 1 , 0 , 0, 1, 0, 0]
                [hcp id], -- concatenate to --> [ 0 , 1, 0 , 1 , 0 , 0, 1, 0, 0]
                [hcp id]  -- concatenate to --> [ 0 , 1, 0 , 1 , 0 , 0, 1, 0, 0]
                ]
                
                '''
                error_rows = np.concatenate((error_hcp.astype(int),error_val.astype(int)),axis=1)

                '''
                put error rows into a dataframe in which the dataframe's columns are the same as the error table's columns
                then append this dataframe to error table
                '''

                self.balance_df['error'][query_name] = e_tb.append(pd.DataFrame(error_rows,columns=e_tb.columns),ignore_index=True)

                '''
                summary_line[1:]
                slice summary_line from the second term to the end in order to skip the sum of all years, which doesn't 
                make sense. 
                
                c_tb.loc[year] =
                
                in the table c_tb, select the row at index 'year' . Then, assign the above sliced summary_line to this
                row
                '''

                c_tb.loc[year] = summary_line[1:]

        return

    def post_analysis(self):
        print("-->Performing Post Analysis")
        mod_list = []
        for frame in self.balance_table_list:
            frame['Expected'] = pd.concat(
                [pd.Series(["NA"]), frame['In Previous'][1:] + frame['In Adds'][1:] - frame['In Deletes'][1:]])
            frame['Actual = Expected'] = pd.concat(
                [pd.Series(["NA"]), frame['Expected'][1:] == frame['In Current'][1:]])
            frame['Adjusted'] = pd.concat(
                [pd.Series(["NA"]), frame['In Previous'][1:] + frame['pAdd'][1:] - frame['pDel'][1:]])
            frame['Is Balanced'] = pd.concat(
                [pd.Series(["NA"]), frame['Adjusted'][1:] == frame['In Current'][1:]])

        print("-->Post Analysis Complete")
        return

    def output_results(self,custom_data=None,custom=False,custom_name=None):
        '''
        Output results for balancing and summarizing. Functionality for individual adhoc reports should be done in it's
        own function.

        :return:
        '''

        ''' Change the writing directory to the designated directory for writing'''
        os.chdir(self.write_to_dir)

        ''' If we allowed summarizing, then output summary results'''
        if self.summarize == 'True':
            print("-->Writing Summary results to file >Summary Result Table.xlsx< at {}".format(self.write_to_dir))

            file_open = False
            '''Try to open the file to see if the file is already opened outside of this program'''
            while file_open == False:
                try:

                    file = open(self.write_to_dir + '/Summary Result Table.xlsxx', 'w')
                    file.close()
                    file_open = True
                except PermissionError: ## trips when file is opened already
                    input("File is open. Please close file and press enter")
                except FileNotFoundError: ## trips when file cannot be found, which is okay.
                    file_open = True

            write_success = False

            ''' Designate a writing machine, pandas.ExcelWriter object.
            
            Add the file name to the write-to directory to build the explicit path to write to
            
            designate the writing engine. The writing engine is the package with which to write the excel file with
            '''
            writer = pd.ExcelWriter(self.write_to_dir + '/Summary Result Table.xlsx', engine='xlsxwriter')

            ''' Create a workbook object'''
            workbook = writer.book

            ''' Add to work book a worksheet'''
            worksheet = workbook.add_worksheet('LMT Summary')
            writer.sheets['LMT Summary'] = worksheet

            ''' For each dataframes in the summary_df dictionary, write the table to the file'''
            last_written_row_position = 0
            for name, table in self.summary_df.items():
                table.to_excel(writer, sheet_name='LMT Summary', startrow=last_written_row_position, startcol=0)
                ''' We increment the last written position to be two rows beyond where the previous table ends'''
                last_written_row_position = last_written_row_position + table.shape[0] + 2

                write_success = True

            ''' Change directory back to the program root'''

            os.chdir(self.rt)

            ''' Tell writer to save the file it is responsible for'''
            writer.save()
        if self.balance == "True":
            print("-->Writing Balancing results to >Balancing Counts.xlsx< at {}".format(self.write_to_dir))
            file_open = False
            while file_open == False:
                try:
                    # trips the exception if opened
                    file = open(self.write_to_dir + '/Balancing Counts.xlsx', 'w')
                    file.close()
                    file = open(self.write_to_dir + '/Balancing Errors.xlsx', 'w')
                    file.close()
                    file_open = True
                except PermissionError:
                    input("File is open. Please close file and press enter")
                except FileNotFoundError:
                    file_open = True

            write_success = False
            writer = pd.ExcelWriter(self.write_to_dir + '/Balancing Counts.xlsx', engine='xlsxwriter')
            workbook = writer.book
            worksheet = workbook.add_worksheet('Balancing Summary')
            writer.sheets['Balancing Summary'] = worksheet
            last_written_row_position = 0
            for name, table in self.balance_df['count'].items():
                table.to_excel(writer, sheet_name='Balancing Summary', startrow=last_written_row_position, startcol=0)
                last_written_row_position = last_written_row_position + table.shape[0] + 2

                write_success = True

            os.chdir(self.rt)
            writer.save()

            print("-->Writing Summary results to >Balancing Errors.xlsx< at {}".format(self.write_to_dir))
            file_open = False
            while file_open == False:
                try:
                    # trips the exception if opened
                    file = open(self.write_to_dir + '/Balancing Errors.xlsx', 'w')
                    file.close()
                    file_open = True
                except PermissionError:
                    input("File is open. Please close file and press enter")
                except FileNotFoundError:
                    file_open = True

            write_success = False
            writer = pd.ExcelWriter(self.write_to_dir + '/Balancing Errors.xlsx', engine='xlsxwriter')
            workbook = writer.book
            worksheet = workbook.add_worksheet('Balancing Errors')
            writer.sheets['Balancing Errors'] = worksheet
            last_written_row_position = 0
            for name, table in self.balance_df['error'].items():

                table.to_excel(writer, sheet_name='Balancing Errors', startrow=last_written_row_position, startcol=0)
                last_written_row_position = last_written_row_position + table.shape[0] + 2

                write_success = True

            os.chdir(self.rt)
            writer.save()
        if custom==True:
            print("-->Writing Custom results to >{}.xlsx< at {}".format(custom_name, self.write_to_dir))
            file_open = False
            while file_open == False:
                try:
                    # trips the exception if opened
                    file = open(self.write_to_dir + '/{}.xlsx'.format(custom_name), 'w')
                    file.close()
                    file_open = True
                except PermissionError:
                    input("File is open. Please close file and press enter")
                except FileNotFoundError:
                    file_open = True

            write_success = False
            writer = pd.ExcelWriter(self.write_to_dir + '/{}.xlsx'.format(custom_name), engine='xlsxwriter')
            workbook = writer.book
            worksheet = workbook.add_worksheet('Results')
            writer.sheets['Results'] = worksheet
            last_written_row_position = 0

            custom_data.to_excel(writer, sheet_name='Results', startrow=0, startcol=0)

            os.chdir(self.rt)
            writer.save()

    def medical_education_analysis(self,data):

        data = data['Yearend']

    def reference_data_extract(self, reference_data_name, is_file=True):

        roster = {

            'UI Afilliated Worksite Id' :
                ["18001",
                  "18003",
                  "18004",
                  "18005",
                  "18006",
                  "18007",
                  "18009",
                  "18010",
                  "18012",
                  "18013",
                  "18014"],

            'UI Afilliated PHA Residency':
                ['61100',
                 '61102',
                 '61600',
                 '61150',
                 '61203'],

            'City Population':
                {'name':'Cities Population Estimate.xls',
                 'sheets':{"Cities": None},
                 'converters':{"Census": int}
                 },

            'City Combinations':
                {'name': 'City Combinations.xls',
                 'sheets': {"In State Combination": None,
                            "Out State Combination": None},
                 'converters': {"City": str,
                                "Nearest Metropolis": str}
                 },

            'Phy Delivery Data':
                {
                'name':'2020 HcpDeliveryData.xls',
                'sheets':{'data':None },
                'converters': {"Hcp Id":str,
                                "Year": int}
                },
            'County by Regions':
                {
                'name':'County by Regions.xls',
                'sheets': {'County by Regions': None},
                'converters': {"County": str}
                },
            'PHY start':1977,
            'APN start':1998,
            'PA start':1995,
            'PHA start':1996,
            'DDS start':1997

        }

        if is_file == True:

            self.reference_dir + "/" + roster[reference_data_name]['name']

            data = self.read_data(
                self.reference_dir + "/" + roster[reference_data_name]['name'],
                sheets=roster[reference_data_name]['sheets'],
                converters=roster[reference_data_name]['converters'],
                header=0
            )[0]

        else:
            data = roster[reference_data_name]

        return data

    def adhoc(self):
        print('adhoc is not defined. overwrite when using adhoc')
        return

def main(balance, end, provider, override_validate, start, summarize,project, adhoc, meded,index_name, **hparams):
    '''
    This is the main function for the entire program. Not the same as the main function for analysis.
    :param balance:
    :param end:
    :param provider:
    :param validate:
    :param start:
    :param summarize:
    :param adhoc:
    :param meded:
    :param hparams:
    :return:
    '''
    '''Prints settings for current run.'''
    run_settings = "\n-->YearendAnalysisDeprec.py is running with the following settings:\nbalance={}\nsummariz={}\n".format(balance,
                                                                                                                summarize)
    print(run_settings)

    '''Creates the analyzer controller object for handling checking, analysis and writing operations.'''

    '''*****************************************************************************************************
    2. To understand code, enter into analysis_controller
    *****************************************************************************************************
    '''

    analysis = analysis_controller(
        start_year=start,
        end_year=end,
        provider=provider,
        validate=override_validate,
        balance=balance,
        summarize=summarize,
        adhoc=adhoc,
        meded=meded,
        project=project,
        index_name=index_name


    )

    '''Check run environment.'''
    analysis.environment_check()
    '''Check directories are in place.'''
    analysis.check_directory()
    '''Conduct analysis based on settings. Initiate_analysis_main controls the order of all analyses.'''
    analysis.yearend_analysis_main()

if __name__ == "__main__":
    '''Arguments
    
    -->Arguments are settings that are given to the program's main function to use. It allows arguments to be passed from
    command line to the program by the following line of code:
    
            py YearendAnalysisDeprec.py
    
    -->Arguments can be added by adding "-ArgumentName=Value" as such:
    
            -balance=True -summarize=False -start=2008 -end=2019
    
    -->All arguments non explicitly mentioned will utilize default values
    
    -->All of the below arguments are required. 
    
    -->All of the below arguments will have default values
    :return: 
    '''
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-balance',
        type=str,
        default='True',
        help="perform balance analysis. Default=True",
        choices=['True','False']
    )

    parser.add_argument(
        '-summarize',
        type=str,
        default='True',
        help="perform yearend summarization analysis. Default=True",
        choices=['True','False'],

    )

    parser.add_argument(
        '-project',
        type=str,
        default='False',
        help="includes projection queries for analysis. Default=True",
        choices=['True', 'False'],

    )

    parser.add_argument(
        '-adhoc',
        type=str,
        default='False',
        help="perform adhoc analysis. Default=False",
        choices=['True','False'],

    )

    parser.add_argument(
        '-meded',
        type = str,
        default = "None",
        choices=["None","Simple","Verbose"],
        help= "perform medical education analysis. Default=None",
    )

    parser.add_argument(
        '-override-validate',
        type=str,
        default='False',
        help="Skip data validation. Set True if already validated. Default=False",
        choices=['True','False']
    )

    parser.add_argument(
        '-start',
        type=int,
        default=2018,
        help="Starting year to be analyze. Must be 1977 or later. Default=1977"
    )

    parser.add_argument(
        '-end',
        type=int,
        default=int(datetime.datetime.now().year) - 1,
        help="Ending year to be analyze. Must not exceed current year. Default = {}".format(int(datetime.datetime.now().year) - 1)
    )

    parser.add_argument(
        '-provider',
        type=str,
        default="PHY",
        choices=["PHY", "APN", "PA",None],
        help="Provider type to be analyzed. Specialty queries are specialized for PHY, APN and PA. Default = PHY"
    )

    parser.add_argument(
        '-index_name',
        type=str,
        default="Hcp Id",
        help="Primary Key column name"
    )

    args = parser.parse_args()
    '''*****************************************************************************************************
    
    1. To understand code, enter into the main function below. This is where all command line arguments are
    passed to the main function that initializes the analyses.
    *****************************************************************************************************
    '''
    main(**vars(args))

