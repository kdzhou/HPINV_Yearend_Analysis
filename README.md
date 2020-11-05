# HPINV_Yearend_Analysis
1. Overview
Overview
Yearend Analysis consists of two reports summarizing the changes in physician flow in the HPINV system. There are also hosts of other miscellaneous reports that uses year end reports as source data. YearendAnalysis.py is not only a program to perform year end reports but it also offers a framework to facilitate running other kinds of reports.
Purpose
Since many of these reports require the same or similar information, we can run these queries once, keep them in a temporary storage, then output numerous reports using the information retrieved. This temporary storage must allow for the following:
1.	Allow numeric analysis of query results (i.e sums, averages, means etc.)
2.	Allow querying of the results (i.e find me all the hcp id’s of people with specialty of family medicine)
3.	Allow analysis and querying across and within different sheets (i.e if person A is a family medicine doctor in 2020 adds, are they also in 2020 yearend?)
Algorithm
The algorithm is primarily built on a NumPy sparse matrix with three axes, referred to as the Result Space.

The guide will be posted here in the future once I find time to figure out how to write a github page.
