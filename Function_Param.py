import pandas as pd
import pyspark as ps # for the pyspark suite
import warnings         # for displaying warning
#Initiate a SparkSession. A SparkSession embeds both a SparkContext and
#a SQLContext to use RDD-based and DataFrame-based functionalities of Spark.
spark = ps.sql.SparkSession.builder \
        .master("local[4]") \
        .appName("df lecture") \
        .getOrCreate()

sc = spark.sparkContext
from string import Template
from datetime import datetime
import pyspark.sql.functions as func
from pyspark.sql.types import IntegerType


class Function_Param():
    '''Data Wrangling : This class is created to extract select,oderby, filter, groupby,
    aggregate and boolean functions and extract respective column names'''


    def get_string(string):
        '''
                INPUT: string
                OUTPUT: list
                Given one string returns the list of strings.
            '''
        string =string.split()
        return string


    def find_parameters(value, items):
        '''
                INPUT: function name:string and user input string in tokenized list format.
                OUTPUT: list
                To get the parameters for each function like select, order by, filter & group by
            '''
        i = items.index(value)
        return items[i+1]


    def fetch_select_param(string):
        '''
                INPUT: string
                OUTPUT: list
                Extract select function parameters in list.
            '''
        # get the maching function in variable
        s_matching = [s for s in string if "-s" in s]
        if s_matching!=[]:
            s_param =Function_Param.find_parameters("-s",string).split(',')
        else:
            return
        return s_param

    def fetch_order_param(string):
        '''
                INPUT: string
                OUTPUT: list
                Extract orderby function parameters in list.
            '''
        o_matching = [o for o in string if "-o" in o]
        if o_matching!=[]:
            o_param =find_parameters("-o",string).split(',')
        else:
            return
        return o_param

    def fetch_filter_param(string):
        '''
                INPUT: string
                OUTPUT: list
                Extract filter function parameters in list.
            '''
        f_matching = [f for f in string if "-f" in f]
        if f_matching != []:
            f_param =find_parameters("-f",string).split(',')
        else:
            return
        return f_param

    def fetch_groupby_param(string):
        '''
                INPUT: string
                OUTPUT: list
                Extract group by function parameters in list.
            '''
        g_matching = [g for g in string if "-g" in g]
        if g_matching !=[]:
            g_param =find_parameters("-g",string).split(',')
        else:
            return
        return g_param

    def fetch_filter_param_strval(string):
        '''
                INPUT: string
                OUTPUT: list
                Extract filter function for the columns whose values are descriptive with 1 white
                space parameters in list.
            '''
        f_matching = [f for f in string if "-f" in f]
        if f_matching != []:
            f_param =find_parameters_filter_boolean_strval("-f",string).split(',')
        else:
            return
        return f_param

    def fetch_OR_boolean_param(string):
        '''
                INPUT: string
                OUTPUT: list
                Extract boolean or function columns names in list.
            '''
        b_matching = [b for b in string if "OR" in b]
        if b_matching2 !=[]:
            b_param =find_parameters("OR",string).split(',')
        else:
            return
        return b_param

    def fetch_AND_boolean_param(string):
        '''
                INPUT: string
                OUTPUT: list
                Extract boolean and function columns names in list.
            '''
        a_matching = [a for a in string if "AND" in a]
        if a_matching1 !=[]:
            a_param =find_parameters("AND",string).split(',')
        else:
            return
        return a_param

   # converts select function list of column names/parameters to each column name variable
    def select_columnnames(s_param):


        for n, val in enumerate(s_param):
            globals()["s_column%d"%n] = val

    # converts orderby function list of column names/parameters to each column name variable
    def orderby_columnnames(o_param):
        for n, val in enumerate(o_param):
            globals()["o_column%d"%n] = val

     # converts group by function list of column names/parameters to each column name variable
    def groupby_columnnames(g_param):
        for n, val in enumerate(g_param):
            globals()["g_column%d"%n] = val

     # converts filter function list of column names/parameters to each column name variable
    def filter_columnnames(f_param):
        f_column = (",".join(s for s in f_param if "=".lower() in s.lower())).split("=",2)
        if f_column !="":
            return f_column[0],f_column[1]
        else:
            return

    # converts aggregate function list of column names/parameters to each column name variable
    def agg_columnnames(ag_param):
        for n, val in enumerate(ag_param):
            globals()["ag_column%d"%n] = val

    # converts boolean AND list of column names/parameters to each column name variable
    def and_columnnames(a_param):
        a_column = (",".join(s for s in a_param if "=" in s.lower())).split("=",2)
        if a_column !="":
            return a_column[0],a_column[1]
        else:
            return

    # converts boolean OR list of column names/parameters to each column name variable
    def or_columnnames(b_param):
        b_column = (",".join(s for s in b_param if "=" in s.lower())).split("=",2)
        if b_column !="":
            return b_column[0],b_column[1]
        else:
            return

    # aggregate function -sum - column names are returned as column name , aggregate function name respectively
    def agg_sum_param(s_param):
        sum_col = (",".join(s for s in s_param if ":sum".lower() in s.lower()))
        sum_col =sum_col.split(":",2)
        if sum_col !="":
            return sum_col[0],sum_col[1]
        else:
            return

    # aggregate function -min - column names are returned as column name , aggregate function name respectively
    def agg_min_param(s_param):
        min_col = (",".join(s for s in s_param if ":min".lower() in s.lower())).split(":",2)
        if min_col !="":
            return min_col[0],min_col[1]
        else:
            return

     # aggregate function -max - column names are returned as column name , aggregate function name respectively
    def agg_max_param(s_param):
        max_col = (",".join(s for s in s_param if ":max".lower() in s.lower())).split(":",2)
        if max_col !="":
            return max_col[0],max_col[1]
        else:
            return

    # aggregate function -count - column names are returned as column name , aggregate function name respectively
    def agg_count_param(s_param):
        count_col = (",".join(s for s in s_param if ":count".lower() in s.lower())).split(":",2)
        if count_col !="":
            return count_col[0],count_col[1]
        else:
            return

     # aggregate function -collect - column names are returned as column name , aggregate function name respectively
    def agg_collect_param(s_param):
        collect_col = (",".join(s for s in s_param if ":COLLECT".lower() in s.lower())).split(":",2)
        if collect_col !="":
            return collect_col[0],collect_col[1]
        else:
            return

    # to get the values of functions which has string values with one space
    def find_parameters_filter_boolean_strval(value, items):
        i = items.index(value)
        x =(" ".join(s for s in items[i+1:i+3]))
        return x


    def fetch_OR_boolean_param_strval(string):
        b_matching = [b for b in string if "OR" in b]
        if b_matching !=[]:
            b_param =find_parameters_filter_boolean_strval("OR",string).split(',')
        else:
            return
        return b_param

     # to get the values of functions which has string values with one space
    def fetch_AND_boolean_param_strval(string):
        a_matching = [a for a in string if "AND" in a]
        a_matching
        if a_matching !=[]:
            a_param =find_parameters_filter_boolean_strval("AND",string).split(',')
        else:
            return
        return a_param
