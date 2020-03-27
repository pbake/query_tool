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


class DML_Functions():
    '''This class is created to build spark sql queries with select,oderby, filter, groupby,
    aggregate and boolean functions '''

    def select_orderby(table_name, select_list,orderby_list):
        '''
        Returns the SQL query for computing column statistics.
        Passing Input table_name, select function column names and
        order by function column names.
        '''
        Function_Param.select_columnnames(selected_list)
        Function_Param.orderby_columnnames(orderby_list)
        query = '''SELECT ${s_col1}, ${s_col2},${s_col3}
                   FROM ${table_name}
        ORDER BY ${o_col1}, ${o_col2}'''
        t = Template(query)
        qry_template= t.substitute(s_col1 = s_column0, s_col2 = s_column1,s_col3 = s_column2,o_col1 = o_column0, o_col2 = o_column1, table_name=table_name)

        return qry_template


        #2.1
    #Function Select_Filter
    #./query -s TITLE,REV,DATE -f DATE=2014-04-01

    def select_filter(table_name, selected_list,filter_list):
        '''
        Returns the SQL query for computing column statistics.
        Passing Input table_name, select function column names and
        filter/where clause column names.
        '''
        Function_Param.select_columnnames(selected_list)
        f_column1,filter_value=Function_Param.filter_columnnames(filter_list)
        if f_column1 == 'DATE':
            filter_value = "CAST('"+ filter_value +"' AS date)"

        query = '''SELECT ${s_col1}, ${s_col2},${s_col3}
                   FROM ${table_name}
                   WHERE ${f_column1} = ${filter_value}'''
        t = Template(query)
        qry_template= t.substitute(s_col1 = s_column0, s_col2 = s_column1,s_col3 = s_column2,f_column1=f_column1, table_name=table_name, filter_value=filter_value)
        return qry_template



    def select_groupby_agg(table_name, selected_list,groupby_list,ag_col1,ag_funct1,ag_col2,ag_funct2):
        '''
        Returns the SQL for computing column statistics.
        Passing Input table_name, select function column names,
        group by function column names and aggregate functions and aggregrate function column names.
        '''
        Function_Param.select_columnnames(selected_list)
        Function_Param.groupby_columnnames(groupby_list)



        query = '''SELECT ${s_col1}, ${ag_funct1}(${ag_col1}),count(distinct(${ag_col2}))
                    FROM ${table_name}
                    group by ${g_column1}'''

        t = Template(query)
        qry_template= t.substitute(s_col1 = s_column0,g_column1=g_column0, table_name=table_name,ag_col1=ag_col1,ag_funct1=ag_funct1,ag_col2=ag_col2,ag_funct2=ag_funct2)
        return qry_template



    def select_advfilter_single_exp(table_name, selected_list,filter_list,operator_list,expression):
        '''
        Returns the SQL for computing column statistics.
        Passing Input table_name, select function column names,
        filter/where clause column names and boolean operators and boolean operators column names.
        '''
        Function_Param.select_columnnames(selected_list)
        f_column1,filter_value=Function_Param.filter_columnnames(filter_list)
        op_column1,op_value=Function_Param.or_columnnames(operator_list)
        if f_column1 == 'DATE':
            filter_value = "CAST('"+ filter_value +"' AS date)"
        elif op_column1 == 'DATE':
            op_value = "CAST('"+ op_value +"' AS date)"

        query = '''SELECT ${s_col1}, ${s_col2}
                   FROM ${table_name}
                   WHERE ${f_column1} = ${filter_value} ${expression} ${op_column1} = ${op_value}'''
        t = Template(query)
        qry_template= t.substitute(s_col1 = s_column0, s_col2 = s_column1,f_column1=f_column1, table_name=table_name, filter_value=filter_value,op_column1=op_column1, expression=expression, op_value=op_value)
        return qry_template


    def select_advfilter_double_exp(table_name, selected_list,filter_list,operator_list1,operator_list2,expression1,expression2):
        '''
        Returns the SQL for computing column statistics.
        Passing Input table_name, select function column names,
        filter/where clause column names and boolean operators and boolean operators column names.
        '''
        Function_Param.select_columnnames(selected_list)
        f_column1,filter_value=Function_Param.filter_columnnames(filter_list)
        op_column1,op_value1=Function_Param.and_columnnames(operator_list1)
        op_column2,op_value2=Function_Param.or_columnnames(operator_list2)
        if f_column1 == 'DATE':
            filter_value = "CAST('"+ filter_value +"' AS date)"
        elif op_column1 == 'DATE':
            op_value = "CAST('"+ op_value +"' AS date)"


        query = '''SELECT ${s_col1}, ${s_col2}
                   FROM ${table_name}
                   WHERE ${f_column1} = ${filter_value} ${expression1} (${op_column1} = ${op_value1} ${expression2} ${op_column2} = ${op_value2})'''
        t = Template(query)
        qry_template= t.substitute(s_col1 = s_column0, s_col2 = s_column1,f_column1=f_column1, table_name=table_name, filter_value=filter_value,op_column1=op_column1,op_column2=op_column2, expression1=expression1,expression2=expression2, op_value1=op_value1,op_value2=op_value2)
        return qry_template
