# Databricks notebook source
# MAGIC %md
# MAGIC # Voice Service requested, Historical to date data processing
# MAGIC * Reads voice data from lake via EDMSource
# MAGIC * Joins activites to SRs (via contact)
# MAGIC * Filters: 
# MAGIC   * only a subset of activity and SR types
# MAGIC   * only text over a certian length
# MAGIC   * where a least one activity associated with a paritculary queue (owner)
# MAGIC   * srs created in date range
# MAGIC * Metadata filter for cosmos based on updated_date
# MAGIC * Text cleaning
# MAGIC * Annonymize data
# MAGIC * Dummy SR, PI and Activity Id 
# MAGIC * Saves a subset of data in Cosmos DB

# COMMAND ----------

!/local_disk0/.ephemeral_nfs/envs/pythonEnv-aeb852df-e877-496b-9d36-b267f4306ccc/bin/python -m pip install --upgrade pip
# !local_disk0/.ephemeral_nfs/envs/pythonEnv-fc084d4f-a845-42ff-b648-d9ec0e8f2951/bin/python -m pip install --upgrade pip

#!pip install presidio-analyzer
#!pip install presidio-anonymizer
!pip install azure-cosmos
#!pip install https://github.com/explosion/spacy-models/releases/download/en_core_web_md-3.0.0/en_core_web_md-3.0.0.tar.gz



# COMMAND ----------

#%pip install spacy==3.2.4 pydantic==1.7.4
# %pip install spacy==3.2.4 pydantic==1.8.2
#!python3 -m spacy download en_core_web_md 
#spark.conf.set("spark.sql.shuffle.partitions", "200")
#import spacy
#nlp = spacy.load("en_core_web_md")
#display(nlp)

# COMMAND ----------

# MAGIC %pip install azure.cosmos azure-identity azure-mgmt-cosmosdb

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_user()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###### EDM and spark_dataframe_operations class definition from common_data_processing/edm_reader.py and team_utilities.spark_dataframe_operations.py respectivity for common code used by data science team from repo: 
# MAGIC https://openuniversity@dev.azure.com/openuniversity/Digital-Technologies/_git/repo-ds-ml-general
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### spark_dataframe_operations functions 

# COMMAND ----------

import pyspark.sql.functions as sf

def filter_to_first_in_window(df, w, keep_ties=False):
  """Filters to the first row(s) of a group from a spark dataframe
  Use with transform method to apply in chain e.g.

  latest_status_window = Window.partitionBy(
      "student_sk", "module_presentation_sk"
  ).orderBy(sf.desc("student_registration_status_update_datetime"))
  factSMPRS.transform(lambda df: filter_to_first_in_window(df, latest_status_window))
  
  Use a window to define ordering
  Note: in event of tie for first, choice is arbitrary when keep_ties is False
  Note: in event of tie for first, mutiple rows are retained when keep_ties is True
  """
  return filter_to_nth_row_in_window(df, w, 1, keep_ties)

def filter_to_nth_row_in_window(df, w, n, keep_ties=False):
  """Filters to the nth row(s) of a group from a spark dataframe
  Use with transform method to apply in chain e.g.

  latest_status_window = Window.partitionBy(
      "student_sk", "module_presentation_sk"
  ).orderBy(sf.desc("student_registration_status_update_datetime"))
  factSMPRS.transform(lambda df: filter_to_first_in_window(df, latest_status_window, 2))
  
  Use a window to define ordering
  Note: in event of tie for first, choice is arbitrary when keep_ties is False
  Note: in event of tie for first, mutiple rows are retained when keep_ties is True
  """
  if keep_ties:
    return (df
            .withColumn("rank", sf.rank().over(w))
            .filter(sf.col("rank")==n)     
            .drop('rank')
         )
  else:
    return (df
            .withColumn("row_number", sf.row_number().over(w))
            .filter(sf.col("row_number")==n)     
            .drop('row_number')
         )
    
def convert_date_sk_to_value(df, dim_date, cols=None):
    """
    convert date_sk to date value using dim_date (from EDM)
    Example usage:
    .transform(lambda df: convert_date_sk_to_value(df, edm.dim_date,
        ['task_submitted_date_sk', 'task_collected_for_marking_by_tutor_date_sk', 'task_score_recorded_date_sk']))
    """

    out_cols = df.columns
    output = df

    #if cols not specifed, do default
    if cols is None: cols = [c for c in out_cols if c.endswith("date_sk")]
    elif isinstance(cols, str): cols = [cols] # so can enter a string or a list of strings
    
    for col in cols:
        #manage column order
        new_col = col.replace('_sk','')
        out_cols.insert(out_cols.index(col)+1, new_col)

        #join the date value
        output = (
        output.join(dim_date
                .select(sf.col("date_value").alias(new_col),
                        sf.col("date_sk").alias(col)
                        ),
                on=col,
                how="left"
                )
        )
    return output.select(out_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### EDM class definition 
# MAGIC

# COMMAND ----------


## EDM class definition from ./repo-ds-ml-general/common_data_processing/edm_reader.py 
from pyspark.sql import functions as sf, SparkSession, DataFrame
from functools import reduce
import os
import re
import datetime
from pyspark.sql.window import Window
# from team_utilities.spark_dataframe_operations import filter_to_first_in_window
# import filter_to_first_in_window
#import sys
#print(sys.path)
class EDMFunctions:

  #class attribute
  default_values = {
      'relative_path': "DataService/EnterpriseDataModel/ConsolidatedExtract/", 
      'path': None ,
      'titan_path': None,
      'file_suffix': ".parquet",
      'file_start': "adlsfile_edmviews_",
      'file_end': "_all.parquet",
      'include_daily': True,
      'use_aml_datastore' : False,
      'override_daily_absence': False,
      'override_required_deduplication': False,
      'table_keys': {
            'fact_agg_module_presentation_time_period': 'module_presentation_time_period_nk',
            'fact_student_module_presentation_vle_visits': 'student_module_vle_visits_nk',
            'fact_agg_student_academic_year': 'student_academic_year_nk',
            'dim_student_scd': ['student_scd_sk','version_number'],
            'metadata_edm_table_attribute': ['schema_name', 'table_name', 'attribute_number']
            }
    }

  deduplication_not_required = ['fact_student_module_presentation_vle_site_usage']
  
  def get_spark_titan_path(self, mounted_required=False):

    if self.config_dict['use_aml_datastore'] == True:
      subscription_id = self.config_dict['subscription_id']
      resource_group_name = self.config_dict['resource_group_name']
      workspace_name = self.config_dict['workspace_name']

      return f"azureml://subscriptions/{subscription_id}/resourcegroups/{resource_group_name}/workspaces/{workspace_name}/datastores/titan_live_adls/paths/" # aml datastore
    
    else:
      
      mounted_path = "dbfs:/mnt/titandatalake/"
      if mounted_required: return mounted_path

      spark = self.spark_session
      
      mounted_only_workspaces = ['4538350219514328'] #[mlops live]
      workspace_lookup = {
          '5749387628222653': 'live',
          '8643143886043982': 'acct',  
          }
      workspace_id = spark.conf.get("spark.databricks.clusterUsageTags.orgId")
      cluster_name = spark.conf.get("spark.databricks.clusterUsageTags.clusterName")


      if (workspace_id in workspace_lookup.keys()) & ('passthr'  in  cluster_name):
          ws = workspace_lookup[workspace_id]
          return f"abfss://outitan{ws}adls@outitan{ws}adlsgen2.dfs.core.windows.net/"
      
      else: #(workspace_id in mounted_only_workspaces):
          return mounted_path

  

      


  

  def __init__(self, config_dict=None):
    # self.spark_session = SparkSession.builder.getOrCreate()
       

    ### modifying spark session
    self.spark_session = SparkSession.builder \
      .appName("OTR") \
      .config("spark.driver.memory", "60g") \
      .getOrCreate()
    self.spark_session.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    # .config("spark.databricks.clusterUsageTags.orgId", "dummy_value") \
    #####

    if config_dict is None: config_dict = {}
    # update config_dict with default values
    self.config_dict = self.default_values | config_dict

    #backward compatability as key names changed
    if 'edm_rel_path' in config_dict.keys():
      self.config_dict['relative_path'] = config_dict['edm_rel_path']
    if 'edm_path' in config_dict.keys():
      self.config_dict['path'] = config_dict['edm_path']

    #contruct path
    if self.config_dict['titan_path'] is None: self.config_dict['titan_path'] = self.get_spark_titan_path()
    if self.config_dict['path'] is None: self.config_dict['path'] = self.config_dict['titan_path'] + self.config_dict['relative_path']

    #warn against unexpected config keys
    unexpected_config_keys = [k for k in self.config_dict.keys() if k not in [*self.default_values.keys(), 'edm_path', 'edm_rel_path'] ]
    if len(unexpected_config_keys) > 0:
        print(f"WARNING: unrecognised config_dict keys will be ignored: {unexpected_config_keys}")
        
  @staticmethod
  def get_mounted_style_path(path):
    """
      Converts an abfss style path to mounted path"

      Args:
          path (String): The filepath to be converted

      Returns:
        String: file path
    """
    return re.sub("abfss.*windows\.net/", "dbfs:/mnt/dummydatalake/", path)

  def get_os_style_path(self, path):
    """
      Converts spark style filepath string to an os style path"

      Args:
          path (String): The filepath to be converted

      Returns:
        String: file path
    """
    path = self.get_mounted_style_path(path)
    if "dbfs" in path:
      return path.replace("dbfs:/", "/dbfs/")
    else:
      return "/dbfs" + path
  
  @staticmethod
  def get_spark_style_path(path):
    """
      Converts os style filepath string to an spark style path"

      Args:
          path (String): The filepath to be converted

      Returns:
        String: file path
    """
    return path.replace("/dbfs/", "dbfs:/")
  
  def trim_long_name(self, name):
    file_start = self.config_dict['file_start']
    file_end = self.config_dict['file_end']

    if name.endswith(file_end): name = name[:-len(file_end)]
    if name.startswith(file_start): name = name[len(file_start):]
    return name

  def untrim_long_name(self, name):
    file_start = self.config_dict['file_start'] 
    file_end=self.config_dict['file_end']
    if not name.endswith(file_end): name += file_end
    if not name.startswith(file_start): name = file_start + name
    return name

  def old_read_table(self, table_name, filter_deleted=True):
    path=self.config_dict['path']
    os_style_path = self.get_os_style_path(path)
    spark = self.spark_session

    # if is a directory
    if os.path.isdir(f"{os_style_path}{self.trim_long_name(table_name)}"):
      #get subdirectories (reading at directory level doesn't read subdirectories. even with wildcard)
      directories_to_read = self.get_subdirectories(f"{os_style_path}{self.trim_long_name(table_name)}")
      directories_to_read = [spark.read.parquet(d) for d in directories_to_read]
      #concatenate dfs
      output = reduce(DataFrame.unionAll, directories_to_read)
    else:
      output = spark.read.parquet(f"{path}/{self.untrim_long_name(table_name)}")

    if filter_deleted: output = output.filter("delete_flag == 'N'")

    return output

  def make_short_name(self, name):
    file_start = self.config_dict['file_start'] 
    file_end=self.config_dict['file_end']

    #convert to all lower case later
    manual_overrides = {
      'dim_corporate_account': 'dim_CAcc',
      'dim_crm_area': 'dim_CRMA',
      'dim_hecos_subject': 'dim_HecosS',
      'dim_hesa_subject': 'dim_HesaS',
      'dim_task_extension_reason': 'dim_TaskER',
      'dim_thesis_extension_reason': 'dim_ThesisER',
      'dim_thesis_degree': 'dim_TDeg',
      'dim_thesis_discipline': 'dim_TDis',
      'fact_student_thesis_submission': 'fact_STSub',
      'fact_student_thesis_suspension': 'fact_STSusp'
      }
    
    name = self.trim_long_name(name)

    if name in manual_overrides.keys():
      return manual_overrides[name].lower()

    words = [w for w in name.split('_')]
    if len(words) <= 2:
      return name
    else:
      #firstword_underscoreinitials
      return f"{words.pop(0).lower()}_{''.join([w[0].lower() for w in words])}"
    
  def get_all_tables_dict(self):
    path = self.config_dict['path']
    file_start = self.config_dict['file_start']
    file_end = self.config_dict['file_end']
    include_daily = self.config_dict['include_daily']
    override_daily_absence = self.config_dict['override_daily_absence']

    os_style_path = self.get_os_style_path(path)

    # creates list of only edm fact and dimension tables, filtering out all other files
    if include_daily & ~override_daily_absence:
      all_edm_files = [f for f in os.listdir(os_style_path) if (('fact' in f ) | ('dim' in f)) & self.check_if_daily_table(f)]
    else:
      all_edm_files = [f for f in os.listdir(os_style_path) if ('fact' in f ) | ('dim' in f)]

    return {n: self.make_short_name(n) for n in all_edm_files}

  def get_subdirectories(self, directory_name):
    os_style_path = self.get_os_style_path(directory_name)
    new_directories = [os_style_path]
    # get all subdirectories 
    directory_list = []
    while len (new_directories) > 0:
        #add new directories to list
        directory_list.extend(new_directories)
        #check new directories for any futher subdirectories 
        to_check = new_directories
        new_directories =[]
        for nd in to_check:
          new_directories.extend([os.path.join(nd,f) for f in os.listdir(nd) if os.path.isdir(os.path.join(nd,f))])
    #print(directory_list)
    return [self.get_spark_style_path(d) for d in directory_list]

  @staticmethod
  def deduplicate_table(t, pk):
      """
      Filters out duplicate rows, taking only the latest updated row for each unique primary key(s) 

      Args:
          t (pyspark df): The dataframe to deduplicated
          pk (string or list of strings): column name(s) which represent primary key of tables

      Returns:
        pyspark df: deduplicated dataframe
    """

      #handle list of strings or single string pk
      if isinstance(pk, str):
        pk = [pk]
      elif isinstance(pk, list):
        pass
      else:
        raise TypeError("unexpected type for primary key argument")
      
      #get column to indentify latest record
      update_col = [c for c in ['updated_datetime', 'updated_date'] if c in c in t.columns][0]
      update_window = Window.partitionBy(*pk).orderBy(sf.desc(update_col))

      #keep only latest records only per pk
      return t.transform(lambda x: filter_to_first_in_window(x, update_window, keep_ties=False))
              
      
  def check_if_daily_table(self, t):
    """Previously only certain tables were updated daily, now expect all tables updated daily"""
    return True

  def check_if_dedupe_required(self, t):
    #This is the only table which doesn't at the moment
    if t in self.deduplication_not_required:
      return False
    else:
      return True
    
  def read_fact_student_module_presentation_vle_site_usage(self, filter_deleted=True):
    # do the hunting subdirectories stuff 
    return self.old_read_table('fact_student_module_presentation_vle_site_usage', filter_deleted)

  #what about complete rewrite tables?

  def read_latest_table(self, table_name):
    #Note no option to filter deleted here, it's dangerous to do this before deduplication, if deduplication wanted/required
    spark = self.spark_session
    return spark.read.parquet(f"{self.config_dict['path']}{self.trim_long_name(table_name)}")

  def read_base_table(self, table_name, filter_deleted=True):
      
      spark = self.spark_session
      os_style_path = self.get_os_style_path(self.config_dict['path'])
      # get the files in the directory #Now two different file naming conventions allowed for (as was changed without warning)
      file_list = [f for f in os.listdir(f"{os_style_path}{self.trim_long_name(table_name)}") if (('_all_' in f) | ('_all.' in f))]
      # read the files 
      file_read_list = [f"{self.get_spark_style_path(os_style_path)}{self.trim_long_name(table_name)}/{f}" for f in file_list]
      #union files
      if filter_deleted:
          output = reduce(DataFrame.unionAll, [spark.read.parquet(f).filter("delete_flag=='N'") for f in file_read_list])
      else:
          output = reduce(DataFrame.unionAll, [spark.read.parquet(f) for f in file_read_list])

      return output
  
  def infer_table_key(self, t, cols_to_check):
      """Identifies as primary key, which can used for deduplication of tables
      Checked it gives correct results (as stated by Integration team) for current known tables (Dec 2024)
      But not guranteed to be correct for future tables (though logic seems likely to 
      give correct results given table design principles observed)"""

      manual_keys = self.config_dict['table_keys']
      #need to override some 
      if t in manual_keys.keys():
        return manual_keys[t]

      stub = t.replace('fact_', '').replace('dim_', '')
      if stub + '_nk' in cols_to_check:
          return stub + '_nk'
      if stub + '_value' in cols_to_check:
          return stub + '_value'
      if stub + '_sk' in cols_to_check:
          return stub + '_sk'
      print(f"Warning uncertain primary key for {t}, assumed {cols_to_check[0]}")
      return cols_to_check[0]


  def read_table(self, table_name, filter_deleted=True, include_daily=None, deduplicate=None):
      path = self.config_dict['path']
      os_style_path = self.get_os_style_path(path)
      spark = self.spark_session
      override_daily_absence = self.config_dict['override_daily_absence']
      override_deduplicaton = self.config_dict['override_required_deduplication']

      if include_daily is None:
        include_daily = self.config_dict['include_daily']

      if deduplicate is None:
        if (not include_daily ) | override_deduplicaton:
          deduplicate = False # should not be needed
        else:
          deduplicate = self.check_if_dedupe_required(table_name)


      if table_name == "fact_dummy_mod_presentation_vle_site_usage":
        return self.read_fact_dummy_mod_presentation_vle_site_usage(filter_deleted)
      else:
        if include_daily:
          if not override_daily_absence:
            assert self.check_if_daily_table(table_name), f"{table_name} is not on updated daily list. Set config_dict['override_daily_absence'] to True to proceed anyway"
          
          df1 = self.read_latest_table(table_name)
          if deduplicate:
            #need an override on pk?
            df1 = self.deduplicate_table(df1, self.infer_table_key(table_name, df1.columns))
          
          if filter_deleted:
            return df1.filter("delete_flag == 'N'")
          else:
            return df1
          
        else:
          return self.read_base_table(table_name, filter_deleted)
        
  def generate_edm_update_check(self, documentation_sdf=None):
      """
      Generate a table of update statuses of EDM tables. 
      It's a check based on file modifed times only; no table content is checked"

      Args:
          documentation_sdf (pyspark df): A df containing table_name column
            Used to check against expected table names against,
            based on metadata_edm_table_attribute (the default)

      Returns:
        pysark df: table of update times
    """
      spark = self.spark_session

      if documentation_sdf is None:
          documentation_sdf = self.read_table('metadata_edm_table_attribute')

      assert 'table_name' in documentation_sdf.columns, "documentation_sdf must contain a column called 'table_name'"

      os_style_path = self.get_os_style_path(self.config_dict['path'])
      found_tables = list(self.get_all_tables_dict().keys())

      latest_update = [
          max([datetime.datetime.fromtimestamp(
              os.path.getmtime(f'{os_style_path}{t}/{f}')) 
                for f in os.listdir(f'{os_style_path}{t}')
                ]) for t in found_tables]
      
      latest_base_update = [
          max([datetime.datetime.fromtimestamp(
              os.path.getmtime(f'{os_style_path}{t}/{f}')) 
          for f in os.listdir(f'{os_style_path}{t}') 
          if (('_all' in f) | (t=='fact_dummy_mod_presentation_vle_site_usage'))]
          ) for t in found_tables]

      raw_data = {'table_name': found_tables,
              'datetime_modified': latest_update,
              'base_datetime_modified': latest_base_update,
              'expect_daily': [self.check_if_daily_table(t) for t in found_tables]}

      #expected tables
      documentation_sdf = (
          documentation_sdf
          .select('table_name', sf.lit(True).alias('documented'))
          .distinct()
          )

      return (
          #found files in df
          spark.createDataFrame(
              [vals for vals in zip(*raw_data.values())], list(raw_data.keys())
              )
          .withColumn('file_found', sf.lit(True))
          #join expected files from documentation
          .join(documentation_sdf, on='table_name', how='outer')
          .fillna({'file_found':False, 'documented':False})
          .withColumn('daily_in_date', 
              sf.datediff(sf.lit(datetime.date.today()), sf.to_date('datetime_modified')) <= 1)
          )
    
class EDM(EDMFunctions):
  
  def _make_tables_dict(self, tables):
    if tables is None: 
      tables_dict = {}
    elif isinstance(tables, dict):
      tables_dict = tables
    elif isinstance(tables, str):
      tables_dict = {tables: self.make_short_name(tables)}
    elif isinstance(tables, list):
      tables_dict = {n: self.make_short_name(n) for n in tables}
    else:
      raise TypeError("unexpected type for tables argument")

    return tables_dict

  def _set_tables(self, tables_d, include_daily):
    for k, v in tables_d.items():
      setattr(self, v, self.read_table(k, include_daily=include_daily))
    
  def __init__(self, tables=None, config_dict=None):
    super().__init__(config_dict)
    include_daily = self.config_dict['include_daily']
    self.tables_dict = self._make_tables_dict(tables)

    no_duplicates_check = len(set(self.tables_dict.values())) == len(self.tables_dict.values())
    assert no_duplicates_check, "Table shortnames are not unique"

    self.tables = list(self.tables_dict.values())
    self._set_tables(self.tables_dict, include_daily)

  def __getitem__(self, table_name):
        """Permit dictionary style getting with key for tables"""
        if table_name in self.tables_dict.values():
            return getattr(self, table_name)
        #match full name for table too if possible
        if table_name in self.tables_dict.keys():
            return getattr(self, self.tables_dict[table_name])
        #if short name (after /) matches any tables uniquely return that table
        matches = [t for t in self.tables_dict.keys() if t.rsplit('/',1)[-1] == table_name]
        if len(matches)==1:
            return getattr(self, (self.tables_dict[matches[0]]))

        return getattr(self, table_name)

  def add_tables(self, new_tables, include_daily=None):
    if include_daily is None:
      include_daily = self.config_dict['include_daily']
  
    new_tables_dict = self._make_tables_dict(new_tables)
    #make temp copy to check no duplicates first
    tmp=self.tables_dict.copy()
    tmp.update(new_tables_dict)
    no_duplicates_check = len(set(tmp.values())) == len(tmp.values())
    assert no_duplicates_check, "Table shortnames are not unique"
    self.tables_dict = tmp
    self.tables = list(self.tables_dict.values())
    self._set_tables(new_tables_dict, include_daily) # only read new tables

  def get_all_tables(self, include_daily=None):
    self.tables_dict = self.get_all_tables_dict()
    if include_daily is None:
      include_daily = self.config_dict['include_daily']
    print(f"Slowness Warning: attempting to read all {len(self.tables_dict.keys())} relevant tables")
    self.add_tables(self.tables_dict, include_daily)

class EDMSource(EDM):
    def __getattribute__(self, name):
        #block atributes which are not appropriate for EDMSource Data
        if name in ['get_all_tables_dict', 'get_all_tables', 'generate_edm_update_check']: 
            raise AttributeError(name)
        else: 
            return super(EDMSource, self).__getattribute__(name)

    def __dir__(self):
        #block atributes which are not appropriate for EDMSource Data
        return sorted(
            (set(dir(self.__class__ )) | set(self.__dict__.keys()))
            - set(['get_all_tables_dict', 'get_all_tables', 'generate_edm_update_check'])
            )
    
    default_values = {
        'relative_path': 'DataService/SourceData/ConsolidatedExtract/',
        'path': None,
        'dummy_path': None,
        'file_suffix': '.parquet',
        'file_start': 'adlsfile_edmviews_',
        'file_end': '_all.parquet',
        'include_daily': False,
        'override_daily_absence': False,
        'override_required_deduplication': False,
        'table_keys': {
            'Circedummy/circedummy_ci_d_posted_stud_fees': 
                ['fee_trnsn_id', 'fee_trnsn_rec_num', 'personal_id'],
            'Voice/voice_s_evt_act': 'row_id',
            'Voice/voice_s_contact': 'row_id',
            'Voice/voice_s_srv_req': 'row_id'
            }
        }
    
    have_daily_updates = [
      'Campaigner/campaigner_campaigner_export',
      'crcdummy/circedummy_ci_d_posted_stud_fees',
      'crcdummy/circedummy_ci_d_stud_trans_fee_statuses',
      'Voice/voice_s_evt_act', 'Voice/voice_s_contact', 'Voice/voice_s_srv_req']

    deduplication_not_required = [
      'cpg/cpg_cpg_export', 
      'crcdummy/circedummy_ci_d_stud_trans_fee_statuses'
      ]
    
    def make_short_name(self, name):
      #just the part after last "/"
      return name.rsplit('/',1)[-1]

#read the model ouputs tables
class ModelOutputs:

    default_values = {
      'relative_path': "DataWork/Teams/DataScience/Other/ModelOutputs/model_outputs_v2_tables/" 
    }

    #methods from EDM class
    _make_tables_dict = EDM._make_tables_dict
    get_spark_dummy_path = EDM.get_spark_dummy_path
    get_os_style_path = EDM.get_os_style_path
    add_tables = EDM.add_tables
    __getitem__ = EDM.__getitem__

    @staticmethod
    def get_mounted_style_path(path):
        return EDM.get_mounted_style_path(path)

    def make_short_name(self, name):
        words = [w for w in name.split('_')]
        return ''.join([w[0].lower() for w in words])
    
    def read_table(self, table_name):
        spark = self.spark_session
        return spark.read.format('delta').load(self.path + table_name)
    
    def _set_tables(self, tables_d):
        for k, v in tables_d.items():
            setattr(self, v, self.read_table(k))

    def add_tables(self, new_tables, include_daily=None):
        new_tables_dict = self._make_tables_dict(new_tables)
        #make temp copy to check no duplicates first
        tmp=self.tables_dict.copy()
        tmp.update(new_tables_dict)
        no_duplicates_check = len(set(tmp.values())) == len(tmp.values())
        assert no_duplicates_check, "Table shortnames are not unique"
        self.tables_dict = tmp
        self.tables = list(self.tables_dict.values())
        self._set_tables(new_tables_dict)

    def get_all_tables_dict(self):
        os_style_path = self.get_os_style_path(self.path)
        all_mo_files = [f for f in os.listdir(os_style_path)]
        return {n: self.make_short_name(n) for n in all_mo_files}

    def __init__(self, tables=None, path=None, relative_path=None, dummy_path=None):
        self.spark_session = SparkSession.builder.getOrCreate()
        #contruct path
        self.relative_path = self.default_values['relative_path'] if relative_path is None else relative_path
        self.dummy_path = self.get_spark_dummy_path() if dummy_path is None else dummy_path
        self.path = self.dummy_path + self.relative_path if path is None else path
        #setup tables
        if tables in ['All','all','ALL']: self.tables_dict = self.get_all_tables_dict()
        else: self.tables_dict = self._make_tables_dict(tables)
        self.tables = list(self.tables_dict.values())
        self._set_tables(self.tables_dict)


# COMMAND ----------

# MAGIC %md
# MAGIC ##### ADF Parameterization of cosmos endpoint

# COMMAND ----------

dbutils.widgets.text("adfglparam_askou_cosmos_endpoint_1", "")
askou_cosmos_endpoint = dbutils.widgets.get("adfglparam_askou_cosmos_endpoint_1")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Create AskOU directory under DataService/Projects/Student for first time only - Created by dummy team do not delete below code 

# COMMAND ----------

import os 

SV_FL_METADATA = "dbfs:/mnt/dummydatalake/DataService/Projects/Student/AskOU/CosmosDB/metadata/"
mtdt_file = "cosmos_metadata.json"
dummypath=f'dbfs:/mnt/dummydatalake/DataService/Projects/Student/'

if 'dummyUN' not in dbutils.fs.ls('dbfs:/mnt/dummydatalake/DataService/Projects/Student/'):
    dbutils.fs.mkdirs(f'dbfs:/mnt/dummydatalake/DataService/Projects/Student/dummyUN/')
    dbutils.fs.mkdirs(f'dbfs:/mnt/dummydatalake/DataService/Projects/Student/dummyUN/CosmosDB/metadata')


# askou_path = dummypath+'dummyUN/'
json_path = SV_FL_METADATA+mtdt_file

# Check if the file exists
try:
    if not any(file.name == mtdt_file for file in dbutils.fs.ls(SV_FL_METADATA)):
    # Create an empty JSON file
        dbutils.fs.put(json_path, '{"sr_history":"","std_data": ""}',True)
        print("Empty JSON file created.")
    else:
        print("File already exists.")
except exception as e:
    dbutils.notebook.exit(f"metatdata file cosmos_metadata.json not exist: {e}")

# COMMAND ----------

import sys
import os
import re
import json
import uuid
import random
import pandas as pd
from datetime import datetime, date
import pyspark.sql.types as T
from pyspark.sql.window import Window
from pyspark.sql import functions as sf, DataFrame
from pyspark.sql.functions import col, count, first, row_number


error =None

# Cosmos DB
from azure.cosmos import CosmosClient, PartitionKey, exceptions

# Read common files
user_id = spark.sql('select current_user() as user').collect()[0]['user']

# COMMAND ----------

VOICE_FILE_PREFIX = "dbfs:/mnt/dummydatalake/DataService/SourceData/ConsolidatedExtract/Voice/"
# Save file path
SAVE_FILE_METDATA = "/dbfs/mnt/dummydatalake/DataService/Projects/Student/dummyUN/CosmosDB/metadata/"
cosmos_endpoint = askou_cosmos_endpoint
cosmos_database = "askou"
output_path = "dbfs:/mnt/dummydatalake/DataService/Projects/Student/dummyUN/CosmosDB/Temp/"
final_output_base_path = "dbfs:/mnt/dummydatalake/DataService/Projects/Student/dummyUN/CosmosDB/dummyhisdata/"

sr_history_container = "sr_history"
metadata_file = "cosmos_metadata.json"
metadata_key = "sr_history"
metadata_path  = f"{SAVE_FILE_METDATA}{metadata_file}"

# Source Tables
tables_dict_source = { 'Voice/voice_s_evt_act': 'voice_act',
                       'Voice/voice_s_contact': 'voice_contact',
                       'Voice/voice_s_srv_req': 'voice_sr'}

# Initial metadata structure with None values
default_metadata = {"sr_history": None, "std_data": None }

# COMMAND ----------

from datetime import datetime

# Input parameters
sr_cols = ['row_id', 'created', 'created_by', 'cst_con_id', 'sr_num', 'sr_stat_id', 'sr_subtype_cd', 'created_date', 'desc_text', 'x_service_rqst_pres_code']
act_cols = ["row_id", "created", "sra_sr_id", "todo_cd", "owner_login", "comments_long", "x_activity_topic_category", "activity_uid", "evt_stat_cd", "updated_date"]
conct_cols = ["row_id", "person_uid", "fst_name", "last_name"]
final_columns = ['sra_sr_id', 'sr_num', 'created', 'todo_cd', 'comments_long', 'owner_login', 'x_activity_topic_category', 'activity_uid', 'x_service_rqst_pres_code', 'person_uid', 'fst_name', 'last_name', 'updated_date']

sr_queues = ["Q-SRS-GENERAL", "Q-SRS-NEW", "Q-SRS-ONLINE-REGISTRATION", "Q-SRS-REGISTRATION", "Q-SRS-RESERVATIONS", "Q-SRSC-INTERNATIONAL"]

test_file_cols = ['sr_num', 'x_activity_topic_category', 'person_uid', 'inbound_email', 'outbound_email', 'dummy_sr_num', 'dummy_pi' ]

# COMMAND ----------

# MAGIC %md
# MAGIC ##UTILITY

# COMMAND ----------

from pyspark.sql import DataFrame
import pyspark.sql.functions as sf
from pyspark.sql.functions import col
import os
import json
from datetime import datetime

class Utils:
    @staticmethod
    def get_current_date():
        return datetime.now().strftime("%Y-%m-%d")

    @staticmethod
    def repartition_for_join(df, columns, partitions=200):
        return df.repartition(partitions, *columns)

    @staticmethod
    def convert_to_timestamp(df, columns, format="yyyy-MM-dd HH:mm:ss.SSSSSSS"):
        for column in columns:
            df = df.withColumn(column, sf.to_timestamp(col(column), format))
        return df

class MetadataHandler:
    @staticmethod
    def read_metadata(metadata_path, default_metadata, key=None):
        try:
            if not os.path.exists(metadata_path):
                with open(metadata_path, "w") as file:
                    json.dump(default_metadata, file, indent=4)
                print("Metadata file initialized.")
                return default_metadata if key is None else {key: default_metadata.get(key)}
            else:
                with open(metadata_path, "r") as file:
                    metadata = json.load(file)
                    return metadata if key is None else {key: metadata.get(key)}
        except Exception as e:
            # error=f"Error initializing or reading metadata: {e}"
            # print(f"Error initializing or reading metadata: {e}")
            dbutils.notebook.exit(f"Error initializing or reading metadata: {e}")
            # raise RuntimeError(f"Error initializing or reading metadata: {e}")

    @staticmethod
    def update_metadata(metadata_path, key, new_value):
        try:
            with open(metadata_path, "r") as file:
                metadata = json.load(file)

            # Ensure new_value is a string before updating
            if isinstance(new_value, datetime):
                new_value = new_value.strftime('%Y-%m-%d %H:%M:%S')

            metadata[key] = new_value

            with open(metadata_path, "w") as file:
                json.dump(metadata, file, indent=4)

            print(f"Metadata updated for key '{key}': {new_value}")
        except Exception as e:
            # error=f"Error updating metadata key: {e}"
            dbutils.notebook.exit(f"Error updating metadata key: {e}")
            # raise RuntimeError(f"Error updating metadata key: {e}")

class DataProcessor:
    @staticmethod
    def filter_sr_data(df: DataFrame, start_date: str, end_date: str, sr_cols: list) -> DataFrame:
        try:
            return (df.filter((col("created") >= start_date) & (col("created") <= end_date))
                    .withColumn("x_service_rqst_pres_code", sf.upper(sf.trim(col("x_service_rqst_pres_code"))))
                    .filter(col("sr_subtype_cd").isin('Email', 'Web Form', 'Form'))
                    .select(*sr_cols))
        except Exception as e:
            # error=f"An unexpected error occurred in filter_sr_data: {e}"
            dbutils.notebook.exit(f"An unexpected error occurred in filter_sr_data: {e}")
            # raise RunTimeError(f"An unexpected error occurred in filter_sr_data: {e}")
    
    @staticmethod
    def join_contact_data(sr: DataFrame, contact: DataFrame) -> DataFrame:
        try:
            return sr.join(contact, sr["cst_con_id"] == contact["row_id"], "left").drop(contact["row_id"])
        except Exception as e:
            # error=f"An unexpected error occurred in joining contact data: {e}"
            dbutils.notebook.exit(f"An unexpected error occurred in joining contact data: {e}")
            # raise RuntimeError(f"An unexpected error occurred in joining contact data: {e}") 

    @staticmethod
    def join_activity_data(sr: DataFrame, activity_orig: DataFrame, sr_cols: list, start_date: str, end_date: str) -> DataFrame:
        try:
            activity_filtered = activity_orig.filter((col("created") >= start_date) & (col("created") <= end_date))
            combined_activity = activity_filtered.filter(col('todo_cd').isin('Email - Inbound', 'Email - Outbound'))
            combined_activity = combined_activity.join(sr.select(*sr_cols), combined_activity.sra_sr_id == sr.row_id, how='inner')
            return combined_activity.orderBy(col("sr_num"), col("created").asc())
        except Exception as e:
            # error=f"An unexpected error occurred in joining activity data: {e}"
            dbutils.notebook.exit(f"An unexpected error occurred in join_activity_data: {e}")
            # raise RuntimeError(f"An unexpected error occurred: {e}")
    
    @staticmethod
    def filter_srs_queues(df: DataFrame, queue: list) -> DataFrame:
        try:
            sr_nums_with_general = df.filter(col("owner_login").isin(queue)).select("sr_num").distinct()
            return df.join(sr_nums_with_general, on="sr_num", how="inner").orderBy("sr_num", col("created").asc())
        except Exception as e:
            # error=f"An unexpected error occurred in filter_srs_queues: {e}"
            dbutils.notebook.exit(f"An unexpected error occurred in filter_srs_queues: {e}")
            # raise RuntimeError(f"An unexpected error occurred in filter_srs_queues: {e}")

    @staticmethod
    def save_file(df: DataFrame, folder_name: str) -> None:
        try:
            df.write.mode('overwrite').parquet(folder_name)
            print(f"Parquet files successfully saved in folder: {folder_name}")
        except Exception as e:
            # error=f"An error occurred while saving as Parquet: {e}"
            dbutils.notebook.exit(f"An error occurred while saving as Parquet: {e}")
            # raise RuntimeError(f"An error occurred while saving as Parquet: {e}")

    @staticmethod
    def clean_and_preprocess_text_data(df: DataFrame, text_column: str, drop_cols: list = None, output_column: str = 'email') -> DataFrame:
        try:
            if drop_cols is None:
                drop_cols = []

            patterns = [
                (r"^.*?WWW:(.*?): ", ""),
                (r"^.*?Query(.*?): ", ""),
                (r"Query from SST web form: ", ""),
                (r"\bSent from my iPhone", ""),
                (r"^.*?Query text ", ""),
                (r"^.*?Message: ", ""),
                (r"([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)", "<EMAIL>"),
                (r"(\S*\d+\S*)", "<NUMBER>"),
                (r"(?s)-----Original Message.*", ""),
                (r"(?s)____.*", ""),
                (r"(?s)-----.*", ""),
                (r"(?s)=====.*", ""),
                (r"(?s)Get Outlook for.*", ""),
                (r"(?s)^> .*", ""),
                (r" +", " ")
            ]

            df_cleaned = df.withColumnRenamed(text_column, 'text')
            for pattern, replacement in patterns:
                df_cleaned = df_cleaned.withColumn('text', sf.regexp_replace('text', pattern, replacement))

            return (df_cleaned
                    .filter(sf.length("text") >= 15)
                    .withColumnRenamed('text', output_column)
                    .drop(*drop_cols)
                    .orderBy(sf.col("sr_num"), sf.col("created").asc()))
        except Exception as e:
            # error=f"An unexpected error occurred in clean_and_preprocess_text_data: {e}"
            dbutils.notebook.exit(f"An unexpected error occurred in clean_and_preprocess_text_data: {e}")
            # raise RuntimeError(f"An unexpected error occurred in clean_and_preprocess_text_data: {e}")
        
    @staticmethod
    def save_parquet_file(df, output_path, final_output_base_path):
        # Remove directory again just in case
        try:
            dbutils.fs.rm(output_path, True)
        except:
            pass

        # Write the Parquet file
        df.coalesce(1).write.mode("overwrite").parquet(output_path)

        # Get the single Parquet file
        parquet_files = [f.path for f in dbutils.fs.ls(output_path) if f.path.endswith(".parquet")]

        if not parquet_files:
            error="No Parquet file found after writing!"
            dbutils.notebook.exit("No Parquet file found after writing!")
            # raise Exception("No Parquet file found after writing!")

        parquet_file = parquet_files[0]

        # Define final output path
        final_output_path = f"{final_output_base_path}sr_history_{datetime.now().strftime('%Y%m%d%H%M%S')}.parquet"

        # Move and rename the file
        dbutils.fs.mv(parquet_file, final_output_path)

        # Remove all files from temp after moving the final file
        try:
            dbutils.fs.rm(output_path, True)
            print(f"Temporary directory {output_path} cleaned up.")
        except:
            # error=f"Warning: Could not clean up {output_path}."
            dbutils.notebook.exit(f"Warning: Could not clean up {output_path}.")

        print(f"File saved at: {final_output_path}")
        return final_output_path

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cosmos DB Handler

# COMMAND ----------

from azure.cosmos import CosmosClient, exceptions, PartitionKey
import uuid
from datetime import datetime
import pandas as pd

class CosmosDBHandler:
    def __init__(self, client: CosmosClient, database_name: str):
        self.client = client
        self.database_name = database_name
        self.database = self.client.create_database_if_not_exists(id=database_name)

    def delete_container(self, container_name: str):
        try:
            # Get the database and container references
            self.database.delete_container(container_name)
            print(f"Container '{container_name}' deleted from database '{self.database_name}'.")
        except exceptions.CosmosHttpResponseError as e:
            # error=f"Error deleting container '{container_name}': {e.message}"
            dbutils.notebook.exit(f"Error deleting container '{container_name}': {e.message}")
            # raise RuntimeError(f"Error deleting container '{container_name}': {e.message}")

    def insert_sr_history(self, df: pd.DataFrame, container_name: str):
        container = self.database.create_container_if_not_exists(
            id=container_name,
            partition_key=PartitionKey(path="/sr_num")
        )

        for index, row in df.iterrows():
            try:
                sr_num = str(row['sr_num'])
                email_entry = {
                    "todo_cd": str(row.get('todo_cd', '')),
                    "email": str(row.get('email', '')),
                    "activity_uid": str(row.get('activity_uid', '')),
                    "created": str(row['created'])
                }

                # Check if the document with the same sr_num already exists
                query = "SELECT * FROM c WHERE c.sr_num = @sr_num"
                params = [{"name": "@sr_num", "value": sr_num}]
                existing_documents = list(container.query_items(query=query, parameters=params, enable_cross_partition_query=True))

                if existing_documents:
                    document = existing_documents[0]
                    emails = document.get("emails", [])
                    emails.append(email_entry)

                    # Sort emails by created date in ascending order
                    emails.sort(key=lambda x: datetime.strptime(x["created"], "%Y-%m-%d %H:%M:%S"))

                    # Update the document with the new emails array
                    document["emails"] = emails
                    container.upsert_item(document)
                else:
                    # Create a new document
                    new_document = {
                        "id": str(uuid.uuid4()),
                        "sr_num": sr_num,
                        "person_uid": str(row.get('person_uid', '')),
                        "x_activity_topic_category": str(row.get('x_activity_topic_category', '')),
                        "emails": [email_entry]
                    }
                    container.upsert_item(new_document)

            except exceptions.CosmosHttpResponseError as e:
                # error=f"Error processing record {row['dummy_sr_num']}: {e.message}"
                dbutils.notebook.exit(f"Error processing record {row['sr_num']}: {e.message}")
                # raise RuntimeError(f"Error processing record {row['dummy_sr_num']}: {e.message}")
            except Exception as e:
                # error=f"Unexpected error for record {row['dummy_sr_num']}: {e}"
                dbutils.notebook.exit(f"Unexpected error for record {row['sr_num']}: {e}")
                # raise RuntimeError(f"Unexpected error for record {row['dummy_sr_num']}: {e}")

    def fetch_sr_history(self, container_name: str, sr_num: str):
        try:
            container = self.database.get_container_client(container_name)
            query = f"SELECT * FROM c WHERE c.sr_num = '{sr_num}'"
            items = list(container.query_items(query=query, enable_cross_partition_query=True))
            return items
        except exceptions.CosmosHttpResponseError as e:
            # error=f"Error querying Cosmos DB: {e}"
            dbutils.notebook.exit(f"Error querying Cosmos DB: {e}")
            return []

# COMMAND ----------

# Sourcing the data
edm_source = EDMSource(
    tables_dict_source, 
    config_dict={
        'include_daily': True,
        'use_aml_datastore': False,  # or True, depending on your requirement
        'dummy_path': None,  # Add other necessary keys with default values
        'path': None,
        'subscription_id': '',  # Add appropriate value
        'resource_group_name': ''  # Add appropriate value
    }
)
voice_act, voice_contact, voice_sr = edm_source.voice_act, edm_source.voice_contact, edm_source.voice_sr

# Metadata Reading
metadata = MetadataHandler.read_metadata(metadata_path, default_metadata, key=metadata_key)
if metadata is not None:
    last_update_str = metadata.get(metadata_key)

    if last_update_str:
        try:
            last_update_datetime = datetime.strptime(last_update_str, '%Y-%m-%d %H:%M:%S')
            print(f"SR history last update: {last_update_datetime}")
        except ValueError as ve:
            dbutils.notebook.exit(f"Invalid date format in metadata: '{last_update_str}'. Error: {ve}")
    else:
        last_update_datetime = None
        print("No last update timestamp found — processing all records.")
else:
    dbutils.notebook.exit("Metadata is None. Please check the metadata path and key.")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Curation
# MAGIC

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, upper, trim, regexp_replace, length, asc, lit, coalesce
from datetime import datetime
import datetime as dt

# Define Columns
sr_cols = ["row_id", "created", "created_by", "cst_con_id", "sr_num", "sr_stat_id", "sr_subtype_cd", "created_date", "desc_text", "x_service_rqst_pres_code"]
act_cols = ["row_id", "created", "sra_sr_id", "todo_cd", "owner_login", "comments_long", "x_activity_topic_category", "activity_uid", "evt_stat_cd", "updated_date"]
conct_cols = ["row_id", "person_uid", "fst_name", "last_name"]
final_cols = ['sr_num', 'created', 'todo_cd', 'email', 'owner_login', 'x_activity_topic_category', 'activity_uid', 'person_uid', 'updated_date']

today = dt.datetime.today()
one_years_ago = today.replace(year=today.year - 1)
start_date = one_years_ago.strftime('%Y-%m-%d')
end_date = datetime.now().strftime('%Y-%m-%d')

# Convert Timestamp
voice_sr = voice_sr.withColumn("created", to_timestamp(col("created"))).select(*sr_cols)
voice_act = voice_act.withColumn("created", to_timestamp(col("created"))).select(*act_cols)
voice_contact = voice_contact.select(*conct_cols)

voice_sr = Utils.repartition_for_join(voice_sr, ["cst_con_id"])
voice_act = Utils.repartition_for_join(voice_act, ["sra_sr_id"])
voice_contact = Utils.repartition_for_join(voice_contact, ["row_id"])

# Step 1: Transform - Filtering and Joining
def filter_sr_data(df: DataFrame) -> DataFrame:
    return (df.filter((col("created") >= start_date) & (col("created") <= end_date))
              .withColumn("x_service_rqst_pres_code", upper(trim(col("x_service_rqst_pres_code"))))
              .filter(col("sr_subtype_cd").isin('Email', 'Web Form', 'Form'))
              .select(*sr_cols))

sr_email = filter_sr_data(voice_sr)


def join_contact_data(sr: DataFrame, contact: DataFrame) -> DataFrame:
    contact = contact.withColumnRenamed("row_id", "contact_row_id")
    return sr.join(contact, sr["cst_con_id"] == contact["contact_row_id"], "left").drop(contact["contact_row_id"])

sr_email_c_data = join_contact_data(sr_email, voice_contact)


def join_activity_data(sr: DataFrame, activity: DataFrame) -> DataFrame:
    activity = activity.withColumnRenamed("created", "activity_created").withColumnRenamed("row_id", "activity_row_id")
    return (activity.filter((col("activity_created") >= start_date) & (col("activity_created") <= end_date))
                    .filter(col('todo_cd').isin('Email - Inbound', 'Email - Outbound'))
                    .join(sr, activity.sra_sr_id == sr.row_id, "inner")
                    .orderBy(col("sr_num"), asc("activity_created")))

transform_df = join_activity_data(sr_email_c_data, voice_act)

# Step 2: Staging - Handling NULL values
def handle_null_values(df: DataFrame) -> DataFrame:
    return (df.withColumn("owner_login", coalesce(col("owner_login"), lit("(Unknown)")))
              .withColumn("x_activity_topic_category", coalesce(col("x_activity_topic_category"), lit("(Unknown)")))
              .withColumn("person_uid", coalesce(col("person_uid"), lit("(Unknown)")))
              .withColumn("created", coalesce(col("created"), lit("1900-01-01 00:00:00")))
              .withColumn("updated_date", coalesce(col("updated_date"), lit("1900-01-01 00:00:00"))))

staging_df = handle_null_values(transform_df)


staging_df = staging_df.withColumn(
    "updated_date",
    to_timestamp(col("updated_date"), "yyyyMMddHHmmss")
)

# Apply last update filter
if last_update_datetime:
    staging_df = staging_df.filter(col("updated_date") > last_update_datetime)
else:
    print("Skipping last update filter as no valid datetime was found.")


# Step 3: EDM - Cleaning and Final Selection
def clean_and_preprocess_text(df: DataFrame) -> DataFrame:
    patterns = [
        (r"^.*?WWW:(.*?): ", ""), (r"^.*?Query(.*?): ", ""),
        (r"Query from SST web form: ", ""), (r"\bSent from my iPhone", ""),
        (r"^.*?Query text ", ""), (r"^.*?Message: ", ""),
        (r"([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)", "<EMAIL>"),
        (r"(\S*\d+\S*)", "<NUMBER>"), (r"(?s)-----Original Message.*", ""),
        (r"(?s)____.*", ""), (r"(?s)-----.*", ""), (r"(?s)=====.*", ""),
        (r"(?s)Get Outlook for.*", ""), (r"(?s)^> .*", "")
    ]
    df_cleaned = df.withColumnRenamed("comments_long", "text")
    for pattern, replacement in patterns:
        df_cleaned = df_cleaned.withColumn("text", regexp_replace("text", pattern, replacement))
    return (df_cleaned
                .filter(length("text") >= 15)
                .withColumnRenamed("text", "email")
                .select(*final_cols)
                .orderBy("sr_num", "created"))

# Final EDM DataFrame

edm_df = clean_and_preprocess_text(staging_df).cache()

def filter_srs_queues(df: DataFrame, queue: list) -> DataFrame:
        sr_nums_with_general = df.filter(col("owner_login").isin(queue)).select("sr_num").distinct()
        return df.join(sr_nums_with_general, on="sr_num", how="inner").orderBy("sr_num", col("created").asc())
srs_df = DataProcessor.filter_srs_queues(edm_df, sr_queues)
srs_count = srs_df.count()
print(f"Filtered SRS DataFrame Count: {srs_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data lake

# COMMAND ----------

from datetime import datetime

# Remove directory if it exists
try:
    dbutils.fs.rm(output_path, True)
except:
    # error=f"Directory {output_path} not found, continuing..."
    dbutils.notebook.exit(f"Directory {output_path} not found, continuing...")

# Create the directory
dbutils.fs.mkdirs(output_path)


# Save DataFrame
final_output_path = DataProcessor.save_parquet_file(srs_df, output_path, final_output_base_path)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Cosmos

# COMMAND ----------

from azure.identity import DefaultAzureCredential
from azure.cosmos import CosmosClient


# # User-Assigned Managed Identity Client ID
# uami_client_id = "d8feb374-5d14-4198-8541-f3dfa30df7f8"

# # Use DefaultAzureCredential with the User-Assigned Managed Identity client ID
# credential = DefaultAzureCredential(managed_identity_client_id=uami_client_id)

import base64
# Use Managed Identity credentials
credential = DefaultAzureCredential()

def pad_base64(b64_string):
    return b64_string + '=' * (-len(b64_string) % 4)

# cosmos_key_padded = pad_base64(cosmos_key.strip())
print(cosmos_endpoint)
print(credential)
cosmos_client = CosmosClient(cosmos_endpoint, credential=credential)
# cosmos_client = CosmosClient(cosmos_endpoint, credential=pad_base64(cosmos_key.strip()))
# cosmos_key
# cosmos_client = CosmosClient(cosmos_endpoint, cosmos_key_padded) 
cosmos_handler = CosmosDBHandler(client=cosmos_client, database_name=cosmos_database)

sr_hist_pd = srs_df.toPandas()

if sr_hist_pd.empty:
    dbutils.notebook.exit(f"SR history DataFrame is empty. Aborting insertion into CosmosDB.")
    print("No data to insert into CosmosDB. Exiting...")
    # raise RuntimeError("SR history DataFrame is empty. Aborting insertion into CosmosDB.")

# Insert SR history data
cosmos_handler.insert_sr_history(df=sr_hist_pd, container_name=sr_history_container)

# COMMAND ----------

# MAGIC %md
# MAGIC ## METADATA

# COMMAND ----------

import json
from datetime import datetime
from pyspark.sql.functions import col
import pyspark.sql.functions as sf

# Extract max updated_date from DataFrame
max_updated_date = srs_df.select(
    sf.max(col("updated_date"))
).collect()[0][0]

# Ensure the value is a datetime object (in case schema changes in future)
if isinstance(max_updated_date, str):
    max_updated_date = datetime.strptime(max_updated_date, '%Y%m%d%H%M%S')

# Update metadata with formatted string
MetadataHandler.update_metadata(
    metadata_path,
    metadata_key,
    max_updated_date.strftime('%Y-%m-%d %H:%M:%S')
)

print(f"Maximum updated_date: {max_updated_date}")


# COMMAND ----------


workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
cluster_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('clusterId')
user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')

# Display the environment details
try:
    print(f"Workspace URL: {workspace_url}")
    print(f"Cluster ID: {cluster_id}")
    print(f"User Name: {user_name}")
except:
    dbutils.notebook.exit(f"Unable to retrieve environment details. Please check the cluster configuration.")

# COMMAND ----------

if error is None:
    # error=f"Success : Maximum updated_date: {max_updated_date}"
    dbutils.notebook.exit(f"SR History Success : Maximum updated_date: {max_updated_date}")