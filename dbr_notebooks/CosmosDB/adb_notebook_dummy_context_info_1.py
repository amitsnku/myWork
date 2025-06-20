# Databricks notebook source
!pip install azure-cosmos azure-identity azure-mgmt-cosmosdb


# COMMAND ----------

# Upgrade typing_extensions package
%pip install typing_extensions==4.5.0

# COMMAND ----------

# !python3 -m spacy download en_core_web_md 

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
      "student_sk", "mod_presentation_sk"
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
      "student_sk", "mod_presentation_sk"
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
      'dummy_path': None,
      'file_suffix': ".parquet",
      'file_start': "adlsfile_edmviews_",
      'file_end': "_all.parquet",
      'include_daily': True,
      'use_aml_datastore' : False,
      'override_daily_absence': False,
      'override_required_deduplication': False,
      'table_keys': {
            'fact_agg_mod_presentation_time_period': 'mod_presentation_time_period_nk',
            'fact_student_mod_presentation_vle_visits': 'student_mod_vle_visits_nk',
            'fact_agg_student_academic_year': 'student_academic_year_nk',
            'dim_student_scd': ['student_scd_sk','version_number'],
            'metadata_edm_table_attribute': ['schema_name', 'table_name', 'attribute_number']
            }
    }

  deduplication_not_required = ['fact_student_mod_presentation_vle_site_usage']
  
  def get_spark_dummy_path(self, mounted_required=False):

    if self.config_dict['use_aml_datastore'] == True:
      subscription_id = self.config_dict['subscription_id']
      resource_group_name = self.config_dict['resource_group_name']
      workspace_name = self.config_dict['workspace_name']

      return f"azureml://subscriptions/{subscription_id}/resourcegroups/{resource_group_name}/workspaces/{workspace_name}/datastores/dummy_live_adls/paths/" # aml datastore
    
    else:
      
      mounted_path = "dbfs:/mnt/dummydatalake/"
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
          return f"abfss://oudummy{ws}adls@oudummy{ws}adlsgen2.dfs.core.windows.net/"
      
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
    if self.config_dict['dummy_path'] is None: self.config_dict['dummy_path'] = self.get_spark_dummy_path()
    if self.config_dict['path'] is None: self.config_dict['path'] = self.config_dict['dummy_path'] + self.config_dict['relative_path']

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
    
  def read_fact_student_mod_presentation_vle_site_usage(self, filter_deleted=True):
    # do the hunting subdirectories stuff 
    return self.old_read_table('fact_student_mod_presentation_vle_site_usage', filter_deleted)

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


      if table_name == "fact_student_mod_presentation_vle_site_usage":
        return self.read_fact_student_mod_presentation_vle_site_usage(filter_deleted)
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
          if (('_all' in f) | (t=='fact_student_mod_presentation_vle_site_usage'))]
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
            'crcstudent/circestudent_ci_d_posted_stud_fees': 
                ['fee_trnsn_id', 'fee_trnsn_rec_num', 'personal_id'],
            'Voice/voice_s_evt_act': 'row_id',
            'Voice/voice_s_contact': 'row_id',
            'Voice/voice_s_srv_req': 'row_id'
            }
        }
    
    have_daily_updates = [
      'Campaigner/cpg_cpg_export',
      'crcstudent/circestudent_ci_d_posted_stud_fees',
      'crcstudent/circestudent_ci_d_stud_trans_fee_statuses',
      'Voice/voice_s_evt_act', 'Voice/voice_s_contact', 'Voice/voice_s_srv_req']

    deduplication_not_required = [
      'Campaigner/cpg_cpg_export', 
      'crcstudent/circestudent_ci_d_stud_trans_fee_statuses'
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
# MAGIC ###### Create dummyUN directory under DataService/Projects/Student for first time only - Created by dummy team do not delete below code 

# COMMAND ----------

import os 

SV_FL_METADATA = "dbfs:/mnt/dummydatalake/DataService/Projects/Student/dummyUN/CosmosDB/metadata/"
mtdt_file = "cosmos_metadata.json"
studentpath=f'dbfs:/mnt/dummydatalake/DataService/Projects/Student/'

if 'dummyUN' not in dbutils.fs.ls('dbfs:/mnt/dummydatalake/DataService/Projects/Student/'):
    dbutils.fs.mkdirs(f'dbfs:/mnt/dummydatalake/DataService/Projects/Student/dummyUN/')
    dbutils.fs.mkdirs(f'dbfs:/mnt/dummydatalake/DataService/Projects/Student/dummyUN/CosmosDB/metadata')


# dummy_path = studentpath+'dummyUN/'
json_path = SV_FL_METADATA+mtdt_file

# Check if the file exists
try:
    if not any(file.name == mtdt_file for file in dbutils.fs.ls(SV_FL_METADATA)):
    # Create an empty JSON file
        dbutils.fs.put(json_path, '{"dummy_history":"","dummy_data": ""}',True)
        print("Empty JSON file created.")
    else:
        print("File already exists.")
except exception as e:
    dbutils.notebook.exit(f"metatdata file cosmos_metadata.json not exist: {e}")

# COMMAND ----------

import sys
import os
## Read common files
user_id = spark.sql('select current_user() as user').collect()[0]['user']  
# sys.path.append(os.path.abspath(f"/Workspace/Repos/{user_id}/repo-ds-ml-general/")) # Temporarly changed by Amit as manually repos created under specific user
# sys.path.append(os.path.abspath(f"/Workspace/Users/{user_id}/repo-ds-ml-general/"))
# sys.path.append(os.path.abspath(f'/Workspace/DataScience1/repo-ds-ml-general/')
# repo_path =f'/Workspace/ds-ml-general/repo-ds-ml-general'
# from common_data_processing.edm_reader import EDM, EDMSource

error= None
import decimal
import re
import json
import pandas as pd
from pyspark.sql.window import Window
from pyspark.sql import functions as sf, DataFrame
import datetime as dt
from pyspark.sql.functions import col, count, when, regexp_replace, first, row_number, concat_ws
import pandas as pd
import pyspark.sql.types as T
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window
import json
import uuid
import random
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, DoubleType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructType, StructField, StringType

#Cosmos DB
from azure.cosmos import CosmosClient, PartitionKey, exceptions
from azure.identity import DefaultAzureCredential, AzureCliCredential
import base64



# COMMAND ----------

dbutils.widgets.text("adfglparam_dummy_cosmos_endpoint_1", "")
dummy_cosmos_endpoint = dbutils.widgets.get("adfglparam_dummy_cosmos_endpoint_1")

# COMMAND ----------

VOICE_FILE_PREFIX = "dbfs:/mnt/dummydatalake/DataService/SourceData/ConsolidatedExtract/Voice/"
# Save file path
SAVE_FILE_METDATA = "/dbfs/mnt/dummydatalake/DataService/Projects/Student/dummyUN/CosmosDB/metadata/"

output_path = "dbfs:/mnt/dummydatalake/DataService/Projects/Student/dummyUN/Studentdata/temp"
final_output_base_path = "dbfs:/mnt/dummydatalake/DataService/Projects/Student/dummyUN/CosmosDB/Studentdata/"

SUFFIX = ".parquet"

# Cosmos configuration

cosmos_endpoint=dummy_cosmos_endpoint

cosmos_database = "dummy"
dummy_data_container = "dummy_data"
metadata_file = "cosmos_metadata.json"
metadata_key = "dummy_data"
metadata_path  = f"{SAVE_FILE_METDATA}{metadata_file}"

# Presentations prior to the given wont be included in the final data
MIN_PRESENTATION_CODE = '2020B'

# Initial metadata structure with None values
default_metadata = {
    "dummy_history": None,
    "dummy_data": None
}

# COMMAND ----------

# # Use Managed Identity credentials
# credential1 = DefaultAzureCredential()

# try:
#     # Ensure the cosmos_key is properly padded
#     def pad_base64(b64_string):
#         return b64_string + '=' * (4 - len(b64_string) % 4)
    
#     cosmos_client1 = CosmosClient(cosmos_endpoint, credential=credential1)

#     # cosmos_key_padded = pad_base64(cosmos_key.strip())
#     # cosmos_client = CosmosClient(url=cosmos_endpoint, credential=cosmos_key_padded)

# # cosmos_client = CosmosClient(cosmos_endpoint, cosmos_key_padded) #Commented by Amit
# # cosmos_handler = CosmosDBHandler(client=cosmos_client, database_name=cosmos_database)

#     # Create database if it doesn't exist
#     database = cosmos_client1.create_database_if_not_exists(id=cosmos_database)

#     # Convert PySpark DataFrame to Pandas
#     # if dummy_data_mod:
#     #     dummy_data_pd = dummy_data_mod.toPandas()
#     #     print("Successfully connected to Cosmos DB and converted DataFrame to Pandas.")
#     # else:
#     #     dbutils.notebook.exit(f"SR history DataFrame is empty. Aborting ")

# except Exception as e:
#     # error="Error connecting to Cosmos DB or PD dataframe conversion: {e}"
#     dbutils.notebook.exit(f"Error connecting to Cosmos DB or PD dataframe conversion: {e}")
#     raise  # Stop execution

# COMMAND ----------

#new edm reading bit
EDM_TABLES_REQUIRED = {
     "dim_assessment_task" :'dim_assess',
     "dim_board_of_study": 'dim_bofs',
     "dim_faculty":'dim_faculty',
     "dim_mod_presentation":'dim_mod_pres',
     "dim_pricing_area":'dim_price_area',
     "dim_student":'dim_student',
     "dim_academic_year":'dim_ay',
     "dim_qualification":'dim_qual',
     "fact_student_crm_interaction_prospectus":'fact_scrmip',
     'fact_student_mod_presentation_progression':'fact_smpp',
     "dim_student_scd":'dim_scd',
     'fact_student_qualification':'fact_std_qual',
     "dim_registration_status":'dim_reg_status',
     "fact_student_mod_presentation_registration_status":'fact_smprs',
     "dim_student_classification":'dim_std_class',
     "dim_date":'dim_date',
     "dim_mod_result":'dim_mod_result',
     "fact_student_basket_item":'fact_std_basket',
     "fact_student_qualification_study_year":'fact_std_qual_ay',
     "dim_qualification_registration_status":'dim_qual_reg_stts',
     "dim_creditsband":"dim_credits_band",
     'fact_student_mod_presentation_qualification':'fact_smpq',
     'dim_fee_code':'dim_fee_code',
     'dim_fee_payment_status':'dim_fee_payment_status',
     'dim_fee_type':'dim_fee_type',
     'fact_agg_student_say_fee_entry':'fact_agfe',
     'fact_student_mod_presentation_fee_entry':'fact_smpfe'
}

#,
edm = EDM(EDM_TABLES_REQUIRED, config_dict={'include_daily': True, 'override_daily_absence': True, 'use_aml_datastore': False})

tables_dict_source = {
            'crcproducts/circeproducts_pr_d_cvp_fees': 'listed_fees'
            }
edm_source = EDMSource(tables_dict_source, config_dict={'use_aml_datastore': False})
listed_fees = (edm_source.listed_fees
             .withColumn("pricing_area_code", sf.trim(sf.col("pricing_area_code")))
             )

dim_assess = edm.dim_assess
dim_bofs = edm.dim_bofs
dim_faculty =edm.dim_faculty
dim_mod_pres =edm.dim_mod_pres
dim_price_area=edm.dim_price_area
dim_student =edm.dim_student
dim_student_scd=edm.dim_scd
dim_ay=edm.dim_ay
fact_std_basket=edm.fact_std_basket
fact_smpp=edm.fact_smpp
fact_std_qual=edm.fact_std_qual
fact_std_qual_ay=edm.fact_std_qual_ay
dim_qual_reg_stts=edm.dim_qual_reg_stts
dim_qual=edm.dim_qual
dim_reg_status=edm.dim_reg_status
fact_smprs=edm.fact_smprs
dim_std_class=edm.dim_std_class
dim_date=edm.dim_date
dim_mod_result=edm.dim_mod_result
dim_credits_band=edm.dim_credits_band
fact_smpq=edm.fact_smpq
dim_fee_code=edm.dim_fee_code
dim_fee_payment_status=edm.dim_fee_payment_status
dim_fee_type=edm.dim_fee_type
fact_agfe=edm.fact_agfe
fact_smpfe=edm.fact_smpfe

# COMMAND ----------


import uuid
#helper functions
def get_date_with_offset(days=0):
    """
    Returns the date after adding the specified number of days to today's date.

    Parameters:
    - days (int): The number of days to add to today's date. Can be negative for past dates.

    Returns:
    - str: The resulting date in "YYYY-MM-DD" format.
    """
    target_date = dt.datetime.now() + dt.timedelta(days=days)
    return target_date.strftime("%Y-%m-%d")

def repartition_for_join(df, columns, partitions=200):
    return df.repartition(partitions, *columns)
    
def convert_to_timestamp(df, columns, format="yyyy-MM-dd HH:mm:ss.SSSSSSS"):
    for column in columns:
        df = df.withColumn(column, sf.to_timestamp(col(column), format))
    return df

def read_metadata(metadata_path, default_metadata, key=None):
    """
    Initializes metadata if the file doesn't exist or reads a specific key if provided.
    """
    try:
        if not os.path.exists(metadata_path):
            # If file doesn't exist, create it with default metadata
            with open(metadata_path, "w") as file:
                json.dump(default_metadata, file, indent=4)
            print("Metadata file initialized.")
            return default_metadata if key is None else {key: default_metadata.get(key)}
        else:
            # Read the existing metadata
            with open(metadata_path, "r") as file:
                metadata = json.load(file)
                return metadata if key is None else {key: metadata.get(key)}
    except Exception as e:
        # error=f"Error initializing or reading metadata: {e}"
        dbutils.notebook.exit(f"Error initializing or reading metadata: {e}")
        return None

def update_metadata_key(metadata_path, key, new_value):
    """
    Updates a specific key in the metadata file with a new value.
    Handles datetime and other non-serializable objects by converting them to JSON-compatible formats.
    
    Parameters:
    - metadata_path (str): Path to the metadata JSON file.
    - key (str): The key to update.
    - new_value: The new value to set. If it's a datetime, it will be converted to a string.
    """
    try:
        # Ensure new_value is JSON-serializable
        if isinstance(new_value, dt.datetime):
            new_value = new_value.isoformat()  # Convert datetime to ISO format string
        
        # Read the existing metadata
        with open(metadata_path, "r") as file:
            metadata = json.load(file)
        
        # Update the key
        metadata[key] = new_value
        
        # Write back the updated metadata
        with open(metadata_path, "w") as file:
            json.dump(metadata, file, indent=4)
        
        print(f"Metadata updated for key '{key}': {new_value}")
    except json.JSONDecodeError as e:
        # error=f"Error: Invalid JSON format in file '{metadata_path}': {e}"
        dbutils.notebook.exit(f"Error: Invalid JSON format in file '{metadata_path}': {e}")
    except FileNotFoundError:
        # error=f"Error: Metadata file '{metadata_path}' not found."
        dbutils.notebook.exit(f"Error: Metadata file '{metadata_path}' not found.")
    except Exception as e:
        # error=f"Error updating metadata key: {e}"
        dbutils.notebook.exit(f"Error updating metadata key: {e}")

def get_fee_data ():
    paid_fee_data = (
    fact_smpfe
    .join(
        dim_mod_pres.drop('updated_datetime'),
        on='mod_presentation_sk',
        how='inner'
    )
    .filter(
        (sf.col('is_latest_student_mod_presentation_fee_type_transaction')==1 ) &
        (sf.col('presentation_code')>= '2012I') &
        (sf.col('is_tuition_fee_type') ==1)
    )
    .select(
        'student_sk',
        'mod_presentation_sk',
        'cumulative_total_student_mod_presentation_fee_type_mod_payment',
        'updated_datetime'
    )
)
    return paid_fee_data

def sanitize_value(value, default=""):
    """Helper function to replace None values with a default value."""
    return value if value is not None else default

## Add this in mod later student_registration_status_update_datetime
def create_mod_data(row):
    """Helper function to create a mod dictionary from a DataFrame row."""
    return {
        # "student_new_continuing_description": sanitize_value(row["student_new_continuing_description"]),
        "mod_code": sanitize_value(row["mod_code"]),
        "meta_status": sanitize_value(row["meta_status"]),
        "mod_short_title": sanitize_value(row["mod_short_title"]),
        "presentation_code": sanitize_value(row["presentation_code"]),
        "academic_year": sanitize_value(row["academic_year"]),
        "mod_study_level_description": sanitize_value(row["mod_study_level_description"]),
        "is_reserved_ever": sanitize_value(row["is_reserved_ever"]),
        "is_registered_currently": sanitize_value(row["is_registered_currently"]),
        "is_passed": sanitize_value(row["is_passed"]),
        "is_withdrawn_currently": sanitize_value(row["is_withdrawn_currently"]),
        "count_of_mods_passed_in_previous_academic_years": sanitize_value(row["count_of_mods_passed_in_previous_academic_years"], 0),
        "designed_tmas": sanitize_value(row["designed_tmas"], 0),
        "earliest_reserved_date": sanitize_value(row["earliest_reserved_date"]),
        "board_of_study_name": sanitize_value(row["board_of_study_name"]),
        "faculty_long_name": sanitize_value(row["faculty_long_name"]),
        "mod_result_description": sanitize_value(row["mod_result_description"]),
        "current_registration_status_description": sanitize_value(row["current_registration_status_description"]),
        "mod_presentation_reservation_close_date": sanitize_value(row["mod_presentation_reservation_close_date"]),
        "mod_presentation_reservation_open_date": sanitize_value(row["mod_presentation_reservation_open_date"]),
        "mod_credits": sanitize_value(row["mod_credits"], 0),
        "mod_presentation_start_date": sanitize_value(row["mod_presentation_start_date"]),
        "total_credits_registered_per_ay": sanitize_value(row["total_credits_registered_per_ay"], 0),
        "qualification_code": sanitize_value(row["qualification_code"]),
        "qualification_title": sanitize_value(row["qualification_title"]),
        "fee_paid_upto_date": sanitize_value(row["fee_paid_upto_date"]),
        "mod_listed_fee": sanitize_value(row["mod_listed_fee"]),
        "balance_fee": sanitize_value(row["balance_fee"]),
        "is_conferred": sanitize_value(row["is_conferred"]),
        "is_eligible_to_confer": sanitize_value(row["is_eligible_to_confer"])
    }



def get_date_two_years_ago():
    today = dt.datetime.today()
    # Subtract 3 years by adjusting the year
    try:
        two_years_ago = today.replace(year=today.year - 2)
    except ValueError:
        # Handle leap year case (e.g., Feb 29 → Feb 28)
        two_years_ago = today.replace(year=today.year - 2, day=28)
    return two_years_ago.strftime('%Y-%m-%d')



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
        dbutils.notebook.exit(f"No Parquet file found after writing to {output_path}!")
        # raise Exception("No Parquet file found after writing!")

    parquet_file = parquet_files[0]

    # Define final output path
    final_output_path = f"{final_output_base_path}student_info_{datetime.now().strftime('%Y%m%d%H%M%S')}.parquet"

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




from azure.cosmos import CosmosClient, PartitionKey, exceptions as cosmos_exceptions
from azure.identity import DefaultAzureCredential
import traceback
import pandas as pd


def upsert_student_data(database_name, container_name, df_spark):
    record_count = df_spark.count()
    target_partitions = min(max(record_count // 10000, 1), 100)

    # Repartition based on record count only
    df_spark = df_spark.repartition(target_partitions)

    def upsert_partition_collect(partition_rows):
        upserted_ids = []

        try:
            credential = DefaultAzureCredential()
            cosmos_client = CosmosClient(cosmos_endpoint, credential=credential)
            container = cosmos_client.get_database_client(database_name).get_container_client(container_name)
        except Exception as e:
            print("Failed to initialize Cosmos client:", traceback.format_exc())
            return iter([])

        for row in partition_rows:
            try:
                row = row.asDict()
                person_uid = sanitize_value(row.get('personal_id'))
                mod_data = create_mod_data(row)
                mod_code = mod_data.get("mod_code")

                if not person_uid or not mod_code:
                    continue

                try:
                    doc = container.read_item(item=person_uid, partition_key=person_uid)
                    exists = True
                except cosmos_exceptions.CosmosHttpResponseError as e:
                    if e.status_code == 404:
                        exists = False
                    else:
                        raise  # Raise the exception for other errors

                if exists:
                    mods = doc.setdefault('mods', [])
                    existing_mod = next((m for m in mods if m.get('mod_code') == mod_code), None)
                    if not existing_mod:
                        print(f"Updating/Adding new mod for {person_uid}: {mod_code}")
                        print(f"Updating mods for {person_uid}")
                        mods.append(mod_data)
                        container.upsert_item(doc)
                        upserted_ids.append((person_uid, mod_code))
                else:
                    print(f"Creating new document for {person_uid}")
                    new_doc = {
                        "id": person_uid,
                        "person_uid": person_uid,
                        "mods": [mod_data]
                    }
                    container.upsert_item(new_doc)
                    upserted_ids.append((person_uid, mod_code))
            except Exception as e:
                print(f"Error processing {person_uid}: {e}")

        return iter(upserted_ids)

    # Collect upserted pairs
    print("======= Upsert Process Started =======")
    upserted_pairs = df_spark.rdd.mapPartitions(upsert_partition_collect).flatMap(lambda x: x).collect()

    print("======= Upsert Process Completed =======")
    #print(f"Total upserted records: {len(upserted_pairs)}")

    # Validation and Re-Upsert Logic
    try:
        credential = DefaultAzureCredential()
        cosmos_client = CosmosClient(cosmos_endpoint, credential=credential)
        container = cosmos_client.get_database_client(database_name).get_container_client(container_name)

        # Re-fetch items from Cosmos DB
        items = list(container.query_items(query="SELECT * FROM c", enable_cross_partition_query=True))
        items_with_mods = [item for item in items if 'mods' in item and isinstance(item['mods'], list) and item['mods']]
        df_cosmos = pd.json_normalize(items_with_mods, record_path=['mods'], meta=['person_uid'])

        # Convert to sets for comparison
        parquet_df = df_spark.toPandas()
        parquet_set = set(zip(parquet_df['personal_id'], parquet_df['mod_code']))
        cosmos_set = set(zip(df_cosmos['person_uid'], df_cosmos['mod_code']))

        missing_pairs = parquet_set - cosmos_set

        # Re-Upsert Logic for Missing Pairs
        if missing_pairs:
            print("==== Re-Upserting Missing Pairs ====")
            for person_uid, mod_code in missing_pairs:
                missing_records = parquet_df[(parquet_df['personal_id'] == person_uid) & (parquet_df['mod_code'] == mod_code)].to_dict(orient='records')
                for record in missing_records:
                    try:
                        mod_data = create_mod_data(record)
                        try:
                            doc = container.read_item(item=person_uid, partition_key=person_uid)
                            # Append mod if not exists
                            existing_mod_codes = {m['mod_code'] for m in doc.get('mods', [])}
                            if mod_code not in existing_mod_codes:
                                doc['mods'].append(mod_data)
                                container.upsert_item(doc)
                                print(f"Re-upserted missing mod for {person_uid}: {mod_code}")
                        except cosmos_exceptions.CosmosHttpResponseError as e:
                            if e.status_code == 404:
                                # Create new document if not found
                                new_doc = {
                                    "id": person_uid,
                                    "person_uid": person_uid,
                                    "mods": [mod_data]
                                }
                                container.upsert_item(new_doc)
                                print(f"Created new document during re-upsert for {person_uid}: {mod_code}")
                    except Exception as e:
                        print(f"Error during re-upsert for {person_uid}, {mod_code}: {e}")

        # Validation Summary
        print("==== Validation Summary Before Re-Upsert ====")
        print(f"Total (person_uid, mod_code) pairs in Parquet: {len(parquet_set)}")
        print(f"Total (person_uid, mod_code) pairs in Cosmos DB: {len(cosmos_set)}")
        print(f"Missing (person_uid, mod_code) pairs in Cosmos DB: {len(missing_pairs)}")
        if missing_pairs:
            print("==== Missing Pairs in Cosmos DB as List ====")
            missing_pairs_list = list(missing_pairs)
            print(missing_pairs_list)

        # Re-Validation After Re-Upsert
        print("==== Re-Validation After Re-Upsert ====")
        try:
            # Re-fetch items from Cosmos DB after re-upsert
            items = list(container.query_items(query="SELECT * FROM c", enable_cross_partition_query=True))
            items_with_mods = [item for item in items if 'mods' in item and isinstance(item['mods'], list) and item['mods']]
            df_cosmos = pd.json_normalize(items_with_mods, record_path=['mods'], meta=['person_uid'])

            # Convert to sets for re-validation
            cosmos_set_after = set(zip(df_cosmos['person_uid'], df_cosmos['mod_code']))
            missing_pairs_after = parquet_set - cosmos_set_after

            # Re-Validation Summary
            print("==== Re-Validation Summary After Re-Upsert ====")
            print(f"Total (person_uid, mod_code) pairs in Parquet: {len(parquet_set)}")
            print(f"Total (person_uid, mod_code) pairs in Cosmos DB After Re-Upsert: {len(cosmos_set_after)}")
            print(f"Missing (person_uid, mod_code) pairs in Cosmos DB After Re-Upsert: {len(missing_pairs_after)}")

            # Print remaining missing pairs as a list
            if missing_pairs_after:
                print("==== Remaining Missing Pairs in Cosmos DB as List After Re-Upsert ====")
                print(list(missing_pairs_after))

        except Exception as e:
            print(f"Error during re-validation: {e}")

    except Exception as e:
        print(f"Error during validation and re-upsert: {e}")







def group_location_column(df, location_col="location", new_col="location_grouped"):
    """
    Adds a new column to a PySpark DataFrame, grouping location values into specified categories.

    Parameters:
        df (DataFrame): The input PySpark DataFrame.
        location_col (str): The name of the column containing location values.
        new_col (str): The name of the new column to be added.

    Returns:
        DataFrame: A DataFrame with the new grouped location column.
    """
    grouped_df = df.withColumn(
        new_col,
        sf.when(col(location_col).rlike("(?i)wales"), "Wales")
        .when(col(location_col).rlike("(?i)scotland"), "Scotland")
        .when(col(location_col).rlike("(?i)republic"), "Republic of Ireland")
        .when(col(location_col).rlike("(?i)northern"), "Northern Ireland")
        .when(col(location_col).rlike("(?i)england"), "England")
        .when(col(location_col).rlike("(?i)worldwide"), "Other European and Worldwide")
        .when(col(location_col).rlike("(?i)kingdom"), "United Kingdom")
        .when(col(location_col).rlike("(?i)approved"), "European Approved Study Area")
        .otherwise("Other")
    )
    return grouped_df

def add_is_ousba_column(df, col1, col2, country_list, new_col_name = "is_ousba"):
    """
    Adds a column to the DataFrame indicating if a country matches a given list, with hierarchy logic.

    Parameters:
    - df (DataFrame): Input PySpark DataFrame.
    - col1 (str): Primary country column name.
    - col2 (str): Secondary country column name.
    - country_list (list): List of countries to check against.
    - new_col_name (str): Name of the new column to be added (default is 'is_ousba').

    Returns:
    - DataFrame: PySpark DataFrame with the new column added.
    """
    return df.withColumn(
        new_col_name,
        when(col(col1).isNotNull() & col(col1).isin(*country_list), 1)
        .when(
            col(col1).isNull() & col(col2).isNotNull() & col(col2).isin(*country_list),
            1,
        )
        .when(col(col1).isNull() & col(col2).isNull(), -1)
        .otherwise(0),
    )

def update_mod_result(df, result_col, status_col, new_col):
    """
    Updates a column based on specific conditions:
    - Keeps the value of `result_col` as is unless the value is '(Not Applicable)'.
    - If the value is '(Not Applicable)', picks the value from `status_col`.

    Parameters:
    - df (DataFrame): Input PySpark DataFrame.
    - result_col (str): Column containing mod result descriptions.
    - status_col (str): Column to use as a fallback when the result is '(Not Applicable)'.
    - new_col (str): Name of the new column to be created (default is 'updated_result').

    Returns:
    - DataFrame: Updated PySpark DataFrame with the new column.
    """
    return df.withColumn(
        new_col,
        when(col(result_col) == "(Not Applicable)", col(status_col))
        .otherwise(col(result_col))
    )



# COMMAND ----------

# MAGIC %md
# MAGIC ### Dates bound 

# COMMAND ----------

from datetime import datetime
try:
    metadata = read_metadata(metadata_path, default_metadata, key=metadata_key)
    last_update_datetime = metadata.get(metadata_key)

    LST_2_YEARS = get_date_two_years_ago()
    if not last_update_datetime:
        DATE_START = get_date_two_years_ago()
    else:
        DATE_START = last_update_datetime

    print(f"Student data last update: {last_update_datetime} \
        Date Start: {DATE_START}")

except Exception as e:
    # error=f"Error reading metadata or setting DATE_START: {e}"
    dbutils.notebook.exit(f"Error reading metadata or setting DATE_START: {e}")
    raise  # Stop execution if an error occurs


# COMMAND ----------

SMPP_COLS = [
    'student_sk',
    'is_reserved_ever',
    'is_registered_currently_and_mod_presentation_not_started',
    'is_registered_at_day_14',
    'is_registered_currently',
    'is_registered_ever',
    'is_registered_at_start_and_not_passed',
    'is_completed_final_assessment_task',
    'is_passed',
    'count_of_mods_started_and_not_completed_in_previous_academic_years',
    'count_of_mods_passed_in_previous_academic_years',
    'is_withdrawn_ever',
    'is_withdrawn_currently',
    'mod_presentation_sk',
    'mod_faculty_sk',
    'mod_board_of_study_sk',
    'pricing_area_sk',
    'mod_result_sk',
    'earliest_reserved_date_sk',
    'latest_withdrawn_date_sk',
    'current_student_mod_presentation_status_date_sk',
    'student_classification_sk',
    'current_registration_status_sk',
    'total_credits_passed_in_previous_academic_years_band_sk',
    'mod_presentation_academic_year_sk',
    'updated_datetime'
    ]

STD_COLS =  [
    'student_sk',
    'personal_id',
    'student_awarded_credit_transfer_ever_flag',
    'student_has_staff_marker_flag',
    'student_gender_description',
    'student_country_description',
    'student_crm_website_country',
    'student_date_of_birth',
    'updated_datetime'
]
# need mod code
MOD_COLS = [
    "mod_presentation_sk",
    'mod_code',
    'presentation_code',
    'mod_short_title',
    'mod_study_level_description',
    'designed_tmas',
    'mod_presentation_start_date',
    'mod_presentation_reservation_close_date',
    'mod_presentation_reservation_open_date',
    'mod_credits',
    # 'mod_presentation_academic_year'
    ]

DIM_AY_COLS = [
    'academic_year_sk',
    'academic_year',
    'academic_year_start_date',
    'academic_year_end_date'
    ]


SMPQ_COLS = [
    'student_sk',
    'mod_presentation_sk',
    'qualification_sk',
    ]

DIM_QUAL_COLS = [
    'qualification_sk',
    'qualification_code',
    'qualification_title'
    ]

SK_COLS = [
    'current_registration_status_sk',
    'qualification_sk',
    'acaademic_year_sk',
    'mod_presentation_academic_year_sk',
    'academic_year_sk'
]

FSQ_COLS = [
    'student_sk',
    'qualification_sk',
    'is_eligible_to_confer',
    'is_conferred',
    'is_awarded_first_or_upper_second_class_degree',
    'is_awarded_first_or_second_class_degree'
]

OUSBA_COUNTRIES = [
    'United Kingdom','Northern Ireland','England','Scotland','Wales','Switzerland','Norway','Iceland','Andorra','Liechtenstein','Monaco','San Marino','Vatican City State',"Austria", "Belgium", "Bulgaria", "Croatia", "Cyprus",'Cyprus, Republic of', "Czech Republic", "Denmark", "Estonia", "Finland", "France", "Germany", "Greece", "Hungary", "Ireland", "Italy", "Latvia", "Lithuania", "Luxembourg", "Malta", "Netherlands", "Poland", "Portugal", "Romania", "Slovakia", "Slovenia", "Spain", "Sweden"
]

# COMMAND ----------

fact_fees_paid = get_fee_data()

# 1. Get new rows from dim_student
new_dim_student_df = (
    dim_student.select(STD_COLS)
    .filter(sf.col("updated_datetime") >=  DATE_START)
)

# 2. Get new rows from fact_smpp
new_fact_smpp_df = (
    fact_smpp.select(SMPP_COLS)
    .filter(sf.col("updated_datetime") >= DATE_START)
)

# 3. Get new rows from fact_fees
new_fact_fees_df = (
    fact_fees_paid
    .filter(sf.col("updated_datetime") >= DATE_START)
)

# 4. Process updated dim_std with fact_smpp & fees
new_std_smpp_fee = (
    new_dim_student_df
    .join(
        fact_smpp
        .select(SMPP_COLS)
        .filter(sf.col("updated_datetime") >= LST_2_YEARS)
        .drop("updated_datetime"),
        on="student_sk",
        how="left")
    .join(
        fact_fees_paid.drop("updated_datetime"),
        on=['student_sk','mod_presentation_sk'],
        how="left"
    )
     # Always matches
)

# 5. Process updated  fact_smpp with dim_std & fees
std_new_smpp_fee = (
    dim_student
    .select(STD_COLS)
    .drop("updated_datetime")
    .join(
        new_fact_smpp_df,
        on="student_sk",
        how="inner")
    .join(
        fact_fees_paid.drop("updated_datetime"),
        on=['student_sk','mod_presentation_sk'],
        how="left"
    )
     # Always matches
)

# 6. Process updated  fees with dim_std & fact_smpp
std_smpp_new_fee = (
    dim_student
    .select(STD_COLS)
    .drop("updated_datetime")
    .join(
        fact_smpp
        .select(SMPP_COLS)
        .drop("updated_datetime"),
        on="student_sk",
        how="inner")
    .join(
        new_fact_fees_df,
        on=['student_sk','mod_presentation_sk'],
        how="inner"
    )
     # Always matches
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Align , union and deduplicate the data

# COMMAND ----------

# Step 1: Align columns across DataFrames
# Get the schema of the first DataFrame to use as reference
reference_columns = new_std_smpp_fee.columns
reference_schema = new_std_smpp_fee.schema

# Align std_new_smpp_fee
std_new_smpp_fee_aligned = std_new_smpp_fee.select([
    std_new_smpp_fee[col].cast(reference_schema[col].dataType) if col in std_new_smpp_fee.columns else sf.lit(None).cast(reference_schema[col].dataType).alias(col)
    for col in reference_columns
])

# Align std_smpp_new_fee
std_smpp_new_fee_aligned = std_smpp_new_fee.select([
    std_smpp_new_fee[col].cast(reference_schema[col].dataType) if col in std_smpp_new_fee.columns else sf.lit(None).cast(reference_schema[col].dataType).alias(col)
    for col in reference_columns
])

# Step 2: Union the DataFrames
all_data_union = new_std_smpp_fee.union(std_new_smpp_fee_aligned).union(std_smpp_new_fee_aligned)

# Step 3: Deduplicate based on ["student_sk", "mod_presentation_sk"]
all_data_deduplicated = all_data_union.dropDuplicates(["student_sk","mod_presentation_sk"])

# display(all_data_deduplicated)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Add other bits of information 

# COMMAND ----------

mode_res = (all_data_deduplicated
            .join(
                dim_mod_pres
                .select(MOD_COLS)
                .filter(f"dummy_mod_flag == 'NO' AND mod_credits > 0 AND mod_code != 'Z999' AND mod_code not like 'X%' ")#[TODO] resits shouldnt be filtered out
                .filter( ~(sf.col('mod_code').contains('XY') | sf.col('mod_code').contains('ZFM')) ) 
                #exclude  apprenticeships and microcredentials 
                ,
                on='mod_presentation_sk',
                how='inner')
            .withColumn("age", 
                          (sf.datediff(sf.current_date(), sf.col("student_date_of_birth")) / 365)
                          .cast("int"))
            )


mod_res_v2 = (mode_res #[TODO] dim_date function
              .join(
                  dim_date.select('date_sk','date_value'),
                  on = mode_res.earliest_reserved_date_sk == dim_date.date_sk,
                  how='inner') #assuming all date_sks be present in dim_date)
                .withColumnRenamed('date_value','earliest_reserved_date')
                .withColumn("earliest_reserved_date", sf.to_date(col("earliest_reserved_date"), "yyyy-MM-dd")) 
                # earliest reserved date should be after the cutoffdate cause thats what we are predicting
                # .filter(sf.col('earliest_reserved_date')> DATE_CUTOFF)
                .drop('date_sk','earliest_reserved_date_sk')
)

mod_res_v3 = (mod_res_v2
              .join(
                  dim_date.select('date_sk','date_value'),
                  on = mod_res_v2.latest_withdrawn_date_sk == dim_date.date_sk,
                  how='inner') #assuming all date_sks be present in dim_date)
                .withColumnRenamed('date_value','latest_withdrawn_date') 
                .drop('date_sk','latest_withdrawn_date_sk')
)


mod_res_v4 = (mod_res_v3
              .join(
                  dim_date.select('date_sk','date_value'),
                  on = mod_res_v3.current_student_mod_presentation_status_date_sk == dim_date.date_sk,
                  how='inner') #assuming all date_sks be present in dim_date)
                .withColumnRenamed('date_value','current_student_mod_presentation_status_date') 
                .drop('date_sk','current_student_mod_presentation_status_date_sk')
)

# Adding board of study informaion
mod_res_v5 = (mod_res_v4
              .join(
                  dim_bofs
                  .select('board_of_study_sk',
                          'board_of_study_name',
                          'board_of_study_code'),
                  on= mod_res_v4.mod_board_of_study_sk == dim_bofs.board_of_study_sk,
                  how= 'inner'
              )
              .drop('mod_board_of_study_sk','board_of_study_sk')
              )

# Adding mod facutly information
mod_res_v6 = (mod_res_v5
              .join(
                  dim_faculty
                  .select('faculty_sk',
                          'faculty_long_name',
                          'faculty_short_name',
                          'faculty_code'),
                  on= mod_res_v5.mod_faculty_sk == dim_faculty.faculty_sk,
                  how= 'inner'
              )
              .drop('mod_faculty_sk','faculty_sk')
              )



mod_res_v7 = (mod_res_v6
              .join(
                  dim_mod_result
                  .select('mod_result_sk',
                          'mod_result_code',
                          'mod_result_description',
                          'mod_result_category_description'),
                  on= 'mod_result_sk',
                  how= 'inner'
              )
              .drop('mod_result_sk')
              )
mod_res_v8 = (mod_res_v7
              .join(
                  dim_price_area
                  .select('pricing_area_sk',
                          'pricing_area_description',
                          'uk_nation_flag',
                          'pricing_area_code'),
                  on= 'pricing_area_sk',
                  how= 'inner'
              )
              .drop('pricing_area_sk')
              )


#Addidng new/cont info 
mod_res_v9 = (mod_res_v8
              .join(
                  dim_std_class
                  .select('student_classification_sk',
                          'student_new_continuing_description',
                          'student_new_continuing_code'),
                  on= 'student_classification_sk',
                  how= 'inner'
              )
              .drop('student_classification_sk')
              )
              

# COMMAND ----------

mod_res_v10 = (mod_res_v9
               .join(
                   dim_reg_status
                   .select('registration_status_sk',
                           'registration_status_code',
                           'registration_status_reason_code',
                           'registration_status_description',
                           'registration_status_reason_description'),
                   on= mod_res_v9.current_registration_status_sk == dim_reg_status.registration_status_sk,
                   how= 'inner'
               )
               .drop('registration_status_sk')
               .withColumnRenamed('registration_status_code','current_registration_status_code')
               .withColumnRenamed('registration_status_reason_code','current_registration_status_reason_code')
               .withColumnRenamed('registration_status_description','current_registration_status_description')
               .withColumnRenamed('registration_status_reason_description','current_registration_status_reason_description')
               )

# COMMAND ----------

mod_res_v11 = (mod_res_v10
               .join(dim_ay.select(DIM_AY_COLS),
                     on= dim_ay.academic_year_sk == mod_res_v10.mod_presentation_academic_year_sk,
                     how='inner'
                     )
               )

# COMMAND ----------

#adding credits per acacemic year 
cr_per_ay = (mod_res_v11
             .select('student_sk',
                     'academic_year',
                     'mod_credits',
                     'is_registered_ever',
                     'is_withdrawn_currently',
                     'is_completed_final_assessment_task')
             .groupby(
                 'student_sk',
                 'academic_year')
             .agg(
                 sf.sum(
                     sf.when(col('is_completed_final_assessment_task')==1, sf.col('mod_credits'))
                     .when(
                         (sf.col('is_registered_ever') == 1) 
                         &
                         (sf.col('is_withdrawn_currently') != 1)
                         , sf.col('mod_credits'))
                     .otherwise(0)
                 ).alias('total_credits_registered_per_ay'),
                 sf.sum(sf.col('mod_credits')
                 ).alias('total_credits_per_ay')
             )
             )

    

# COMMAND ----------

mod_res_v12 = (mod_res_v11
               .join(
                   cr_per_ay,
                   on = ['student_sk','academic_year'],
                   how = 'inner'
               )
)

# COMMAND ----------

mod_res_v13 = (mod_res_v12
               .join(
                   fact_smpq.select(SMPQ_COLS),
                   on = ['student_sk','mod_presentation_sk'],
                   how = 'inner')
               .join(
                   dim_qual.select(DIM_QUAL_COLS),
                   on='qualification_sk',
                   how='inner'
               )
               )

# COMMAND ----------


# Define a Window partitioned by student_sk and qualification_sk and ordered by current_status_date descending
window_spec = Window.partitionBy("student_sk", "qualification_sk").orderBy(sf.desc("current_status_date"))

# Add a row_number column to identify the most recent row for each student_sk and qualification_sk
fact_std_qual_with_rn = fact_std_qual.withColumn("row_number", sf.row_number().over(window_spec))

# Filter only the most recent rows (row_number = 1)
fact_std_qual_filtered = (fact_std_qual_with_rn
                          .filter(sf.col("row_number") == 1)
                          .drop("row_number")
                          )

# COMMAND ----------

mod_res_v14 = (mod_res_v13
               .join(
                   fact_std_qual_filtered
                   .select(FSQ_COLS),
                   on=["student_sk", "qualification_sk"],
                   how="left")
               .withColumnRenamed('pricing_area_code','pricing_area_code_v14')
               )

# COMMAND ----------


mod_res_v15 = (
    mod_res_v14
    .join(
        listed_fees.select('course_code','pres_code_5','pricing_area_code','price_amount'),
        on= 
        (mod_res_v14.mod_code==listed_fees.course_code) & 
        (mod_res_v14.presentation_code==listed_fees.pres_code_5) & 
        (mod_res_v14.pricing_area_code_v14==listed_fees.pricing_area_code),
        how='left'
    )
    .drop('course_code','pres_code_5','pricing_area_code_v14')
    .withColumn("mod_listed_fee", 
                sf.when(col('is_withdrawn_currently')==1, 0)
                .otherwise(col("price_amount").cast(DoubleType())))
    .drop('price_amount')
    .withColumn("fee_paid_upto_date", sf.col("cumulative_total_student_mod_presentation_fee_type_mod_payment").cast(DoubleType()))
    .fillna({"fee_paid_upto_date": -1 , "mod_listed_fee": -1,"is_conferred":-1,"is_eligible_to_confer": -1 ,"is_awarded_first_or_upper_second_class_degree":-1 ,"pricing_area_description":-1 ,"mod_study_level_description":-1})
    .withColumn('balance_fee',
    sf.when(sf.col('fee_paid_upto_date') == -1, -1) #If 'fee_paid_upto_date' is -1, set 'balance_fee' to -1
     .when(sf.col('mod_listed_fee') == -1, -1)
     .otherwise(sf.col('mod_listed_fee') - sf.col('fee_paid_upto_date'))
     )
    # .withColumnRenamed('pricing_area_description','location')
    .filter(
        sf.col('presentation_code') >= MIN_PRESENTATION_CODE
    ) # Otherwise, calculate the balance
)

# Define the Window partitioned by PI and mod_code, ordered by date descending
window_spec = Window.partitionBy("personal_id", "mod_code").orderBy(col("current_student_mod_presentation_status_date").desc())
#[TODO] keep if they fail and come for resit and then come back
# Add a row_number column to identify the most recent row for each PI and mod_code
mod_res_v15_with_row_number = mod_res_v15.withColumn("row_number", row_number().over(window_spec))

# Filter rows where row_number is 1
mod_res_v16 = mod_res_v15_with_row_number.filter(col("row_number") == 1).drop("row_number")

mod_res_v17 = group_location_column(mod_res_v16, location_col="pricing_area_description", new_col="location")

mod_res_v18 = add_is_ousba_column(mod_res_v17,'student_country_description','student_crm_website_country',OUSBA_COUNTRIES)

mod_res_v19 = update_mod_result(mod_res_v18, 'mod_result_description', 'current_registration_status_description', 'meta_status')

mod_pop_final = (
    mod_res_v19
    .dropDuplicates(['personal_id', 'mod_presentation_sk'])
    .drop(*SK_COLS)
    
)



# display(mode_res_std.select(sf.countDistinct('student_sk').alias('mod_res_sks')))
# display(mode_pop_final_grpd.select(sf.countDistinct('student_sk').alias('mode_pop_final_grpd_sks')))
display(mod_pop_final.count())


# COMMAND ----------

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

# COMMAND ----------

try:
    if mod_pop_final.rdd.isEmpty():
        raise RuntimeError("No new rows found. Stopping execution before upserting to Cosmos DB.")
    else:
        print("New rows found. Proceeding with processing.")

except AnalysisException as spark_error:
    # error=f"Spark Analysis Exception: {spark_error}"
    dbutils.notebook.exit(f"Spark Analysis Exception: {spark_error}")
    raise
except Exception as e:
    # error=f"Unexpected error in checking DataFrame: {e}"
    dbutils.notebook.exit(f"Unexpected error in checking DataFrame: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### Group mods data into a list 

# COMMAND ----------

try:
    dummy_data_mod = mod_pop_final

    # Cast types for columns based on schema
    for field in dummy_data_mod.schema.fields:
        if isinstance(field.dataType, DecimalType):
            dummy_data_mod = dummy_data_mod.withColumn(field.name, dummy_data_mod[field.name].cast(DoubleType()))
        if isinstance(field.dataType, DateType):
            dummy_data_mod = dummy_data_mod.withColumn(field.name, dummy_data_mod[field.name].cast(StringType()))

    # Display the updated DataFrame schema
    print("Schema successfully updated and displayed.")

except Exception as e:
    # error=f"Error processing data to change datatypes: {e}"
    dbutils.notebook.exit(f"Error processing data to change datatypes: {e}")
    raise  # Stop execution

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Lake
# MAGIC

# COMMAND ----------

from datetime import datetime

try:
    # Remove directory if it exists
    try:
        dbutils.fs.rm(output_path, True)
    except Exception:
        print(f"Directory {output_path} not found, continuing...")

    # Create the directory
    dbutils.fs.mkdirs(output_path)

    # Save DataFrame
    final_output_path = save_parquet_file(dummy_data_mod, output_path, final_output_base_path)
    print(f"Successfully saved DataFrame to Parquet at {final_output_path}")

except Exception as e:
    # error=f"Error saving DataFrame to Parquet: {e}"
    dbutils.notebook.exit(f"Error saving DataFrame to Parquet: {e}")
    raise  # Stop execution


# COMMAND ----------

from pyspark.sql.utils import AnalysisException

try:
    dummy_data_mod_parq = spark.read.parquet(final_output_path)
    print("Parquet file loaded successfully.")
except AnalysisException as ae:
    print(f"AnalysisException occurred: {ae}")
except Exception as e:
    print(f"An error occurred while reading the Parquet file: {e}")


# COMMAND ----------

try:
    upsert_student_data(cosmos_database, dummy_data_container, dummy_data_mod_parq)# pyspark dataframe dummy_data_mod
    print("Successfully upserted data into Cosmos DB.")
except Exception as e:
    dbutils.notebook.exit(f"Error upserting data into Cosmos DB: {e}")
    raise  # Stop execution

# COMMAND ----------

# MAGIC %md
# MAGIC #### Update metadata based on most recent update_datetime

# COMMAND ----------

try:
    # Calculate the maximum date
    max_updated_date = (
        std_new_smpp_fee
        .select(sf.max(col("updated_datetime")))
        .collect()[0][0]
    )

    # If the calculated max date is None, retain the previous date from metadata
    if max_updated_date is None:
        max_updated_date = DATE_START  # Retain the previous update date

    # Display the maximum date for debugging purposes
    print(f"Calculated max_updated_date: {max_updated_date}")

    # Update metadata with the determined date
    update_metadata_key(metadata_path, metadata_key, max_updated_date)
    print(f"Maximum updated_date: {max_updated_date} updated successfully.")

except Exception as e:
    dbutils.notebook.exit(f"Error updating update_datetime in metadata file: {e}")
    raise  # Stop execution


# COMMAND ----------

if error is None:
    # error=f"Success : Maximum updated_date: {max_updated_date}"
    dbutils.notebook.exit(f"STD Data Processed Successfully : Maximum updated_date: {max_updated_date}")
