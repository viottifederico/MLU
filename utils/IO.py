# HERE ALL FUNCTIONS/CLASSES THAT ARE USEFUL FOR READING DATA AND WRITING DATA 
# - read write files from bucket
# - perform queries against delta lake

import delta
from delta import DeltaTable
from pyspark.sql import SparkSession

def load_file_from_bucket(bucket_name, namespace, filename):
    """_summary_

    Args:
        bucket_name (_type_): _description_
        namespace (_type_): _description_
        filename (_type_): _description_
    """
    return(pd.read_parquet(f"oci://{bucket_name}@{namespace}/{filename}"))



#TODO add possibility to decide which format to write between parquet/csv/excel/json...
def write_file_in_bucket(dataset_to_write, bucket_name, namespace, filename):
    """_summary_

    Args:
        dataset_to_write (_type_): _description_
        bucket_name (_type_): _description_
        namespace (_type_): _description_
        filename (_type_): _description_
    """
    dataset_to_write.to_parquet(f"oci://{bucket_name}@{namespace}/{filename}", compression='gzip', index=False)
    
    
    

class DeltaLakeUtils:
    def __init__(self, table_path):
        self.table_path = table_path
        self.spark = self._create_spark_session()

    def _create_spark_session(self):
        return SparkSession.builder \
            .appName("DeltaLakeUtils") \
            .getOrCreate()

    def read_table_as_dataframe(self):
        try:
            df = self.spark.read.format("delta").load(self.table_path)
            return df
        except Exception as e:
            print(f"Error reading Delta Lake table: {str(e)}")
            return None
        
    def run_sql_query(self, query):
        try:
            result = self.spark.sql(query)
            return result
        except Exception as e:
            print(f"Error running Spark SQL query: {str(e)}")
            return None

    def write_dataframe_to_table(self, dataframe):
        """_summary_

        Args:
            dataframe (_type_): _description_
        """
        try:
            dataframe.write.format("delta").mode("overwrite").save(self.table_path)
            print(f"Data written to Delta Lake table at {self.table_path}")
        except Exception as e:
            print(f"Error writing DataFrame to Delta Lake table: {str(e)}")

    def close(self):
        self.spark.stop()


