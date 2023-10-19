# HERE ALL FUNCTIONS/CLASSES THAT ARE USEFUL FOR READING DATA AND WRITING DATA 
# - read write files from bucket
# - perform queries against delta lake

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
    
    
    
def query_from_delta(**args):
    """_summary_
    """
    pass



def write_to_delta(**args):
    """_summary_
    """
    pass