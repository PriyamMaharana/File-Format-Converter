import glob, pandas as pd, os, re, json
from sqlalchemy import create_engine, text

engine = create_engine('postgresql://demo_user:demo_user@localhost:5432/demo_db')

def convert_type():
    return {
        "string": "text",
        "integer": "int",
        "float": "float",
        "boolean": "boolean",
        "datetime": "timestamp",
        "date": "date",
        "time": "time",
        "decimal": "numeric",
        "timestamp": "timestamp"
    }
type_map = convert_type()

def get_column(schemas, ds_name):
    return [col['column_name'] for col in schemas[ds_name]]

def read_csv(file, schemas):
    file_path_list = re.split('[/\\\]', file)
    ds_name = file_path_list[-2]
    file_name = file_path_list[-1]
    columns = get_column(schemas, ds_name)
    df = pd.read_csv(file, names=columns)
    return df

def to_json(df, tgt_base_dir, ds_name, file_name):
    json_file_path = f'{tgt_base_dir}/{ds_name}/{file_name}'
    os.makedirs(f'{tgt_base_dir}/{ds_name}/', exist_ok=True)
    df.to_json(json_file_path, orient='records', lines=True)
    
    
def create_table(engine, schemas, ds_name):
    column_str = ", ".join(
        f"{col['column_name']} {type_map.get(col['data_type'])}"
        for col in schemas[ds_name]
    )
    create_table_query = f'create table if not exists {ds_name} \
        ({column_str});'
        
    with engine.begin() as conn:
        conn.execute(text(create_table_query))

    
def file_converter(src_base_dir, tgt_base_dir, ds_name, schemas):
    # schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    files = glob.glob(f'{src_base_dir}/{ds_name}/part-*')
    
    create_table(engine, schemas, ds_name)
    
    for file in files:
        df = read_csv(file, schemas)
        
        file_name = re.split('[/\\\]', file)[-1]
        to_json(df, tgt_base_dir, ds_name, file_name)
        
        df.to_sql(ds_name, engine, if_exists='append', index=False)
        print(f'Data Inserted into {ds_name}.')
    
def process_files(ds_names=None):
    src_base_dir = 'data/retail_db'
    tgt_base_dir = 'output/retail_db_json'
    
    # src_base_dir = input("Enter or Paste the path of source directory: ")
    # tgt_base_dir = input("Enter or Paste the path of target directory: ")
    
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    
    if not ds_names:
        ds_names = schemas.keys()
        
    for ds_name in ds_names:
        print(f'Processing... {ds_name}')
        file_converter(src_base_dir, tgt_base_dir, ds_name, schemas)
    
    print(f'\nAll Files Processed & Loaded to Database..!!\n')   
    
if __name__ == "__main__":
    process_files()