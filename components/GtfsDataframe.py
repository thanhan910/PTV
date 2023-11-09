import pandas as pd


def filename_pattern_encode(table_name: str, version_id: str, branch_id: str):
    return f"{table_name}-{version_id}-{branch_id}"


def filepath_pattern_decode(filepath: str):
    filename_array = filepath.split('/')[-1].split('.')[0].split('-')
    table_name = filename_array[0]
    version_id = filename_array[1]
    branch_id = filename_array[2]
    return table_name, version_id, branch_id


class GtfsDataframe:
    df: pd.DataFrame
    table_name: str
    branch_id: str
    version_id: str

    def __init__(self, df: pd.DataFrame, table_name: str, branch_id: str, version_id: str) -> None:
        self.df = df
        self.table_name = table_name
        self.branch_id = branch_id
        self.version_id = version_id
        self.file_name = filename_pattern_encode(table_name, version_id, branch_id)

    def __repr__(self) -> str:
        return f"[REPR]: Table {self.table_name}, Branch {self.branch_id}, Version {self.version_id}, Num rows {len(self.df)}), Columns {list(self.df.columns)}"
    
    def save(self, output_dir: str, file_name: str = None):
        if file_name is not None:
            file_name = self.file_name
        self.df.to_csv(f"{output_dir}/{file_name}.csv", index=False)
    
    @staticmethod
    def load(file_path: str):
        table_name, version_id, branch_id = filepath_pattern_decode(file_path)
        df = pd.read_csv(file_path, keep_default_na=False, low_memory=False)
        return GtfsDataframe(df, table_name, branch_id, version_id)
    

def convert_gtfs_df_list_to_df(gtfs_df_list: list[GtfsDataframe]) -> pd.DataFrame:
    return pd.DataFrame([
        {
            'table_name': gtfs_df.table_name,
            'branch_id': gtfs_df.branch_id,
            'version_id': gtfs_df.version_id,
            'df': gtfs_df.df
        }
        for gtfs_df in gtfs_df_list
    ])