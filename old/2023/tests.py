import unittest

# Write an example test case

def count_diff_rows(df1, df2):
    return len(df1) - len(df1.merge(df2, how='inner', on=list(df1.columns)))

class TestX(unittest.TestCase):

    def test_count_diff_rows(self):
        import pandas as pd

        df1 = pd.DataFrame({'a': [1, 2, 3], 'b': [3, 4, 5]})
        df2 = pd.DataFrame({'a': [1, 2, 3], 'b': [3, 4, 5]})
        self.assertEqual(count_diff_rows(df1, df2), 0)
        

    def test_compare_versions2(self):
        import pyptvgtfs
        import os
        import logging
        dfs = {}
        for dirpath, dirnames, filenames in os.walk('downloads'):
            for filename in filenames:
                gtfs_zip_path = os.path.join(dirpath, filename)
                logging.info(gtfs_zip_path)
                version_id = gtfs_zip_path.split(os.sep)[-2]
                dfx = pyptvgtfs.process_gtfs_zip(gtfs_zip_path, version_id)
                dfs[version_id] = dfx

        # compare all versions to all other versions
        for version_id1, df1 in dfs.items():
            for version_id2, df2 in dfs.items():
                if version_id1 == version_id2:
                    continue
                diff = pyptvgtfs.compare_ptv_gtfs_versions(df1, df2)
                self.assertNotEqual(len(diff), 0)
        

if __name__ == '__main__':
    unittest.main()