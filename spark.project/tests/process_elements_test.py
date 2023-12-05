import os, sys, inspect

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
maindir = os.path.join(parentdir, 'code')
sys.path.insert(0, maindir)

import unittest
from pyspark.testing.utils import assertDataFrameEqual
from code.spark_session import sparkSession
from code.process_elements import ProcessElements
from pyspark.sql import Row
from unittest.mock import MagicMock
from code.source_schemas import mock_df_schema

class TestProcessElements(unittest.TestCase):
    
    def setUp(self):
        sc = sparkSession('R&D Spark','2g','2g','4')
        self.spark_session = sc.generate_spark_session()


    def _data_df(self):
        dataset = [
                {'Year':2006,
                'DwellRecType':1,
                'DwellStatus':2,
                'Area':77,
                'Count':159222},

                {'Year':2006,
                'DwellRecType':2,
                'DwellStatus':11,
                'Area':1,
                'Count':405},

                {'Year':2006,
                'DwellRecType':2,
                'DwellStatus':11,
                'Area':1,
                'Count':398678}
             ]
        
        dataset_df = self.spark_session.createDataFrame([Row(**i) for i in dataset])

        return dataset_df
    
    def _output_df(self):
        cols = mock_df_schema.fieldNames()
        output = [
                [2006, 1, 2, 77, 159222, '2006-159222'],
                [2006, 2, 11, 1, 405, '2006-405']
            ]
        
        output_df = self.spark_session.createDataFrame(output).toDF(*cols)

        return output_df
    
    def test_get_data_df(self):
        self.process_elements = ProcessElements()
        self.process_elements.get_sources_to_df = MagicMock()
        self.process_elements.get_sources_to_df.return_value = self._data_df()
        result_df = self.process_elements.get_concat_data_df()
        assertDataFrameEqual(result_df, self._output_df())

if __name__ == '__main__':
    unittest.main()
