import os, sys, inspect

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
maindir = os.path.join(parentdir, 'code')
sys.path.insert(0, maindir)

################## Export PythonPath #####################
# Linux
# export PYTHONPATH="/home/schronak/git.source.code/spark.project:$PYTHONPATH"
# export PYTHONPATH=$PYTHONPATH:/home/schronak/git.source.code/spark.project
# Windows
# set PYTHONPATH=%PYTHONPATH%;C:\path\to\your\project\
###########################################################

import unittest
from unittest import mock
from unittest.mock import MagicMock
from pyspark.testing.utils import assertDataFrameEqual
from code.spark_session import sparkSession
from code.process_elements import ProcessElements
from pyspark.sql import Row
from code.source_schemas import mock_df_schema

class TestProcessElements(unittest.TestCase):
    
    def setUp(self):
        sc = sparkSession('R&D Spark','2g','2g','4')
        self.spark_session = sc.generate_spark_session()

    def _init_clients_df(self):
        dataset = [
                {'client_id':'C00000001',
                 'birth_number':706213,
                 'district_id':18},

                {'client_id':'C00000010',
                 'birth_number':430501,
                 'district_id':57},
             ]
        
        _init_clients_dataset_df = self.spark_session.createDataFrame([Row(**i) for i in dataset])

        return _init_clients_dataset_df
    
    def _complete_clients_df(self):
        dataset = [
                {'client_id':'C00000001',
                 'first':'EMMA',
                 'last':'SMITH',
                 'fulldate':'1990-12-13',
                 'social':'926-93-2157',
                 'phone':'367-171-6840',
                 'email':'emma.smith@gmail.com',
                 'address_1':'387 Wellington Ave.',
                 'city':'Albuquerque',
                 'zipcode':47246,
                 'district_id':18},

                {'client_id':'C00000010',
                 'first':'ETHAN',
                 'last':'HARRIS',
                 'fulldate':'1963-05-01',
                 'social':'295-22-6122',
                 'phone':'508-902-5510',
                 'email':'ethan.harris9@gmail.com',
                 'address_1':'754 Grandrose St.',
                 'city':'New Bedford',
                 'zipcode':0o2740,
                 'district_id':57},
             ]
        
        _complete_clients_dataset_df = self.spark_session.createDataFrame([Row(**i) for i in dataset])

        return _complete_clients_dataset_df
    
    def _output_df(self):
        cols = mock_df_schema.fieldNames()
        output = [
                ['C00000001', 'SMITH EMMA', '1990-12-13', '926-93-2157', '367-171-6840', 'emma.smith@gmail.com', '387 Wellington Ave.', 'Albuquerque', 47246, 18],
                ['C00000010', 'HARRIS ETHAN', '1963-05-01', '295-22-6122', '508-902-5510', 'ethan.harris9@gmail.com', '754 Grandrose St.', 'New Bedford', 0o2740, 57]
            ]
        
        output_df = self.spark_session.createDataFrame(output).toDF(*cols)

        return output_df

    @mock.patch('process_elements.ProcessElements.get_sources_to_df')
    def test_get_data_df(self, get_sources_to_df_mocked): # get_sources_to_df_mocked
        self.process_elements = ProcessElements()

        self.process_elements.get_init_clients_df = MagicMock()
        self.process_elements.get_init_clients_df.return_value = self._init_clients_df()

        self.process_elements.get_complete_clients_df = MagicMock()
        self.process_elements.get_complete_clients_df.return_value = self._complete_clients_df()

        result_df = self.process_elements.get_enriched_clients_df()
        assertDataFrameEqual(result_df, self._output_df())

if __name__ == '__main__':
    unittest.main()
