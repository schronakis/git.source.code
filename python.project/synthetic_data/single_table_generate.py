import pandas as pd
from sdv.metadata import SingleTableMetadata
from sdv.single_table.copulagan import CopulaGANSynthesizer
from sdv.single_table.copulas import GaussianCopulaSynthesizer
from sdv.single_table.ctgan import CTGANSynthesizer, TVAESynthesizer
from sdmetrics.reports.single_table import QualityReport
from sdmetrics.visualization import get_column_pair_plot
from sdv.evaluation.single_table import get_column_plot
from sdmetrics.single_column import StatisticSimilarity, BoundaryAdherence, KSComplement, TVComplement

class SyntheticDataSingleTableGen():

    def __init__(self, file_path, filename):
        self.file_path = file_path
        self.filename = filename 
    
    def _gen_metadata_single_table(self,real_data):
        metadata = SingleTableMetadata()
        metadata.detect_from_dataframe(real_data)
        return metadata

    def _gen_metadata_update_columns(self, flag, _metadata, column_name, sdtype, **kwargs):
        if flag == 0:
           _metadata.update_column(column_name = column_name,
                                    sdtype = sdtype,
                                    **kwargs)  
        elif flag == 1:
            _metadata.update_columns(column_names = column_name,
                                     sdtype = sdtype,
                                     **kwargs)  
        return _metadata

    def _gen_data_single_table(self, sep, header, skip):
        real_data = pd.read_csv(f'{self.file_path}/{self.filename}', delimiter=sep, header=header, skiprows=skip)
        return real_data
         
    def get_synthesizer(self, synthesizer_type, rows, **kwargs):

        get_data = self._gen_data_single_table(',', 0, 0)
        get_meta = self._gen_metadata_single_table(get_data)

        ################## Columns Metadata Update #####################
        self._gen_metadata_update_columns(0, get_meta, '_id', 'id', regex_format='[0-9]{2}')
        self._gen_metadata_update_columns(1, get_meta, ['min', 'max', 'average'], 'numerical', computer_representation='Int64')
        ###############################################################

        synthesizer = synthesizer_type(get_meta,
                                       **kwargs)
        synthesizer.fit(get_data)
        _sample = synthesizer.sample(num_rows=rows)
        return _sample
    
# t = SyntheticDataSingleTableGen('C:/Users/schronak/Downloads/data/bank.refined/subset/numerical', 'averages.csv')
# synth = t.get_synthesizer(GaussianCopulaSynthesizer, 20, enforce_min_max_values=True, numerical_distributions={'min': 'norm', 'max': 'norm', 'average': 'norm'})
# print(synth)
