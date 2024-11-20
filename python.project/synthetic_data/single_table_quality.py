import pandas as pd
from single_table_generate import SyntheticDataSingleTableGen
from sdv.metadata import SingleTableMetadata
from sdv.single_table.copulagan import CopulaGANSynthesizer
from sdv.single_table.copulas import GaussianCopulaSynthesizer
from sdv.single_table.ctgan import CTGANSynthesizer, TVAESynthesizer
from sdmetrics.reports.single_table import QualityReport
from sdmetrics.visualization import get_column_pair_plot
from sdv.evaluation.single_table import get_column_plot
from sdmetrics.single_column import StatisticSimilarity, BoundaryAdherence, KSComplement, TVComplement

class SyntheticDataQualityGen(SyntheticDataSingleTableGen):

    def __init__(self, file_path, filename, sep, header, skip, row_num):
        get_data = SyntheticDataSingleTableGen(file_path, filename)
        self.real_data = get_data._gen_data_single_table(sep, header, skip)
        self.metadata = get_data._gen_metadata_single_table(self.real_data)
        self.synthetic_data = get_data.get_synthesizer(GaussianCopulaSynthesizer, 
                                                       enforce_min_max_values=True, 
                                                       rows=row_num,
                                                       numerical_distributions={'min': 'norm', 'max': 'norm', 'average': 'norm'}
                                                       )

    def generate_report(self, text, sdmetric, real_column, synthetic_column, metric=None):

        ################## Columns Metadata Update #####################
        self._gen_metadata_update_columns(0, self.metadata, '_id', 'id', regex_format='[0-9]{2}')
        self._gen_metadata_update_columns(1, self.metadata, ['min','max','average'], 'numerical', computer_representation='Int64')
        ###############################################################

        meta_to_dict = (self.metadata.to_dict())

        report = QualityReport()
        report.generate(self.real_data, self.synthetic_data, meta_to_dict)

        sdmetrics = sdmetric.compute(
        real_data = self.real_data[real_column],
        synthetic_data = self.synthetic_data[synthetic_column],
        statistic = metric)
    
        return f'{text}: {sdmetrics}'


# print(report.get_details(property_name='Column Pair Trends'))
# print(report.get_details(property_name='Column Shapes'))

# fig_1 = report.get_visualization(property_name='Column Shapes')
# fig_2 = report.get_visualization(property_name='Column Pair Trends')

# fig_3 = KSComplement.compute(
#     real_data=real_data['average'],
#     synthetic_data=synthetic_data['average']) # Column Marginal Distribution

# fig_4 = BoundaryAdherence.compute(
#     real_data=real_data['average'],
#     synthetic_data=synthetic_data['average'])


# fig_1.show()
# fig_2.show()
# print(fig_3)
# print(fig_4)

t = SyntheticDataQualityGen('C:/Users/schronak/Downloads/data/bank.refined/subset/numerical', 'averages.csv', ',', 0, 0, 20)
tt = t.generate_report('This is the StatisticSimilarity of average column', StatisticSimilarity, 'average', 'average', 'mean')
print(tt)