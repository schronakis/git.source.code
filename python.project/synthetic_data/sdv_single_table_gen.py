import pandas as pd
from sdv.metadata import SingleTableMetadata
from sdv.single_table.copulagan import CopulaGANSynthesizer
from sdv.single_table.copulas import GaussianCopulaSynthesizer
from sdv.single_table.ctgan import CTGANSynthesizer, TVAESynthesizer
from sdmetrics.reports.single_table import QualityReport
from sdmetrics.visualization import get_column_pair_plot
from sdmetrics.single_column import KSComplement, TVComplement

file_path = 'C:/Users/schronak/Downloads/data/bank.refined/subset/numerical'
filename = 'averages.csv'

real_data = pd.read_csv(f'{file_path}/{filename}', delimiter=',', header=0, skiprows=0)

metadata = SingleTableMetadata()
metadata.detect_from_dataframe(real_data)
# metadata.remove_primary_key()

# metadata.update_column(
#     column_name='client_id',
#     sdtype='id',
#     regex_format='[0-9]{8}',
#     )

# metadata.update_column(
#     column_name='account_id',
#     sdtype='id',
#     regex_format='[0-9]{8}',
#     )

# metadata.update_column(
#     column_name='disp_id',
#     sdtype='id',
#     regex_format='[0-9]{8}',
#     )

# metadata.update_column(
#     column_name='age',
#     sdtype='numerical',
#     )

# metadata.update_column(
#     column_name='email',
#     sdtype='email',
#     pii=True
#     )

# metadata.update_column(
#     column_name='address_1',
#     sdtype='street_address',
#     pii=False
#     )

# metadata.update_columns(
#     column_names=['_id','trans_id','account_id'],
#     sdtype='id',
#     regex_format='[0-9]{8}',
#     )

# metadata.update_column(
#     column_name='account',
#     sdtype='numerical',
#     computer_representation='Int64',
#     )

# metadata.update_column(
#     column_name='fulltime',
#     sdtype='datetime',
#     datetime_format='%H:%M:%S',
#     )

metadata.update_columns(
    column_names=['min','max','average'],
    sdtype='numerical',
    computer_representation='Int64',
    )

metadata_dict = (metadata.to_dict())

synthesizer = GaussianCopulaSynthesizer(metadata,
                                        enforce_min_max_values=True,
                                        default_distribution='norm',
                                        # numerical_distributions={'min': 'norm',
                                        #                          'max': 'norm',
                                        #                          'average': 'norm'},
                                        )
synthesizer.fit(real_data)

synthetic_data = synthesizer.sample(num_rows=10)

# print(real_data)
# print(synthetic_data)

########################## Data Evaluation ################################
report = QualityReport()

report.generate(real_data, synthetic_data, metadata_dict)
print(report.get_details(property_name='Column Pair Trends'))

fig_1 = report.get_visualization(property_name='Column Shapes')
fig_2 = report.get_visualization(property_name='Column Pair Trends')

fig_3 = KSComplement.compute(
    real_data=real_data['average'],
    synthetic_data=synthetic_data['average']) # Column Marginal Distribution

# fig_1.show()
# fig_2.show()

print(fig_3)