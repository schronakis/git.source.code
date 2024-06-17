from sdv.io.local import CSVHandler
from sdv.multi_table import HMASynthesizer
from sdv.metadata.multi_table import MultiTableMetadata
from sdv.utils import poc
from sdv.evaluation.multi_table import run_diagnostic, evaluate_quality
from sdv.evaluation.multi_table import get_column_plot

file_path = 'C:/Users/schronak/Downloads/data/bank.refined/subset'

########################## Load/Process/Create Data ################################

handler = CSVHandler()
data = handler.read(folder_name=file_path)

metadata = MultiTableMetadata()
metadata.detect_from_dataframes(data)

# ############## metadata for client ##############
# metadata.update_column(
#     table_name='completedclient',
#     column_name='client_id',
#     sdtype='id',
#     regex_format='[0-9]{8}',
#     )

# metadata.update_column(
#     table_name='completedclient',
#     column_name='district_id',
#     sdtype='id',
#     regex_format='[0-9]{4}',
#     )

# metadata.update_columns(
#     table_name='completedclient',
#     column_names=['age','year'],
#     sdtype='numerical',
#     computer_representation = 'Int64',
#     )

# ############## metadata for acct ##############
# metadata.update_column(
#     table_name='completedacct',
#     column_name='account_id',
#     sdtype='id',
#     regex_format='[0-9]{8}',
#     )

# metadata.update_column(
#     table_name='completedacct',
#     column_name='district_id',
#     sdtype='id',
#     regex_format='[0-9]{4}',
#     )

# ############## metadata for loan ##############
# metadata.update_columns(
#     table_name='completedloan',
#     column_names=['loan_id','account_id'],
#     sdtype='id',
#     regex_format='[0-9]{8}',
#     )

# metadata.update_columns(
#     table_name='completedloan',
#     column_names=['amount','payments'],
#     sdtype='numerical',
#     computer_representation = 'Int64',
#     )

# ############## metadata for distrinct ##############
# metadata.update_column(
#     table_name='completeddistrict',
#     column_name='district_id',
#     sdtype='id',
#     regex_format='[0-9]{4}',
#     )

# ############## metadata for order ##############
# metadata.update_column(
#     table_name='completedorder',
#     column_name='account_id',
#     sdtype='id',
#     regex_format='[0-9]{8}',
#     )

# metadata.update_column(
#     table_name='completedorder',
#     column_name='account_to',
#     sdtype='numerical',
#     computer_representation='Int64',
#     )

# ############## metadata for position ##############
# metadata.update_columns(
#     table_name='completeddisposition',
#     column_names=['client_id','disp_id','account_id'],
#     sdtype='id',
#     regex_format='[0-9]{8}',
#     )

# ############## metadata for cards ##############
# metadata.update_columns(
#     table_name='completedcard',
#     column_names=['card_id','disp_id'],
#     sdtype='id',
#     regex_format='[0-9]{8}',
#     )

############## metadata for transactions ##############
metadata.update_columns(
    table_name='completedtrans',
    column_names=['_id','trans_id','account_id'],
    sdtype='id',
    regex_format='[0-9]{8}',
    )

metadata.update_column(
    table_name='completedtrans',
    column_name='fulltime',
    sdtype='datetime',
    datetime_format='%H:%M:%S',
    )

##################################################

# simplified_data, simplified_metadata = poc.simplify_schema(data, metadata)

##################################################


synthesizer = HMASynthesizer(metadata)

synthesizer.set_table_parameters(
    table_name='completedtrans',
    table_parameters={'enforce_min_max_values': True,
                      'default_distribution': 'norm',
                      'numerical_distributions': {'year': 'norm',
                                                  'month': 'norm',
                                                  'day': 'norm',
                                                  'fulldate': 'norm',
                                                #   'fulltime': 'norm',
                                                #   'fulldatewithtime': 'norm',
                                                  }
                    })

synthesizer.fit(data)

synthetic_data = synthesizer.sample(1.0)

print(synthetic_data)

########################## Saving Data ################################

# handler.write(
#   synthetic_data,
#   folder_name='C:/Users/schronak/Downloads/data/bank.synthetic.data',
#   file_name_suffix='_v1', 
#   mode='x')

########################## Data Evaluation ################################

## 1. perform basic validity checks
# diagnostic = run_diagnostic(data, synthetic_data, metadata)

## 2. measure the statistical similarity
# quality_report = evaluate_quality(data, synthetic_data, metadata)

# quality_report.get_details(property_name='Column Shapes', table_name='completedclient')

## 3. plot the data
# fig = get_column_plot(
#     real_data=data,
#     synthetic_data=synthetic_data,
#     metadata=metadata,
#     table_name='completedclient',
#     column_name='city',
#     plot_type='bar'
# )
    
# fig.show()