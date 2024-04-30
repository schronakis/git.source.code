import pandas as pd

def read_asc_files(file):
    folder_location = r"C:\\Users\\schronakis\\Downloads\\RnD.Stuff\\bank.data\\berka.dataset\\"

    with open(f'{folder_location}{file}', 'r') as asc_file:
        source_data_df = pd.read_csv(asc_file, header=0, delimiter=';')
        return source_data_df


clients_df = read_asc_files('client.csv').head(10)
accounts_df = read_asc_files('account.csv').head(10)

print(clients_df.loc[(clients_df['client_id']).isin([1,2])])


