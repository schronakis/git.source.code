
def check_datatypes(df, col):
### Check Columns DataTypes 
    datatype_values = df[col].apply(type).value_counts()
    print(datatype_values)

def check_datatypes_values(df, col, _datatype):
### Check Values for a DataType
    float_values = df[df[col].apply(lambda x: isinstance(x, _datatype))]
    print(float_values[col])

def check_for_duplicates(df, subset_cols, find_all=True):
### Check for all Duplicates
    if find_all:
        find_duplicates = df[df.duplicated()]
### Check for a subset Duplicates
    else:
        subset_df = df[subset_cols]
        find_duplicates = subset_df[subset_df.duplicated()]
    print(find_duplicates)

