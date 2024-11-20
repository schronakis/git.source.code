import pandas as pd
import re
import matplotlib.pyplot as plt
import seaborn as sns

import random


def uniqueIDs(filepath, id_column):

    df = pd.read_csv(filepath)

    idList = df[id_column].unique()

    return [re.sub(r'[a-zA-Z]', '', id) for id in idList]


def categoricalWeights (filepath, categoricalVals):

    df = pd.read_csv(filepath)
    
    # We need to make this dictionary first in order to relate values with probabilities 
    values_probs = {}
    for col in categoricalVals:
        value_counts = df[col].value_counts(normalize=True)
        values_probs[col] = [{'value': value, 'probability': prob} for value, prob in value_counts.items()]
    

    # Then extract a list with values and a list with probabilities
    values = {}
    probs = {}
    for col, items in values_probs.items():
        values[col] = [item['value'] for item in items]
        probs[col] = [item['probability'] for item in items]


    return values, probs


def quantitiveWeights (filepath, quantitiveVals):

    df = pd.read_csv(filepath)
    
    Numbins = 10
    bins = {}
    weights = {}
    for col in quantitiveVals:
        df['bins'] = pd.cut(df[col], bins=Numbins)
        bins[col] = df['bins'].cat.categories
        
        count = []
        for bin in bins.get(col):
            count.append(df[(df[col]>bin.left)&(df[col]<=bin.right)].shape[0])
        
        normalized = [float(num)/sum(count) for num in count]
        weights[col] = normalized


    return bins,weights

    # print(count)
    # print(f"Bins List: {bins}")
    # print("\n======================================================================\n")
    # print(f"Weights List: {weights}")

    
    


if __name__ == '__main__':

    # ==================================================================================
    #                              CATEGORICAL VALUES TESTS
    # ==================================================================================

    #-------------------------------- TEST FUNCTION ------------------------------------
    # catValues, catProbs = categoricalWeights('./bank.refined/completedloan.csv', ['duration', 'status', 'purpose'])
    
    # values = catValues.get('status')
    # probs = catProbs.get('status')
    
    # print(catValues)
    # print(catProbs)
    
    # sns.barplot( x=values, y=probs)
    # plt.show()


    #---------------------------- COMPARE WITH CSV -------------------------------------
    # df = pd.read_csv('./bank.refined/completedclient.csv')
    
    # vb = df['age'].value_counts(normalize=True).reset_index()
    # vb.columns = ['age', 'probs']

    # sns.barplot(vb, x=vb['age'], y=vb['probs'])
    # # plt.hist(df['status'])

    # plt.show()


    # ==================================================================================
    #                              QUANTITIVE VALUES TESTS
    # ==================================================================================

    
    #-------------------------------- TEST FUNCTION ------------------------------------
    bins, weights = quantitiveWeights('./bank.refined/completedloan.csv', ['amount','payments'])

    print(bins)
    print(weights)
    # amount = []
    # payments = []
    # n = 5000
    # for i in range(n):
    #     # Pick a random bin with corresponding weight
    #     binA = random.choices(bins.get('amount'), weights.get('amount'))
    #     binB = random.choices(bins.get('payments'), weights.get('payments'))

    #     # Extract Interval
    #     intervalA = binA[0]
    #     intervalB = binB[0]

    #     # Generate Float into this interval
    #     rand_floatA = random.uniform(intervalA.left, intervalB.right)
    #     amount.append(rand_floatA)

    #     rand_floatB = random.uniform(intervalB.left, intervalB.right)
    #     payments.append(rand_floatB)
        
    
    #---------------------------- COMPARE WITH CSV -------------------------------------
    # df = pd.read_csv('./bank.refined/completedloan.csv')

    # plt.hist(df['payments'], bins=10)
    # plt.show()