import random
import pandas as pd

### Write a program to find from string if this string is a "Beautiful String". Return True otherwise False.
_string = 'xxxttaabbccccaaaabbbeee'

def beautiful_string(input_string):
    result = {}
    result_list = []
    for i in input_string:
        result[i] = result.get(i, 0) + 1
        sorted_dict = dict(sorted(result.items()))

    for i in sorted_dict.keys():
        result_list.append(sorted_dict[i])

    return result_list

def find_greater_values(lst):
    greater_values = []
    for i in range(1, len(lst)):
        if lst[i] > lst[i - 1]:
            greater_values.append(lst[i])
    return greater_values

result = find_greater_values(beautiful_string(_string))
print("Values greater than the item before it:", result)

def is_previous_greater(lst):
    for i in range(1, len(lst)):
        if lst[i - 1] > lst[i]:
            return False
    return True

result = is_previous_greater(beautiful_string(_string))
print("Previous value greater than current one:", result)
###########################################################################################################################

### Write a program to iterate the first 10 numbers, and in each iteration, print the sum of the current and previous number.
def first_numbers_sum(range_num):
    result_of_sum = []
    for i in range(range_num):
        if i == 0:
            result_of_sum.append(f'Current Number:{i} Previous Number:{i} Sum:{i}')
        else:
            result_of_sum.append(f'Current Number:{i} Previous Number:{i-1} Sum:{i+(i-1)}')
    return result_of_sum

print(first_numbers_sum(10))
###########################################################################################################################

### Write a Python program to find a list of integers with exactly two occurrences of nineteen and at least three occurrences of five. Return True otherwise False.
list_of_integers = [19, 19, 15, 5, 3, 5, 5, 2]

def test(nums):
    return nums.count(19) == 2 and nums.count(5) >= 3

print(test(list_of_integers))
###########################################################################################################################


d = {'VENEZUELA':'CARACAS', 'CANADA':'OTTAWA'}
# country, capital = random.choice(list(d.items()))
capital = random.choice(list(d.keys()))
print(capital)

###########################################################################################################################

list1 = [2, 3, 4, 5]
result_map = list(map(lambda x: pow(x, 2), list1))

df = pd.DataFrame(
    {"name": ["IBRAHIM", "SEGUN", "YUSUF", "DARE", "BOLA", "SOKUNBI"],
     "score": [50, 32, 45, 45, 23, 45]})
df["lower_name"] = df["name"].apply(lambda x: x.lower())

print(result_map)

print(df)