_string = 'xxxtttaabbcccaaaaxxxbbbeee'


def beautiful_string(input_string):
    result = {}
    for i in input_string:
        result[i] = result.get(i, 0) + 1
    return dict(sorted(result.items()))


# print(beautiful_string(_string))


### Write a program to iterate the first 10 numbers, and in each iteration, print the sum of the current and previous number. ###

for i in range(10):
    if i == 0:
        print(f'Current Number:{i} Previous Number:{i} Sum:{i}')
    else:
        print(f'Current Number:{i} Previous Number:{i-1} Sum:{i+(i-1)}')
