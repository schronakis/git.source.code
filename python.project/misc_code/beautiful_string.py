_string = 'xxxtttaabbcccaaaaxxxbbbeee'


def beautiful_string(input_string):
    result = {}
    for i in input_string:
        result[i] = result.get(i, 0) + 1
    return dict(sorted(result.items()))


print(beautiful_string(_string))
