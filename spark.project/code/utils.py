
def convertCase(str):
    resStr = ""
    arr = str.split(" ")
    for x in arr:
       resStr = resStr + x[0:1].upper() + x[1:len(x)] + " "
    return resStr

def upperCase(str):
    return str.upper()