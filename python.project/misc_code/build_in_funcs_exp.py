from collections import defaultdict, namedtuple
from dataclasses import dataclass, make_dataclass, field, fields
from typing import List

# my_list = [1, 2, 3, 2, 4, 1, 5, 4, 3]
with open('C:/Users/schronakis/Downloads/git_repos/data.txt','r') as cus:
    lines = (cus.readlines())
    print(lines)
 
def group_list_to_dict(lst):
    groups = defaultdict(list)
    for item in lst:
        item_split = (item.split(','))
        groups[item_split[1]].append(item)
    return groups

result = group_list_to_dict(lines)
print(result)
########################################################################

################################# Yield ################################
def infinite_sequence():
    num = 0
    while True:
        yield num
        num += 1

# def is_palindrome(num):
#     # Skip single-digit inputs
#     if num // 10 == 0:
#         return False
#     temp = num
#     reversed_num = 0

#     while temp != 0:
#         reversed_num = (reversed_num * 10) + (temp % 10)
#         temp = temp // 10

#     if num == reversed_num:
#         return num
#     else:
#         return False

# for i in infinite_sequence():
#      pal = is_palindrome(i)
#      if pal:
#          print(i)

# gen = infinite_sequence()
# print(next(gen), next(gen))

# for i in infinite_sequence():
#     print(i, end=" ")
########################################################################

############################### NamedTuple #############################
Car = namedtuple('Car' , 'color mileage')

getRedCar = Car('red', 3812.4)
print(getRedCar.color)

color, mileage = getRedCar
print(color, mileage)
########################################################################

############################## DataClasses #############################

########## create dataclass ##########
@dataclass
class Position:
    name: str
    lon: float = 0.0
    lat: float = 0.0
########## or ##########
Position_ = make_dataclass('Position', ['name', 'lat', 'lon'])
###################################

print(Position(f'______Null Island',1.2,4.5))
###################################
RANKS = '2 3 4 5 6 7 8 9 10 J Q K A'.split()
SUITS = '♣ ♢ ♡ ♠'.split()

@dataclass
class PlayingCard:
    rank: str
    suit: str

def make_french_deck():
    return [PlayingCard(r, s) for s in SUITS for r in RANKS]

@dataclass
class Deck:
    # cards: List[PlayingCard]
    cards: List[PlayingCard] = field(default_factory=make_french_deck)

queen_of_hearts = PlayingCard('Q', 'Hearts')
ace_of_spades = PlayingCard('A', 'Spades')
two_cards = Deck([queen_of_hearts, ace_of_spades])

# print(make_french_deck())
print(f'------{fields(two_cards)}')
print(two_cards)
# print(Deck())
########################################################################
