from datetime import date
import math

class Student:
    def __init__(self, name, age):
        self.name = name  # instance attribute
        self.age = age # instance attribute

    @classmethod
    def getobject(cls):
        return cls('Steve', 25)

std = Student.getobject()

print(std.name)  #'Steve'    
print(std.age)   #25

#######################################################################################

class Pizza: 
    def __init__(self, radius, ingredients):
        self.radius = radius
        self.ingredients = ingredients
    
    def __repr__(self):
        return (f'Pizza({self.radius!r}, 'f'{self.ingredients!r})')
    
    def area(self):
        return self.circle_area(self.radius)
    
    @classmethod
    def margherita(cls):
        return cls(4, ['mozzarella', 'tomatoes'])

    @classmethod
    def prosciutto(cls):
        return cls(8, ['mozzarella', 'tomatoses', 'prosciutto'])

    @staticmethod
    def circle_area(r):
        return r ** 2 * math.pi

print(Pizza.margherita())
print(Pizza.prosciutto())
print(Pizza.circle_area(4))

p = Pizza(4, ['mozzarella', 'tomatoes'])
print(p.area())