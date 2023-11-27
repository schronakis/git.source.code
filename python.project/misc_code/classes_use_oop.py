########################### Abstraction #################################
from abc import ABC, abstractmethod

class One(ABC):
   @abstractmethod
   def calculate(self, a):
       pass
   
   def m1(self):
       print("implemented method")

class Square(One):
   def calculate(self, a):
       print("square: ", (a*a))

class Cube(One):
   def calculate(self, a):
       print("cube: ", (a*a*a))

s = Square()
c = Cube()

s.calculate(2)
c.calculate(2)

########################################################################

class AbstractClass(ABC):
    def template_method(self):
        self.primitive_operation1()
        self.primitive_operation2()

    @abstractmethod
    def primitive_operation1(self):
        pass

    @abstractmethod
    def primitive_operation2(self):
        pass

class ConcreteClass(AbstractClass):
    def primitive_operation1(self):
        print('Primitive operation 1 implementation')

    def primitive_operation2(self):
        print('Primitive operation 2 implementation')

concrete = ConcreteClass()
concrete.template_method()

########################################################################

########################## Encapsulation ###############################

class Access:
    def __init__(self):
        # public attribute
        self.name = input('Enter your name: ')
        # private attribute
        self.__secretcode = input("Enter your secret code: ")          

    def permit(self):
        if self.name =='abc' and self.__secretcode == '123!':
         print("Access Granted" )
        else:
         print('Access Denied')

    # public attribute assigned to private attribute
    def access_private_attr(self):
       code = self.__secretcode
       return code


ob = Access()
ob.permit()

print(ob.__secretcode)
print(ob.name) # public attribute is visible to external world
print(ob.access_private_attr())