import httpx
from tkinter import *

client = httpx.Client()
response = httpx.get("https://www.admie.gr/getFiletypeInfoGR")

json_list = response.json()

values = set()

for i in json_list:
    values.add(i['filetype'])

# print(values)
# print(json_list)

# Create object 
root = Tk() 
  
# Adjust size 
root.geometry("400x400") 
  
# Change the label text 
def show(): 
    label.config(text = clicked.get()) 
  
# Dropdown menu options 
options = values
  
# datatype of menu text 
clicked = StringVar() 
  
# initial menu text 
# clicked.set("Monday") 
  
# Create Dropdown menu 
drop = OptionMenu(root , clicked , *options) 
drop.configure(background="white", activebackground="white")
drop["menu"].configure(bg="white")
drop.pack() 
  
# Create button, it will change label text 
button = Button(root , text = "click Me" , command = show).pack() 
  
# Create Label 
label = Label(root , text = "") 
label.pack() 
  
# Execute tkinter 
root.mainloop()

