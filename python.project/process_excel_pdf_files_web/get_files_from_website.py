import os
import requests
import urllib
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import tkinter as tk
from tkinter import simpledialog

url = simpledialog.askstring(title="download_url", prompt="Please enter a valid url::")

# url = input("Please enter a valid url: ")
# url = "https://www.admie.gr/agora/enimerotika-deltia/miniaia-deltia-energeias?since=01.08.2023&until=11.08.2023"

#If there is no such folder, the script will create one automatically
folder_location = r"C:\\Users\\schronakis\\Downloads\\enel\\"
if not os.path.exists(folder_location):os.mkdir(folder_location)

response = requests.get(url)
soup = BeautifulSoup(response.text, "html.parser")     
for link in soup.select("a[href$='.pdf']"):

    #Name the pdf files using the last portion of each link which are unique in this case
    filename = os.path.join(folder_location,link['href'].split('/')[-1])
    with open(filename, 'wb') as f:
        f.write(requests.get(urljoin(url,link['href'])).content)
##############################################################################################

############## url_parameters ##############
start_date = '2022-11-01'
end_date = '2022-11-01'
file_type = 'ISP1UnitAvailabilities'
############################################

folder_location = r"C:\\Users\\schronakis\\Downloads\\enel\\"

response = requests.get(f"https://www.admie.gr/getOperationMarketFile?dateStart={start_date}&dateEnd={end_date}&FileCategory={file_type}")
json_list = response.json()

for i in json_list:
    # requests.get(i['file_path'])
    # values.add(i['file_path'])
    filename_full_path = (f"{folder_location}{i['file_path'].rsplit('/', 1)[-1]}")
    urllib.request.urlretrieve(i['file_path'], filename_full_path)