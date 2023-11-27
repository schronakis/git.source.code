import requests
import json

############################# Get Requests #############################

# response_API = requests.get('https://api.covid19india.org/state_district_wise.json')
# data = response_API.text
# parse_json = json.loads(data)
# active_case = parse_json['Andaman and Nicobar Islands']['districtData']['South Andaman'] #['active']
# print(response_API.status_code)
# print("Active cases in South Andaman:", active_case)


# response_API = requests.get('https://gmail.googleapis.com/$discovery/rest?version=v1')
# data = response_API.text
# parse_json = json.loads(data)
# info = parse_json['description']
# key = parse_json['parameters']['key']['description']
# print(response_API.status_code)
# print("Info about API:\n", info)
# print("\nDescription about the key:\n",key)

# response = requests.get("https://randomuser.me/api/")
# response = requests.get("https://api.thecatapi.com/v1/breeds")

response = requests.get("https://www.admie.gr/getFiletypeInfoEN")
json_list = response.json()

values = set()

for i in json_list:
    values.add(i['filetype'])

print(values)

# print(response.headers)
# print(response.json())

############################# Post Requests #############################

# new_data = {
#     "userID": 20,
#     "id": 200,
#     "title": "Test a POST request",
#     "body": "This is the test data."
# }

# # The API endpoint to communicate with
# url_post = "https://jsonplaceholder.typicode.com/posts"

# # A POST request to tthe API
# post_response = requests.post(url_post, json=new_data)

# # Print the response
# post_response_json = post_response.json()
# print(post_response_json)

# req = requests.post('https://en.wikipedia.org/w/index.php', data = {'search':'Nanotechnology'})
# req.raise_for_status()
# with open('Nanotechnology.html', 'wb') as fd:
#     for chunk in req.iter_content(chunk_size=50000):
#         print(chunk)
         #fd.write(chunk)
