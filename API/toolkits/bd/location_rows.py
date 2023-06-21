import pandas as pd
import requests
import json

import API.toolkits.smartsheet as s

smart = s.Smartsheet('4564736578375556')
smart = smart.smart_df()

smart['# of Locations'] = smart['# of Locations'].astype(int)

smart2 = pd.DataFrame(smart.values.repeat(smart['# of Locations'],axis=0))

print(smart2)

url = 'https://api.smartsheet.com/2.0/sheets/2173856127078276/rows'
header = {
    'Authorization': 'Bearer 8QG48BPqXExoBp6Ebm9xODcKNzjLP7CClwkWG',
    'Content-Type': 'application/json'
}

# for row, tag in smart2.items():
#     payload = "{\"id\":" + '''"''' + str(row) + '''"''' ", \"cells\": [{\"columnId\": 6965625461100420,\"value\": " + '''"''' + str(tag) + '''"''' + "}]}"
#     response = requests.request('PUT',url, data=payload, headers=header)

# response = requests.get(url, headers=header)
# data = json.loads(response.text)

print(smart2)