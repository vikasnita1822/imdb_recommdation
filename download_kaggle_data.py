###1

# import opendatasets as od

# od.download("https://www.kaggle.com/datasets/whenamancodes/blood-transfusion-dataset")
# print('Download completed successfully')

###2

# import requests
# url = 'https://www.kaggle.com/datasets/whenamancodes/blood-transfusion-dataset'

# act = {"username":"vikasyttn","key":"34d279ba4b1e10181116e1939e6e2063"}

# response = requests.get(url,act)
# open('data_demo2.csv','wb').write(response.content)

# import requests
# url = 'https://www.kaggle.com/datasets/whenamancodes/blood-transfusion-dataset'
# username = 'vikasyttn'
# password = '34d279ba4b1e10181116e1939e6e2063'
# # response = requests.get(url, auth=(username, password)).content
# open('data_demo2','wb').write(requests.get(url, auth=(username, password)).content)

###3
# import wget

# URL = 'https://www.kaggle.com/datasets/whenamancodes/blood-transfusion-dataset'

# response = wget.download(URL, "demo_data3.csv")

###4
# from urllib import request

# url = "https://www.kaggle.com/datasets/whenamancodes/blood-transfusion-dataset"

# response = request.urlretrieve(url, "demo_data4.csv")

###5

# import os
# os.environ['KAGGLE_USERNAME'] = "vikasyttn"
# os.environ['KAGGLE_KEY'] = "34d279ba4b1e10181116e1939e6e2063"
# !kaggle datasets download -d whenamancodes/blood-transfusion-dataset


# ##6

import kaggle

kaggle.api.authenticate()

kaggle.api.dataset_download_files('whenamancodes/blood-transfusion-dataset', path='/home/vikash/Desktop/Project_self/', unzip=True)