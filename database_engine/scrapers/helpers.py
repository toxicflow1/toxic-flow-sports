"""
This module includes helper functions for webscraping
"""
import requests
import os
from pathlib import Path

def page_downloader(url, fname):
  """
  Download the webpage at 'url' and save it to a HTML file called 'fname.html' in the current
  working directory

  Parameters 
  ----------
  url : string
    The url for the webpage being downloaded
  fname : string
    The name of the HTML file the webpage is saved to
  """
  res = requests.get(url)
  file = open(f"{fname}.html","wb")
  for chunck in res.iter_content(1000000):
    file.write(chunck)
  file.close()

def fill_folder_html(folder_name, base_url, end_urls):
  """
  Fill the specified folder with HTML files specified by the URLS

  This function fills the folder named 'folder_name' in the current working directory with HTML files.
  First it then forms a list of urls by combining the 'base_url' with the 'end_urls'.
  These urls specify the webpages that are downloaded and saved in the folder as HTML files.

  This function is useful when there is a webpage with a lot of links and you want to download the webpages at all the links.  

  Parameters
  ----------
  folder_name : string
    The name of the folder to save the HTML files to
  base_url : string
    The first part of the URL of the webpages being downloaded
  end_urls : list of str
    List containing the last part of the URLs of the webpages being downloaded

  Examples
  --------
  >>> fill_folder_html(
  "Seasons",
  "https://cuetracker.net/seasons",
  ["2022-2023?status=professional","2022-2023?status=non-professional"]
  )

  This would download the webpages at https://cuetracker.net/seasons/"2022-2023?status=professional
  and https://cuetracker.net/seasons/"2022-2023?status=non-professional and save them to the folder
  called 'Seasons'
  """
  number_of_webpages = len(end_urls)
  ten_percent = number_of_webpages//10
  print(f"Downloading webpages {number_of_webpages} from {base_url}")

  # Change the directory to the folder you want to save the files to
  folder_path = Path.cwd() / Path("Seasons")
  os.chdir(folder_path)

  for i, end_url in enumerate(end_urls):
    url = base_url + '/' + end_url
    page_downloader(url, end_url)
    if i % ten_percent == 0:
      print(f"{i+1} pages downloaded...")

  # Change back to the programs directory
  os.chdir(folder_path.parent)