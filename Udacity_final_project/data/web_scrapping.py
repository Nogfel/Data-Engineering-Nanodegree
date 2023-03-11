import requests
from bs4 import BeautifulSoup

html = requests.get('https://zbordirect.com/en/tools/iata-airlines-codes').content

soup = BeautifulSoup(html, 'html.parser')

# for table in soup.find_all('table'):
#     print(table.get('class'))

# Output from lines 8 and 9: ['table', 'table-hover', 'table-bordered']

table = soup.find('table', class_='table-hover')
print(table)


