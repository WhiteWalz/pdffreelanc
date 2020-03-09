from bs4 import BeautifulSoup
import requests
import csv
import sqlite3 as sql

BASEURL = 'https://www.mobilehomeparkstore.com/mobile-home-park-directory/usa/page/'

def start():
    conn = sql.connect('MobileHomes.db')
    conn.execute('''CREATE TABLE IF NOT EXISTS mobile_homes(id INTEGER PRIMARY KEY, name TEXT, address TEXT, zip INTEGER, state TEXT, city TEXT, lots TEXT, seniors INTEGER);''')
    outFile = open('Mobile_Home_ParkStore_Results.csv', 'a')
    outFile.write('Index,Name,Address,Zip,State,City,Lots,Seniors\n')
    with conn:
        for i in range(1, 10000):
            rawHTML = requests.get(BASEURL + str(i))
            parsedHTML = parseHTML(rawHTML.text, i-1)
            if len(parsedHTML) == 0 or not rawHTML.ok:
                break
            write = csv.DictWriter(outFile, parsedHTML[0].keys())
            write.writerows(parsedHTML)
            for row in parsedHTML:
                dictRow = [row.get('Index'), row.get('Name'), row.get('Address'), row.get('Zip'), row.get('State'), row.get('City'), row.get('Lots'), row.get('Seniors')]
                conn.execute('''INSERT INTO mobile_homes VALUES (?,?,?,?,?,?,?,?)''', dictRow)
            print(str(i))
    outFile.close()
    conn.close()
        

def parseHTML(raw, index10s):
    index10s = index10s * 10
    soup = BeautifulSoup(raw, 'html.parser')
    entryList = soup.find_all('div', class_='item-info')
    parsedList = []
    di = {}
    for ele in entryList:
        di['Index'] = index10s + entryList.index(ele)
        di['Name'] = ele.find('a').string
        
        temp = ele.find('span', itemprop='streetAddress')
        if temp != None:
            di['Address'] = temp.string
        else:
            di['Address'] = temp
        
        temp = ele.find('span', itemprop='postalCode')
        if temp != None:
            di['Zip'] = temp.string
        else:
            di['Zip'] = None

        temp = ele.find('span', itemprop='addressRegion')
        if temp != None:
            di['State'] = temp.string
        else:
            di['State'] = None
        
        temp = ele.find('span', itemprop='addressLocality')
        if temp != None:
            di['City'] = temp.string
        else:
            di['City'] = None
        
        temp = ele.find('span', title='Number of Mobile Home Lots')
        if temp != None:
            di['Lots'] = temp.string
        else:
            di['Lots'] = None
        
        temp = ele.find('span', title='55 and over community')
        if temp == None:
            di['Seniors'] = False
        else:
            di['Seniors'] = True
        parsedList.append(di.copy())
    return parsedList



if __name__ == '__main__':
    start()