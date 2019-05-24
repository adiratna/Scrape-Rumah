# -*- coding: utf-8 -*-
#rachmat_sn

import sqlite3
import json

conn = sqlite3.connect('jualrumahOLX.db')
c = conn.cursor()

data = {} #dictionary
status = ["Dijual", "Disewakan"]

#SELECT city FROM rumah GROUP BY city ORDER BY COUNT(city) DESC LIMIT 7

kota = []
query = c.execute("SELECT city FROM rumah GROUP BY city ORDER BY COUNT(city) DESC LIMIT 7")
ftch = c.fetchall()
for i in range (len(ftch)):
    kota.append(ftch[i][0])
data['labels'] = kota
print (kota)

#cari penjualan di tiap kota
for i in range(len(kota)):
    result = []
    for j in range(len(status)):
        query = c.execute("SELECT count(city) FROM rumah WHERE (city == '"+kota[i]+"' and status == '"+status[j]+"') GROUP BY city ORDER BY COUNT(city) DESC")
        ftch = c.fetchall()
        if not ftch:
            hsl = 0
        else:
            hsl = int(ftch[0][0])
        result.append(hsl)
    data["k"+str(i+1)] = result

print(data)

#simpan file
with open('dictionary_rumah.txt', 'w') as fp:
    json.dump(data, fp)
    
c.close()
conn.close()
