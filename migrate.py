#!/usr/bin/env python3

import logging
import pymysql
from influxdb import InfluxDBClient
from datetime import datetime

logging.basicConfig(filename = "migrate.log", format = "%(asctime)s - %(message)s", level = logging.INFO)

def main():

    logging.info("Connecting to mysql")
    mysql_con = pymysql.connect(host='localhost', user='', password='', database='')

    logging.info("Connecting to influxdb")
    influx_con = InfluxDBClient('localhost', '8086')
    
    logging.info("Create DB")
    influx_con.create_database('sensors')
    influx_con.switch_database('sensors')

# SELECT `ts`,`sensor`,`value` FROM `sensors` WHERE `sensor` = "heater_temp" OR `sensor` = "inside_temp" OR `sensor` = "Kotel" OR `sensor` = "Kotelnaya" OR `sensor` = "Obratka" OR `sensor` = "outside_temp" OR `sensor` = "Podacha" ORDER BY `ts` ASC;
#co2
#co2temp
#fullspectrum
#humidity
#hum_temp
#pm10
#pm25
#pressure
#press_temp

    cur = mysql_con.cursor()
    cur.execute("SELECT `ts`,`sensor`,`value` FROM `sensors` WHERE `sensor` = 'heater_temp' OR `sensor` = 'inside_temp' OR `sensor` = 'Kotel' OR `sensor` = 'Kotelnaya' OR `sensor` = 'Obratka' OR `sensor` = 'outside_temp' OR `sensor` = 'Podacha' ORDER BY `ts` ASC")
    if cur.rowcount:
        data_list =[]
        n = 0
        p = 0
        for row in cur:
            if n<1000:
                data_point = {"measurement": "temperature",
                              "tags" : {"sensor_place":row[1]},
                              "time" : "%sZ"%row[0],
                              "fields" : {"value" : row[2]}
                             }
                data_list.append(data_point)
                n=n+1
            else:
                n = 0
                logging.info("Write a part of data points %s"%p)
                influx_con.write_points(data_list,time_precision='s')
                data_list =[]
                p = p + 1

        logging.info("Write the last part of data points %s"%n)
        influx_con.write_points(data_list,time_precision='s')

    cur.close()

    logging.info("Closing mysql")
    mysql_con.close()

if __name__ == '__main__':
    main()


