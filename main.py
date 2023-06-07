#!/usr/bin/python3
# crawle www.chinapesticide.org.cn

from bs4 import BeautifulSoup
import requests
import time
import pymongo
import pymysql
import os

mgdb_con_str = os.getenv("MGDB_CON_STR", "mongodb://localhost:27017/")
mgdb_user = os.getenv("MGDB_USER", "admin")
mgdb_pwd = os.getenv("MGDB_PWD", "admin")

mysql_host = os.getenv("MYSQL_HOST", "localhost")
mysql_port = os.getenv("MYSQL_PORT", 3306)
mysql_user = os.getenv("MYSQL_USER", "root")
mysql_pwd = os.getenv("MYSQL_PWD", "root")
mysql_db = os.getenv("MYSQL_DB", "db")

mysql_con = pymysql.connect(host=mysql_host, port=mysql_port, user=mysql_user, password=mysql_pwd, database=mysql_db)

db_use = "mgdb"

def crawler_nong_yao():
  def parse_lines(bs) -> list:
    lines_list = []
    index = 2
    for index in range(2, 22):
      line_list = []
      djzh = bs.select("#tab > tr:nth-child({}) > td:nth-child(1) > span".format(index))
      nymc = bs.select("#tab > tr:nth-child({}) > td:nth-child(2) > span".format(index))
      nylb = bs.select("#tab > tr:nth-child({}) > td:nth-child(3) > span".format(index))
      jx = bs.select("#tab > tr:nth-child({}) > td:nth-child(4) > span".format(index))
      zhl = bs.select("#tab > tr:nth-child({}) > td:nth-child(5) > span".format(index))
      yxrq = bs.select("#tab > tr:nth-child({}) > td:nth-child(6) > span".format(index))
      djzcyr = bs.select("#tab > tr:nth-child({}) > td:nth-child(7) > span > a".format(index))
      line_list.append(djzh[0].text.strip())
      line_list.append(nymc[0].text.strip())
      line_list.append(nylb[0].text.strip())
      line_list.append(jx[0].text.strip())
      line_list.append(zhl[0].text.strip())
      line_list.append(yxrq[0].text.strip())
      line_list.headers = {
      "Content-Type": "application/x-www-form-urlencoded"
    }append(djzcyr[0].text.strip())
      lines_list.append(line_list)
    return lines_list
  url_page1 = "https://www.icama.cn/BasicdataSystem/pesticideRegistration/queryselect.do"
  param = "pageNo={}&pageSize=20&djzh=&nymc=&cjmc=&sf=&nylb=&zhl=&jx=&zwmc=&fzdx=&syff=&dx=&yxcf=&yxcf_en=&yxcfhl=&yxcf2=&yxcf2_en=&yxcf2hl=&yxcf3=&yxcf3_en=&yxcf3hl=&yxqs_start=&yxqs_end=&yxjz_start=&yxjz_end=&accOrfuzzy=2"
  total_page = 0
  result = requests.post(url_page1, param.format(1))
  if result.status_code != 200:
    raise Exception("crawle paeg 1 failed  status code {}, "+
                    "result is {} ".format(1, result.status_code, result))
  bs = BeautifulSoup(result.content, features="html.parser")
  tag_a = bs.select("body > div.web_ser_body_right_main_search > div > ul > li:nth-child(11) > a")
  total_page = int(tag_a[0].getText())

  line_collection = []
  for index in range(1, total_page):
    time.sleep(3)
    print("get page {} , total {}".format(index, total_page))
    headers = {
      "Content-Type": "application/x-www-form-urlencoded"
    }
    result = requests.post(url_page1, param.format(index), headers=headers)
    if result.status_code != 200:
      raise Exception("crawle failed in page {} status code {}, "+
                      "result is {} ".format(index, result.status_code, result))
    dec_res = result.content.decode("u8")
    bs = BeautifulSoup(dec_res, features="html.parser")
    line_list = parse_lines(bs)
    for item in line_list:
      if db_use == "mysql":
        save_mysql(item)
      else:
        save(item)


def save(line):
  line_obj = {
    "djzh": line[0],
    "nymc": line[1],
    "nylb": line[2],
    "jx": line[3],
    "zhl": line[4],
    "yxrq": line[5],
    "djzcyr": line[6]
  }
  mgcli = pymongo.MongoClient(mgdb_con_str, username=mgdb_user, password=mgdb_pwd)
  # mgcli.nong_yao.authenticate(mgdb_user, mgdb_pwd, mechanism='MONGODB-CR')
  nong_yao_db = mgcli['crawler_nongzi']
  nong_yao_collection = nong_yao_db['nong_yao']
  insert_res = nong_yao_collection.insert_one(line_obj)
  print("data {} inserted to mongodb, id is {}".format(line_obj, insert_res.inserted_id))

def save_mysql(line):
  try:
      #mysql_con.autocommit(True)
      cursor = mysql_con.cursor()
      cursor.execute("insert into nz_cralwe_chinapesticide (djzh, nymc, nylb, jx, zhl, yxrq, djzcyr) values (%s, %s, %s, %s, %s, %s, %s)",
                  (line[0], line[1], line[2], line[3], line[4], line[5], line[6]))
      print("data {} inserted to mysql".format(line))
  finally:
      cursor.close()

if __name__ == "__main__":
    try:
        crawler_nong_yao()
    finally:
      mysql_con.close()
