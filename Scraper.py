#imports
import string
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
from csv import writer
import os
# from pyspark.sql import SparkSession
# import nltk
# nltk.download("stopwords")
from nltk.corpus import stopwords

#setting up chrome driver and using english version of the website
url = "https://www.google.com/maps/place/Common+Grounds+JBR/@25.0761678,55.1318728,18z/data=!4m14!1m6!3m5!1s0x3e5f436ae71d43a7:0xe8bbdded984af7dd!2sIbn+Battuta+Mall!8m2!3d25.0445481!4d55.1202965!3m6!1s0x3e5f1511c0322ed5:0x968b9495687f7e6a!8m2!3d25.0770291!4d55.1323619!9m1!1b1"
options = Options()
options.add_argument('--lang=en-GB')
options.headless = True
chromeDriver = webdriver.Chrome(options=options)
chromeDriver.get(url)
time.sleep(5)
#setting up the scroller function to load all reviews
scroller = chromeDriver.find_element("xpath", "/html/body/div[3]/div[9]/div[9]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]")
scrollCounter = 0 #number of times the scroller function is called

#loop to scroll all the way to the end so that all data is loaded
last_height = chromeDriver.execute_script("return arguments[0].scrollHeight", scroller)
while True:
    chromeDriver.execute_script('arguments[0].scrollTo(0, arguments[0].scrollHeight)', scroller)
    scrollCounter += 1
    time.sleep(5)
    new_height = chromeDriver.execute_script("return arguments[0].scrollHeight", scroller)
    if new_height == last_height:
        break
    last_height = new_height

#variables for the required data for extraction
authors = chromeDriver.find_elements(By.CLASS_NAME, "d4r55")
dates = chromeDriver.find_elements(By.CLASS_NAME, "rsqaWe")
reviewTexts = chromeDriver.find_elements(By.CLASS_NAME, "wiI7pd")
ratings = chromeDriver.find_elements(By.CLASS_NAME, "kvMYJc")
moreBtn = chromeDriver.find_elements(By.CLASS_NAME, "w8nwRe.kyuRq")

#function to click the "more" button to load all text for a review
if moreBtn:
    for b in range(len(moreBtn)):
        moreBtn[b].click()
#breaking down the all the class_name items and storing them into an array
author_list, date_list, reviewTexts_list, rating_list = ([],[],[],[])
if authors:
    for a in range(len(authors)):
        author_list.append(authors[a].text)
if dates:
    for d in range(len(dates)):
        date_list.append(dates[d].text)
if reviewTexts:
    total_ratings = 0
    empty_ratings = 0
    for rt in range(len(reviewTexts)):
        reviewTexts_list.append(reviewTexts[rt].text)
        if(not reviewTexts[rt].text):
            empty_ratings += 1
if ratings:
    for r in range(len(ratings)):
        rating_list.append(ratings[r].get_attribute("aria-label"))

#displaying all extracted data in a table format -> PANDA'S DATAFRAME
data_tuples = list(zip(author_list[0:], rating_list[0:], reviewTexts_list[0:], date_list[0:]))
df = pd.DataFrame(data_tuples, columns=['Name','Rating','Review','Date'])
#df.style
print(df.to_string())

# SPARK VERSION
#creating the SPARK DATAFRAME
# spark = SparkSession.builder.master("local[*]").appName("Pyspark-Prototype").getOrCreate()
# spark.conf.set("spark.sql.execution.arrow.enabled","true")
#converting panda's to spark
# sparkDF = spark.createDataFrame(df)
# sparkDF.printSchema()
# sparkDF.show()


chromeDriver.close()
#delete exist output file
if os.path.exists("Reviews.csv"):
    os.remove("Reviews.csv")
#convert the extracted data into json and exporting it
df.to_csv(r'Output.csv')

# total_ratings = str(rating_list)
# empty_ratings = str(empty_ratings)
print(f"Total Ratings: {len(rating_list)} \nRatings withouth reviews:  {empty_ratings}")
