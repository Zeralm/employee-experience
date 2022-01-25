from lib2to3.pgen2 import driver
import re
from time import time
import selenium
import math
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import os
import pandas as pd
import numpy as np
import mysql.connector
import datetime
from dbconnect import insert, send
import sys
# 975
# Divide work

def retry(func):
    def retry_2():
        result = False
        while result == False:
            try:
                result = func()
            except Exception as err:
                timer_err = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                with open(os.path.join(parent_dir , "src/errorlog.txt"), "a") as errorlog:
                    try:
                        errorlog.writelines(f"\n {timer_err}: ERROR {err} at page {page} while attempting parsing from {sys.argv[1]} to {sys.argv[2]}\n")
                        print(f"\n {timer_err} ERROR {err} at page {page} while attempting parsing from {sys.argv[1]} to {sys.argv[2]}\n")
                        
                    except NameError:
                        errorlog.writelines(f"\n {timer_err}: ERROR {err} at page {sys.argv[1]} while attempting parsing from {sys.argv[1]} to {sys.argv[2]}\n")
                        print(f"\n {timer_err} ERROR {err} at page {sys.argv[1]} while attempting parsing from {sys.argv[1]} to {sys.argv[2]}\n")
            finally:
                driver.quit()
            
    return retry_2
@retry
def crawl():
        
            global next_low_end
            global page
            global driver
            chrome_options = Options()
            #chrome_options.add_argument("--headless")
            driver = webdriver.Chrome(os.path.join(parent_dir , "src/chromedriver"), options=chrome_options)
            print("Chrome initialized")
            

            # No intention to generalize the parser soon, but just in case...

            driver.get("https://www.glassdoor.com/Reviews/Salesforce-Reviews-E11159.htm?countryRedirect=true")
            driver.get("https://www.glassdoor.com/Reviews/Salesforce-Reviews-E11159.htm?countryPickerRedirect=true")
            driver.get("https://www.glassdoor.com/Reviews/Salesforce-Reviews-E11159.htm?sort.sortType=RD&sort.ascending=true&countryPickerRedirect=true")
            # Detect problem if glassdoor is blocked
            assert "Glassdoor" in driver.title
            print("Web source ready")

            reviewcount = int(driver.find_element_by_xpath('//h2[@data-test="overallReviewCount"]/span/strong[1]').text.replace(",",""))              
            nr_pages = math.trunc(reviewcount/10) + 1
            print(str(nr_pages) + " pages")
            if int(sys.argv[1]) == 1:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH,'(//div[@class="gdReview"])[10]'))
                    )

                results = driver.find_elements_by_xpath('//div[@class="gdReview"]')
                glass_ids = driver.find_elements_by_xpath("//div[@id='ReviewsRef']/div/ol/li")
                table_results = pd.DataFrame([[results[o].find_element_by_xpath(info_paths[i]).get_attribute('textContent') for i in info_paths] + [datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S") ,company ,glass_ids[o-1].get_attribute("id")] for o in range(10)])
                aggr_table = table_results.copy()

            for page in range(next_low_end, high_end+1):
                driver.get(f"https://www.glassdoor.com/Reviews/Salesforce-Reviews-E11159_P{page}.htm?sort.sortType=RD&sort.ascending=true&filter.iso3Language=eng")
                results = driver.find_elements_by_xpath('//div[@class="gdReview"]')
                glass_ids = driver.find_elements_by_xpath("//div[@id='ReviewsRef']/div/ol/li")
                table_results = pd.DataFrame([[results[o].find_element_by_xpath(info_paths[i]).get_attribute('textContent') for i in info_paths] + [datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S") ,company ,glass_ids[o-1].get_attribute("id")] for o in range(10)])
            
                # We get rid of unnecessary data already loaded in DB. We only load at 100 by 100 bits.
                print(aggr_table)
                try: 
                    if page == high_end:
                        aggr_table = pd.concat([aggr_table, table_results])
                        insert(aggr_table, database=sys.argv[3]) 
                        next_low_end = page
                        print(aggr_table)              
                    elif aggr_table.shape[0] >= 100:
                        insert(aggr_table, database=sys.argv[3])
                        next_low_end = page
                        aggr_table = table_results.copy()       
                    else:
                        aggr_table = pd.concat([aggr_table, table_results])
                    
                except NameError:
                    aggr_table = table_results
            return True



if __name__ == "__main__":
    
    print(f"Beginning:{sys.argv[1]}, End: {sys.argv[2]}")

    # Many things about to be deprecated

    parent_dir = os.path.abspath(os.path.join(__file__, os.pardir))
    
    company = "Salesforce"
    high_end = int(sys.argv[2])
    low_end = int(sys.argv[1])
    next_low_end = max(2, low_end)
    
    info_paths = {"rating":".//span[@class='ratingNumber mr-xsm']", 
            "position":".//span[@class='authorInfo']",
            "title":".//a[@class='reviewLink']",
            "employment":".//span[@class='pt-xsm pt-md-0 css-1qxtz39 eg4psks0']",
            "pros":".//span[@data-test='pros']",
            "cons":".//span[@data-test='cons']",
             }

    
    crawl()

        
            
    print(f"Done! ({sys.argv[1]} - {sys.argv[2]})")
    # Index them by id (page | pos)
    # Recurrent calls to staging DB




# DB fields
# id, rating, info, title, employment, pros, cons, time, company, glass_id 
# For staging DB also the time or stage of arrival and for final too

# print(pd.DataFrame([[driver.find_elements_by_xpath('//div[@class="gdReview"]')[o].find_element_by_xpath(info_paths[i]).get_attribute('textContent') for i in info_paths] for o in range(10)]))
# [print(driver.find_elements_by_xpath('//div[@class="gdReview"]')[0].find_element_by_xpath(info_paths[i]).get_attribute('textContent')) for i in info_paths] # A test of the content of the review
# Something's wrong alert xpath: //div[@class='text']/strong where the text comprises: "Something's wrong"