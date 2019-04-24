# -*- coding: utf-8 -*-
"""
Created on Mon Apr  1 12:08:43 2019

@author: YoupSuurmeijer
"""

#%% 
""" Loading Required libraries & packages """ 
import os
import re
import pandas as pd
import numpy as np
from selenium.common.exceptions import TimeoutException
from selenium import webdriver 
import time
import threading
#from threading import Thread
import requests
from bs4 import BeautifulSoup


#%%
""" Object definitions """ 

class URLScraper():
    """Scraper algorithm that finds all URLS of pages containing data

    Attributes:
        attr1 (str): Description of `attr1`.
        attr2 (:obj:`int`, optional): Description of `attr2`.

    """
    def __init__(self, N, search_array):
        """Initializer funtion for the scraping algorithm including multi-threading
        Args:
            N: Amount of threads to be created (integer)
            search_array: Array of search terms (string)
    
        Returns:
             List of thread objects executing scraping algorithm
    
        """
        
        self.search_array = search_array
        self.thread_list = list()    # Initiate list of threads as global
        self.results = [{} for x in range(N)] # Initiate list of results as global such that each thread can allocate their results to it

        #Split the search array into N equal sections
        temp = np.array_split(search_array, N)
        # Start N threads
        for i in range(N):
            t = threading.Thread(name='Driver {}'.format(i), target = self.scrape, args = [temp[i], i])
            t.start()
            time.sleep(1)
            print("-----------------------------")
            print(t.name + ' started!')
            print("-----------------------------")
            self.thread_list.append(t)
            
    def to_search_string(self, base_string, search_string):
        """funtion that converts search string to url
        Args:
            base_string: first section of the search url (website search page)
            search_string: string to be found
    
        Returns:
             url with search results for the search term
        """
        temp = base_string + "?q=" + search_string.replace(" ", "+") + "&qt=beer"
        return temp
    
    def get_non_empty(self, list_of_strings):
        """function that returns all non-empty strings in a list of strings
        Args:
            list_of_strings: list of strings, empty and non-empty
    
        Returns:
             list with only non-empty strings
        """
        for s in list_of_strings:
            if s:
                return s
    
    def page_checker_a(self, driver):
        """Checks if we land on the beer profile page or another page
        Args:
           driver: active selenium driver
    
        Returns:
            boolean
        """
        if not "https://www.beeradvocate.com/beer/profile" in driver.current_url:
            #If you're not directly sent to the profile page, get all the urls web elements from the search results
            return(driver.find_elements_by_xpath('//div[@id = "ba-content"]/div/div/span|//div[@id = "ba-content"]/div/div/a'))
        else:
            return(False)
    
    def page_checker_b(self, url_list, driver):
        """Checks if we land on the beer profile page or another page
        Args:
           driver: active selenium driver
           url_list: list of urls
    
        Returns:
            list of non-empty result urls
        """
        
        if url_list:
            temp = []
            for j in url_list: temp.append(j.get_attribute("href"))
            if self.get_non_empty(temp):
                return(self.get_non_empty(temp))
            else: 
                return(False)
        else:
            return(False)
    
    def page_checker_c(self, driver):
        """Checks if last page with search results is current page, if not it return the last page url
        Args:
           driver: active selenium driver
    
        Returns:
            boolean/url
        """
        #Find the element containing the url to the last page with reviews
        temp = driver.find_elements_by_xpath('//span/*[text() = "last"]')
        if temp:
            return(temp)
        else:
            return(False)
    
    def get_one_url(self, driver, search_term):
        """Scrapes url if only one page with reviews
        Args:
           driver: active selenium driver
           search_term: beer being searched for
    
        Returns:
            dataframe object with required data
        """
        beer_found = driver.find_elements_by_xpath('//h1')[0].text

        df_temp = pd.DataFrame({'Beer_search': search_term, 
                                'Beer_found': beer_found,  
                                'Beer_link': driver.current_url, 
                                'Beer_link_N': 0}, index = [0])
        return(df_temp)
    
    def get_na_url(self, search_term):
        """Returns a line of NA results if no reviews were found
        Args:
           search_term: beer being searched for
    
        Returns:
            dataframe object with required data
        """
        df_temp = pd.DataFrame({'Beer_search': search_term, 
                                'Beer_found': "NA",  
                                'Beer_link': "NA", 
                                'Beer_link_N': 1}, index = [0])
        return(df_temp)
    
    def get_many_url(self, last_object, search_term, driver):
        """Scrapes urls if many pages are found
        Args:
           driver: active selenium driver
           search_term: beer being searched for
           last_object: HTML object containing last page url
    
        Returns:
            dataframe object with required data
        """
        beer_found = driver.find_elements_by_xpath('//h1')[0].text
      
        #Get the url to the last page with reviews
        last_page_url = last_object[0].get_attribute("href")
    
        #Get the last number (as pages go by 25 reviews) to ease the search
        last_page_number = int(re.search("([^=]*$)", last_page_url)[1])
    
        #Build a set of urls to visit all pages that contain reviews
        pages_numbers = list(range(0,last_page_number, 25))
        pages_links = [None]*len(pages_numbers)
        
        for j in range(len(pages_numbers)):
            pages_links[j] = re.search("([^=]*)", last_page_url)[1] + '=beer&sort=&start=' + str(pages_numbers[j])
        
        df_temp = pd.DataFrame({'Beer_search': np.repeat(search_term, len(pages_numbers)), 
                                'Beer_found': np.repeat(beer_found, len(pages_numbers)),  
                                'Beer_link': pages_links, 
                                'Beer_link_N': range(len(pages_numbers))})
        return(df_temp)
    
    def scrape(self, search_array, index):
        """Main scraper function containing search logic
        Args:
           search_array: array of search terms to scrape
           index: thread number used in writing results to memory
    
        Returns:
            boolean
        """
        
        #Initiate scraper profile and set base search string
        profile = webdriver.FirefoxProfile()                    
        base_string = "https://www.beeradvocate.com/search/"    
        
        #Set profile preferences to disable flash and disable webrtc
        profile.set_preference('dom.ipc.plugins.enabled.libflashplayer.so',False)
        profile.set_preference("media.peerconnection.enabled", False)
        profile.set_preference("http.response.timeout", 5)
        profile.set_preference("dom.max_script_run_time", 5)
        #profile.update_preferences()                        
    
        driver = webdriver.Firefox(firefox_profile = profile, executable_path = r'C:/Users/YoupSuurmeijer/Documents/geckodriver/geckodriver.exe') #Initiate driver
        
        output_searcher = pd.DataFrame(columns = ['Beer_search', 'Beer_found', 'Beer_link', 'Beer_link_N']) #Initiate output dataframe
        start = time.time()
        
        for i  in search_array:
            #Direct browser to search window and wait for the page to load
            try: 
                driver.get(self.to_search_string(base_string, i))
            except TimeoutException:
                print("TimeOutExpection raised: ", i)
                driver.get(self.to_search_string(base_string, i))

            #Check if you get directed to the profile page or to the search results
            if self.page_checker_a(driver):
                #If you get directed to the search results, check if there are any results
                if self.page_checker_b(self.page_checker_a(driver), driver):
                    #If there are search results, go to the URL of the top result
                    try: 
                        driver.get(self.page_checker_b(self.page_checker_a(driver), driver))
                    except TimeoutException:
                        print("TimeOutExpection raised at getting last page: ", i)
                        driver.get(self.page_checker_b(self.page_checker_a(driver), driver))
                    #Check if there are multiple pages with reviews
                    if self.page_checker_c(driver):
                        #If there are multiple pages with reviews list the pages
                        temp = self.get_many_url(self.page_checker_c(driver), i, driver)
                        output_searcher = output_searcher.append(temp)
                    else:
                        #If there is only one page with reviews, list the one page
                        temp = self.get_one_url(driver, i)
                        output_searcher = output_searcher.append(temp)
                else:
                    #If there are no search results, append an NA line to the output dataframe
                    temp = self.get_na_url(i)
                    output_searcher = output_searcher.append(temp)
            else:
                #Check if there are multiple pages with reviews
                if self.page_checker_c(driver):
                    #If there are multiple pages with reviews list the pages
                    temp = self.get_many_url(self.page_checker_c(driver), i, driver)
                    output_searcher = output_searcher.append(temp)
                else:
                    #If there is only one page with reviews, scrape the one page
                    temp = self.get_one_url(driver, i)
                    output_searcher = output_searcher.append(temp)

        print("-----------------------------")
        print("Driver ", index, "completed!")
        print("Number of urls scraped: "  + str(len(output_searcher)))
        print("Time elapsed: "  + str(int(time.time() - start)) + " Seconds" )
        print("-----------------------------")

        self.results[index] = output_searcher
        driver.close()
        return(True)
    
    def compile_results(self):
        """function to compile all results from seperate threads into one dataframe"""
        for thread in self.thread_list:
            thread.join()
        
        self.output = pd.DataFrame() 
        
        for rslt in self.results:
            if len(rslt) > 0:
                self.output = pd.concat([self.output, rslt])

class ReviewScraper():
    """Scraper algorithm that retrieves the data from all URLs obtained by URL scraper

    Attributes:
        attr1 (str): Description of `attr1`.
        attr2 (:obj:`int`, optional): Description of `attr2`.

    """
    def __init__(self, data, urls = 'Beer_link', pause = 2):
        
        self.data = data
        self.urls = self.data[urls]
        self.pause = pause
        self.reviews = []
        self.counter = 0
        
    def scrape(self):
        """ Main scraper function containing search logic retrieves raw HTML """
        for url in self.urls:
            temp = requests.get(url)
            self.counter += 1
            print(self.counter, "/", len(self.urls), " completed")
            time.sleep(self.pause)
            
            if temp.ok:
                html = BeautifulSoup(temp.text, 'html.parser')
                review = html.findAll("div", class_ = "user-comment")
                temp_output = pd.DataFrame({'url' : url, 'html' : html.content, 'review' : review})
                self.reviews.append(temp_output)
            else:
                temp_output = pd.DataFrame({'url' : url, 'html' : "N/A", 'review' : "N/A"}, index = [0])
                self.reviews.append(temp_output)
                
        self.reviews = pd.concat(self.reviews)
        print("----------------------")
        print("Scraper complete!")
        print("----------------------")
        
    def compile_results(self):
        """function to convert all HTML into required data and list into one dataframe"""
        self.output = []
        
        for i in range(len(self.reviews)):
            review = self.reviews['review'].iloc[i]
            url = self.reviews['url'].iloc[i]
            
            temp_text = re.search(r'overall: \d+(.*?)character', review.text).group(1) if re.search(r'overall: \d+(.*?)character', review.text) is not None else "N/A"
            temp_date = ','.join(review.text.split(",")[-2:])
            temp_overall = review.find("span", class_ = "BAscore_norm").text
            temp_rdev = review.find("span", class_ = "rAvg_norm").text
            temp_look = re.search('look: (\d+\.?\d?)', review.text).group(1) if re.search('look: (\d+\.?\d?)', review.text) is not None else "N/A"
            temp_feel = re.search('feel: (\d+\.?\d?)', review.text).group(1) if re.search('feel: (\d+\.?\d?)', review.text) is not None else "N/A"
            temp_smell = re.search('smell: (\d+\.?\d?)', review.text).group(1) if re.search('smell: (\d+\.?\d?)', review.text) is not None else "N/A"
            temp_taste = re.search('taste: (\d+\.?\d?)', review.text).group(1) if re.search('taste: (\d+\.?\d?)', review.text) is not None else "N/A"
                      
            temp_output = pd.DataFrame({'Overall' : temp_overall, 'Rdev' : temp_rdev, 'Text' : temp_text, 
                                        'Look' : temp_look, 'Feel' : temp_feel, 'Smell' : temp_smell, 
                                        'Taste' : temp_taste, 'Date': temp_date, 'url' : url} , index = [0])
            self.output.append(temp_output)
        self.output = pd.concat(self.output)
        self.output = pd.merge(self.output, self.data, how='left', left_on= ['url'], right_on = 'Beer_link')
            

#%%
if __name__ == '__main__':
    """" Other Preparations & Data Load """
    os.chdir("C:/Users/YoupSuurmeijer/Documents/Swinckels/Supermarkt/Data")
    
    df_class = pd.read_csv("beer_classification.csv", sep = ";") 
    df_class['ProdName'] = df_class['Product_MAJOR_BRAND'] + " " + df_class['Product_VARIANT']
    
    """" Defining Search Strings """
    name_array = np.unique(df_class['ProdName'].values)
    
    """" Running the URL search string algorithm"""
    search_array = name_array[0:200]
    N = 2   # Number of browsers to spawn
    
    url_scraper = URLScraper(N, search_array)
    url_scraper.compile_results()
    
    """" Exporting the results to csv """
    name = ("C:/Users/YoupSuurmeijer/Documents/Swinckels/Supermarkt/Data/" + "URL - " + 
            url_scraper.output['Beer_search'].iloc[0] + " - " + 
            url_scraper.output['Beer_search'].iloc[-1] + ".csv")
    url_scraper.output.to_csv(path_or_buf = name, sep = ";")

    """" Running the data retrieval algorithm"""
    review_scraper = ReviewScraper(url_scraper.output, pause = 1)
    review_scraper.scrape()
    review_scraper.compile_results()
    
    """" Exporting the results to csv """
    name = ("C:/Users/YoupSuurmeijer/Documents/Swinckels/Supermarkt/Data/" +  "DATA - " +
            review_scraper.output['Beer_search'].iloc[0] + " - " + 
            review_scraper.output['Beer_search'].iloc[-1] + ".csv")
    
    review_scraper.output.to_csv(path_or_buf = name, sep = ";")


