# -*- coding: utf-8 -*-
"""
Created on Tue Apr 23 12:15:10 2019

@author: YoupSuurmeijer
"""

from requests import get
from requests.exceptions import RequestException
from contextlib import closing
from bs4 import BeautifulSoup
import re

def simple_get(url):
    """
    Attempts to get the content at `url` by making an HTTP GET request.
    If the content-type of response is some kind of HTML/XML, return the
    text content, otherwise return None.
    """
    try:
        with closing(get(url, stream=True)) as resp:
            if is_good_response(resp):
                return resp.content
            else:
                return None

    except RequestException as e:
        log_error('Error during requests to {0} : {1}'.format(url, str(e)))
        return None


def is_good_response(resp):
    """
    Returns True if the response seems to be HTML, False otherwise.
    """
    content_type = resp.headers['Content-Type'].lower()

    return (resp.status_code == 200 
            and content_type is not None 
            and content_type.find('html') > -1)
    return(True)

def log_error(e):
    """
    It is always a good idea to log errors. 
    This function just prints them, but you can
    make it do anything.
    """
    print(e)
    
def get_names():
    """
    Downloads the page where the list of mathematicians is found
    and returns a list of strings, one per mathematician
    """
    url = 'http://www.fabpedigree.com/james/mathmen.htm'
    response = simple_get(url)

    if response is not None:
        html = BeautifulSoup(response, 'html.parser')
        names = set()
        for li in html.select('li'):
            for name in li.text.split('\n'):
                if len(name) > 0:
                    names.add(name.strip())
        return list(names)

    # Raise an exception if we failed to get any data from the url
    raise Exception('Error retrieving contents at {}'.format(url))

    
def get_hits_on_name(name):
    """
    Accepts a `name` of a mathematician and returns the number
    of search results for the Toronto Public Library website as an `int`
    """
    # url_root is a template string that is used to build a URL.
    url_root = 'https://www.torontopubliclibrary.ca/search.jsp?Ntt={name}'
    name = name.replace(" ", "+")
    url = url_root.format(name=name)
    print("GET request for: ", url)
    response = simple_get(url)

    if response is not None:
        html = BeautifulSoup(response, 'html.parser')

        hit_link = html.find("h3", {"class": "item-count"})

        if len(hit_link) > 0:
            # Strip all non alpha-numeric characters (note I'm using Regex!)
            link_text = hit_link.text
            link_text = re.sub('[^0-9]','', link_text)
            try:
                # Convert to integer
                return int(link_text)
            except:
                log_error("couldn't parse {} as an `int`".format(link_text))

    log_error('No pageviews found for {}'.format(name))
    return None

if __name__ == '__main__':
    print('Getting the list of names....')
    names = get_names()
    print('... done.\n')

    results = []

    print('Getting stats for each name....')

    for name in names:
        try:
            hits = get_hits_on_name(name)
            if hits is None:
                hits = -1
            results.append((hits, name))
        except:
            results.append((-1, name))
            log_error('error encountered while processing '
                      '{}, skipping'.format(name))

    print('... done.\n')

    results.sort()
    results.reverse()

    if len(results) > 5:
        top_marks = results[:5]
    else:
        top_marks = results

    print('\nThe most literate mathematicians are:\n')
    for (mark, mathematician) in top_marks:
        print('{} with {} results'.format(mathematician, mark))

    no_results = len([res for res in results if res[0] == -1])
    print('\nBut we did not find results for '
          '{} mathematicians on the list'.format(no_results))