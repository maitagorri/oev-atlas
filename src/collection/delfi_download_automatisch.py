#!/usr/bin/env python
# coding: utf-8

# Importe


import time
import os.path

from selenium import webdriver
#from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.wait import WebDriverWait

from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.utils import ChromeType
from selenium.webdriver.chrome.options import Options


# Parameter

download_dir = "../../data/raw/delfi/dl/"

# Delfi-Zugangsdaten sollten in einer separaten Textdatei hinterlegt werden
with open("/home/jupyter-maita.schade/delfi_cred.txt") as f:
    uname, pwd = [l.strip() for l in f.readlines()]


# Webdriver setup


options = Options()
options.add_argument("--no-sandbox")
options.add_argument('--disable-dev-shm-usage')
options.add_argument("--headless")
options.add_experimental_option("prefs",{"download.default_directory": download_dir,
                                         "download.prompt_for_download":False
                                        })

options.binary_location = "/usr/bin/chromium-browser"
# options.add_argument("--disable-gpu")
# options.add_argument("--disable-dev-shm-usage")
# options.add_argument("--window-size=1920,1200")

driver = webdriver.Chrome('/snap/bin/chromium.chromedriver', options = options)#, executable_path=)

# ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install()

# Zugriff auf Webseite


driver.get("https://www.opendata-oepnv.de/ht/de/organisation/delfi/startseite?tx_vrrkit_view%5Bdataset_name%5D=deutschlandweite-sollfahrplandaten-gtfs&tx_vrrkit_view%5Baction%5D=details&tx_vrrkit_view%5Bcontroller%5D=View")


# In[311]:


# Cookie button
try:
    cookie_load = WebDriverWait(driver, 5).until(
        expected_conditions.element_to_be_clickable((By.CSS_SELECTOR,
                                                     "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowallSelection")
                                                   )
    )
except:
    print("Problem with cookie acceptance")
driver.find_element_by_css_selector("#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowallSelection").click()
print("Clicked cookie button")
WebDriverWait(driver, 5)


# In[312]:


# Anmeldung

try:
    print('Anmeldung laden')
    anmeldung_load = WebDriverWait(driver, 5).until(
        expected_conditions.element_to_be_clickable((By.CSS_SELECTOR,
                                                     "a[data-target='#login']")
                                                   )
    )
    print('Anmeldung geladen')
except:
    print("Problem with Anmelde-Button")

anmelde_button = driver.find_elements_by_css_selector("a[data-target='#login']")
if anmelde_button:
    print ("Anmeldung...")
    anmelde_button[0].click()

    print ("Anmeldebutton geklickt")
    driver.find_element_by_css_selector("#user").send_keys(uname)
    print('username')
    driver.find_element_by_css_selector("#pass").send_keys(pwd)
    print('pwd')
    driver.find_element_by_css_selector("input[value='Anmelden']").click()
    print('Anmeldebutton 2')
#     acceptterms = driver.find_elements_by_css_selector("#acceptterms")
#     if acceptterms:
#         terms_load = WebDriverWait(driver, 5).until(
#             expected_conditions.element_to_be_clickable((By.CSS_SELECTOR,
#                                                          "#acceptterms")
#                                                         )
#         )
#         acceptterms[0].click()
# except:
#     print("Problem with Anmeldung")


# In[313]:


# Download
download_buttons = driver.find_elements_by_css_selector("td>a[href^='https://www.opendata-oepnv.de/fileadmin/datasets/delfi']")
if download_buttons:
    print('download button gefunden')
    data_url = download_buttons[0].get_attribute("href")
    download_buttons[0].click()
    print('download button geklickt')
    
    try:
        print('terms laden')
        finalterms_load = WebDriverWait(driver, timeout=5) #.until(
        #     expected_conditions.element_to_be_clickable((By.CSS_SELECTOR,
        #                                                  "a[class='btn btn-primary acceptdownload'")
        #                                                )
        # )
        # print('terms sind clickable')

        finalterms = driver.find_elements_by_css_selector("a[class='btn btn-primary acceptdownload'")
        if finalterms:
            print("Downloading " + data_url)
            print(finalterms[0].get_attribute('href'))
            finalterms[0].click()
            print("clicked final terms")
    except:
        pass
# except:
#     print('Problem with Download')
print("got past download try")

# See if I can wget it now... don't think so
# import wget
# print("trying wget")
# wget.download(data_url, out = download_dir)
# print("completed wget")
# In[ ]:

# Try opening data url in new tab
driver.get(data_url)
print('trying to get via new tab')
# Monitoring download

download_path = download_dir + data_url.split('/')[-1]


# In[ ]:


while not os.path.exists(download_path):
    time.sleep(1)
    
print('Download finished.')


# In[305]:


driver.close()


# In[ ]:




