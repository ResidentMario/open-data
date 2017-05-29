from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException


driver = webdriver.PhantomJS()
driver.get('https://data.cityofnewyork.us/d/did2-qzw3')
WebDriverWait(driver, 10).until(
    EC.presence_of_element_located((By.CLASS_NAME, "dataset-contents"))
)
dataset_contents_list = driver.find_elements_by_class_name('dataset-contents')
assert len(dataset_contents_list) == 1  # check that the UI is what we expect it to be
metadata_pairs = dataset_contents_list[0].find_elements_by_class_name('metadata-pair')
assert len(metadata_pairs) >= 2

print("Success!")