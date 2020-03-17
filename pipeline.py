__name__ = 'pipeline'

from importlib import reload

import projectconfig as pc
reload(pc)

import datamanagement as dm
reload(dm)

import string
import secrets

uc = []
uc.append("https://www.yoururlgoeshere.tld")

# Parameters to be set manually for controlling the pipeline
# 0 = don't execute the respective task, 1 = execute the task
add_new_urls = 0
read_htmldata_from_urls = 0
read_htmldata_from_storage = 0

# Filenames and directories to be used (see projectconfig.py)
fn_dsc = pc.getConfig('base_dir') + pc.getConfig('db_dir') + 'datasource_collection_urls.csv'
fn_stored_htmldata = pc.getConfig('base_dir') + pc.getConfig('db_dir') + 'datastorage_collection_htmldata.csv'

# Add new URLs to database/ collection
if add_new_urls:
    uc = list(set(uc))
    ul = open(fn_dsc, 'a', encoding='utf-8')
    for url in uc:
        new_entry = dm.generateId() + '#;' + url + '\n'
        ul.write(new_entry)
    ul.close()

# Read html data from URLs and store it locally
if read_htmldata_from_urls:

    # Read URLs to be processed from database/ collection
    uf = open(fn_dsc, 'r', encoding='utf-8')
    ud = uf.readlines()
    uf.close()
    
    ud = list(set(ud))
    url_coll = []

    for line in ud:
        url_coll.append([str(line.split('#;')[0]),
                        str(line.split('#;')[1])])

    # Read data from each URL and store raw data locally
    for entry in url_coll:
        id = entry[0]
        url = entry[1]

        raw_data = dm.DataSource(id, url)

        stored_data_id = dm.generateId()
        dm.DataStorage(stored_data_id, raw_data).storeData()

# Read html data from local storage and extract plain text
if read_htmldata_from_storage:

    uf = open(fn_stored_htmldata, 'r', encoding='utf-8')
    ud = uf.readlines()
    uf.close()

    ud = list(set(ud))

    for filename in ud:

        if filename:
            filename = pc.getConfig('base_dir') + pc.getConfig('htmldata_dir') + filename.strip()
            dso = dm.DataStorage('', ['', filename, ''])
            raw_data = dso.retrieveStoredData()

            if raw_data:
                #do something with the data