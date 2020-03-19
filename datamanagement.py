# ---------------------------------------------------------------------------------------
# Module "datamanagement"
# Purpose: Retrieve data from web resource and store it locally
# ---------------------------------------------------------------------------------------
__name__ = 'datamanagement'

from importlib import reload

import projectconfig as pc     # Import the project's configuration
reload(pc)                     # Reload the project's configuration in order to apply changes

from datetime import datetime
import hashlib

import string
import secrets

# ---------------------------------------------------------------------------------------
# Class "DataSource"
# Purpose: Retrieve HTML data from URL and return data "as it is"
# ---------------------------------------------------------------------------------------
class DataSource:

    # -----------------------------------------------------------------------------------
    # Constructor
    # -----------------------------------------------------------------------------------
    def __init__(self, id, url):
        self.id = id
        self.url = url
        self.data = None
        self.data_package = []
    # -----------------------------------------------------------------------------------

    def getId(self):
        return self.id
    
    def getUrl(self):
        return self.url

    def getData(self):
        return self.data

    def setData(self, data):
        self.data = data

    def setDataPackage(self, id, data):
        self.data_package = [id, data]

    def getDataPackage(self):
        self.setDataPackage(self.getId(), self.retrieveHtmlDataFromSource())
        return self.data_package
    
   def retrieveHtmlDataFromSource(self):

        # Import modules needed for loading data from web resources
        try:
            import requests
        except Exception:
            print ("Error: Importing Python modules for web scraping failed")

        # Read HTML data from given URL using Request
        try:
            request = requests.get(self.getUrl())
        except Exception:
            print ("Error: Reading HTML data from given URL failed")

        # Encode HTML data retrieved from URL
        try:
            request.encoding = request.apparent_encoding
            html_data = request.text
        except Exception:
            print ("Error: Encoding the HTML data retrieved failed")

        # Return raw_data
        self.setData(html_data)
        return self.getData()
# ---------------------------------------------------------------------------------------

# ---------------------------------------------------------------------------------------
# Class "DataStorage"
# Purpose: Save data to local storage "as it is" or load data from storage
# ---------------------------------------------------------------------------------------
class DataStorage:

    # -----------------------------------------------------------------------------------
    # Constructor
    # -----------------------------------------------------------------------------------
    def __init__(self, id, data_package):
        try:
            self.id = id
            self.delimiter = pc.getConfig('csv_delimiter')
            if isinstance(data_package, DataSource):
                self.data_package = data_package
                self.source_id = self.data_package.getId()
                self.source_url = self.data_package.getUrl()
                self.data = self.data_package.retrieveHtmlDataFromSource()
            else:
                self.data_package = data_package
                self.source_id = data_package[0]
                self.source_url = data_package[1]
                self.data = data_package[2]
        except Exception:
            print ("Error: Creating an instance of DataStorage failed")
    # -----------------------------------------------------------------------------------

    def getId(self):
        if self.id:
            return self.id
        else:
            if self.getDataSourceUrl() and '/' in self.getDataSourceUrl():
                try:
                    # Structure of filenames used for extracting the ID:
                    # <base_dir>/<storage_directory>/<storage_id>_<source_id> ...
                    fn = str(self.getDataSourceUrl()).strip()
                    fn = fn.split('/')[-1] # last part of directory and filename
                    id = fn.split('_')[1] # second part of filename
                    self.setId(id)
                    return self.id
                except Exception:
                    print ("Error: Deriving storage_id from filename failed")   

    def setId(self, id):
        self.id = id

    def getDelimiter(self):
        return self.delimiter
    
    def setDelimiter(self, delimiter):
        self.delimiter = delimiter

    def getDataSourceUrl(self):
        return self.source_url
    
    def setDataSourceUrl(self, url):
        self.source_url = url

    def getDataSourceId(self):
        return self.source_id

    def getData(self):
        return self.data
    
    def setData(self, data):
        self.data = data

    def getDataPackage(self):
        return self.data_package

    def storeData(self):

        # Handle data, if a DataSource object is provided
        if isinstance(self.data_package, DataSource):

            # Build name of targeted output file
            # Format: <DataStorage.Id>_<URL-Hash>_<DateOfStoring>_<TimeOfStoring>.html
            try:
                inserted_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                filename = 'htmldata_' + self.getId() + '_' + self.getDataSourceId() + '_' + inserted_datetime
                filename_plain = filename
                filename = pc.getConfig('base_dir') + pc.getConfig('htmldata_dir') + filename
            except Exception:
                print ("Error: Building the output filename failed")

            # Create new file under filename and write data into file
            try:
                hData = open(filename, "w", encoding='utf-8')
                hData.write(str(self.getData()))
                hData.close()

                self.setDataSourceUrl(filename)

                collection = 'datastorage_collection_htmldata.csv'
                self.addStoredDataToCollection(filename_plain, collection)

                self.setData(self.retrieveStoredData())
                return filename
            except Exception:
                print ("Error: Writing data into the output file failed")
        
        # Handle data, if data_package (Array) is provided
        else:
            
            # Build name of targeted output file
            try:
                inserted_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                filename = self.getId() + '_' + inserted_datetime
            except Exception:
                print ("Error: Building the output filename failed")

            # Create new file under filename and write data into file
            try:

                if 'plaintext' in filename:                   # plaintext directory
                    collection = 'datastorage_collection_plaintext.csv'
                    filename_plain = filename
                    filename = pc.getConfig('base_dir') + pc.getConfig('ptext_dir') + filename
                    self.setDataSourceUrl(filename)
                
                data = self.getDataPackage()
                data_string = ''

                if isinstance(data, list):
                    for item in data:
                        if item:
                            counter = 0
                            if isinstance (item, list):
                                for variable in item:
                                    if counter > 0:
                                        data_string += self.getDelimiter()
                                    data_string += str(variable)
                                    counter += 1
                            else:
                                if counter > 0:
                                    data_string += self.getDelimiter()
                                data_string += str(item)
                                counter += 1
                            data_string += '\n'
                else:
                    data_string = data
                
                hData = open(filename, "w", encoding='utf-8')
                hData.write(data_string)
                hData.close()

                self.addStoredDataToCollection(filename_plain, collection)

                self.setData(self.retrieveStoredData())
                return filename
            except Exception:
                print ("Error: Writing data into the output file failed")

    def retrieveStoredData(self):
        if self.getDataSourceUrl():
            try:
                fn_data_stored = str(self.getDataSourceUrl()).strip()
                hData = open(fn_data_stored, 'r', encoding='utf-8')
                self.setData(hData.read())
                hData.close()
                return self.getData()
            except Exception:
                print ("Error: Reading stored file from given path failed")
        else:
            exit
    
    def addStoredDataToCollection(self, filename, collection):
        try:
            if not collection:
                collection = 'datastorage_collection_htmldata.csv'
            if pc.getConfig('db_dir') not in collection:
                collection = pc.getConfig('base_dir') + pc.getConfig('db_dir') + collection
            directory = open(collection, 'a', encoding='utf-8')
            new_entry = '\n' + filename
            directory.write(new_entry)
            directory.close()
        except Exception:
            print ("Error: Writing new entry to storage directory failed")
# ---------------------------------------------------------------------------------------

# ---------------------------------------------------------------------------------------
# Method "generateId"
# Purpose: Return a random string that can be used for creating IDs of files, for example
# ---------------------------------------------------------------------------------------
def generateId(size=10, chars=string.ascii_lowercase + string.digits):
    return ''.join(secrets.choice(chars) for _ in range(size))
# ---------------------------------------------------------------------------------------
