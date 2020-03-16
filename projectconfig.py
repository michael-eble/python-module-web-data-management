__name__ = 'projectconfig'

config_data = {
    'project_name': 'yourprojectname',
    'base_dir': '',
    'db_dir' : 'yourdatabasedir/',
    'htmldata_dir' : 'yourdirforhtmldata/',
    'ptext_dir' : 'yourdirforplaintext/',
    'stext_dir' : 'yourdirforstructuredtext/',
    'csv_delimiter' : '#;'
}

def getConfig(key):
    return config_data.get(key)