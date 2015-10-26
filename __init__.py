import common.logger as logger
import common.rules as common_rules
import yaml
import os
import datetime
import importlib
import xml.etree.ElementTree as Tree
import ftplib
import gzip
import shutil
import zipfile
import urllib.request
import urllib.error
import urllib.parse
import collections
import re


def download(source_url, username, password, destination_filepath):
    """ Download a file from any URL"""

    # Create destination directory
    os.makedirs(destination_filepath.rsplit('/', 1)[0], exist_ok=True)

    try:

        # Try to download with urllib
        password_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
        top_level_url = "http://example.com/foo/"
        password_mgr.add_password(None,
                          source_url,
                          username,
                          password)
        auth_handler = urllib.request.HTTPBasicAuthHandler(password_mgr)
        opener = urllib.request.build_opener(auth_handler)
        urllib.request.install_opener(opener)
        urllib.request.urlretrieve(source_url, destination_filepath)

    except urllib.error.URLError:

        # Parse the source URL to extract elements 
        parsed_url = urllib.parse.urlparse(source_url)
        server = parsed_url.netloc
        path = parsed_url.path.rsplit('/', 1)[0]
        filename = parsed_url.path.split('/')[-1]

        # Try again with ftplib
        with ftplib.FTP(server) as ftp:
            ftp.login(user=username, passwd=password)
            ftp.cwd(path)
            ftp.retrbinary(
                    'RETR '+ filename,
                    open(destination_filepath, 'wb').write
            )


class Import():
    def __init__(self, working_dir, tmp_dl_filename='downloaded.txt', tmp_src_filename="unpacked.txt", tmp_dest_filename="edited.txt", tmp_directory = "tmp/"): 
        log.info("Creating new Import instance")
        self.tmp_directory = os.path.join(working_dir, tmp_directory)
        self.tmp_src_filename = tmp_src_filename
        self.tmp_dest_filename = tmp_dest_filename
        self.tmp_dl_filename = tmp_dl_filename
        self.rules = []
        self.modules = collections.deque()
        self.modules.append(common_rules)
        self.static_items = []

    
    
    def find_rule(self, name, modules):
        for module in modules:
            if hasattr(module, name):
                function = getattr(module, name)
                return function
        raise ImportError("The function {name} can't be found".format(name=name))


    def download(self, source):
        log.info("Downloading from {url}".format(url=source['path']))
        download(
                source['path'],
                source['user'],
                source['passwd'],
                self.tmp_directory + self.tmp_dl_filename
                )

    
    def unpack(self):
        log.info("Unpacking")
        # Try to gunzip source file
        gz_failed = False
        try:
            with gzip.open( self.tmp_directory + self.tmp_dl_filename, 'rb') as f_in:
                with open(self.tmp_directory + self.tmp_src_filename, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
        except IOError:
            gz_failed = True
        # Try to unzip source file
        zip_failed = False
        try:
            with zipfile.ZipFile(self.tmp_directory + self.tmp_dl_filename) as zip_files:
                with zip_files.open(zip_files.namelist()[0]) as f_in:
                    with open(self.tmp_directory + self.tmp_src_filename, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
        except zipfile.BadZipFile:
            zip_failed = True
        # Or just rename file
        if ( gz_failed and zip_failed ):
            os.rename(
                    self.tmp_directory + self.tmp_dl_filename,
                    self.tmp_directory + self.tmp_src_filename
            )

    
    def process_rules(self, node_name, root_node="processed_by_preimporter"):
        """ Function to write a valid XML by adding static items and applying rules on original XML """
        log.info("Processing rules and write to file")
        
        with open( self.tmp_directory + self.tmp_dest_filename, "wb" ) as destination_file:

            # First write the opening root node
            destination_file.write(b"<" + root_node.encode("utf-8") + b">\n")

            # Then write the static items if any
            for item in self.static_items:
                element = Tree.Element('product')
                for key in item:
                    value = item[key]
                    if key.startswith('_'): 
                        element.set(key[1:], value)
                    else:
                        subelement = Tree.Element(key)
                        subelement.text = value
                        element.append(subelement)

                element_string = Tree.tostring(element, encoding='utf-8', method='xml')
                destination_file.write(element_string+b'\n')
                element.clear()

            # Now iterate throught the original file to apply the rules
            iterator = Tree.iterparse(self.tmp_directory + self.tmp_src_filename)
            for _, element in iterator:
                if element.tag == node_name:
                    for rule in self.rules:
                        func = self.find_rule(rule['function'], self.modules)
                        if 'args' in rule:
                            element = func(element, **rule['args'])
                        else:
                            element = func(element)

                    element_string = Tree.tostring(element, encoding='utf-8', method='xml')
                    destination_file.write(element_string+b'\n')
                    element.clear()

            # Finally write closing root node
            destination_file.write(b"</" + root_node.encode("utf-8") + b">\n")


    def upload(self, destination):
        log.info("Uploading to {server}".format(server=destination['server']))
        # Upload new file
        
        # Find the file to upload
        for filename in [tmp_dest_filename, tmp_src_filename, tmp_dl_filename]:
            filepath = os.path.join(self.tmp_directory, filename)
            if os.path.isfile(filepath):
                break
        with open(filepath, "rb") as destination_file:
            with ftplib.FTP(destination['server']) as ftp:
                ftp.login(user=destination['user'], passwd=destination['passwd'])
                try:
                    ftp.mkd(destination['folder'])
                except:
                    pass
                ftp.cwd(destination['folder'])
                ftp.storbinary('STOR '+ destination['filename'], destination_file)
        log.info("Uploading to {server} finished".format(server=destination['server']))


    def import_rules(self, file_path):
        module_name = re.sub(r'\.py$', '', file_path).replace('/','.')
        module = importlib.import_module(module_name)
        self.modules.appendleft(module)



class ConfigLoader():
    def __init__(self, partners_folder, config_filename="init.yml", rules_filename="rules.py"):
        self.partners_folder = partners_folder
        self.config_filename = config_filename
        self.rules_filename = rules_filename
        self.current_hour = datetime.datetime.now().hour


    def run_imports(self):
    
        folders = [x for x in os.listdir(self.partners_folder) if not os.path.isfile(x)]
    
        for folder in folders:
            # Set constants
            partner_path = os.path.join(self.partners_folder, folder)
            conf_path = os.path.join(partner_path, self.config_filename)
    
            #  Import config file
            if os.path.isfile(conf_path):
                with open(conf_path, 'r') as f:
                    config = yaml.load(f)
                self.run_import(config, partner_path)


    def run_import(self, config, partner_path):
        # Configure logger
        global log 
        log = logger.setup(config['owners'], partner_path)
        
        rules_path = os.path.join(partner_path, self.rules_filename)
    
        try:
            # Launch import if it is the right time
            if 'import_hours' in config:
                for hour in config['import_hours']:
                    if hour == self.current_hour or hour == 'now':
                        imp = Import(partner_path)
                        if 'source' in config:
                            imp.download(config['source'])
                            imp.unpack()
                        if os.path.isfile(rules_path):
                            imp.import_rules(rules_path)
                        if 'rules' in config:
                            imp.rules.extend(config['rules'])
                        if 'static_items' in config:
                            imp.static_items.extend(config['static_items'])
                        if 'node_name' in config:
                            imp.process_rules(config['node_name'])
                        if 'destinations' in config:
                            for destination in config['destinations']:
                                imp.upload(destination)
                        break

        except Exception:
            log.exception("Uncaught exception:")
    


if __name__ == '__main__':
    partners_folder = 'partners'
    loader = ConfigLoader(partners_folder)
    loader.run_imports()
