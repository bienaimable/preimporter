#import common.logger as logger
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
import logging
import logging.handlers
import sys

class CustomLogging():
    def __init__(self, toaddrs, folder):
        FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        formatter = logging.Formatter(FORMAT)
        self.logger = logging.getLogger(folder)
        self.logger.setLevel(logging.DEBUG)
        self.logger.propagate = 0
        
        # Log debug in file
        if not os.path.exists(folder): os.makedirs(folder)
        file_handler = logging.handlers.RotatingFileHandler(os.path.join(folder, 'logging.out'), maxBytes=50000, backupCount=5)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        
        # Log exception to email
        mailhost = 'smtp.criteois.lan'
        fromaddr = 'f.pillot@criteo.com'
        subject = "Uncaught exception"
        mail_handler = logging.handlers.SMTPHandler(mailhost, fromaddr, toaddrs, subject, credentials=None)
        mail_handler.setLevel(logging.ERROR)
        mail_handler.setFormatter(formatter)
        self.logger.addHandler(mail_handler)
        
        # Log info to stdout
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(logging.INFO)
        self.logger.addHandler(stdout_handler)

    def clear(self):
        self.logger.handlers = []


class Tools():
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


class FeedManipulator():
    def __init__(self, working_dir, 
            tmp_dl_filename='downloaded.txt', 
            tmp_src_filename="unpacked.txt", 
            tmp_dest_filename="edited.txt", 
            tmp_directory = "tmp/"): 
        partner_log.logger.info("Creating new Import instance for {directory}".format(directory=working_dir))
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
        partner_log.logger.info("Downloading from {url}".format(url=source['path']))
        Tools.download(
                source['path'],
                source['user'],
                source['passwd'],
                self.tmp_directory + self.tmp_dl_filename
                )

    
    def unpack(self):
        partner_log.logger.info("Unpacking")
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

    
    def manipulate(self, node_name, root_node="processed_by_preimporter"):
        """ Function to write a valid XML by adding static items and applying rules on original XML """
        partner_log.logger.info("Processing rules and write to file")
        
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
        partner_log.logger.info("Uploading to {server}".format(server=destination['server']))
        # Upload new file
        
        # Find the file to upload
        for filename in [self.tmp_dest_filename, self.tmp_src_filename, self.tmp_dl_filename]:
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
        partner_log.logger.info("Upload finished".format(server=destination['server']))


    def import_rules(self, file_path):
        module_name = re.sub(r'\.py$', '', file_path).replace('/','.')
        module = importlib.import_module(module_name)
        self.modules.appendleft(module)



class Scanner():
    def __init__(self, folder, config_file, rules_file, force_file, log_folder):
        self.folder = folder
        self.config_filename = config_file
        self.rules_filename = rules_file
        self.force_filename = force_file
        self.log_folder = log_folder
        self.current_hour = datetime.datetime.now().hour


    def run_import(self, config, subfolder_path):
        feed = FeedManipulator(subfolder_path)
        rules_path = os.path.join(subfolder_path, self.rules_filename)
        if 'source' in config:
            feed.download(config['source'])
            feed.unpack()
        if os.path.isfile(rules_path):
            feed.import_rules(rules_path)
        if 'rules' in config:
            feed.rules.extend(config['rules'])
        if 'static_items' in config:
            feed.static_items.extend(config['static_items'])
        if 'node_name' in config:
            feed.manipulate(config['node_name'])
        if 'destinations' in config:
            for destination in config['destinations']:
                feed.upload(destination)


    def parse_folder(self, subfolder_path):
        conf_path = os.path.join(subfolder_path, self.config_filename)
        force_path = os.path.join(subfolder_path, self.force_filename)
        log_path = os.path.join(subfolder_path, self.log_folder)
        if os.path.isfile(conf_path):
            with open(conf_path, 'r') as f: config = yaml.load(f)
            global partner_log 
            partner_log = CustomLogging(config['owners'], log_path)
            try:
                # Launch import if there is a 'force' file in the partner folder
                if '--force' in sys.argv:
                    if os.path.isfile(force_path):
                        os.remove(force_path)
                        self.run_import(config, subfolder_path)

                # Launch import if it is the right time
                elif 'import_hours' in config:
                    for hour in config['import_hours']:
                        if hour == self.current_hour or hour == 'every':
                            self.run_import(config, subfolder_path)
                            break

            except Exception:
                partner_log.logger.exception("Uncaught exception:")
            partner_log.clear()
    

    def manipulate_feeds(self):
        subfolders = [x for x in os.listdir(self.folder) if not os.path.isfile(x)]
        for subfolder in subfolders:
            subfolder_path = os.path.join(self.folder, subfolder)
            try:
                self.parse_folder(subfolder_path)
            except Exception:
                logging.exception("Uncaught exception:")


if __name__ == '__main__':
    root_log = CustomLogging("f.pillot@criteo.com", 'log/')
    try:
        scanner = Scanner(
                folder="partners/",
                config_file="init.yml",
                rules_file="rules.py",
                force_file="force.txt",
                log_folder="log/"
                )
        scanner.manipulate_feeds()
    except Exception:
                root_log.logger.exception("Uncaught exception:")
