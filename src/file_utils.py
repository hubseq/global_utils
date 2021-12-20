#
# file_utils
#
# Utility functions for file I/O. Includes JSON standards for files.
#
# file hierarchy:
# /user_id/run_id/file_id/module_id/<file_id>...<file_extension>

# SEARCH FOR A SINGLE SAMPLE:
# data_file_search_json:
# ['file_location'] = <FOLDER>
# ['file_extensions'] = <LIST OF EXTENSIONS OR PREFIXES TO SEARCH> - extension has ^.bam or ^myfile_ or ^I1^. Found file must match all extensions.
# ['file_type'] = <STRING> - file type to search for. ONLY A SINGLE FILE TYPE

# PROCESSING OF A SINGLE SAMPLE:
# data_file_json:
    # ['user_id'] = STRING <USER_ID>
    # ['run_id'] = STRING <pipeline RUN_ID / JOB_ID>
    # ['file_location'] = STRING <FOLDER>
    # ['file_id'] = <STRING - this is the name of the original input file minus extension, or the group_id for analyses.>
    # ['file_name'] = <STRING> - full path will be 'file_location' + 'file_name'
    # ['file_type'] = <STRING> - BAM, BED, LOG, etc...
    # ['module_id'] = <STRING> - name of module that was run on this file.
    # ['module_version_id'] = <STRING format: yyyymmdd> - version of module that was run on this file.
    # ['json_version_id'] = <STRING FORMAT: yyyymmdd>

# file hierarchy for groups:
# /user_id/group_run_id/group_id/group_module_id/<group_id>...<file_extension>

# PROCESSING OF A GROUP OF SAMPLES:
# data_file_group_json: (useful for grouping files for meta-analysis)
# ['user_id']
# ['group_id'] = <often will be ANALYSIS_ID if this is a meta-analysis of many data files>
# ['group_run_id'] = <the run/job id of this analysis>
# ['group_data_files'] = <list of input data file JSONs>
# ['group_module_id'] = <STRING> - name of module that was run on this file. Note - if this is a custom notebook, then this is the meta_id of the notebook
# ['group_module_version_id'] = <STRING FORMAT: yyyymmdd> - version of module that was run on this file. Note - if this is a custom notebook, then this is the timestamp the notebook was last saved.
# ['json_version_id'] = <STRING FORMAT: yyyymmdd>

from module_utils import getModuleVersion
import global_keys

DATA_FILE_JSON_VERSION = '20211219'
GROUP_JSON_VERSION = '20211219'

VALID_FILETYPES = ['FASTQ', 'BAM', 'SAM', 'BED', 'TXT', 'CSV', 'JSON', 'GZ', 'FASTQ.GZ', 'WIG']
COMBO_FILETYPES = ['FASTQ.GZ']

# file hierarchy:
# /user_id/run_id/file_id/module_id/<file_id>...<file_extension>
def getUserIdFromLocation(file_folder):
    return file_folder.split('/')[1]

def getRunIdFromLocation(file_folder):
    return file_folder.split('/')[2]

def getFileIdFromLocation(file_folder):
    return file_folder.split('/')[3]

def getModuleIdFromLocation(file_folder):
    return file_folder.split('/')[4]


def listFiles( _dir, _file_system = 'local' ):
    """
    Lists files in an input directory.

    _dir: directory / folder / location
    _file_system: file system - local, s3

    return: LIST of files
    """
    file_list = []

    if _file_system == 'local':
        file_list = os.listdir(_dir)

    return file_list


def isValidFileType( _ft ):
    """
    Checks if input file type is an accepted file type.

    _ft: filetype STRING (e.g., 'TXT')
    return: BOOL
    """
    return (True if (_ft.upper() in VALID_FILETYPES) else False)


def inferFileType( _fn ):
    """
    Infer the file type of input filename (file extension).

    _fn: filename STRING (e.g., 'myfile.txt')

    return: STRING
    """
    return _fn.split('.')[-1] if len(filter(lambda combo: _fn.upper() in combo, COMBO_FILETYPES)) == 0 else _fn.split('.')[-2]+'.'+_fn.split('.')[-1]


def createDataFileJSON( _filename, _filefolder, _user_id, _run_id, _module_id, _filetype = '', _file_id = ''):
    """
    Creates a data file JSON from input file information.

    _filename: filename STRING (e.g., 'myfile.fastq')
    _filefolder: file folder / location STRING (e.g., '/myfolder/bwamem/')
    _user_id: unique user id (e.g., jerrychen)
    _run_id: run / job id with format <user_id>_<yyyymmddhhmmss> (e.g., jerrychen_20211219221055)
    _module_id: module name (e.g., bwamem)
    _filetype: file type (e.g., BAM) (optional)
    _file_id: file ID - usually sample ID, which is derived from run input file name (e.g., encode-hepg2) (optional)

    return: JSON with the key-value pairs defined for data files
    """
    _file_id_local = getFileId(_filename) if _file_id=='' else _file_id
    return {global_keys.KEY_USER_ID: _user_id,
            global_keys.KEY_RUN_ID: _run_id,
            global_keys.KEY_FILE_ID: _file_id_local,
            global_keys.KEY_FILE_NAME: _filename,
            global_keys.KEY_FILE_LOCATION: _filefolder if _filefolder!='' else getFileLocation(_user_id, _run_id, _file_id_local, _module_id),
            global_keys.KEY_FILE_TYPE: _filetype if isValidFileType(_filetype) else inferFileType(_filename),
            global_keys.KEY_MODULE_ID: _module_id,
            global_keys.KEY_MODULE_VERSION_ID: getModuleVersion(_module_id),
            global_keys.KEY_JSON_VERSION_ID: DATA_FILE_JSON_VERSION}


def getFileLocation( _user_id, _run_id, _file_id, _module_id, _subfolder = ''):
    """
    Gets the file folder path given file metadata. Currently
    /user_id/run_id/file_id/module_id/<_subfolder>

    _user_id: STRING
    _run_id: STRING
    _file_id: STRING
    _module_id: STRING
    _subfolder: STRING

    return: STRING
    """
    return os.path.join( '/', _user_id, _run_id, _file_id, _module_id, _subfolder)


def getFilePath( _user_id, _run_id, _file_id, _module_id, _subfolder = ''):
    """
    Alias for getFileLocation()
    """
    return getFileLocation( _user_id, _run_id, _file_id, _module_id, _subfolder)
