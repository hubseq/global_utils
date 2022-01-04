#
# file_utils
#
# Utility functions for file I/O. Includes JSON standards for files.
#
# file hierarchy:
# /user_id/pipeline_id/run_id/module_id/file_id/<file_id>...<file_extension>
#
# each run creates a /user_id/pipeline_id/run_id/.run.json file that contains all data file information for that run.
# each pipeline will contain a .pipeline.json that contains information on all runs. /user_id/pipeline_id/.pipeline.json
#
# SEARCH FOR A SINGLE SAMPLE:
# data_file_search_json:
# ['file_location'] = <FOLDER>
# ['file_extensions'] = <LIST OF EXTENSIONS OR PREFIXES TO SEARCH> - extension has ^.bam or ^myfile_ or ^I1^. Found file must match all extensions.
# ['file_type'] = <STRING> - file type to search for. ONLY A SINGLE FILE TYPE

# PROCESSING OF A SINGLE SAMPLE:
# data_file_json:
    # ['user_id'] = STRING <USER_ID>
    # ['pipeline_id'] = STRING - which pipeline was run on this sample
    # ['run_id'] = STRING <pipeline RUN_ID / JOB_ID>
    # ['file_location'] = STRING <FOLDER>
    # ['file_id'] = <STRING - this is the name of the original input file minus extension, or the group_id for analyses.>
    # ['file_name'] = <STRING> - full path will be 'file_location' + 'file_name'
    # ['file_type'] = <STRING> - BAM, BED, LOG, etc...
    # ['module_id'] = <STRING> - name of module that was run on this file.
    # ['module_version_id'] = <STRING format: yyyymmdd> - version of module that was run on this file.
    # ['pipeline_version_id'] = STRING - version of the pipeline run on this sample.
    # ['json_version_id'] = <STRING FORMAT: yyyymmdd>

# file hierarchy for groups:
# /user_id/group_run_id/group_module_id/group_id/<group_id>...<file_extension>

# PROCESSING OF A GROUP OF SAMPLES:
# data_file_group_json: (useful for grouping files for meta-analysis)
# ['user_id']
# ['group_id'] = <often will be ANALYSIS_ID if this is a meta-analysis of many data files>
# ['group_run_id'] = <the run/job id of this analysis>
# ['group_data_files'] = <list of input data file JSONs>
# ['group_module_id'] = <STRING> - name of module that was run on this file. Note - if this is a custom notebook, then this is the meta_id of the notebook
# ['group_module_version_id'] = <STRING FORMAT: yyyymmdd> - version of module that was run on this file. Note - if this is a custom notebook, then this is the timestamp the notebook was last saved.
# ['json_version_id'] = <STRING FORMAT: yyyymmdd>

import os, sys, json, subprocess, boto3
import global_keys
import aws_s3_utils

PIPELINE_DNASEQ_TARGETED_JSON_VERSION = '20211219'
DATA_FILE_JSON_VERSION = '20211219'
GROUP_JSON_VERSION = '20211219'

VALID_FILETYPES = ['FASTQ', 'BAM', 'SAM', 'BED', 'TXT', 'CSV', 'JSON', 'GZ', 'FASTQ.GZ', 'WIG', 'HTML']
COMBO_FILETYPES = ['FASTQ.GZ']

def getFullPath(root_folder, files):
    """ Given a root_folder and a file STRING or LIST of files, return the full paths to these file(s).

    >>> getFullPath( 's3://mybam', 'hello.bam' )
    's3://mybam/hello.bam'
    
    """
    if type(files) == type([]):
        full_paths = []
        for f in files:
            if root_folder in f:
                # if root folder is already specified in file argument
                full_paths.append(f)
            else:
                # add root folder to file path
                full_paths.append(os.path.join(root_folder, f))
    elif type(files) == type(''):
        full_paths = ''
        if root_folder  in files:
            full_paths = files
        else:
            full_paths = os.path.join(root_folder, files)
    else:
        full_paths = files
    return full_paths


def downloadFiles( files, root_folder, file_system = 's3', mock = False):
    """
    >>> downloadFiles( ['my.bed'], 's3://bed', 's3', True )
    ['s3://bed/my.bed']
    """
    files_fullpath = getFullPath(root_folder, files)
    if mock == True:
        return files_fullpath
    elif file_system.lower() == 'local':
        return files_fullpath
    elif file_system.lower() == 's3':
        return aws_s3_utils.downloadFiles_S3(files_fullpath, files)
    else:
        return files_fullpath


def downloadFolder( folder, root_folder, file_system = 's3', mock = False):
    """
    >>> downloadFolder( ['subbed'], 's3://bed', 's3', True )
    ['s3://bed/subbed']
    """    
    folder_fullpath = getFullPath(root_folder, folder)
    if mock == True:
        return folder_fullpath
    elif file_system.lower() == 'local':
        return folder_fullpath
    elif file_system.lower() == 's3':
        return aws_s3_utils.downloadFolder_S3(folder_fullpath, folder)
    else:
        return folder_fullpath


def getRunJSONs( userid, pipelineid, rids):
    """ Gets run JSON given list of run IDs.
    userid: STRING
    pipelineid: STRING
    rids: LIST of run IDs
    return: LIST path to .run.json files
    """
    run_jsons = []
    for rid in rids:
        run_jsons.append(getRunJSON(userid, pipelineid, rid))
    return run_jsons


def getRunJSON( userid, pipelineid, rid):
    """ Gets run JSON that contains all run information.
    userid: STRING
    pipelineid: STRING
    rid: run ID
    return: path to .run.json file
    """
    return os.path('/', userid, pipelineid, rid, '.run.json')


def getPipelineJSON( userid, pipelineid):
    """ Gets pipeline JSON (.pipeline.json) that contains information on all runs for this pipeline.

    userid: STRING
    pipelineid: STRING
    return: path to .pipeline.json file
    """
    return os.path('/', userid, pipeline, '.pipeline.json')


def getRunJSON_FileIds( run_json ):
    """ Gets all file IDs for a given run - in .run.json file
    For now: {'file_ids': [...]}
    """
    rj = json.load(run_json)
    return rj['file_ids']


def getRunJSON_sampleOutputFolders( run_json, moduleids = [], sampleids = []):
    """ For a given run, get all sample output folder paths.
    Can subset and only get folders for a subset of modules.
    Can also get a subset of sample output folders by passing a sampleids list.

    run_json: {'pipeline_run_metadata': {'fastqc': {'samples': {'sample_id': {'output_folder': '...', 'input_folder': '...'}}}
    """
    sample_output_folders = []
    rj = json.load(run_json)
    for _module in rj['pipeline_run_metadata']:
        if moduleids == [] or (moduleids!=[] and _module in moduleids):
            for _sample in rj['pipeline_run_metadata'][_module]['samples']:
                if sampleids == [] or (sampleids!=[] and _sample in sampleids):
                    sample_output_folders.append(rj['pipeline_run_metadata'][_module]['samples'][_sample]['output_folder'])
    return sample_output_folders


def getPipelineJSON_RunIds( pipeline_json ):
    """ Gets all run IDs for a given pipeline
    For now: {'run_ids': [...]}
    """
    pj = json.load(pipeline_json)
    return pj['run_ids']


def getRunFileIds( userid, pipelineid, runids):
    """ Get all existing file IDs for a given set of runs from a pipeline.

    userid: STRING
    pipelineid: STRING
    runids: LIST of run IDs
    return: LIST of file IDs

    FUTURE: check for existence of folders (in case user deletes).
    """
    fileids = []
    for rid in runids:
        run_json = getRunJSON( userid, pipelineid, rid)
        fileids = fileids + getRunJSON_FileIds( run_json )
    return fileids


def getRunSampleOutputFolders( userid, pipelineid, runids, moduleids = [], sampleids = []):
    """ Get all sample output folders for a given set of runs from a pipeline.
    Can subset and only get a subset of modules.
    Can also get a subset of sample output folders by passing a sampleids list.
    """
    output_folders = []
    for rid in runids:
        run_json = getRunJSON( userid, pipelineid, rid)
        output_folders = output_folders + getRunJSON_sampleOutputFolders( run_json, moduleids, sampleids)
    return output_folders


def getRunIds( userid, pipelineid):
    """ Get all existing run IDs for a given set of runs for a pipeline.

    userid: STRING
    pipelineid: STRING
    return: LIST of run IDs

    FUTURE: check for existence of runs (in case user deletes).
    """
    runids = []
    pipeline_json = getPipelineJSON( userid, pipelineid)
    return getPipelineJSON_RunIds( pipeline_json )


# file hierarchy:
# /user_id/run_id/file_id/module_id/<file_id>...<file_extension>
def getUserIdFromLocation(file_folder):
    return file_folder.split('/')[1]

def getPipelineIdFromLocation(file_folder):
    return file_folder.split('/')[2]

def getRunIdFromLocation(file_folder):
    return file_folder.split('/')[3]

def getModuleIdFromLocation(file_folder):
    return file_folder.split('/')[4]

def getFileIdFromLocation(file_folder):
    return file_folder.split('/')[5]


def listFiles( _dir, _file_system = 'local' ):
    """ Lists files in an input directory.

    _dir: directory / folder / location
    _file_system: file system - local, s3

    return: LIST of files
    """
    file_list = []

    if _file_system == 'local':
        file_list = os.listdir(_dir)

    return file_list


def isValidFileType( _ft ):
    """ Checks if input file type is an accepted file type.

    _ft: filetype STRING (e.g., 'TXT')
    return: BOOL
    """
    return (True if (_ft.upper() in VALID_FILETYPES) else False)


def inferFileType( _fn ):
    """ Infer the file type of input filename (file extension).

    _fn: filename STRING (e.g., 'myfile.txt') or LIST ['myfile.txt']

    return: STRING (e.g., TXT)
    """
    if type(_fn) == type(''):
        return _fn.split('.')[-1] if len(list(filter(lambda combo: _fn.upper() in combo, COMBO_FILETYPES))) == 0 else _fn.split('.')[-2]+'.'+_fn.split('.')[-1]
    elif type(_fn) == type([]):
        return _fn[0].split('.')[-1] if len(list(filter(lambda combo: _fn[0].upper() in combo, COMBO_FILETYPES))) == 0 else _fn[0].split('.')[-2]+'.'+_fn[0].split('.')[-1]
    else:
        return ''

def createSearchFileJSONs( _folder_list, _extensions, _filetype):
    """ Creates a search file query JSON given list of folders to search, file extensions to search for and file type to search for.
    Note that this is intended to search for a single file type (one per JSON).

    return: LIST of search JSONs
    """
    search_jsons = []
    for _folder in _folder_list:
        search_jsons.append(createSearchFileJSON( _folder, _extensions, _filetype))


def createSearchFileJSON( _folder, _extensions, _filetype):
    """ Creates a search file query JSON given a folder to search, file extensions to search for and file type to search for.
    Note that this is intended to search for a single file type (one per JSON).

    >>> createSearchFileJSON( '/1/2/3', '.txt', 'TXT')
    {'file_location': '/1/2/3/', 'file_extensions': ['.txt'], 'file_type': 'TXT'}
    """
    return {global_keys.KEY_FILE_LOCATION: _folder.rstrip('/')+'/',
            global_keys.KEY_FILE_EXTENSIONS: _extensions if type(_extensions)==type([]) else [_extensions],
            global_keys.KEY_FILE_TYPE: str(_filetype).upper()}


def createDataFileJSON( _filename, _filefolder, _filetype = '',  _file_id = '', _user_id = '', _pipeline_id = '', _run_id = '', _module_id = ''):
    """ Creates a data file JSON from input file information.

    _filename: filename STRING (e.g., 'myfile.fastq') (REQUIRED)
    _filefolder: file folder / location STRING (e.g., '/myfolder/bwamem/') (REQUIRED)
    _filetype: file type (e.g., BAM) (optional but prefered)
    _user_id: unique user id (e.g., jerrychen) (optional but prefered - can infer from filefolder if necessary)
    _run_id: run / job id with format <user_id>_<yyyymmddhhmmss> (e.g., jerrychen_20211219221055)
    _module_id: module name (e.g., bwa)
    _file_id: file ID - usually sample ID, which is derived from run input file name (e.g., encode-hepg2) (optional)

    return: JSON with the key-value pairs defined for data files
    """
    _module_id_local = _module_id if _module_id != '' else getModuleIdFromLocation(_filefolder)

    return {global_keys.KEY_USER_ID: _user_id if _user_id != '' else getUserIdFromLocation(_filefolder),
            global_keys.KEY_PIPELINE_ID: _pipeline_id if _pipeline_id != '' else getPipelineIdFromLocation(_filefolder),
            global_keys.KEY_RUN_ID: _run_id if _run_id != '' else getRunIdFromLocation(_filefolder),
            global_keys.KEY_FILE_ID: _file_id if _file_id != '' else getFileIdFromLocation(_filefolder),
            global_keys.KEY_FILE_NAME: _filename,
            global_keys.KEY_FILE_TYPE: _filetype if isValidFileType(_filetype) else inferFileType(_filename),
            global_keys.KEY_FILE_LOCATION: _filefolder,
            global_keys.KEY_MODULE_ID: _module_id_local,
            global_keys.KEY_JSON_VERSION_ID: DATA_FILE_JSON_VERSION}


def getDataSampleIds( _datafilejsons ):
    """ From an input list of data files (in JSON format), get all sample IDs.
    """
    sampleids = []
    for _dfjson in _datafilejsons:
        sampleids.append(getDataSampleId(_dfjson))
    return sampleids


def getDataSampleId( _datafilejson, fullpath = False ):
    """ From an input data file (in JSON format), get SampleId.
    """
    return _dfjson[global_keys.KEY_FILE_ID]


def getDataFileNames( _datafilejsons, fullpath = False ):
    """ From an input list of data files (in JSON format), get all file names.
    If fullpath is True, then return the full path to these files.
    """
    filenames = []
    for _dfjson in _datafilejsons:
        filenames.append(getDataFileName(_dfjson, fullpath))
    return filenames


def getDataFileName( _datafilejson, fullpath = False ):
    """ From an input data file (in JSON format), get file name.
    If fullpath is True, then return the full path to this file.
    """
    if fullpath==True:
        return os.path.join(_dfjson[global_keys.KEY_FILE_LOCATION], _dfjson[global_keys.KEY_FILE_NAME])
    else:
        return _dfjson[global_keys.KEY_FILE_NAME]


def searchDataFile( _folder, _extensions, _file_system = 'local' ):
    """ Given a folder to search, searches and returns data files matching the input extensions.

    _folder:     STRING e.g., '/myfolder/subfolder/'
    _extensions: LIST e.g., ['.bam^', '^hepg2', 'I1'] where
                 '^.bam' => file ends with BAM
                 'hepg2^' => file begins with hepg2
                 'I1' => file contains the word I1

    return: LIST of data file JSONs
    """
    # local function for searching for a pattern match
    def _findMatch(f, p):
        _isMatch = False
        # file extension at end of filename
        if p[0]=='^':
            if f.endswith(p[1:]):
                _isMatch = True
        # prefix - file extension at beginning of filename
        elif p[-1]=='^':
            if f.startswith(p[0:-1]):
                _isMatch = True
        # search pattern ONLY in the middle
        elif p.rfind('^') > p.find('^'):
            i = p.find('^')
            j = p.rfind('^')
            if f.find(p[i+1:j]) >= 0 and not f.startswith(p[i+1:j]) and not f.endswith(p[i+1:j]):
                _isMatch = True
        # search pattern anywhere in file name
        else:
            if f.find(p) >= 0:
                _isMatch = True
        return _isMatch

    # main search through all files in the directory
    all_files = listFiles( _folder, _file_system)
    matched_files = []
    for f in all_files:
        isMatch = False
        for p in _extensions:
            if _findMatch(f, p):
                matched_files.append(f)
    return matched_files


def getDataFiles( _sample_file_search_json_list, _file_system = 'local' ):
    """ Given a list of JSON file search queries, function searches and returns found data files (as JSON).
    See repo > global_utils > file_utils.py for JSON specifications for file search queries and data files.

    _sample_file_search_json_list: LIST(JSON) - list of search query JSONs

    return: LIST(JSON) - list of data file JSONs

    >>> getDataFiles([])
    []
    """
    _sample_file_json_list = []
    try:
        # if single JSON then convert to single-element list
        if type(_sample_file_search_json_list ) == type({'a': 2}):
            _sample_file_search_json_list = [_sample_file_search_json_list]
        elif type(_sample_file_search_json_list ) == type([1,2]):
            pass
        else:
            raise IOError

        for _sample_file_search_json in _sample_file_search_json_list:
            file_folder = _sample_file_search_json[global_keys.KEY_FILE_LOCATION]
            file_type = _sample_file_search_json[global_keys.KEY_FILE_TYPE]
            file_extensions = _sample_file_search_json[global_keys.KEY_FILE_EXTENSIONS]
            sample_files = searchDataFile( file_folder, file_extensions, _file_system )
            for sample_file in sample_files:
                sample_file_json = createDataFileJSON(sample_file,
                                              file_folder,
                                              file_type)
                _sample_file_json_list.append(sample_file_json)

    except IOError as e:
        print('ERROR in getDataFiles(): input needs to be a list of search JSONs.')
    return _sample_file_json_list


def getFileLocation( _user_id, _run_id, _file_id, _module_id, _subfolder = ''):
    """ Gets the file folder path given file metadata. Currently
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
    """ Alias for getFileLocation()
    """
    return getFileLocation( _user_id, _run_id, _file_id, _module_id, _subfolder)


def getFileOnly( file_fullpath ):
    if type(file_fullpath) == type([]):
        files_only = []
        for f in file_fullpath:
            files_only.append(f.split('/')[-1])
    elif type(file_fullpath) == type(''):
        files_only = file_fullpath.split('/')[-1]
    else:
        files_only = ''
    return files_only


def getFileFolder( file_fullpath ):
    if type(file_fullpath) == type([]):
#        folders_only = []
        # get directory of first file
        folders_only = file_fullpath[0][0:file_fullpath[0].rfind('/')]+'/'
#        for f in file_fullpath:
#            folders_only.append(f[0:f.rfind('/')]+'/')
    elif type(file_fullpath) == type(''):
        folders_only = file_fullpath[0:file_fullpath.rfind('/')]+'/'
    else:
        folders_only = ''
    return folders_only


def inferFileSystem( filepath ):
    if filepath.startswith('s3:/'):
        return 's3'
    else:
        return 'local'
