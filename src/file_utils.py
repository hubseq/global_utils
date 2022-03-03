#
# file_utils
#
# Utility functions for file I/O. Includes JSON standards for files.
#
# file hierarchy:
# /team_id/user_id/pipeline_id/run_id/sample_id/module_id/<sample_id>...<file_extension>
#
# team_id:     unique ID for team/lab/group/department - e.g., "ngspipelines-mylab". Alphanumeric and '-'
# user_id:     unique ID for user - e.g., "jchen". Alphanumeric and '_' and '-'
# pipeline_id: ID for this pipeline - e.g., "dnaseq_targeted"
# run_id:      unique run ID - e.g., "run1_xxxxx" where xxxx are first 5 alphanumeric of the run ID.
# sample_id:   sample ID that labels a single dataset or an analysis of multiple datasets, provided by user upon run/job submission.
# module_id:   module name - e.g., bwamem
#
# For pipeline runs, a file can be used to submit jobs with the following structure:
# 1) bwamem --sample_id <> --input ...
# 2)
# 3/1,2)  - job 3 depends on 1 and 2...
#
# Each run creates a <FULL_RUN_ID>.run.log file that contains a JSON with information on all the individual jobs, as follows:
# {"run": {"run_id": <FULL_RUN_ID>, "pipeline_id": <FULL_PIPELINE_ID>, "pipeline_version": <VERSION>,
#          "jobs": [{"id": 1, "job_id": <FULL_JOB_ID>, "module_name": "bwamem", "sample_id": <SID>, "cmd": "bwamem --sample_id...", "dependent_ids": []},
#                   {"id": 2, "job_id": <FULL_JOB_ID>, "module_name": "mpileup", "sample_id": <SID>, "cmd": "mpileup --sample_id...", "dependent_ids": [1]},
#                    ...]
# }}
# This .run.log file is located in /team_id/user_id/pipeline_id/runlogs/
#
# Individual job logs will be output to /team_id/user_id/pipeline_id/run_id/sample_id/module_id/<JOB_ID>.job.log
# These job logs will be parsed to extract and output sample file information and job metadata.
#
# SEARCH FOR A SINGLE SAMPLE:
# data_file_search_json:
# ['file_location'] = <FOLDER>
# ['file_extensions'] = <LIST OF EXTENSIONS OR PREFIXES TO SEARCH> - extension has ^.bam or ^myfile_ or ^I1^. Found file must match all extensions.
# ['file_type'] = <STRING> - file type to search for. ONLY A SINGLE FILE TYPE
#
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
GROUP_JSON_VERSION = '20211219'

VALID_FILETYPES = ['FASTQ', 'BAM', 'SAM', 'BED', 'TXT', 'CSV', 'JSON', 'GZ', 'FASTQ.GZ', 'WIG', 'HTML']
COMBO_FILETYPES = ['FASTQ.GZ']

def getFromDictList( L, K, returnEmpty = None ):
    """ Get values of a particular key from a list of dicts, returned as a list.
    L: list of dicts
    K: key to search in each dict - corresponding value is returned.
    returnEmpty: default value to return if key is not found

    >>> getFromDictList( [{'a': 1, 'b': 2}, {'a' : 3}, {'b' : 4, 'c' : 5}], 'a')
    [1, 3]
    >>> getFromDictList( [{'a': 1, 'b': 2}, {'a' : 3}, {'b' : 4, 'c' : 5}], 'a', '')
    [1, 3, '']
    """
    values = []
    for sub in L:
        if K in sub:
            values.append(sub[K])
        elif returnEmpty != None:
            values.append(returnEmpty)
    return values


def writeJSON( myjson, fout_name ):
    """ Writes (dumps) a JSON as a string to a file.
    """
    with open(fout_name,'w') as fout:
        json.dump(myjson, fout)
    return

def getJSON( fname ):
    return loadJSON(fname)

def loadJSON( fname ):
    """ Loads JSON from file named 'fname' into a JSON object and return this object.

    >>> loadJSON( "foo.json" )
    JSON ERROR - JSON NOT FORMATTED CORRECTLY OR FILE NOT FOUND: [Errno 2] No such file or directory: 'foo.json'
    {}
    >>> loadJSON( '../test/test.json' )
    {'module': 'bwamem'}
    """
    try:
        if type(fname) == type([]):
            fname = fname[0] if fname != [] else ''

        with open(fname,'r') as f:
            myjson = json.load(f)
    except Exception as e:
        print('JSON ERROR - JSON NOT FORMATTED CORRECTLY OR FILE NOT FOUND: '+str(e))
        return {}
    return myjson


def getFullPath(root_folder, files, convert2string = False):
    """ Given a root_folder and a file STRING or LIST of files, return the full paths to these file(s).

    >>> getFullPath( 's3://mybam', 'hello.bam' )
    's3://mybam/hello.bam'
    >>> getFullPath( 's3://mybam', ['hello.bam', 'hello2.bam'] )
    ['s3://mybam/hello.bam', 's3://mybam/hello2.bam']
    >>> getFullPath( 's3://mybam/', [''] )
    ['s3://mybam/']
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

    if convert2string == False or type(full_paths) == type(''):
        return full_paths
    elif type(full_paths) == type([]) and full_paths != []:
        return full_paths[0]
    else:
        return full_paths


def copyLocalFiles( local_files, dest_folder, linkonly = False ):
    """ Copies local file(s) to a destination folder.
    If linkonly is True, only set up a symbolic link.
    """
    if type(local_files) == type(''):
        if linkonly == False:
            subprocess.check_call(['cp', local_files, dest_folder])
        else: # linkonly = True
            subprocess.check_call(['ln','-s',local_files, os.path.join(dest_folder, getFileOnly(local_files))])
        return getFullPath(dest_folder, getFileOnly(local_files))
    elif type(local_files) == type([]) and local_files != []:
        for local_file in local_files:
            if linkonly == False:
                subprocess.check_call(['cp', local_file, dest_folder])
            else: # linkonly = True
                subprocess.check_call(['ln','-s',local_file, os.path.join(dest_folder, getFileOnly(local_file))])
        return getFullPath(dest_folder, getFileOnly(local_files))
    else:
        return dest_folder


def copyLocalFolder( local_folder, dest_folder ):
    """ Copies contents of local folder to a destination folder
    """
    if os.listdir(local_folder) != []:
        subprocess.check_call(' '.join(['cp', '-R', local_folder.rstrip('/')+'/*', dest_folder]), shell=True)
    else:
        print('copyLocalFolder(): local_folder {} is empty - nothing copied.'.format(str(local_folder)))
    return dest_folder


def downloadFile( files, dest_folder, file_system = 'local', mock = False, linkonly = False):
    return downloadFiles( files, dest_folder, file_system = 'local', mock = False)


def downloadFiles( files, dest_folder, file_system = 'local', mock = False, linkonly = False):
    """
    mock: do a mock run - don't download anything
    linkonly: if possible, set up a symbolic link only

    >>> downloadFiles( '/bed1/my1.bed', '/data/bed/', 'local', True )
    Downloading file(s) /bed1/my1.bed to /data/bed/.
    '/data/bed/my1.bed'

    >>> downloadFiles( ['/bed1/my.bed'], '/data/bed/', 'local', True )
    Downloading file(s) ['/bed1/my.bed'] to /data/bed/.
    ['/data/bed/my.bed']

    >>> downloadFiles( ['/bedin/my1.bed', '/bedin/my2.bed'], '/data/bed/', 'local', True )
    Downloading file(s) ['/bedin/my1.bed', '/bedin/my2.bed'] to /data/bed/.
    ['/data/bed/my1.bed', '/data/bed/my2.bed']

    >>> downloadFiles( 's3://npipublicinternal/test/fastq/dnaseq_test_R1.fastq.gz', '/Users/jerry/icloud/Documents/ngspipelines/global_utils/test/', 's3' )
    Downloading file(s) s3://npipublicinternal/test/fastq/dnaseq_test_R1.fastq.gz to /Users/jerry/icloud/Documents/ngspipelines/global_utils/test/.
    Downloading from S3 - s3://npipublicinternal/test/fastq/dnaseq_test_R1.fastq.gz to /Users/jerry/icloud/Documents/ngspipelines/global_utils/test/
    '/Users/jerry/icloud/Documents/ngspipelines/global_utils/test/dnaseq_test_R1.fastq.gz'

    >>> downloadFiles( ['s3://npipublicinternal/test/fastq/dnaseq_test_R1.fastq.gz', 's3://npipublicinternal/test/fastq/dnaseq_test_R2.fastq.gz'], '/Users/jerry/icloud/Documents/ngspipelines/global_utils/test/', 's3' )
    Downloading file(s) ['s3://npipublicinternal/test/fastq/dnaseq_test_R1.fastq.gz', 's3://npipublicinternal/test/fastq/dnaseq_test_R2.fastq.gz'] to /Users/jerry/icloud/Documents/ngspipelines/global_utils/test/.
    Downloading from S3 - s3://npipublicinternal/test/fastq/dnaseq_test_R1.fastq.gz to /Users/jerry/icloud/Documents/ngspipelines/global_utils/test/
    Downloading from S3 - s3://npipublicinternal/test/fastq/dnaseq_test_R2.fastq.gz to /Users/jerry/icloud/Documents/ngspipelines/global_utils/test/
    ['/Users/jerry/icloud/Documents/ngspipelines/global_utils/test/dnaseq_test_R1.fastq.gz', '/Users/jerry/icloud/Documents/ngspipelines/global_utils/test/dnaseq_test_R2.fastq.gz']

    """
    print('Downloading file(s) {} to {}.'.format(str(files), str(dest_folder)))
    dest_fullpath = getFullPath(dest_folder, getFileOnly(files))
    if mock == True:
        return dest_fullpath
    elif file_system.lower() == 's3' or 's3:/' in str(files):
        return aws_s3_utils.downloadFiles_S3(files, dest_folder)
    elif file_system.lower() == 'local':
        return copyLocalFiles( files, dest_folder, linkonly )
    else:
        return dest_fullpath


def downloadFolder( folder_fullpath, dest_folder, file_system = 'local', mock = False):
    """
    >>> downloadFolder( ['s3://bed/subbed'], '/data/bed/', 's3', True )
    Downloading folder ['s3://bed/subbed'] to /data/bed/.
    '/data/bed/'

    >>> downloadFolder( 's3://bed1/subbed', '/data/bed/', 's3', True )
    Downloading folder s3://bed1/subbed to /data/bed/.
    '/data/bed/'

    >>> downloadFolder('s3://npipublicinternal/test/fastqtest/', '/Users/jerry/icloud/Documents/ngspipelines/global_utils/test/', 's3' )
    Downloading folder s3://npipublicinternal/test/fastqtest/ to /Users/jerry/icloud/Documents/ngspipelines/global_utils/test/.
    '/Users/jerry/icloud/Documents/ngspipelines/global_utils/test/'
    """
    print('Downloading folder {} to {}.'.format(str(folder_fullpath), str(dest_folder)))

    # if folder input is wrapped in a list
    if type(folder_fullpath) == type([]) and folder_fullpath != []:
        folder_fullpath = folder_fullpath[0]

    # if path to a file is supplied as folder_fullpath, then we want to download all files in the containing folder, and return downloaded file path - this is a special case for bwa mem where we want the FASTA but also want the supporting genome index files.
    dest_folder_extended = ''
    if '.' in folder_fullpath.split('/')[-1]:
        dest_folder_extended = dest_folder.rstrip('/')+'/'+folder_fullpath.split('/')[-1]
        folder_fullpath =  folder_fullpath[0:folder_fullpath.rfind('/')]+'/'

    if mock == True:
        return dest_folder
    elif file_system.lower() == 's3' or 's3:/' in str(folder_fullpath):
        aws_s3_utils.downloadFolder_S3(folder_fullpath, dest_folder)
        return dest_folder_extended if dest_folder_extended != '' else dest_folder
    elif file_system.lower() == 'local':
        return copyLocalFolder( folder_fullpath, dest_folder )
    else:
        return dest_folder


def uploadFolder( local_folder, remote_folder, file_system = 'local', mock = False):
    """
    >>> uploadFolder( '/data/bed', 's3://bed1/', 's3', True )
    Uploading folder /data/bed to s3://bed1/.
    's3://bed1/'

    >>> uploadFolder('/Users/jerry/icloud/Documents/ngspipelines/global_utils/test/', 's3://npipublicinternal/test/fastqout/', 's3')
    Uploading folder /Users/jerry/icloud/Documents/ngspipelines/global_utils/test/ to s3://npipublicinternal/test/fastqout/.
    's3://npipublicinternal/test/fastqout/'
    """
    print('Uploading folder {} to {}.'.format(str(local_folder), str(remote_folder)))
    if mock == True:
        return remote_folder
    elif file_system.lower() == 's3' or ('s3:/' in str(remote_folder)):
        return aws_s3_utils.uploadFolder_S3( local_folder, remote_folder)
    elif file_system.lower() == 'local':
        return copyLocalFolder( local_folder, remote_folder )
    else:
        return remote_folder


def uploadFiles(localfile, remote_path, file_system = 'local', mock = False):
    return uploadFile(localfile, remote_path, file_system, mock)


def uploadFile(localfile, remote_path, file_system = 'local', mock = False):
    """ Securely uploads a local file to a path in S3.
        Full path of localfile should be specified in the input.
    """
    print('Uploading file {} to {}.'.format(str(localfile), str(remote_path)))
    if mock == True:
        return remote_path
    elif file_system.lower() == 's3' or ('s3:/' in str(remote_path)):
        return aws_s3_utils.uploadFiles_S3( localfile, remote_path)
    elif file_system.lower() == 'local':
        return copyLocalFiles( localfile, remote_path )
    else:
        return remote_path


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


def getRunFileIds( root_folder, teamid, userid, pipelineid, runids):
    """ Get all existing file IDs for a given set of runs from a pipeline.

    teamid: STRING
    userid: STRING
    pipelineid: STRING
    runids: LIST of run IDs
    return: LIST of file IDs, LIST of associated run IDs (ordered)

    FUTURE: check for existence of folders (in case user deletes).
    samples cannot be named "fastq" for now.
    """
    fileids = []
    runids_ordered = []
    for runid in runids:
        _run_fileids = getSubFolders( os.path.join(root_folder, teamid, userid, pipelineid, runid), [], ['fastq'] )
        for fid in _run_fileids:
            runids_ordered.append(runid)
        fileids += _run_fileids
    return (fileids, runids_ordered)


def getDataFiles( data_folders, extensions2include = [], extensions2exclude = [] ):
    """ Gets data files in the selected data folders that match extensions2include and DO NOT match extensions2exclude.

    data_folders: LIST of data folders to search. Can be local or on S3.
    extensions2include: LIST of extension patterns to search for. If empty, then get all files.
    extensions2exclude: LIST of extension patterns to exclude. If empty, then do not exclude any files.
    return: LIST of data files, LIST of sample IDs (file IDs) for those data files

    >>> getDataFiles([])
    []

    """
    print('IN GETDATAFILES(). DATA_FOLDERS: {}, EXTNSIONS2INCLUDE: {}, EXTENSIONS2EXCLUDE: {}'.format(str(data_folders), str(extensions2include), str(extensions2exclude)))
    data_files_json_list = []
    # sample_ids = []
    if type(data_folders) == str:
        data_folders = [data_folders]

    for data_folder in data_folders:
        data_files_new = getSubFiles( data_folder, extensions2include, extensions2exclude )
        # data_files = data_files + data_files_new
        for i in range(0,len(data_files_new)):
            data_files_json_list.append(createDataFileJSON( data_files_new[i] ))
            # sample_ids.append( getFileIdFromLocation(data_folder) )
    return data_files_json_list


def createDataFileJSON( _filename ):
    """ Creates a data file JSON from input file information.
    Data file must be in the defined hierarchy for NGS Pipelines:
    /team_id/user_id/pipeline_id/run_id/sample_id/data_file_name.ext

    Data file JSON stores important information for a data file and has the format:
    {"file_name": FILE_NAME,
     "file_type": FILE_TYPE,
     "team_id": TEAM_ID,
     "user_id": USER_ID,
     "pipeline_id": PIPELINE_ID,
     "module_id": MODULE_ID,
     "run_id": RUN_ID,
     "sample_id": SAMPLE_ID (FILE_ID),
     "file_json_version_id": VERSION_ID,
     }

    _filename: filename STRING - full path of file (e.g., 'myfile.fastq') (REQUIRED)

    return: JSON with the key-value pairs defined for data files
    """
    return {global_keys.KEY_FILE_NAME: _filename,
            global_keys.KEY_FILE_TYPE: inferFileType(_filename),
            global_keys.KEY_TEAM_ID: getTeamIdFromLocation(_filename),
            global_keys.KEY_USER_ID: getUserIdFromLocation(_filename),
            global_keys.KEY_PIPELINE_ID: getPipelineIdFromLocation(_filename),
            global_keys.KEY_RUN_ID: getRunIdFromLocation(_filename),
            global_keys.KEY_FILE_ID: getFileIdFromLocation(_filename),
            global_keys.KEY_MODULE_ID: getModuleIdFromLocation(_filename),
            global_keys.KEY_FILE_JSON_VERSION_ID: global_keys.DATA_FILE_JSON_VERSION}


def getSubFiles( root_folder, patterns2include = [], patterns2exclude = [] ):
    """ For a given root folder, get all files (NOT directories) in that folder. Do not include files in subfolders.

    root_folder: STRING folder to search. Can be local or on S3.
    patterns2include: LIST of file patterns to include. Include all if empty.
    patterns2exclude: LIST of file patterns to exclude. Exclude none if empty.
    return: FULL PATH of all found files

    patterns follow this notation: e.g., ['.bam^', '^hepg2', 'I1'] where
                 '^.bam' => file ends with BAM
                 'hepg2^' => file begins with hepg2
                 '^R1^' => file contains R1 in file extension (sep from base file name by one of [_,-,.]: e.g., myfile_R1.fastq.gz
                 'I1' => file contains the word I1 anywhere
    """
    def listSubFilesLocal( root_folder, patterns2include = [], patterns2exclude = [] ):
        try:
            rfiles = []
            subfiles = os.listdir(root_folder)
            for subfile in subfiles:
                if not os.path.isdir(subfile):
                    if (patterns2include != [] and aws_s3_utils._findMatches(subfile, patterns2include)) and \
                       (patterns2exclude != [] and not aws_s3_utils._findMatches(subfile, patterns2exclude)):
                        rfiles.append(subfile)
            return rfiles
        except FileNotFoundError:
            return []

    if type(patterns2include) == str:
        patterns2include = [patterns2include]
    if type(patterns2exclude) == str:
        patterns2exclude = [patterns2exclude]

    if root_folder.lstrip(' \t').startswith('s3://'):
        print('FILES FOUND ON S3: {}'.format(str(aws_s3_utils.listSubFiles( root_folder, patterns2include, patterns2exclude ))))
        return getFullPath( root_folder, aws_s3_utils.listSubFiles( root_folder, patterns2include, patterns2exclude ))
    elif root_folder.lstrip(' \t').startswith('/') or root_folder.lstrip(' \t').startswith('~/'):
        return getFullPath( listSubFilesLocal( root_folder, patterns2include, patterns2exclude ))
    else:
        return []


def getSubFolders( root_folder, sub_folders = [], folders2exclude = [] ):
    """ For a given root folder, get all listed subfolders, excluding any mentioned folders.
    This currently works for local or S3 paths.

    root_folder: STRING (PATH)
    sub_folders: LIST of subfolders to get (empty list = get all sub_folders)
    folders2exclude: LIST of subfolders to exclude

    >>> import os
    >>> cwd = os.getcwd()
    >>> getSubFolders( cwd, [], ['__pycache__'] )
    ['temp']
    """
    def listSubFoldersLocal( root_folder, folders2include = [], folders2exclude = [] ):
        try:
            rdirs = []
            subdirs = os.listdir(root_folder)
            for subdir in subdirs:
                if os.path.isdir(subdir) and ((subdir not in folders2exclude) and (folders2include == [] or subdir in folders2include)):
                    rdirs.append(subdir)
            return rdirs
        except FileNotFoundError:
            return []

    # in case inputs are strings - convert to single item lists
    if type(sub_folders) == str:
        sub_folders = [sub_folders]
    if type(folders2exclude) == str:
        folders2exclude = [folders2exclude]

    if root_folder.lstrip(' \t').startswith('s3://'):
        return aws_s3_utils.listSubFolders( root_folder, sub_folders, folders2exclude )
    elif root_folder.lstrip(' \t').startswith('/') or root_folder.lstrip(' \t').startswith('~/'):
        return listSubFoldersLocal( root_folder, sub_folders, folders2exclude )
    else:
        return []


def createSampleFilePath( root_folder, teamid, userid, pipelineid, runid, sampleid, moduleid ):
    """ Create a base file path for a given sample.
    Assumes hierarchy of sample folders within a pipeline run as:
     /teamid/userid/pipelineid/runid/moduleid/sampleid/<SAMPLE-DATA-FILES>
   
    root_folder: STRING - root folder for all team folders. Usually 's3://' (for S3) or '/' (for root local)
    """
    fpath = os.path.join( root_folder, teamid, userid, pipelineid, runid,  moduleid, sampleid )
    return fpath.rstrip('/')+'/'


def getRunSampleOutputFolders( root_folder, teamid, userids = [], pipelineids = [], runids = [], sampleids = [], moduleids = []):
    """ Get all sample output folders for a given set of users, pipelines, runs, modules, or samples.
    Note that this is flexible in getting ALL folders or a subset of folders within a team root directory.
    This function assumes the hierarchy for sample folders as:
    /teamid/userid/pipelineid/runid/moduleid/sampleid/<SAMPLE-DATA-FILES>

    root_folder: STRING - root folder for all team folders. Usually 's3://' (for S3) or '/' (for root local)

    >>> getRunSampleOutputFolders( 's3://', 'npipublicinternal', ['test'], ['dnaseq_targeted'], ['run_test1'], ['dnaseq_test'], ['bwamem', 'mpileup'])
    ['s3://npipublicinternal/test/dnaseq_targeted/run_test1/dnaseq_test/bwamem', 's3://npipublicinternal/test/dnaseq_targeted/run_test1/dnaseq_test/mpileup']

    """
    # There are many nested for-loops to allow flexibility, but number of folders should be small enough, should be ok.
    output_folders = []
    # if userids is empty list, then this gets all userids
    userids = getSubFolders( os.path.join(root_folder, teamid), userids)
    for userid in userids:
        # if pipelineids is empty list, then this gets all pipeline ids
        pipelineids = getSubFolders( os.path.join(root_folder, teamid, userid), pipelineids )
        for pipeid in pipelineids:
            # if runids is empty list, then this gets all run ids
            runids = getSubFolders( os.path.join(root_folder, teamid, userid, pipeid), runids )
            for rid in runids:
                # if sampleids is empty list, then this gets all sample ids
                sampleids = getSubFolders( os.path.join(root_folder, teamid, userid, pipeid, rid), sampleids )
                for sid in sampleids:
                    # if moduleids is empty list, then this gets all module ids
                    moduleids = getSubFolders( os.path.join(root_folder, teamid, userid, pipeid, rid, sid), moduleids )
                    for moduleid in moduleids:
                        output_folders.append( os.path.join(root_folder, teamid, userid, pipeid, rid, sid, moduleid))
    return output_folders


def getRunIds( root_folder, teamid, userid, pipelineid):
    """ Get all existing run IDs for a given set of runs for a pipeline.

    teamid: STRING
    userid: STRING
    pipelineid: STRING
    return: LIST of run IDs

    FUTURE: check for existence of runs (in case user deletes) and excluded folders.
    """
    runids = getSubFolders( os.path.join(root_folder, teamid, userid, pipelineid) )
    return runids

    # pipeline_json = getPipelineJSON( userid, pipelineid)
    # return getPipelineJSON_RunIds( pipeline_json )


def groupInputFilesBySample( input_files_list ):
    """ Groups all input files according to the full path and sample ID embedded in the names of the input files.
    Input can also be directories with the following syntax:
      /dir/*  gets all files in a dir
      /dir/^fastq  gets all files that end with FASTQ
      /dir/sample^  gets all files that start with sample
    """
    # getSubFiles( root_folder, patterns2include = [], patterns2exclude = [] ):
    groups = {}
    print('INPUT FILES LIST: '+str(input_files_list))
    for input_file in input_files_list:
        # if we are looking within a whole directory
        if '*' in input_file or '^' in input_file:
            # get files that match the pattern we are looking for
            if '*' in input_file:
                files = getSubFiles( input_file.rstrip('*') )
            elif '^' in input_file:
                files = getSubFiles( input_file[0:input_file.rfind('/')], [input_file[input_file.rfind('/')+1:]])
            print('SUBFILES: '+str(files))
            # group those files
            for f in files:
                sampleid = inferSampleID( getFileOnly(f) )
                groups[sampleid] = groups[sampleid] + [f] if sampleid in groups else [f]
        # otherwise we have a list of individual files
        else:
            sampleid = inferSampleID( getFileOnly(input_file) )
            groups[sampleid] = groups[sampleid] + [input_file] if sampleid in groups else [input_file]
    print('GROUPS: '+str(groups))
    return groups


def getSampleIDfromFASTQ( f ):
    text2search = ['_L001','_L002','_L003','_L004','_R1','_R2','_I1','_I2']
    for t in text2search:
        if f.rfind(t) > -1:
            return f[0:f.rfind(t)]

def inferSampleID( file_name ):
    """ Given a sample file name, infer the sample ID. This won't be perfect but should work 99% of time.
    """
    f = file_name.split('.')[0]
    if 'fastq' in file_name:
        sampleid = getSampleIDfromFASTQ( f )
    else:
        sampleid = f
    return sampleid

# file hierarchy:
# /team_id/user_id/run_id/file_id/module_id/<file_id>...<file_extension>
def getSubPath(file_folder, loc):
    # print('SUBPATH FOR: '+str(file_folder))
    if file_folder.startswith('s3://'):
        return file_folder[4:].split('/')[loc] if len(file_folder[4:].split('/')) > loc else ''
    elif file_folder.startswith('/') or file_folder.startswith('~/'):
        return file_folder.split('/')[loc] if len(file_folder.split('/')) > loc else ''
    else:
        return file_folder.split('/')[loc-1] if len(file_folder.split('/')) > loc - 1 else ''

def getTeamIdFromLocation(file_folder):
    return getSubPath(file_folder, 1)

def getUserIdFromLocation(file_folder):
    return getSubPath(file_folder, 2)

def getPipelineIdFromLocation(file_folder):
    return getSubPath(file_folder, 3)

def getRunIdFromLocation(file_folder):
    return getSubPath(file_folder, 4)

def getFileIdFromLocation(file_folder):
    return getSubPath(file_folder, 5)

def getModuleIdFromLocation(file_folder):
    return getSubPath(file_folder, 6)

def getSampleIdFromLocation(file_folder):
    return getSubPath(file_folder, 5)


def getRunBaseFolder( file_fullpath ):
    """ 
    >>> getRunBaseFolder( '/teamid/userid/pipelineid/runid/sampleid/moduleid/sample.txt' )
    '/teamid/userid/pipelineid/runid/'
    """
    p = os.path.join(getTeamIdFromLocation(file_fullpath), \
                     getUserIdFromLocation(file_fullpath), \
                     getPipelineIdFromLocation(file_fullpath), \
                     getRunIdFromLocation(file_fullpath))
    fs = getFileSystem(file_fullpath)
    return fs + p.lstrip('/').rstrip('/')+'/'

def getSampleBaseFolder( file_fullpath ):
    """ 
    >>> getSampleBaseFolder( '/teamid/userid/pipelineid/runid/sampleid/moduleid/sample.txt' )
    '/teamid/userid/pipelineid/runid/sampleid/'
    """
    base = getRunBaseFolder( file_fullpath )
    p = getSampleIdFromLocation(file_fullpath)
    return base + p.lstrip('/').rstrip('/')+'/'

def getModuleBaseFolder( file_fullpath ):
    """ 
    >>> getModuleBaseFolder( '/teamid/userid/pipelineid/runid/sampleid/moduleid/sample.txt' )
    '/teamid/userid/pipelineid/runid/sampleid/moduleid/'
    """
    base = getSampleBaseFolder( file_fullpath )
    p = getModuleIdFromLocation(file_fullpath)
    return base + p.lstrip('/').rstrip('/')+'/'


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

    >>> inferFileType( 'blah.fastq' )
    'fastq'
    >>> inferFileType( 'blah.fastq.gz' )
    'fastq.gz'
    >>> inferFileType( 'a/folder')
    ''
    >>> inferFileType( 'a/folder/')
    ''
    >>> inferFileType( ['blah1.fastq', 'blah2.fastq'] )
    'fastq'
    >>> inferFileType( ['a/folder', 'blah2.fastq'] )
    ''
    """
    if type(_fn) == type('') and '.' in _fn:
        return _fn.split('.')[-1] if len(list(filter(lambda combo: _fn.upper().endswith(combo), COMBO_FILETYPES))) == 0 else _fn.split('.')[-2]+'.'+_fn.split('.')[-1]
    elif type(_fn) == type([]) and _fn != [] and '.' in _fn[0]:
        return _fn[0].split('.')[-1] if len(list(filter(lambda combo: _fn[0].upper().endswith(combo), COMBO_FILETYPES))) == 0 else _fn[0].split('.')[-2]+'.'+_fn[0].split('.')[-1]
    else:
        return ''


def getFileSystem( file_fullpath ):
    """ Gets the file system s3:// or / or gs://
    """
    fs = '/'
    if type(file_fullpath) == type([]) and file_fullpath != []:
        if file_fullpath[0].startswith('s3:'):
            fs = 's3://'
        else:
            fs = '/'
    elif type(file_fullpath) == type(''):
        if file_fullpath.startswith('s3:'):
            fs = 's3://'
        else:
            fs = '/'
    else:
        fs = '/'
    return fs


def getFileOnly( file_fullpath ):
    """ Gets the file only from a full file path
    >>> getFileOnly( '/this/is/a/path/to.txt' )
    'to.txt'
    """
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
    """ Gets folder path from a full file path
    >>> getFileFolder( '/this/is/a/path' )
    '/this/is/a/path/'
    >>> getFileFolder( '/this/is/a/path/' )
    '/this/is/a/path/'
    >>> getFileFolder( '/this/is/a/path/to.txt' )
    '/this/is/a/path/'
    >>> getFileFolder( ['/this/is/a/path/to.txt'] )
    '/this/is/a/path/'
    """
    if type(file_fullpath) == type([]) and file_fullpath != []:
        # get directory of first file
        if '.' in file_fullpath[0].split('/')[-1]:
            # if file is specified at end
            folders_only = file_fullpath[0][0:file_fullpath[0].rfind('/')]+'/'
        else:
            # if just folder path is passed
            folders_only = file_fullpath[0].rstrip('/')+'/'
    elif type(file_fullpath) == type(''):
        if '.' in file_fullpath.split('/')[-1]:
            folders_only = file_fullpath[0:file_fullpath.rfind('/')]+'/'
        else:
            folders_only = file_fullpath.rstrip('/')+'/'
    else:
        folders_only = ''
    return folders_only


def inferFileSystem( filepath ):
    """ Accepts a single string or a list of filepaths.
    """
    fs = 'local'  # default is local
    if type(filepath) == list or type(filepath) == tuple:
        for f in filepath:
            if f == '' or type(f) != str:
                pass
            elif f.startswith('s3:/') or ('amazon' in f and 'aws' in f and 's3' in f):
                fs = 's3'
                break
            else:
                fs = 'local'
                break
    elif type(filepath) == str:
        if filepath.startswith('s3:/') or ('amazon' in f and 'aws' in f and 's3' in f):
            fs = 's3'
        else:
            fs = 'local'
    return fs
