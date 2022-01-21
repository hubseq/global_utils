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
DATA_FILE_JSON_VERSION = '20211219'
GROUP_JSON_VERSION = '20211219'

VALID_FILETYPES = ['FASTQ', 'BAM', 'SAM', 'BED', 'TXT', 'CSV', 'JSON', 'GZ', 'FASTQ.GZ', 'WIG', 'HTML']
COMBO_FILETYPES = ['FASTQ.GZ']

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
    return: LIST of file IDs
    
    FUTURE: check for existence of folders (in case user deletes).
    """
    fileids = getSubFolders( os.path.join(root_folder, teamid, userid, pipelineid, runids) )
    return fileids


def getDataFiles( data_folders, extensions2include = [], extensions2exclude = [] ):
    """ Gets data files in the selected data folders that match extensions2include and DO NOT match extensions2exclude.
    
    data_folders: LIST of data folders to search. Can be local or on S3.
    extensions2include: LIST of extension patterns to search for. If empty, then get all files.
    extensions2exclude: LIST of extension patterns to exclude. If empty, then do not exclude any files.
    return: LIST of data files, LIST of sample IDs (file IDs) for those data files

    >>> getDataFiles([])
    ([], [])

    """
    data_files = []
    sample_ids = []
    if type(data_folders) == str:
        data_folders = [data_folders]
        
    for data_folder in data_folders:
        data_files_new = getSubFiles( data_folder, extensions2include, extensions2exclude )
        data_files = data_files + data_files_new
        for i in range(0,len(data_files_new)):
            sample_ids.append( getFileIdFromLocation(data_folder) )
    return (data_files, sample_ids)


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


# file hierarchy:
# /team_id/user_id/run_id/file_id/module_id/<file_id>...<file_extension>
def getSubPath(file_folder, loc):
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


def createDataFileJSON( _filename, _filefolder, _filetype = '',  _file_id = '', _team_id = '', _user_id = '', _pipeline_id = '', _run_id = '', _module_id = ''):
    """ Creates a data file JSON from input file information.

    _filename: filename STRING (e.g., 'myfile.fastq') (REQUIRED)
    _filefolder: file folder / location STRING (e.g., '/myfolder/bwamem/') (REQUIRED)
    _filetype: file type (e.g., BAM) (optional but prefered)
    _team_id: unique team id (e.g., chenlab) (optional but prefered - can infer from filefolder if necessary)
    _user_id: unique user id (e.g., jerrychen) (optional but prefered - can infer from filefolder if necessary)
    _run_id: run / job id with format <user_id>_<yyyymmddhhmmss> (e.g., jerrychen_20211219221055)
    _module_id: module name (e.g., bwa)
    _file_id: file ID - usually sample ID, which is derived from run input file name (e.g., encode-hepg2) (optional)
    
    return: JSON with the key-value pairs defined for data files
    """
    _module_id_local = _module_id if _module_id != '' else getModuleIdFromLocation(_filefolder)

    return {global_keys.KEY_TEAM_ID: _team_id if _team_id != '' else getTeamIdFromLocation(_filefolder),
            global_keys.KEY_USER_ID: _user_id if _user_id != '' else getUserIdFromLocation(_filefolder),
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

    return: LIST of data files in folder that match extension patterns searched for
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


def getDataFilesJSON( _sample_file_search_json_list, _file_system = 'local' ):
    """ Given a list of JSON file search queries, function searches and returns found data files (as JSON).
    See repo > global_utils > src > file_utils.py for JSON specifications for file search queries and data files.
    
    _sample_file_search_json_list: LIST(JSON) - list of search query JSONs
    
    return: LIST(JSON) - list of data file JSONs

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
    if filepath.startswith('s3:/'):
        return 's3'
    else:
        return 'local'
