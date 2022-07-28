import os, boto3, subprocess, uuid, json, datetime
from dateutil.tz import *

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

def _findMatches(f, patterns, matchAll = False):
    """ Wrapper for _findMatch to search for multiple patterns. If matchAll is True, must match all patterns.
    f: file name, e.g. 'hello.txt', STR
    patterns: patterns to search, e.g. '^.txt', ['^bam', 'myfile'], STR or LIST
    matchAll: do we need to match all patterns? BOOL
    RETURN: whether file matches patterns - True/False BOOL

    >>> _findMatches('myfile.txt', '^.txt')
    True
    >>> _findMatches('myfile.txt', 'myf^')
    True
    >>> _findMatches('myfile.txt', 'file^')
    False
    >>> _findMatches('myfile.bam', 'bam')
    True
    >>> _findMatches('myfile.txt', ['^.txt', 'super'])
    True
    >>> _findMatches('myfile.txt', ['^.txt', 'super'], True)
    False
    """
    matches = []
    if type(patterns) == str:
        patterns = [patterns] if patterns != '' else []

    # if patterns is empty, we return True by default
    if patterns == []:
        return True

    for p in patterns:
        matches.append(_findMatch(f, p))
    if matchAll == False:
        if True in matches:
            return True
        else:
            return False
    elif matchAll == True:
        if False in matches:
            return False
        else:
            return True


def _findMatch(f, p):
    """ function for searching for a pattern match for a file name f.
    Pattern can have the following:
    'bam': file name f contains 'bam'
    '^.txt': file name f ends in .txt
    'myfile^': file name begins with myfile

    f: file name, e.g. 'hello.txt' STR
    p: pattern to search, e.g. '^.txt' STR
    RETURN: if pattern is in file name BOOL

    >>> _findMatch('hello.fastq','^fastq')
    True
    >>> _findMatch('myfile.txt', '^.txt')
    True
    >>> _findMatch('myfile.txt', 'myf^')
    True
    >>> _findMatch('myfile.txt', 'file^')
    False
    >>> _findMatch('myfile.txt', 'file')
    True
    >>> _findMatch('myfile.txt', '')
    True
    """
    # print('FINDMATCH: file {}, pattern {}'.format(str(f), str(p)))
    _isMatch = False
    f = str(f).lower()
    p = str(p).lower()
    # if empty string
    if p == '[]' or p == "['']" or p == '':
        _isMatch = True
    # suffix - file extension at end of filename
    elif p[0]=='^' and p[-1]!='^':
        if f.endswith(p[1:]):
            _isMatch = True
    # prefix - file extension at beginning of filename
    elif p[-1]=='^' and p[0]!='^':
        if f.startswith(p[0:-1]):
            _isMatch = True
    # search pattern somewhere in file extension, separated from base file name by one of [_,-,.]
    elif p.rfind('^') > p.find('^'):
        i = p.find('^')
        j = p.rfind('^')
        if (f[f.find('_'):].find(p[i+1:j]) >= 0) or (f[f.find('.'):].find(p[i+1:j]) >= 0) or (f[f.find('-'):].find(p[i+1:j]) >= 0):
            _isMatch = True
    # search pattern anywhere in file name
    else:
        if f.find(p) >= 0:
            _isMatch = True
    return _isMatch


def downloadFile_S3(s3path, dir_to_download):
    """ Downloads an object from S3 to a local file.
        Returns full file path of downloaded local file.
    s3path: S3 file path, s3://hubseq/myfile.bam, STR
    dir_to_download: local directory to download to, /local/dir, STR
    RETURN: full file path of downloaded local file, /local/dir/myfile.bam, STR

    >>> downloadFile_S3('s3://hubpublicinternal/test/aws_s3_utils/test-R1.fastq.gz', './testout/')
    Downloading from S3 - s3://hubpublicinternal/test/aws_s3_utils/test-R1.fastq.gz to ./testout/
    './testout/test-R1.fastq.gz'
    """
    # input checks
    if s3path == '' or s3path == []:
        return ''
    elif type(s3path) == type([]):
        s3path = s3path[0]

    print('Downloading from S3 - {} to {}'.format(s3path, dir_to_download))
    bucket = s3path.split('/')[2]
    key = '/'.join(s3path.split('/')[3:])

    object_filename = key.split('/')[-1]
    local_filename = os.path.join(dir_to_download, object_filename)

    s3.Object(bucket,key).download_file(local_filename)

    return local_filename

def downloadFiles_S3(s3paths, dir_to_download):
    """ Downloads a list of file objects from S3 to local files.
        If STRING is provided, then just one file.
        Returns full file path of downloaded local files.

    s3paths: list of S3 file paths, ['s3://hubseq/myfile1.bam', 's3://hubseq/myfile2.bam'], STR
    dir_to_download: local directory to download to, /local/dir, STR
    RETURN: full file path of downloaded local files, ['/local/dir/myfile1.bam', '/local/dir/myfile2.bam'], STR

    >>> downloadFiles_S3(['s3://hubpublicinternal/test/aws_s3_utils/test-R1.fastq.gz', 's3://hubpublicinternal/test/aws_s3_utils/test-R2.fastq.gz'], './testout/')
    Downloading from S3 - s3://hubpublicinternal/test/aws_s3_utils/test-R1.fastq.gz to ./testout/
    Downloading from S3 - s3://hubpublicinternal/test/aws_s3_utils/test-R2.fastq.gz to ./testout/
    ['./testout/test-R1.fastq.gz', './testout/test-R2.fastq.gz']
    """
    if type(s3paths) == type([]):
        local_filenames = []
        for s3path in s3paths:
            local_filenames.append(downloadFile_S3(s3path, dir_to_download))
    elif type(s3paths) == type(''):
        local_filenames = downloadFile_S3(s3paths, dir_to_download)
    else:
        local_filenames = ''
    return local_filenames


def downloadFolder_S3(s3path, localdir):
    """ Downloads a folder (and sub-folders) from S3 to a local directory.
        Returns local directory name.
    >>> downloadFolder_S3('s3://hubpublicinternal/test/aws_s3_utils/', './testout/')
    './testout/'
    """
    cmd = ['aws','s3','cp','--recursive',s3path.rstrip('/')+'/',localdir]
    subprocess.check_call(cmd)

    return localdir


def downloadFiles_Pattern_S3(s3_path, directory_to_download, pattern):
    """
    Downloads files from S3 that match a specific pattern
    :param s3_path: s3 folder path
    :param directory_to_download: path to download the directory to
    :param pattern: file pattern to search for (e.g., '*.fastq.gz')
    :return: files downloaded
    >>> downloadFiles_Pattern_S3('s3://hubpublicinternal/test/aws_s3_utils/', './testout/', '*-R1.fastq.gz')
    './testout/'
    """
    pattern_with_quotes = '"'+pattern+'"'
    cmd = 'aws s3 cp --recursive %s %s --exclude "*" --include %s' \
        % (s3_path, directory_to_download, pattern_with_quotes)

    # output of S3 copy to temporary file
    # fout = open('dfilestmptmp.tmp','w')
    subprocess.check_call(cmd.split(' ')) #, stdout=fout)
    # fout.close()

    # get a list of all downloaded files
    # dfiles = []
    # with open('dfilestmptmp.tmp','r') as f:
    #    for r in f:
    #        dfiles.append(str(r.rstrip(' \t\n').split(' ')[-1]))

    # remove temporary file
    # rm_command = ['rm','dfilestmptmp.tmp']
    # subprocess.check_call(rm_command)
    return directory_to_download
#    return dfiles


def uploadFile_S3(localfile, s3path):
    """ Securely uploads a local file to a path in S3.
        Full path of localfile should be specified in the input.
    >>> uploadFile_S3('test/test-upload-R1.fastq.gz', 's3://hubpublicinternal/test/aws_s3_utils/')
    Uploading to s3 - test/test-upload-R1.fastq.gz to s3://hubpublicinternal/test/aws_s3_utils/
    's3://hubpublicinternal/test/aws_s3_utils/'
    """
    # input checks
    if localfile == '' or localfile == []:
        return ''
    elif type(localfile) == type([]):
        localfile = localfile[0]

    print('Uploading to s3 - {} to {}'.format(str(localfile), str(s3path)))

    bucket = s3path.split('/')[2]
    key = os.path.join('/'.join((s3path.rstrip('/')+'/').split('/')[3:-1]),localfile.split('/')[-1])

    response = s3.Object(bucket,key).upload_file(localfile, ExtraArgs=dict(ServerSideEncryption='AES256'))
    return s3path


def uploadFiles_S3(localfiles, s3path):
    """ Securely uploads a list of files from local to s3.
        Full path of localfiles should be specified in the input.
    >>> uploadFiles_S3(['test/test-upload-R1.fastq.gz', 'test/test-upload-R2.fastq.gz'], 's3://hubpublicinternal/test/aws_s3_utils/')
    Uploading to s3 - test/test-upload-R1.fastq.gz to s3://hubpublicinternal/test/aws_s3_utils/
    Uploading to s3 - test/test-upload-R2.fastq.gz to s3://hubpublicinternal/test/aws_s3_utils/
    's3://hubpublicinternal/test/aws_s3_utils/'
    """
    if type(localfiles) == type([]):
        local_filenames = []
        for localfile in localfiles:
            uploadFile_S3(localfile, s3path)
    elif type(localfiles) == type(''):
        uploadFile_S3(localfiles, s3path)
    return s3path


def uploadFolder_S3(localdir, s3path, files2exclude = ''):
    """ Uploads all files in a folder (and sub-folders) from S3 to a local directory.
        Automatically use server-side encryption.
        Returns upload response.
    >>> uploadFolder_S3('./test/', 's3://hubpublicinternal/test/aws_s3_utils/')
    's3://hubpublicinternal/test/aws_s3_utils/'
    """
    cmd = ['aws','s3','cp','--recursive','--sse','AES256',localdir.rstrip('/')+'/',s3path.rstrip('/')+'/']
    if files2exclude != '':
        for f in files2exclude:
            cmd += ['--exclude', f]
    response = subprocess.check_call(cmd)
    return s3path


def listSubFiles(s3_path, patterns2include, patterns2exclude):
    """
    Lists files from S3 that match a specific pattern
    :param s3_path: s3 folder path
    :param patterns2include: LIST of file patterns to search for.
    :param patterns2exclude: LIST of file patterns to exclude.
    :return: found files matching pattern

    patterns follow this notation: e.g., ['^.bam', 'hepg2^', 'I1'] where
                 '^.bam' => file ends with BAM
                 'hepg2^' => file begins with hepg2
                 '^fastq^' => file contains fastq in file extension (sep from base file name by [-,_,.]: e.g., myfile.fastq.gz
                 'I1' => file contains the word I1 anywhere

    >>> listSubFiles('s3://hubpublicinternal/test/aws_s3_utils/', 'test', 'R1')
    ['test-R2.fastq.gz', 'test-upload-R2.fastq.gz', 'test-upload.create_fastq.log', 'test.create_fastq.log']
    >>> listSubFiles('s3://hubpublicinternal/test/aws_s3_utils/', 'test', ['^R1^','^R2^'])
    ['test-upload.create_fastq.log', 'test.create_fastq.log']
    """
    if type(patterns2include) == str:
        patterns2include = [patterns2include]
    if type(patterns2exclude) == str:
        patterns2exclude = [patterns2exclude]

    if type(s3_path) == type([]) and s3_path != []:
        s3_path = s3_path[0]
    elif type(s3_path) == type([]) and s3_path == []:
        s3_path = ''
    
    cmd = 'aws s3 ls %s' % (s3_path.rstrip('/')+'/')
    dfiles = []
    uid = str(uuid.uuid4())[0:6]  # prevents race conditions on tmp file
    # output of S3 copy to temporary file
    try:
        # I think calledprocesserror will be called if file not found
        with open(uid+'_dfilestmptmp.tmp','w') as fout:
            subprocess.check_call(cmd.split(' '), stdout=fout)
        # get a list of all downloaded files
        with open(uid+'_dfilestmptmp.tmp','r') as f:
            for r in f:
                rp = r.split(' ')[-1].lstrip(' \t').rstrip(' \t\n')
                # '.' indicates its a file
                if '.' in rp and _findMatches(rp, patterns2include) and not (patterns2exclude != [] and _findMatches(rp, patterns2exclude)):
                    dfiles.append(rp)

        # remove temporary file
        rm_command = ['rm',uid+'_dfilestmptmp.tmp']
        subprocess.check_call(rm_command)
    except subprocess.CalledProcessError:
        print('CALLED PROCESS ERROR in aws_s3_utils.listSubFiles() or FILES NOT FOUND')
        if os.path.exists(uid+'_dfilestmptmp.tmp'):
            rm_command = ['rm',uid+'_dfilestmptmp.tmp']
            subprocess.check_call(rm_command)
        return []
    return dfiles


def listSubFolders(s3_path, folders2include = [], folders2exclude = [], options = ''):
    """ Lists all immediate subfolders under a given S3 path.
    :param s3_path: s3 folder path
    :param folders2include: LIST, if specified, only include these folders
    :param folders2exclude: LIST of folders to exclude
    :param options: options to include with ls call
    :return: found subfolders
    >>> listSubFolders('s3://hubpublicinternal/test/', ['aws_s3_utils'])
    ['aws_s3_utils']
    """
    if type(s3_path) == type([]) and s3_path != []:
        s3_path = s3_path[0]
    elif type(s3_path) == type([]) and s3_path == []:
        s3_path = ''
    cmd = 'aws s3 ls {} {}'.format(options, (s3_path.rstrip('/')+'/'))
    dfolders = []
    uid = str(uuid.uuid4())[0:6]  # prevents race conditions on tmp file
    try:
        fout = open(uid+'_dfolderstmptmp.tmp','w')
        subprocess.check_call(cmd.split(' '), stdout=fout)
        fout.close()
        with open(uid+'_dfolderstmptmp.tmp','r') as f:
            for r in f:
                rp = r.lstrip(' \t').rstrip(' \t\n')
                if rp.startswith('PRE'):  # tags a folder - hopefully this doesn't change on AWS side with aws s3 ls
                    folder = rp.split(' ')[1].rstrip('/')
                    if (folder not in folders2exclude) and (folders2include == [] or folder in folders2include):
                        dfolders.append(folder)

        # remove temporary file
        rm_command = ['rm',uid+'_dfolderstmptmp.tmp']
        subprocess.check_call(rm_command)
        return dfolders
    except subprocess.CalledProcessError:
        return []

def get_json_object( s3paths ):
    """ Gets content of JSON files in S3, specified by S3 paths.
    s3paths: string of comma-delimited S3 paths to JSON files - 'obj1,obj2,...'
    returns: corresponding list of JSON content (ordered)
    """
    json_list = []
    s3paths_list = s3paths.split(',')
    for s3path in s3paths_list:
        bucket = s3path.split('/')[2]
        key = '/'.join(s3path.split('/')[3:])

        obj = s3.Object(bucket, key)
        data = obj.get()['Body'].read().decode('utf-8')
        json_data = json.loads(data)
        json_list.append(json_data)
    return json_list

def add_to_json_object( s3path, newpairs ):
    """ Adds new key value pairs to an existing S3 JSON object
    """
    bucket = s3path.split('/')[2]
    key = '/'.join(s3path.split('/')[3:])
    json_data = get_json_object( s3path )[0]
    # if json_data is itself a list, we can just append. Otherwise we add key by key.
    if type(json_data) == type([]):
        json_data.append( newpairs )
    elif type(json_data) == type({}):
        for k, v in newpairs.items():
            if k not in json_data:
                json_data[k] = v
    
    response = s3_client.put_object( Body=json.dumps(json_data).encode(), Bucket=bucket, Key=key )
    return response


def _filter_list_objects_response( response, search_pattern ):
    """ private function
    Narrow down file list response object if we are searching for a specific file pattern
    """
    files2remove = []
    for file_info in response["Contents"]:
        if "Key" in file_info and search_pattern not in file_info["Key"]:
            files2remove.append(file_info)
    for file2remove in files2remove:
        response["Contents"].remove(file2remove)
    return response


def list_objects( s3path, searchpattern = '' ):
    """ List objects in an S3 object path. Returns a JSON response with format:
      {'ResponseMetadata': 
         {'RequestId': ...,
          'HostId': ...
          'HTTPStatusCode': 200,
          'HTTPHeaders': ...
          'IsTruncated': False,
          'Contents': 
            [{'Key': '<filename>' 'LastModified': ...', 'Size': '...'...},
             {'Key': '...
            ]
         }}}

    searchpattern: only include files that contain the passed searched pattern (optional)

    example of returned file info:
    {"Contents": [
        {
            "Key": "hubseq/jira.pdf",
            "LastModified": "2022-07-27 23:11:22+00:00",
            "ETag": "\"46213a6178ac104ba811cc5bb0133218\"",
            "Size": 159226,
            "StorageClass": "STANDARD"
        },...
    ]}
    """
    bucket = s3path.split('/')[2]
    key = str('/'.join(s3path.split('/')[3:])).rstrip('/') + '/'
    region = 'us-west-2'
    # s3_endpoint = '{}.s3.{}.amazonaws.com/{}'.format(bucket,region,key)
    response = s3_client.list_objects_v2( Bucket=bucket, Prefix=key, Delimiter='/' )
    ## narrow down file list if we are searching for a specific file pattern
    if searchpattern != '' and "Contents" in response:
        return _filter_list_objects_response( response, searchpattern )
    else:
        return response

def list_objects_nested( s3path, searchpattern ):
    """ Similar to list_objects but will list objects in nested subfolders
    Nested subfolders are found in the "CommonPrefixes" key:
    {
    "CommonPrefixes": [
        {
            "Prefix": "hubseq/guestuser/"
        },
        {
            "Prefix": "hubseq/test/"
        },...
        ]
    }
    """
    
    return

def get_metadata( s3paths ):
    """ Gets metadata for objects (files or folders) at the given S3 paths (string-list)
    s3paths: string of comma-delimited S3 paths - 'obj1,obj2,...'
    returns: corresponding list of tags
    """
    metadata_list = []
    s3paths_list = s3paths.split(',')
    for s3path in s3paths_list:
        bucket = s3path.split('/')[2]
        key = '/'.join(s3path.split('/')[3:])
        response = s3_client.get_object_tagging(Bucket=bucket,Key=key)
        if 'TagSet' in response:
            metadata = response['TagSet']
        else:
            metadata = {}
        metadata_list.append( metadata )
    return metadata_list

def set_metadata( s3paths, tags_dict, overwrite = 'True' ):
    """ Sets metadata for objects (files or folders) at the given S3 paths as the passed in tags.
        If tags exist, it overwrites if overwrite = True (default).
        Currently, can have up to 10 tags.
        All objects will get the same tags.

        s3paths: string of comma-delimited objects in S3 - 'obj1,obj2,...'
        tags_dict: DICT - e.g., {'project': 'epigenome', 'id': 'ID-01', 'tissue': 'heart'}
        returns: set tags (list)
    """
    def getCurrentTag( k, tags ):
        """ List of tags format as [{'Key': k, 'Value': v}, {'Key2': k2, 'Value2': v2},...]
            return value for key k
        """
        for t in tags:
            if k == t['Key']:
                return t
        return {}
    
    tag_sets = []
    s3paths_list = s3paths.split(',')
    for s3path in s3paths_list:
        bucket = s3path.split('/')[2]
        key = '/'.join(s3path.split('/')[3:])
        # get the current tags, and only overwrite if overwrite = True
        current_tags = get_metadata( s3path )[0]
        tag_set = []
        for k, v in tags_dict.items():
            new_tag = {'Key': k, 'Value': v}
            # only overwrite if we specify so, or if tag key does not exist
            if overwrite[0].upper()=='T' or (k not in list(map(lambda kv: kv['Key'], current_tags))):
                tag_set.append(new_tag)
            else:
                # otherwise keep the current tag
                current_tag = getCurrentTag( k, current_tags )
                tag_set.append( current_tag )
        # update metadata for S3 object
        response = s3_client.put_object_tagging(Bucket=bucket,Key=key, Tagging={'TagSet': tag_set})
        tag_sets.append(tag_set)
    return tag_sets
    
def dateConverter(json_object):
    """ AWS response objects often contain datetime objects. 
    For proper JSON stringify, need to convert these datetime objects to string date format.
    """
    if isinstance(json_object, datetime.datetime):
        return json_object.__str__()
