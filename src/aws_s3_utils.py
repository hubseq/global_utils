import os, boto3, subprocess, uuid

s3 = boto3.resource('s3')

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
        fout = open(uid+'_dfilestmptmp.tmp','w')
        subprocess.check_call(cmd.split(' '), stdout=fout)
        fout.close()

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
        print('CALLED PROCESS ERROR in aws_s3_utils.listSubFiles()')
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
