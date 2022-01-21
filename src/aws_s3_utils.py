import os, boto3, subprocess, uuid

s3 = boto3.resource('s3')

def _findMatches(f, patterns, matchAll = False):
    """ Wrapper for _findMatch to search for multiple patterns. If matchAll is True, must match all patterns.
    """
    matches = []
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
    """ function for searching for a pattern match for a file f.
    """
    _isMatch = False
    if type(p) == str:
        p = [p]
    # file extension at end of filename
    if p == [] or p == ['']:
        _isMatch = True
    elif p[0]=='^':
        if f.endswith(p[1:]):
            _isMatch = True
    # prefix - file extension at beginning of filename
    elif p[-1]=='^':
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
    """
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


def uploadFiles_S3(localfiles, s3path):
    """ Securely uploads a list of files from local to s3.
        Full path of localfiles should be specified in the input.
    """
    if type(localfiles) == type([]):
        local_filenames = []
        for localfile in localfiles:
            uploadFile_S3(localfile, s3path)
    elif type(localfiles) == type(''):
        uploadFile_S3(localfiles, s3path)        
    return s3path


def uploadFile_S3(localfile, s3path):
    """ Securely uploads a local file to a path in S3.
        Full path of localfile should be specified in the input.
    """
    print('Uploading to s3 - {} to {}'.format(str(localfile), str(s3path)))
    if type(localfile) == type([]) and localfile != []:
        localfile = localfile[0]
    
    bucket = s3path.split('/')[2]
    key = os.path.join('/'.join((s3path.rstrip('/')+'/').split('/')[3:-1]),localfile.split('/')[-1])

    response = s3.Object(bucket,key).upload_file(localfile, ExtraArgs=dict(ServerSideEncryption='AES256'))
    return s3path


def uploadFolder_S3(localdir, s3path, files2exclude = ''):
    """ Uploads all files in a folder (and sub-folders) from S3 to a local directory.
        Automatically use server-side encryption.
        Returns response.
    """
    cmd = ['aws','s3','cp','--recursive','--sse','AES256',localdir.rstrip('/')+'/',s3path.rstrip('/')+'/']
    if files2exclude != '':
        for f in files2exclude:
            cmd += ['--exclude', f]
    response = subprocess.check_call(cmd)
    return response


def downloadFolder_S3(s3path, localdir):
    """ Downloads a folder (and sub-folders) from S3 to a local directory.
        Returns local directory name.
    """
    cmd = ['aws','s3','cp','--recursive',s3path.rstrip('/')+'/',localdir]
    subprocess.check_call(cmd)

    return localdir


def uploadFolder_S3(localdir, s3path, files2exclude = ''):
    """ Uploads all files in a folder (and sub-folders) from S3 to a local directory.
        Automatically use server-side encryption.
        Returns response.
    """
    cmd = ['aws','s3','cp','--recursive','--sse','AES256',localdir.rstrip('/')+'/',s3path.rstrip('/')+'/']
    if files2exclude != '':
        for f in files2exclude:
            cmd += ['--exclude', f]
    response = subprocess.check_call(cmd)  # 0 if successful

    return s3path


def downloadFiles_Pattern_S3(s3_path, directory_to_download, pattern):
    """
    Downloads files from S3 that match a specific pattern
    :param s3_path: s3 folder path
    :param directory_to_download: path to download the directory to
    :param pattern: file pattern to search for (e.g., '*.fastq.gz')
    :return: files downloaded
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
                 '^R1^' => file contains R1 in file extension (sep from base file name by one of [_,-,.]: e.g., myfile_R1.fastq.gz
                 'I1' => file contains the word I1 anywhere
    """
    if type(patterns2include) == str:
        patterns2include = [patterns2include]
    if type(patterns2exclude) == str:
        patterns2exclude = [patterns2exclude]
        
    cmd = 'aws s3 ls %s' % (s3_path)
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
                if _findMatches(rp, patterns2include) and not (pattern2exclude != [] and _findMatches(rp, patterns2exclude)):
                    dfiles.append(rp)
        
        # remove temporary file
        rm_command = ['rm',uid+'_dfilestmptmp.tmp']
        subprocess.check_call(rm_command)
    except subprocess.CalledProcessError:
        return []
    return dfiles


def listSubFolders(s3_path, folders2include = [], folders2exclude = []):
    """ Lists all immediate subfolders under a given S3 path.
    :param s3_path: s3 folder path
    :param folders2include: LIST, if specified, only include these folders
    :param folders2exclude: LIST of folders to exclude
    :return: found subfolders
    """    
    cmd = 'aws s3 ls %s' % (s3_path.rstrip('/')+'/')
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
