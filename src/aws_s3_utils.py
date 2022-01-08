import os, boto3, subprocess

s3 = boto3.resource('s3')

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


def listFiles(s3_path, pattern):
    """
    Lists files from S3 that match a specific pattern
    :param s3_path: s3 folder path
    :param pattern: file pattern to search for (e.g., '*.fastq.gz')
    :return: found files matching pattern
    """
    patterns = pattern.split('*')
    cmd = 'aws s3 ls %s' % (s3_path)
    dfiles = []

    # output of S3 copy to temporary file
    try:
        fout = open('dfilestmptmp.tmp','w')
        subprocess.check_call(cmd.split(' '), stdout=fout)
        fout.close()

        # get a list of all downloaded files
        with open('dfilestmptmp.tmp','r') as f:
            for r in f:
                isFound = True
                for p in patterns:
                    if p not in r:
                        isFound = False
                if isFound == True:
                    dfiles.append(str(r.rstrip(' \t\n').split(' ')[-1]))

        # remove temporary file
        rm_command = ['rm','dfilestmptmp.tmp']
        subprocess.check_call(rm_command)
    except subprocess.CalledProcessError:
        return []
    return dfiles
