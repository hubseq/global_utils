import os

def getParameter( param_dict, k, v_default ):
    """ Return value of key k in param_dict, if found - otherwise return v_default.
    """
    if k in param_dict:
        return param_dict[k]
    else:
        return v_default

def getS3path( partialFilePaths, teamid = '', userid = '', useBaseDir = 'false' ):
    """ Given a list of partial input file paths (comma-separated string or list),
        prepends the S3 bucket name.
        Return full file paths in the same type provided as input.

        useBaseDir - whether or not to use the basedir on an empty string
    
    >>> lambda_utils.getS3path('hubseq/test/file1.pdf')
    's3://hubtenants/hubseq/test/file1.pdf'
    >>> lambda_utils.getS3path('hubseq/test/file1.pdf,hubseq/file2.txt')
    's3://hubtenants/hubseq/test/file1.pdf,s3://hubtenants/hubseq/file2.txt'
    >>> lambda_utils.getS3path(['hubseq/test/file1.pdf'])
    ['s3://hubtenants/hubseq/test/file1.pdf']
    >>> lambda_utils.getS3path(['hubseq/test/file1.pdf', 'hubseq/file2.txt'])
    ['s3://hubtenants/hubseq/test/file1.pdf', 's3://hubtenants/hubseq/file2.txt']
    >>> lambda_utils.getS3path(['s3://hubseq-data/test/file1.pdf', 's3://hubseq-data/test/file2.txt'])
    ['s3://hubseq-data/test/file1.pdf', 's3://hubseq-data/test/file2.txt']
    """
    TEAM_BUCKET = 's3://hubtenants/'
    if teamid != '':
        TEAM_BUCKET += teamid+'/'
    # if userid != '':
    #    TEAM_BUCKET += userid+'/'

    # create list of partial file paths from input
    if type(partialFilePaths)==type(''):
        partialFilePathsList = partialFilePaths.lstrip("'").lstrip('"').rstrip("'").rstrip('"').split(',')
        returnType = "string"
    elif  type(partialFilePaths)==type([]):
        partialFilePathsList = partialFilePaths
        returnType = "list"
    else:
        partialFilePathsList = []
        returnType = "list"

    # create full filepaths
    fullPaths = []
    for f in partialFilePathsList:
        if ('s3://' not in f and f not in ['~/','',"''",'""']) or (f in ['~/','',"''",'""'] and useBaseDir.lower() != 'false'):
            fullPaths.append(os.path.join(TEAM_BUCKET, f.lstrip('/')))
        elif useBaseDir.lower() == 'false' and f in ['',"''",'""']:
            fullPaths.append("''")
        else:
            fullPaths.append(f)

    # format and return full filepaths
    if returnType=="string":
        return ','.join(fullPaths)
    else:
        return fullPaths
    

def getS3path_args( argslist_input, teamid = '', userid = '' ):
    """ Takes an arguments list (either space-delimited string or a list) and
        for any paths that are marked with ~/
        replaces with S3 TEAM_BUCKET name.

        Special case if we have a string of strings (module_args in run_pipeline)
         - just replace ~/ with S3 TEAM BUCKET name.

        useBaseDir - whether or not to use the basedir on an empty string
    
    >>> lambda_utils.getS3path_args(['-a','-b','~/test/temp.txt','/blah'])
    ['-a', '-b', 's3://hubtenants/test/temp.txt', '/blah']
    >>> lambda_utils.getS3path_args('-a -b ~/test/temp.txt /blah')
    '-a -b s3://hubtenants/test/temp.txt /blah'
    """
    TEAM_BUCKET = 's3://hubtenants/'
    if teamid != '':
        TEAM_BUCKET += teamid+'/'
    if userid != '':
        TEAM_BUCKET += userid+'/'
    
    # create list of partial file paths from input
    if type(argslist_input)==type(''):
        return argslist_input.replace('~/',TEAM_BUCKET)
    elif type(argslist_input)==type([]):
        argsList = argslist_input
        # create full filepaths
        fullArgsList = []
        for f in argsList:
            if f.startswith('~/'):
                fullArgsList.append(os.path.join(TEAM_BUCKET, f[2:]))
            else:
                fullArgsList.append(f)
        return fullArgsList
    else:
        return []
    
