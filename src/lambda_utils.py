import os

def getParameter( param_dict, k, v_default ):
    """ Return value of key k in param_dict, if found - otherwise return v_default.
    """
    if k in param_dict:
        return param_dict[k]
    else:
        return v_default

def getS3path( partialFilePaths ):
    """ Given a list of partial input file paths (comma-separated string or list),
        prepends the S3 bucket name.
        Return full file paths in the same type provided as input.

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

    # create list of partial file paths from input
    if type(partialFilePaths)==type(''):
        partialFilePathsList = partialFilePaths.split(',')
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
        if not f.startswith('s3://'):
            fullPaths.append(os.path.join(TEAM_BUCKET, f.lstrip('/')))
        else:
            fullPaths.append(f)

    # format and return full filepaths
    if returnType=="string":
        return ','.join(fullPaths)
    else:
        return fullPaths
    
