#
# module_utils
#
# Utility functions for modules
#
#
# Program Arguments
import file_utils
import os, subprocess, sys, json, uuid
from argparse import ArgumentParser

"""
ORDER OF RUNNING IN A DOCKER MODULE VIA PIPELINE:
run_args_json = parseRunInput( run_input_arguments )
-> HERE, CAN CHECK DEPENDENCIES, CAN CHECK IF DRY-RUN,...
io_json = createIOJSON( run_args_json )
-> THIS SHOULD BE WRITTEN TO .run.json
-> GET TEMPLATE FOR THIS DOCKER MODULE
module_instance_json = createModuleInstanceJSON( template, io_json )
-> RUN PROGRAM
startProgram( module_instance_json, fileout )
-> UPLOAD RESULTS - UPLOAD ENTIRE OUTPUT FOLDER (KEEP TRACK OF WHICH FILES TO IGNORE)

JSON format:
{'program_name': 'bwa',
 'program_subname': 'mem',
 'program_version': '0.7.17',
 'program_arguments': '...',
 'input_file_system': 's3/local/gcp...',
 'output_file_system': 's3/local/gcp...',
 'program_input': {'input': [LIST],
                   'input_type': 'file/folder',
                   'input_file_type': 'FASTQ/BAM/...',
                   'input_directory': STRING,
                   'input_position': -1,
                   'input_prefix': '-i'},
 'program_output': {'output': [LIST],
                   'output_type': 'file/folder',
                   'output_file_type': 'FASTQ/BAM/...',
                   'output_directory': STRING,
                   'output_position': -2,
                   'output_prefix': '-o'},
 'alternate_inputs': [{'input': [LIST]
                       'input_type': 'file/folder',
                       'input_file_type': 'BED/FASTA/...',
                       'input_directory': STRING,
                       'input_position': 0,
                       'input_prefix': '-L'},
                      },...]
 'alternate_outputs': [{'output': [LIST],
                        'output_type': 'file/folder',
                        'output_file_type': 'FASTQ/BAM/...',
                        'output_directory': STRING,
                        'output_position': -100,
                        'output_prefix': '-o'},...]
 }
alternate input. This would be for example, a FASTA file, or a BED file, or an entire genome index (folder).
alternate input type. File or folder.
alternate input file type. FASTA, BED, etc.
alternate input position. Desired argument position of alternate input name for the COMPLETE program call that includes inputs and oupputs. Note that if there are several inputs they are considered a single unit, so for a command like:

“bwa mem -L <BED_FILE> input1.fastq input2.fastq”

We could specify the BED input position as -2. The main FASTQ input position is -1.

alternate input prefix. Prefix for the alternate input file in the program arguments. For this:

“bwa mem -L <BED_FILE> input1.fastq input2.fastq”

The BED file prefix would be “-L”.

alternate index.
alternate prefix.

NOTE: many alternate inputs could be specified for a given program.


Jerry Chen
December 5, 2021, 10:17 PM

Each docker module (app) should specify
program arguments. Should be in exact order as program is called.

input(s). These are either files or folders. (LIST)
input type. File or folder
input file type(s). The types of the primary input files for a program (e.g., FASTQ, BAM, etc.). This property is used to determine if the input of this app is compatible with the output of the previous app.
input directory. Location of input(s).
input position. Which position in the program arguments that the input should be placed when program is called.

0: before all program arguments
-1: AFTER all program arguments

input prefix. If program requires a prefix before specifying the input, e.g., “-i” if  “-i <INPUT>” is how inputs are specified when calling the program.
output(s). Name of output files or folder.
output type. Files or folder.
output file type(s). The types of the primary output files for a program (e.g., FASTQ, BAM, etc.). This is especially useful for programs that write a lot of files to an output folder, but we want to know which files in that folder are the main outputs. Note that this property is used to determine if the output of this app is compatible with the input of the next app.
output position. Which position in the program arguments the output name should be placed when program is called.

0: before all program arguments
-1. AFTER all program arguments.
-2. Just before the last program argument.
-100. Output is not specified in the program arguments (implied). -100 is a special integer to indicate this.

“None”. Indicates that NO output is specified in the program arguments (default output name or folder is somehow implied).

TEMPLATE DIR: /npimodules/templates/
"""

MODULE_TEMPLATE_PATH = 's3://npipublicinternal/test/templates/' 
MODULE_DIR = 's3://npipublicinternal/test/modules/'

def getModuleDirectory():
    return MODULE_DIR

def getModuleTemplateLocation( which_module ):
    return getModuleTemplate( which_module )

def getModuleTemplate( which_module ):
    """ Returns the template module JSON file path for input docker module
    """
    return os.path.join(MODULE_TEMPLATE_PATH, which_module+'.template.json')


def downloadModuleTemplate( which_module, dest_folder ):
    """ Downloads the module template to the destination directory
    """
    module_template_file = getModuleTemplate( which_module )
    module_template_path = file_utils.downloadFile( module_template_file, dest_folder )
    return module_template_path
    


def getModule_vcpus( module_json ):
    return module_json['compute']['vcpus']


def getModule_memory( module_json ):
    return module_json['compute']['memory']


def getModule_environment( module_json ):
    return module_json['compute']['environment']



def generateWorkingDir(base_dir):
    """ Creates a unique working data directory for this Docker run, if possible.
        This allows multiple dockers running on same machine to have their own working directories.
    """
    working_dir = os.path.join(base_dir, str(uuid.uuid4()))
    try:
        os.mkdir(working_dir)
    except Exception as e:
        return base_dir
    return working_dir


def getRunArgs( ):
    """ Dockerfile ENTRYPOINT is always the wrapper script run_program.py. This wrapper script takes the arguments:
        (1) --module_name: name of the docker module
        (2) --run_arguments: run arguments JSON file
        (3) --working_dir: the working_directory for running and saving stuff. This could be a directory on a mounted volume.

        Returns arguments
    """
    print('Parsing run input arguments')
    argparser = ArgumentParser()
    file_path_group = argparser.add_argument_group(title='File arguments')
    file_path_group.add_argument('--module_name', help='name of docker module', required=True)    
    file_path_group.add_argument('--run_arguments', help='path to run_arguments.json', required=True)
    file_path_group.add_argument('--working_dir', help='working data directory for docker run', required=True)
    args = argparser.parse_args()
    return args


def insertArgument(arg_list, arg, pos):
    """ Insert arg into arg_list at pos. Integers, strings or lists can be inserted.
    -100 is a special pos that says 'don't insert argument'.

    >>> insertArgument([1, 2, 3, 4, 5], 'a', 0)
    ['a', 1, 2, 3, 4, 5]
    
    >>> insertArgument([1, 2, 3, 4, 5], 'a', 2)
    [1, 2, 'a', 3, 4, 5]

    >>> insertArgument([1, 2, 3, 4, 5], 'a', -1)
    [1, 2, 3, 4, 5, 'a']

    >>> insertArgument([1, 2, 3, 4, 5], 'a', -2)
    [1, 2, 3, 4, 'a', 5]

    >>> insertArgument([1, 2, 3, 4, 5], 'a', -100)
    [1, 2, 3, 4, 5]

    >>> a = [1, 2, 3, 4, 5]
    >>> b = ['a', 'b']
    >>> c = 2
    >>> insertArgument(a, b, c)
    [1, 2, ['a', 'b'], 3, 4, 5]
    """
    if arg == '' or arg == []:
        pass
    if pos == -1:
        arg_list.insert(len(arg_list), arg)
    elif pos == -100:
        pass
    elif pos < -1 and pos > -99:
        arg_list.insert(pos+1, arg)
    elif pos >= 0:
        arg_list.insert(pos, arg)
    else:
        pass
    return arg_list


def flattenList( arg_list_2d ):
    """ Flattens a list containing some lists into a long list. List can be 3 layers deep.

    >>> a = [1, 2, 3, 4]
    >>> b = [5, 6, 7]
    >>> c = [8, 9, 10]
    >>> d = [a, b, c]
    >>> flattenList( d )
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    
    >>> a = [1, 2]
    >>> b = [5, 6]
    >>> c = [a, 3, 4, b, 7]
    >>> flattenList( c )
    [1, 2, 3, 4, 5, 6, 7]
    """
    flat_list = []
    # Iterate through the outer list
    for e in arg_list_2d:
        if type(e) is list:
            # If the element is of type list, iterate through the sublist
            for e2 in e:
                if type(e2) is list:
                    for e3 in e2:
                        flat_list = flat_list + [e3] if e3 != '' else flat_list
                else:
                    flat_list = flat_list + [e2] if e2 != '' else flat_list
        else:
            flat_list = flat_list + [e] if e != '' else flat_list
    return flat_list


def getModuleVersion( _module ):
    # placeholder - just return constant for now
    return '20211219'


def parseRunInput( run_input_arguments ):
    """ Given the raw input for running a batch job, parses the run input and creates a run arguments JSON.

    >>> parseRunInput('mpileup -sampleid=MYSAMPLE -input=s3://bams/my.bam -output="s3://pileup/my.pileup" -pargs="-q 20 -Q 20 -u -f s3://fasta/my.fasta" -dryrun')
    {'program_name': 'mpileup', 'program_subname': '', 'sampleid': 'MYSAMPLE', 'input': 's3://bams/my.bam', 'output': 's3://pileup/my.pileup', 'pargs': '-q 20 -Q 20 -u -f s3://fasta/my.fasta', 'dryrun': ''}

    """
    run_args_JSON = {'program_name': '', 'program_subname': ''}
    rargs_list = run_input_arguments.split(' ')
    try:
        # get program name
        if rargs_list[0][0] == '-':
            print("ERROR: first run argument needs to be a program name: e.g., bwa mem")
            raise IOError
        else:
            run_args_JSON['program_name'] = rargs_list[0]
            rargs_list = rargs_list[1:]
        # get program subname, if provided
        if rargs_list[0][0] != '-':
            run_args_JSON['program_subname'] = rargs_list[0]
            rargs_list = rargs_list[1:]
        # get rest of arguments - arguments themselves cannot have quotes
        while len(rargs_list) > 0:
            if rargs_list[0][0] != '-':
                print("ERROR: "+str(rargs_list[0])+" is not a valid run argument. Every argument needs to begin with -")
                raise IOError
            elif rargs_list[0][0] == '-' and 'pargs' not in rargs_list[0]:
                arg_tuple = str(rargs_list[0].lstrip('-')).split('=')
                run_args_JSON[arg_tuple[0]] = str(arg_tuple[1].lstrip('"').lstrip("'").rstrip('"').rstrip("'")) if len(arg_tuple) > 1 else ''
            elif rargs_list[0][0] == '-' and 'pargs' in rargs_list[0]:
                rargs_list[0] = rargs_list[0].split('=')[-1].lstrip('"').lstrip("'")
                pargs = ''
                # read all program arguments
                while '"' not in rargs_list[0] and "'" not in rargs_list[0]:
                    pargs += rargs_list[0] + ' '
                    rargs_list = rargs_list[1:]
                pargs += rargs_list[0].rstrip('"').rstrip("'")
                run_args_JSON['pargs'] = pargs
            rargs_list = rargs_list[1:]
    
    except IOError:
        print("ERROR: Cannot parse job input arguments - please check: "+str(run_input_arguments))
    return run_args_JSON


def isDryRun( args_json ):
    if 'dryrun' in args_json:
        return True
    else:
        return False
    

def createIOJSON( run_args_json ):
    """ Given run args JSON created by parseRunInput(), creates an IO JSON.
         RUN ARGS JSON:
         {'program_name': 'mpileup', 'program_subname': '', 'input': 's3://bams/my.bam', 'output': 's3://pileup/my.pileup', 'alternate_inputs': 'input1.fasta,input2.bed'...
         or
         {'program_name': 'mpileup', 'program_subname': '', 'input': 'my.bam', 'inputdir': 's3://bams/', 'output': 'my.pileup', 'output_dir': 's3://pileup/'...

         I/O JSON EXAMPLE:
         {'input': ['/fullpath/my-R1.fastq.gz', '/fullpath/my-R2.fastq.gz'],
          'output': ['/fullpathout/my.sam'],
          'alternate_inputs': ['/fullpath/hg38.fasta', '/fullpath/hg38.targets.bed'],
          'program_arguments': '...',
          'sample_id': ''}
    
    >>> createIOJSON( {'sampleid': 'MYSAMPLE', 'program_name': 'mpileup', 'program_subname': '', 'input': 's3://bams/my.bam', 'output': 's3://pileup/my.pileup', 'alternate_inputs': 'input1.fasta,input2.bed', 'dryrun': ''} )
    {'input': ['s3://bams/my.bam'], 'output': ['s3://pileup/my.pileup'], 'alternate_inputs': ['input1.fasta', 'input2.bed'], 'alternate_outputs': [], 'program_arguments': '', 'sample_id': 'MYSAMPLE', 'dryrun': ''}
    >>> createIOJSON( {'sampleid': 'MYSAMPLE', 'program_name': 'mpileup', 'program_subname': '', 'input': 'my.bam', 'output': 'my.pileup', 'inputdir': 's3://bams', 'outputdir': 's3://pileup', 'alternate_inputs': 's3://fasta/input1.fasta,s3://bed/input2.bed', 'dryrun': ''} )
    {'input': ['s3://bams/my.bam'], 'output': ['s3://pileup/my.pileup'], 'alternate_inputs': ['s3://fasta/input1.fasta', 's3://bed/input2.bed'], 'alternate_outputs': [], 'program_arguments': '', 'sample_id': 'MYSAMPLE', 'dryrun': ''}
    """
    io_json = {'input': [], 'output': [], 'alternate_inputs': [], 'alternate_outputs': [], 'program_arguments': '', 'sample_id': ''}
    try:
        input_list = run_args_json['input'].split(',')
        input_list_final = []
        for _input in input_list:
            # input file name contains full path
            if '/' in _input:
                input_list_final.append(_input)
            else:
                if 'inputdir' not in run_args_json:
                    print('ERROR: inputdir needs to be specified.')
                    raise IOError
                input_list_final.append(os.path.join(run_args_json['inputdir'], _input))
        io_json['input'] = input_list_final

        output_list = run_args_json['output'].split(',')
        output_list_final = []
        for _output in output_list:
            # output file name contains full path
            if '/' in _output:
                output_list_final.append(_output)
            else:
                if 'outputdir' not in run_args_json:
                    print('ERROR: outputdir needs to be specified.')
                    raise IOError
                output_list_final.append(os.path.join(run_args_json['outputdir'], _output))
        io_json['output'] = output_list_final

        alternate_inputs_list = run_args_json['alternate_inputs'].split(',') if 'alternate_inputs' in run_args_json else []
        io_json['alternate_inputs'] = alternate_inputs_list

        alternate_outputs_list = run_args_json['alternate_outputs'].split(',') if 'alternate_outputs' in run_args_json else []
        io_json['alternate_outputs'] = alternate_outputs_list
        
        io_json['program_arguments'] = run_args_json['pargs'] if 'pargs' in run_args_json else ''
        if 'sampleid' in run_args_json:
            io_json['sample_id'] = run_args_json['sampleid']
        elif 'sample_id' in run_args_json:
            io_json['sample_id'] = run_args_json['sample_id']
        else:
            io_json['sample_id'] = ''

        if 'dryrun' in run_args_json:
            io_json['dryrun'] = ''
    
    except IOError:
        print('RUN ARGUMENTS NOT SPECIFIED CORRECTLY.')
    return io_json


def createModuleInstanceJSON( module_template_json, io_json, file_system = 's3' ):
    """ Given a module template (each program has a module template) and a list of input/output files,
    create a module instance JSON, from which program arguments will eventually be written.

    MODULE TEMPLATE EXAMPLE:
    {'program_name': 'bwa',
     'program_subname': 'mem',
     'program_version': '0.7.17',
     'program_arguments': '...',
     'program_input': [{'input_type': 'file',
                       'input_file_type': 'FASTQ',
                       'input_position': -1,
                       'input_prefix': '-i'},
                      {'input_type': 'file',
                       'input_file_type': 'FASTQ.GZ',
                       'input_position': -1,
                       'input_prefix': '-i'}]
     'program_output': [{'output_type': 'file',
                       'output_file_type': 'SAM',
                       'output_position': 0,
                       'output_prefix': '-o'}]
     'alternate_inputs': [{'input_type': 'file',
                           'input_file_type': 'BED',
                           'input_position': 0,
                           'input_prefix': '-L'},
                          },
                          {'input_type': 'file',
                           'input_file_type': 'FASTA',
                           'input_position': -2,
                           'input_prefix': ''},
                          }],
     'alternate_outputs': [],
     }

     I/O JSON EXAMPLE:
     {'input': ['/fullpath/my-R1.fastq.gz', '/fullpath/my-R2.fastq.gz'],
      'output': ['/fullpathout/my.sam'],
      'alternate_inputs': ['/fullpath/hg38.fasta', '/fullpath/hg38.targets.bed']}

    >>> io_json = {'input': ['s3://fastq/my.fastq'], 'output': ['s3://align/my.sam'], 'alternate_inputs': ['s3://fasta/input1.fasta', 's3://bed/input2.bed'], 'alternate_outputs': [], 'program_arguments': '', 'sample_id': 'MYSAMPLE', 'dryrun': ''}
    >>> mi_template_json = {'program_name': 'bwa', 'program_subname': 'mem', 'program_version': '0.7.17', 'program_arguments': '-S -t 4', 'program_input': [{'input_type': 'file', 'input_file_type': 'FASTQ', 'input_position': -1, 'input_prefix': '-i'}, {'input_type': 'file', 'input_file_type': 'FASTQ.GZ', 'input_position': -1, 'input_prefix': '-i'}], 'program_output': [{'output_type': 'file', 'output_file_type': 'SAM', 'output_position': 0, 'output_prefix': '-o'}], 'alternate_inputs': [{'input_type': 'file', 'input_file_type': 'BED', 'input_position': 0, 'input_prefix': '-L'}, {'input_type': 'file', 'input_file_type': 'FASTA', 'input_position': -2, 'input_prefix': ''}], 'alternate_outputs': []}
    >>> createModuleInstanceJSON( mi_template_json, io_json )
    {'program_input': {'input': ['my.fastq'], 'input_type': 'file', 'input_file_type': 'FASTQ', 'input_directory': 's3://fastq/', 'input_position': -1, 'input_prefix': '-i'}, 'program_output': {'output': ['my.sam'], 'output_type': 'file', 'output_file_type': 'SAM', 'output_directory': 's3://align/', 'output_position': 0, 'output_prefix': '-o'}, 'alternate_inputs': [{'input': 'input1.fasta', 'input_type': 'file', 'input_file_type': 'FASTA', 'input_directory': 's3://fasta/', 'input_position': -2, 'input_prefix': ''}, {'input': 'input2.bed', 'input_type': 'file', 'input_file_type': 'BED', 'input_directory': 's3://bed/', 'input_position': 0, 'input_prefix': '-L'}], 'alternate_outputs': [], 'program_name': 'bwa', 'program_subname': 'mem', 'program_version': '0.7.17', 'program_arguments': '-S -t 4', 'sample_id': 'MYSAMPLE', 'dryrun': ''}
    
    """
    def getDirectory( input_file, input_dir ):
        # if full path is embedded in input file name
        if '/' in str(input_file):
            return file_utils.getFileFolder(input_file)
        # if full path is specified in input_dir
        elif input_dir != '':
            return input_dir
        else:
            print('ERROR in getDirectory(): DIRECTORY NOT SPECIFIED FOR {} {}.'.format(input_file, input_dir))
            return ''
    
    mi_json = {'program_input': {}, 'program_output': {}, 'alternate_inputs': [], 'alternate_outputs': []}
    mi_json['program_name'] = module_template_json['program_name']
    mi_json['program_subname'] = module_template_json['program_subname']
    mi_json['program_version'] = module_template_json['program_version']
    mi_json['program_arguments'] = io_json['program_arguments'] if io_json['program_arguments'] != '' else module_template_json['program_arguments']
    mi_json['sample_id'] = io_json['sample_id']
    for pi in module_template_json['program_input']:
        print('INPUT INFER FILE TYPES: {} vs {}'.format(file_utils.inferFileType(io_json['input']).upper(), pi['input_file_type'].upper()))
        if file_utils.inferFileType(io_json['input']).upper() == pi['input_file_type'].upper():
            mi_json['program_input'] = {'input': file_utils.getFileOnly(io_json['input']),
                                        'input_type': pi['input_type'],
                                        'input_file_type': pi['input_file_type'],
                                        'input_directory': getDirectory( io_json['input'], io_json['inputdir'] if 'inputdir' in io_json else '')
                                        'input_position': pi['input_position'],
                                        'input_prefix': pi['input_prefix']}
    for pi in module_template_json['program_output']:
        if file_utils.inferFileType(io_json['output']).upper() == pi['output_file_type'].upper():
            mi_json['program_output'] = {'output': file_utils.getFileOnly(io_json['output']),
                                        'output_type': pi['output_type'],
                                        'output_file_type': pi['output_file_type'],
                                        'output_directory': getDirectory(io_json['output'], io_json['outputdir'] if 'outputdir' in io_json else '')
                                        'output_position': pi['output_position'],
                                        'output_prefix': pi['output_prefix']}
    for alt_input in io_json['alternate_inputs']:
        for pi in module_template_json['alternate_inputs']:
            if file_utils.inferFileType(alt_input).upper() == pi['input_file_type'].upper():
                mi_json['alternate_inputs'].append({'input': file_utils.getFileOnly(alt_input),
                                                    'input_type': pi['input_type'],
                                                    'input_file_type': pi['input_file_type'],
                                                    'input_directory': file_utils.getFileFolder(alt_input),
                                                    'input_position': pi['input_position'],
                                                    'input_prefix': pi['input_prefix']})
    for alt_output in io_json['alternate_outputs']:
        for pi in module_template_json['alternate_outputs']:
            if file_utils.inferFileType(alt_output).upper() == pi['output_file_type'].upper():
                mi_json['alternate_outputs'].append({'output': file_utils.getFileOnly(alt_output),
                                                    'output_type': pi['output_type'],
                                                    'output_file_type': pi['output_file_type'],
                                                    'output_directory': file_utils.getFileFolder(alt_output),
                                                    'output_position': pi['output_position'],
                                                    'output_prefix': pi['output_prefix']})

    if 'dryrun' in io_json:
        mi_json['dryrun'] = ''
    
    return mi_json


def getOutputDirectory( mi_json ):
    return mi_json['program_output']['output_directory']


def getOutputFile( mi_json ):
    return mi_json['program_output']['output']


def createProgramArguments( module_instance_json, input_working_dir, output_working_dir, rtype = 'str', mock = False ):
    """ Given an instance of a module, create program arguments to be run by the program.
    Needs local input and output directories to read/write files to.
    Starts with "pre-"program_arguments from JSON and add input, output and alternate files to complete program arguments.
    'alternate_inputs': [{'inputs': [LIST]
                          'input_type': 'file/folder',
                          'input_file_type': 'BED/FASTA/...',
                          'input_directory': STRING,
                          'input_position': 0,
                          'input_prefix': '-L'},
                         },...]

    rtype: return type. 'list' or 'str'
    
    A given docker module will have one or more module instance JSONs from which programs will be run in succession.

    Need to determine correct order to insert arguments for: 'program_input', 'program_output', 'alternate_inputs', 'alternate_outputs'
    
    >>> mi_json = {'program_input': {'input': ['my.fastq'], 'input_type': 'file', 'input_file_type': 'FASTQ', 'input_directory': 's3://fastq/', 'input_position': -1, 'input_prefix': ''}, 'program_output': {'output': ['my.sam'], 'output_type': 'file', 'output_file_type': 'SAM', 'output_directory': 's3://align/', 'output_position': 0, 'output_prefix': '-o'}, 'alternate_inputs': [{'input': 'input1.fasta', 'input_type': 'file', 'input_file_type': 'FASTA', 'input_directory': 's3://fasta/', 'input_position': -2, 'input_prefix': ''}, {'input': 'input2.bed', 'input_type': 'file', 'input_file_type': 'BED', 'input_directory': 's3://bed/', 'input_position': 0, 'input_prefix': '-L'}], 'alternate_outputs': [], 'program_name': 'bwa', 'program_subname': 'mem', 'program_version': '0.7.17', 'program_arguments': '-S -t 4', 'sample_id': 'MYSAMPLE', 'dryrun': ''}
    >>> createProgramArguments( mi_json, '/data/input_folder/', '/data/output_folder/', 'str', True )
    'bwa mem -L /data/input_folder/input2.bed -o /data/output_folder/my.sam -S -t 4 /data/input_folder/input1.fasta /data/input_folder/my.fastq -dryrun'
    """
    
    def determineInputOutputOrder( mi_json ):
        # need to start from outside and go inside
        main_input_pos = mi_json['program_input']['input_position']
        main_output_pos = mi_json['program_output']['output_position']
        alt_input_poss = [alt_i['input_position'] for alt_i in mi_json['alternate_inputs']]
        alt_output_poss = [alt_o['output_position'] for alt_o in mi_json['alternate_outputs']]        
        return 0
    
    pargs = ''
    pargs_list = []
    # load JSON containing all program argument info
    mi_json = module_instance_json # json.load(module_instance_json)
    
    # start with program arguments in JSON - these are grabbed from defaults and/or input by user. Convert to list.
    pargs_list = mi_json['program_arguments'].split(' ')
    
    # determine the correct order to insert input and output arguments
    
    # add primary input files
    input_json = mi_json['program_input']
    if input_json['input_type'].lower() == 'folder':
        pargs_list = insertArgument(pargs_list, \
                                    [input_json['input_prefix'], \
                                     file_utils.downloadFolder(file_utils.getFullPath(input_json['input_directory'], input_json['input']), \
                                                               input_working_dir, \
                                                               file_utils.inferFileSystem(input_json['input_directory']), \
                                                               mock)], \
                                    input_json['input_position'])
    else: # input_json['input_type'].lower() == 'file':
        pargs_list = insertArgument(pargs_list, \
                                    [input_json['input_prefix'], \
                                     file_utils.downloadFiles(file_utils.getFullPath(input_json['input_directory'], input_json['input']), \
                                                              input_working_dir, \
                                                              file_utils.inferFileSystem(input_json['input_directory']), \
                                                              mock)], \
                                     input_json['input_position'])
    
    # add primary output files
    output_json = mi_json['program_output']
    pargs_list = insertArgument(pargs_list, \
                                [output_json['output_prefix'], file_utils.getFullPath(output_working_dir, output_json['output'])], \
                                output_json['output_position'])
    
    # add alternate input files
    for alt_input_json in mi_json['alternate_inputs']:
        if alt_input_json['input_type'].lower() == 'folder':
            pargs_list = insertArgument(pargs_list, \
                                        [alt_input_json['input_prefix'], \
                                         file_utils.downloadFolder(file_utils.getFullPath(alt_input_json['input_directory'], alt_input_json['input']), \
                                                                   input_working_dir, \
                                                                   file_utils.inferFileSystem(alt_input_json['input_directory']), \
                                                                   mock)], \
                                         alt_input_json['input_position'])
        else: # alt_input_json['input_type'].lower() == 'file':
            pargs_list = insertArgument(pargs_list, \
                                        [alt_input_json['input_prefix'],
                                         file_utils.downloadFiles(file_utils.getFullPath(alt_input_json['input_directory'], alt_input_json['input']), \
                                                                  input_working_dir, \
                                                                  file_utils.inferFileSystem(alt_input_json['input_directory']), \
                                                                  mock)], \
                                        alt_input_json['input_position'])
    
    # add alternate output files
    for alt_output_json in mi_json['alternate_outputs']:
        pargs_list = insertArgument(pargs_list, \
                                    [alt_output_json['output_prefix'], file_utils.getFullPath(output_working_dir, alt_output_json['output'])], \
                                    alt_output_json['output_position'])
    
    # finally insert program (+ subprogram) name
    pargs_list = insertArgument(pargs_list, mi_json['program_subname'], 0)
    pargs_list = insertArgument(pargs_list, mi_json['program_name'], 0)
    
    # convert list back to string of arguments
    pargs_string_final = ''
    if rtype[0:3].lower() == 'str':
        pargs_string_final = ' '.join(flattenList(pargs_list))
    else:
        pargs_string_final = flattenList(pargs_list)
    
    # if dryrun then indicate so
    if 'dryrun' in mi_json:
        pargs_string_final += ' -dryrun'
    
    return pargs_string_final


def runProgram( pargs, fout_name = ''):
    """ Given a string of full program arguments (including program name), run the program.

    pargs: STRING
    fout_name: file name to output, otherwise empty string

    >>> runProgram('bwa mem -L s3://bed/input2.bed -o s3://align/my.sam -S -t 4 s3://fasta/input1.fasta s3://fastq/my.fastq -dryrun')
    DRYRUN - NOTHING SUBMITTED: bwa mem -L s3://bed/input2.bed -o s3://align/my.sam -S -t 4 s3://fasta/input1.fasta s3://fastq/my.fastq -dryrun
    ''
    
    """
    if '-dryrun' in pargs:
        print('DRYRUN - NOTHING SUBMITTED: '+str(pargs))
    elif fout_name != '':
        with open(fout_name,'w') as fout:
            subprocess.check_call(pargs.split(' '), stdout=fout)
    else:
        subprocess.check_call(pargs.split(' '))
    return fout_name


def startProgram( pargs, fout_name ):
    return runProgram( pargs, fout_name )
