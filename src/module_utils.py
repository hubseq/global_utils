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

Jerry Chen
December 5, 2021

Each docker module (app) should specify program arguments. Should be in exact order as program is called.

TEMPLATE DIR: /npimodules/templates/
"""

MODULE_TEMPLATE_PATH = 's3://hubseq-data/templates/' 
MODULE_DIR = 's3://hubseq-data/modules/'

def getModuleDirectory():
    return MODULE_DIR

def getModuleIODirectory( module ):
    return os.path.join( getModuleDirectory(), module, 'io/' )

def getModuleJobDirectory( module ):
    return os.path.join( getModuleDirectory(), module, 'job/' )

def getModuleRunIOFilePath( module, job_id ):
    return os.path.join( getModuleIODirectory(module), getModuleRunNameID( module, job_id, 'io_json' ))

def getModuleRunJobFilePath( module, job_id ):
    return os.path.join( getModuleJobDirectory(module), getModuleRunNameID( module, job_id, 'job_json' ))

def getModuleRunIOFileJSON( module, job_id, local_dir ):
    return file_utils.loadJSON( file_utils.downloadFile( getModuleRunIOFilePath(module, job_id), local_dir ))

def getModuleRunJobFileJSON( module, job_id, local_dir ):
    if '_test' in job_id and 'dryrun' in job_id:
        return {}
    else:
        return file_utils.loadJSON( file_utils.downloadFile( getModuleRunJobFilePath(module, job_id), local_dir ))


def getModuleRunNameID( module, job_id, name_type ):
    """ Returns a unique ID or file name for a given run (job) of a module.
    There are different types of IDs / file names needed for a given job.
    """
    if name_type == 'io_json':
        return module+'.'+job_id+'.io.json'
    elif name_type == 'job_json':
        return module+'.'+job_id+'.job.json'
    elif name_type == 'job_name':
        return 'job_{}_{}'.format(module, job_id)
    elif name_type == 'job_def':
        return 'jdef_{}_{}'.format(module, job_id)
    else:
        return '{}.{}'.format(module, job_id)

def getModuleTemplateLocation( which_module ):
    return getModuleTemplate( which_module )


def getModuleTemplate( which_module, which_submodule = '', module_basedir = MODULE_TEMPLATE_PATH ):
    """ Returns the template module JSON file path for input docker module
    """
    if which_submodule == '':
        return os.path.join(module_basedir, which_module+'.template.json')
    else:
        return os.path.join(module_basedir, which_module+'.'+str(which_submodule)+'.template.json')        


def downloadModuleTemplate( which_module, dest_folder, which_submodule = '', filesystem = 's3' ):
    """ Downloads the module template to the destination directory.
        If local, assumes the module template file is in the same directory as the calling function.
    """
    if filesystem == 's3':
        module_template_file = getModuleTemplate( which_module, which_submodule, MODULE_TEMPLATE_PATH )
        module_template_path = file_utils.downloadFile( module_template_file, dest_folder )
    elif filesystem == 'local':
        module_template_path = getModuleTemplate( which_module, which_submodule, os.getcwd() )
    else:
        module_template_path = ''
    return module_template_path
    

def getModuleTemplateInputFileTypes( template_file ):
    """ Given the path to a downloaded module template file, get a list of the possible input file types.
    """
    module_template_json = file_utils.loadJSON(template_file)
    input_file_types = []
    for pi in module_template_json['program_input']:
        if 'input_file_type' in pi:
            input_file_types.append(str(pi['input_file_type']).lower())
    return input_file_types


def getModuleTemplateOutputFileTypes( template_file ):
    """ Given the path to a downloaded module template file, get a list of the possible output file types.
    """
    module_template_json = file_utils.loadJSON(template_file)
    output_file_types = []
    for pi in module_template_json['program_output']:
        if 'output_file_type' in pi:
            output_file_types.append(str(pi['output_file_type']).lower())
    return output_file_types


def getModuleTemplateAltInputFileTypes( template_file ):
    """ Given the path to a downloaded module template file, get a list of the possible alternate input file types.
    """
    module_template_json = file_utils.loadJSON(template_file)
    alt_input_file_types = []
    for pi in module_template_json['alternate_inputs']:
        if 'input_file_type' in pi:        
            alt_input_file_types.append(str(pi['input_file_type']).lower())
    return alt_input_file_types


def getModuleTemplateAltOutputFileTypes( template_file ):
    """ Given the path to a downloaded module template file, get a list of the possible alternate output file types.
    """
    module_template_json = file_utils.loadJSON(template_file)
    alt_output_file_types = []
    for pi in module_template_json['alternate_outputs']:
        if 'output_file_type' in pi:    
            alt_output_file_types.append(str(pi['output_file_type']).lower())
    return alt_output_file_types

def getModuleTemplateDefaults( template_file ):
    """ Given the path to a downloaded module template file, get a list of default arguments.
    """
    module_template_json = file_utils.loadJSON(template_file)
    defaults = module_template_json['defaults'] if 'defaults' in module_template_json else {}
    return defaults

def getModuleTemplateDefaultOutput( template_file ):
    defaults = getModuleTemplateDefaults( template_file )
    return defaults['output_file'] if 'output_file' in defaults else ''

def getModuleTemplateDefaultAltInputs( template_file ):
    defaults = getModuleTemplateDefaults( template_file )
    out = defaults['alternate_inputs'] if 'alternate_inputs' in defaults else ''
    if type(out) == type([]):
        out = ','.join(out)
    return out

def getModuleTemplateDefaultAltOutputs( template_file ):
    defaults = getModuleTemplateDefaults( template_file )
    out = defaults['alternate_outputs'] if 'alternate_outputs' in defaults else ''
    if type(out) == type([]):
        out = ','.join(out)
    return out

def getModule_vcpus( module_json ):
    return module_json['compute']['vcpus'] if ('compute' in module_json and 'vcpus' in module_json['compute']) else 1


def getModule_memory( module_json ):
    return module_json['compute']['memory'] if ('compute' in module_json and 'memory' in module_json['compute']) else 2000


def getModule_environment( module_json ):
    return module_json['compute']['environment'] if ('compute' in module_json and 'environment' in module_json['compute']) else 'aws'



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
    file_path_group.add_argument('--submodule_name', help='name of subprogram in module', required=False, default='')
    file_path_group.add_argument('--run_arguments', help='path to run_arguments.json', required=True)
    file_path_group.add_argument('--working_dir', help='working data directory for docker run', required=True)
    args = argparser.parse_args()
    return args


def getSubprogram( rargs ):
    """ Given the run arguments as a JSON, returns the value of the subprogram key.
    """
    return str(rargs['program_subname'])


def getRunProgramArguments( rargs ):
    """ Given the run arguments as a JSON, returns the value of the program_arguments key.
    """
    return str(rargs['program_arguments'])

    
def getArgument(pargs, arg_tag, return_type = 'implicit', default_value = '' ):
    """ Given a list of program arguments, gets the argument(s) after a given tag. Can pass a Python list or command-line string.
    By default, returns a list of there is more than one program argument, otherwise returns a string.
    Can force a list return type by return_type = 'list'
    default_value is the default value to return if arg_tag is not found.
    
    >>> getArgument( ['bwa','mem','-i','my.fastq','-o','my.bam'], '-o' )
    'my.bam'
    >>> getArgument( ['bwa','mem','-i','my.R1.fastq','my.R2.fastq','-o','my.bam'], '-i' )
    ['my.R1.fastq', 'my.R2.fastq']
    >>> getArgument( ['bwa','mem','-i','my.R1.fastq','my.R2.fastq','-o','my.bam'], '-p' )
    ''
    """
    # convert command-line string to list
    if type(pargs) == str:
        pargs = pargs.split(' ')
    # find the next argument
    if arg_tag in pargs:
        return_args = []
        pargs_parse = pargs[pargs.index(arg_tag)+1:]
        while len(pargs_parse) > 0 and pargs_parse[0][0] != '-':
            return_args.append(pargs_parse[0])
            pargs_parse = pargs_parse[1:]
        if len(return_args) > 1 or return_type.lower() == 'list':
            return return_args
        elif len(return_args) == 1:
            return return_args[0]
        else:
            return default_value if return_type != 'list' else []
    else:
        return default_value if return_type != 'list' else []
    

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

    >>> a = [1, 2, 3]
    >>> b = ['-i', ['R1.fastq.gz', 'R2.fastq.gz']]
    >>> c = -1
    >>> insertArgument(a, b, c)
    [1, 2, 3, ['-i', ['R1.fastq.gz', 'R2.fastq.gz']]]
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


def isDryRun( args_json ):
    if 'dryrun' in args_json and args_json['dryrun'] == True:
        return True
    else:
        return False
    

def removeBlanks( L ):
    """ Remove empty strings from a list of strings
    """
    return [i for i in L if i]


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
    #def inferSampleID( input_file ):
    #    sid = ''
    #    if type(input_file) == type([]) and input_file != []:
    #        sid = input_file[0].split('/')[-1].split('.')[0]
    #    elif input_file != [] and input_file != '':
    #        sid = input_file.split('/')[-1].split('.')[0]            
    #    return sid

    def formatIOFile( io_file, sid ):
        # if no file name is specified, use sample ID
        if '.' in io_file.split('/')[-1]:
            return io_file
        else:
            return os.path.join(io_file, sid+'.'+str(file_utils.inferFileType( io_file )).lower())
    
    io_json = {'input': [], 'output': [], 'alternate_inputs': [], 'alternate_outputs': [], 'program_arguments': '', 'sample_id': ''}
    try:
        # GET SAMPLE ID FIRST
        if 'sampleid' in run_args_json:
            io_json['sample_id'] = run_args_json['sampleid']
        elif 'sample_id' in run_args_json:
            io_json['sample_id'] = run_args_json['sample_id']
        else:
            io_json['sample_id'] = file_utils.inferSampleID(run_args_json['input'])
        
        input_list = run_args_json['input'].split(',') if type(run_args_json['input']) == str else run_args_json['input']
        input_list_final = []
        for _input in input_list:
            # input file name contains full path
            if '/' in _input:
                input_list_final.append(_input)
            else:
                # input only conatins file name - needs corresponding inputdir
                if 'inputdir' not in run_args_json:
                    print('ERROR: inputdir needs to be specified.')
                    raise IOError
                input_list_final.append(file_utils.getFullPath(run_args_json['inputdir'], _input))
        io_json['input'] = input_list_final
        
        output_list = run_args_json['output'].split(',') if type(run_args_json['output']) == str else run_args_json['output']
        output_list_final = []
        for _output in output_list:
            # output file name contains full path
            if '/' in _output:
                output_list_final.append(_output)
                # output_list_final.append(formatIOFile(_output, io_json['sample_id']))
            else:
                # output only contains file name - needs corresponding outputdir
                if 'outputdir' not in run_args_json:
                    # if no outputdir is specified, assume it is same as inputdir
                    output_list_final.append(file_utils.getFullPath(run_args_json['inputdir'], _output))                    
                # output_list_final.append(os.path.join(run_args_json['outputdir'], formatIOFile(_output, io_json['sample_id'])))
                output_list_final.append(file_utils.getFullPath(run_args_json['outputdir'], _output))
        io_json['output'] = output_list_final
        
        alternate_inputs_list = run_args_json['alternate_inputs'].split(',') if 'alternate_inputs' in run_args_json else []
        io_json['alternate_inputs'] = removeBlanks(alternate_inputs_list)

        alternate_outputs_list = run_args_json['alternate_outputs'].split(',') if 'alternate_outputs' in run_args_json else []
        io_json['alternate_outputs'] = removeBlanks(alternate_outputs_list)
        
        io_json['program_arguments'] = run_args_json['pargs'] if ('pargs' in run_args_json and run_args_json['pargs'] not in ['', None]) else ''
        
        # subprogram to run, if given
        io_json['program_subname'] = run_args_json['program_subname'] if ('program_subname' in run_args_json) else ''
        
        # program options - module-specific
        if ('options' in run_args_json and run_args_json['options'] != ''):
            io_json['options'] = run_args_json['options']
        
        if ('dryrun' in run_args_json and run_args_json['dryrun'] == ''):
            io_json['dryrun'] = run_args_json['dryrun'] 
    
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
    >>> mi_template_json = {'module_version': '00.00.00', 'program_name': 'bwa', 'program_subname': 'mem', 'program_version': '0.7.17', 'program_arguments': '-S -t 4', 'program_input': [{'input_type': 'file', 'input_file_type': 'FASTQ', 'input_position': -1, 'input_prefix': '-i'}, {'input_type': 'file', 'input_file_type': 'FASTQ.GZ', 'input_position': -1, 'input_prefix': '-i'}], 'program_output': [{'output_type': 'file', 'output_file_type': 'SAM', 'output_position': 0, 'output_prefix': '-o'}], 'alternate_inputs': [{'input_type': 'file', 'input_file_type': 'BED', 'input_position': 0, 'input_prefix': '-L'}, {'input_type': 'file', 'input_file_type': 'FASTA', 'input_position': -2, 'input_prefix': ''}], 'alternate_outputs': []}
    >>> createModuleInstanceJSON( mi_template_json, io_json )
    {'program_input': {'input': ['my.fastq'], 'input_type': 'file', 'input_file_type': 'FASTQ', 'input_directory': 's3://fastq/', 'input_position': -1, 'input_prefix': '-i'}, 'program_output': {'output': ['my.sam'], 'output_type': 'file', 'output_file_type': 'SAM', 'output_directory': 's3://align/', 'output_position': 0, 'output_prefix': '-o'}, 'alternate_inputs': [{'input': 'input1.fasta', 'input_type': 'file', 'input_file_type': 'FASTA', 'input_directory': 's3://fasta/', 'input_position': -2, 'input_prefix': ''}, {'input': 'input2.bed', 'input_type': 'file', 'input_file_type': 'BED', 'input_directory': 's3://bed/', 'input_position': 0, 'input_prefix': '-L'}], 'alternate_outputs': [], 'options': '', 'program_name': 'bwa', 'program_subname': 'mem', 'program_version': '0.7.17', 'module_version': '00.00.00', 'program_arguments': '-S -t 4', 'sample_id': 'MYSAMPLE', 'dryrun': ''}

    >>> io_json = {'input': ['s3://npipublicinternal/test/bcl/test.samplesheet.csv'], 'output': ['s3://npipublicinternal/test/bcl_out/'], 'alternate_inputs': [], 'alternate_outputs': [], 'program_arguments': '', 'sample_id': 'MYRUN', 'dryrun': ''}
    >>> mi_template_json = {'module_version': '00.00.00', 'program_name': 'bcl2fastq', 'program_subname': '', 'program_version': '2.20.0', 'program_arguments': '-R /home/module_out/', 'program_input': [{'input_type': 'file', 'input_file_type': 'CSV', 'input_position': -1, 'input_prefix': '--sample-sheet'}], 'program_output': [{'output_type': 'folder', 'output_file_type': '', 'output_position': 0, 'output_prefix': '-o'}], 'alternate_inputs': [], 'alternate_outputs': []}
    >>> createModuleInstanceJSON( mi_template_json, io_json )
    {'program_input': {'input': ['test.samplesheet.csv'], 'input_type': 'file', 'input_file_type': 'CSV', 'input_directory': 's3://npipublicinternal/test/bcl/', 'input_position': -1, 'input_prefix': '--sample-sheet'}, 'program_output': {'output': [''], 'output_type': 'folder', 'output_file_type': '', 'output_directory': 's3://npipublicinternal/test/bcl_out/', 'output_position': 0, 'output_prefix': '-o'}, 'alternate_inputs': [], 'alternate_outputs': [], 'options': '', 'program_name': 'bcl2fastq', 'program_subname': '', 'program_version': '2.20.0', 'module_version': '00.00.00', 'program_arguments': '-R /home/module_out/', 'sample_id': 'MYRUN', 'dryrun': ''}
    
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
    
    mi_json = {'program_input': {}, 'program_output': {}, 'alternate_inputs': [], 'alternate_outputs': [], 'options': ''}
    mi_json['program_name'] = module_template_json['program_name']
    mi_json['program_subname'] = io_json['program_subname'] if ('program_subname' in io_json and io_json['program_subname'] != '') else module_template_json['program_subname']
    mi_json['program_version'] = module_template_json['program_version']
    mi_json['module_version'] = module_template_json['module_version']    
    mi_json['program_arguments'] = io_json['program_arguments'] if io_json['program_arguments'] not in ['', None] else module_template_json['program_arguments']
    mi_json['options'] = io_json['options'] if ('options' in io_json and io_json['options'] != '') \
                                            else (module_template_json['options'] if ('options' in module_template_json) else '')
    mi_json['sample_id'] = io_json['sample_id']
    # main input
    for pi in module_template_json['program_input']:
        if file_utils.inferFileType(io_json['input']).upper() == pi['input_file_type'].upper():
            mi_json['program_input'] = {'input': io_json['input'], \
                                        'input_type': pi['input_type'],
                                        'input_file_type': pi['input_file_type'],
                                        'input_directory': io_json['inputdir'] if 'inputdir' in io_json else '',
                                        'input_position': pi['input_position'],
                                        'input_prefix': pi['input_prefix']}
    # main output
    for pi in module_template_json['program_output']:
        if file_utils.inferFileType(io_json['output']).upper() == pi['output_file_type'].upper():
            mi_json['program_output'] = {'output': io_json['output'], \
                                         'output_type': pi['output_type'],
                                         'output_file_type': pi['output_file_type'],
                                         'output_directory': io_json['outputdir'] if 'outputdir' in io_json else '',
                                         'output_position': pi['output_position'],
                                         'output_prefix': pi['output_prefix']}
    # alternate input - input and input_directory needs to be fixed
    for alt_input in io_json['alternate_inputs']:
        for pi in module_template_json['alternate_inputs']:
            if file_utils.inferFileType(alt_input).upper() == pi['input_file_type'].upper():
                mi_json['alternate_inputs'].append({'input': file_utils.getFileOnly(alt_input),
                                                    'input_type': pi['input_type'],
                                                    'input_file_type': pi['input_file_type'],
                                                    'input_directory': getDirectory(alt_input['input'], alt_input['inputdir']) if 'inputdir' in alt_input else file_utils.getFileFolder(alt_input),
                                                    'input_position': pi['input_position'],
                                                    'input_prefix': pi['input_prefix']})
    # altenrate output
    for alt_output in io_json['alternate_outputs']:
        for pi in module_template_json['alternate_outputs']:
            if file_utils.inferFileType(alt_output).upper() == pi['output_file_type'].upper():
                mi_json['alternate_outputs'].append({'output': file_utils.getFileOnly(alt_output),
                                                    'output_type': pi['output_type'],
                                                    'output_file_type': pi['output_file_type'],
                                                    'output_directory': getDirectory(alt_output['output'], alt_output['outputdir']) if 'outputdir' in alt_output else file_utils.getFileFolder(alt_output),
                                                    'output_position': pi['output_position'],
                                                    'output_prefix': pi['output_prefix']})

    if 'dryrun' in io_json and io_json['dryrun'] == '':
        mi_json['dryrun'] = ''
    
    return mi_json


def getInputDirectory( mi_json ):
    if 'input_directory' in mi_json['program_input'] and mi_json['program_input']['input_directory'] not in ['', None]:
        return mi_json['program_input']['input_directory']
    elif 'input' in mi_json['program_input'] and mi_json['program_input']['input'] not in ['', None]:
        return file_utils.getFileFolder(mi_json['program_input']['input'])


def getInputFile( mi_json ):
    return file_utils.getFileOnly(mi_json['program_input']['input']) if ('input' in mi_json['program_input'] and mi_json['program_input']['input'] not in ['', None]) else ''


def getOutputDirectory( mi_json ):
    if ('output_directory' in mi_json['program_output']) and (mi_json['program_output']['output_directory'] not in ['', None]):
        return mi_json['program_output']['output_directory']
    elif ('output' in mi_json['program_output']) and (mi_json['program_output']['output'] not in ['', None]):
        return file_utils.getFileFolder(mi_json['program_output']['output'])


def getOutputFile( mi_json ):
    return file_utils.getFileOnly(mi_json['program_output']['output']) if ('output' in mi_json['program_output'] and mi_json['program_output']['output'] not in ['', None]) else ''


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
    Downloading file(s) ['s3://fastq/my.fastq'] to /data/input_folder/.
    Downloading file(s) s3://fasta/input1.fasta to /data/input_folder/.
    Downloading file(s) s3://bed/input2.bed to /data/input_folder/.
    'bwa mem -L /data/input_folder/input2.bed -o /data/output_folder/my.sam -S -t 4 /data/input_folder/input1.fasta /data/input_folder/my.fastq -dryrun'

    >>> mi_json = {'program_input': {'input': ['test.samplesheet.csv'], 'input_type': 'file', 'input_file_type': 'CSV', 'input_directory': 's3://npipublicinternal/test/bcl/', 'input_position': -1, 'input_prefix': '--sample-sheet'}, 'program_output': {'output': [''], 'output_type': 'folder', 'output_file_type': '', 'output_directory': 's3://npipublicinternal/test/bcl_out/', 'output_position': 0, 'output_prefix': '-o'}, 'alternate_inputs': [], 'alternate_outputs': [], 'program_name': 'bcl2fastq', 'program_subname': '', 'program_version': '2.20.0', 'program_arguments': '--create-fastq-for-index-reads -R /home/', 'sample_id': 'MYRUN', 'dryrun': ''}
    >>> createProgramArguments( mi_json, '/home/', '/home/module_out/', 'str', True )
    Downloading file(s) ['s3://npipublicinternal/test/bcl/test.samplesheet.csv'] to /home/.
    'bcl2fastq -o /home/module_out/ --create-fastq-for-index-reads -R /home/ --sample-sheet /home/test.samplesheet.csv -dryrun'
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

    # need to handle the case where inputs are such: -1 <INPUT1> -2 <INPUT2> - in these cases we MUST specify two inputs
    if len(str(input_json['input_prefix']).split(',')) > 1 and \
       ((type(input_json['input']) == str and len(input_json['input'].split(',')) > 1) or
        (type(input_json['input']) == type([]) and len(input_json['input']) > 1)):
        input_prefixes_temp = str(input_json['input_prefix']).split(',')
        input_files_temp = input_json['input'].split(',') if type(input_json['input']) == str else input_json['input']
        for i in range(0,len(str(input_json['input_prefix']).split(','))):
            # input is a folder
            if input_json['input_type'].lower() == 'folder':
                print('INPUT FILES TEMP: '+str(input_files_temp))
                print('INPUT PREFIXES TEMP: '+str(input_prefixes_temp))                
                pargs_list = insertArgument(pargs_list, \
                                            [input_prefixes_temp[i], \
                                             file_utils.downloadFolder(file_utils.getFullPath(input_json['input_directory'], input_files_temp[i]), \
                                                                    input_working_dir, \
                                                                    file_utils.inferFileSystem(file_utils.getFullPath(input_json['input_directory'], input_files_temp[i])), \
                                                                    mock)], \
                                            input_json['input_position'])
            else: # input_json['input_type'].lower() == 'file':
                pargs_list = insertArgument(pargs_list, \
                                            [input_prefixes_temp[i], \
                                             file_utils.downloadFiles(file_utils.getFullPath(input_json['input_directory'], input_files_temp[i]), \
                                                                    input_working_dir, \
                                                                    file_utils.inferFileSystem(file_utils.getFullPath(input_json['input_directory'], input_files_temp[i])), \
                                                                    mock)], \
                                            input_json['input_position'])
    # single input
    else:
        # input is a folder
        if input_json['input_type'].lower() == 'folder':
            pargs_list = insertArgument(pargs_list, \
                                        [input_json['input_prefix'], \
                                         file_utils.downloadFolder(file_utils.getFullPath(input_json['input_directory'], input_json['input']), \
                                                                   input_working_dir, \
                                                                   file_utils.inferFileSystem(file_utils.getFullPath(input_json['input_directory'], input_json['input'])), \
                                                                   mock)], \
                                        input_json['input_position'])                      
        else: # input_json['input_type'].lower() == 'file':
            pargs_list = insertArgument(pargs_list, \
                                        [input_json['input_prefix'], \
                                         file_utils.downloadFiles(file_utils.getFullPath(input_json['input_directory'], input_json['input']), \
                                                                  input_working_dir, \
                                                                  file_utils.inferFileSystem(file_utils.getFullPath(input_json['input_directory'], input_json['input'])), \
                                                                  mock)], \
                                        input_json['input_position'])
    
    # add primary output files
    output_json = mi_json['program_output'] if 'program_output' in mi_json else {}
    if output_json != {}:
        # need to handle the case where outputs are such: -1 <OUTPUT1> -2 <OUTPUT2> - in these cases we MUST specify two outputs
        output_prefixes_temp = output_json['output_prefix'].split(',') if 'output_prefix' in output_json else []
        output_files_temp = output_json['output'].split(',') if ('output' in output_json and type(output_json['output'])==str) \
            else (output_json['output'] if ('output' in output_json and type(output_json['output'])==type([])) \
                  else [])
        
        for i in range(0,len(output_prefixes_temp)):
            pargs_list = insertArgument(pargs_list, \
                                        [output_prefixes_temp[i], \
                                         file_utils.getFullPath(output_working_dir, file_utils.getFileOnly(output_files_temp[i]))], \
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
                                    [alt_output_json['output_prefix'], file_utils.getFullPath(output_working_dir, file_utils.getFileOnly(alt_output_json['output']))], \
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


def executeProgram( pargs, fout_name = '' ):
    """ Given a string of full program arguments (including program name), execute the program command.

    pargs: STRING
    fout_name: file name to output, otherwise empty string

    >>> executeProgram('bwa mem -L s3://bed/input2.bed -o s3://align/my.sam -S -t 4 s3://fasta/input1.fasta s3://fastq/my.fastq -dryrun')
    DRYRUN - NOTHING SUBMITTED: bwa mem -L s3://bed/input2.bed -o s3://align/my.sam -S -t 4 s3://fasta/input1.fasta s3://fastq/my.fastq -dryrun
    ''
    
    """
    if '-dryrun' in pargs:
        print('DRYRUN - NOTHING SUBMITTED: '+str(pargs))
    elif fout_name != '' and '.' in fout_name.split('/')[-1]:
        with open(fout_name,'w') as fout:
            subprocess.check_call(pargs.split(' '), stdout=fout)
    else:
        subprocess.check_call(pargs.split(' '))
    return fout_name


def uploadOutput( local_out, remote_out ):
    """ Upload data output files
    """
    print('Uploading output data files...')
    file_utils.uploadFolder(local_out, remote_out)
    return


def runProgram( program_arguments, local_output_file = '' ):
    """ Runs the program specified in program arguments
    """
    # run program - this should run program w arguments via command line on local machine / container
    print('RUNNING PROGRAM...')
    print('CMD: '+str(program_arguments))
    executeProgram( program_arguments, local_output_file )
    return


def initProgram( ):
    """ Entrypoint for initializing program arguments before run.
    """
    # parse run input arguments
    args = getRunArgs( )
    
    # create a working directory
    print('Creating working directory')
    DOCKER_DIR = os.getcwd()
    WORKING_DIR = generateWorkingDir(args.working_dir)
    os.chdir(WORKING_DIR)
    OUT_DIR = os.path.join(WORKING_DIR, 'module_out')
    os.mkdir(OUT_DIR)
    
    # setup I/O
    print('Setting up I/O')
    run_arguments_file = file_utils.downloadFile(args.run_arguments, WORKING_DIR)
    run_arguments_json = file_utils.loadJSON( run_arguments_file )
    run_module_name = args.module_name
    run_submodule_name = args.submodule_name if 'submodule_name' in args and args.submodule_name not in [[], '', None] else ''
    run_job_id = str(args.run_arguments).split('/')[-1].split('.')[1]
    
    # get module template for this docker module
    module_template_path = getModuleTemplate( args.module_name, run_submodule_name )
    module_template_file = file_utils.downloadFile( module_template_path, WORKING_DIR )
    module_template_json = file_utils.loadJSON( module_template_file )
    
    # parse run arguments and create program arguments to be run via command line
    module_instance_json = createModuleInstanceJSON( module_template_json, run_arguments_json )
    print(str(module_instance_json))
    remote_input_directory = getInputDirectory( module_instance_json )
    remote_input_file = getInputFile( module_instance_json )    
    remote_output_directory = getOutputDirectory( module_instance_json )
    remote_output_file = getOutputFile( module_instance_json )
    local_input_file = file_utils.getFullPath(WORKING_DIR, remote_input_file, True)
    local_output_file = file_utils.getFullPath(OUT_DIR, remote_output_file, True)    
    program_arguments = createProgramArguments( module_instance_json, WORKING_DIR, OUT_DIR )  # files will be downloaded here
    
    run_json = {'module': run_module_name, 'submodule': run_submodule_name, \
                'run_job_id': run_job_id, 'docker_entry_dir': DOCKER_DIR, \
                'local_input_dir': WORKING_DIR, 'local_output_dir': OUT_DIR, \
                'remote_input_dir': remote_input_directory, 'remote_output_dir': remote_output_directory, \
                'local_input_file': local_input_file, 'local_output_file': local_output_file, \
                'program_arguments': program_arguments, 'run_arguments': run_arguments_json, \
                'module_instance_json': module_instance_json, 'job_json': getModuleRunJobFileJSON(run_module_name, run_job_id, WORKING_DIR)}
    
    return run_json


def logRun( run_json, output_folder ):
    """ Log all relevant metadata for this container module run
    """
    RUN_LOG_FILE = file_utils.getFullPath(output_folder, '{}.{}.job.log'.format(run_json['module'], run_json['run_job_id']))
    with open(RUN_LOG_FILE,'w') as fout:
        json.dump(run_json, fout)
    return

        
def formatModules( mlist ):
    """ Format list of modules
    """
    mod = ['bcl2fastq', 'bowtie2', 'bwamem', 'cellranger', 'cutadapt', 'david_go', 'deseq2', 'fastqc', 'homer', 'macs2',
           'qorts', 'mileup', 'rnaseqc', 'seurat', 'varscan2']
    mlist_new = []
    for m in mlist:
        if m in mod:
            mlist_new.append(m+'_saas')
        else:
            mlist_new.append(m)
    return mlist_new
