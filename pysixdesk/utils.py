import os
import io
import re
import sys
import gzip
import shutil
import logging
import difflib
import traceback

# Gobal variables
PYSIXDESK_ABSPATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def check(files):
    '''Check the existence of the files and rename them if the files is a dict
    which looks like {'file1_oldName': 'file1_newName',
    'file2_oldName': 'file2_newName'}
    '''
    status = False
    if isinstance(files, dict):
        for key, value in files.items():
            if os.path.isfile(key):
                if key != value:
                    os.rename(key, value)
            else:
                print("The file %s isn't generated successfully!" % key)
                return status
    elif isinstance(files, list):
        for key in files:
            if not os.path.isfile(key):
                print("The file %s isn't generated successfully!" % key)
                return status
    else:
        print("The input must be a list or dict!")
        return status
    status = True
    return status


def download_output(filenames, dest, zp=True):
    '''Download the requested files to the given destinaion.
    If zp is true, then zip the files before download.
    '''
    status = False
    if not os.path.isdir(dest):
        os.makedirs(dest, 0o755)

    for filename in filenames:
        if os.path.isfile(filename):
            if zp:
                out_name = os.path.join(dest, filename + '.gz')
                with open(filename, 'rb') as f_in, gzip.open(out_name, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            else:
                shutil.copy(filename, dest)
        else:
            print("The file %s doesn't exist, download failed!" % filename)
            return status
    status = True
    return status


def replace(patterns, replacements, source, dest):
    '''Reads a source file and writes the destination file.
    In each line, replaces patterns with repleacements.

    Performance can be improved by compiling the regex prior to
    the looping.
    '''
    if not os.path.isfile(source):
        raise FileNotFoundError("The file %s doesn't exist!" % source)

    with open(source, 'r') as fin:
        fin_lines = fin.readlines()

    with open(dest, 'w') as fout:
        for line in fin_lines:
            for pat, rep in zip(patterns, replacements):
                line = substitute(pat, rep, line)
            fout.write(line)


def substitute(pat, rep, line):
    '''
    If a replacement pattern is provided, match with '%PAT={number}',
    and replace with 'rep'.
    If a replacement is not provided, match with '%PAT={number}' and
    remove '%PAT='.

    The idea is to give the user making the mask file the ability to leave in
    default values, with the following syntax:
        qx0 = %QX=62.28
    (note the lack of space around the '=')
    If a replacement is provided, then replace '%Qx=62.28' with the replacement,
    otherwise, remove '%Qx='
    '''
    if rep is not None:
        # replaces '%PAT= -1.23e12' with rep
        rep_line = re.sub(fr'{pat}( *= *-?\d*\.?\d*((e|E)-?\d+)?|)', str(rep), line)
    else:
        # replaces '%PAT ={number}' with ''
        rep_line = re.sub(fr'{pat} *= *(?=(-?\d*\.?\d*((e|E)-?\d+)?))', '', line)
    return rep_line


def encode_strings(inputs):
    '''Convert list or directory to special-format string'''
    status = False
    if isinstance(inputs, list):
        output = ','.join(map(str, inputs))
    elif isinstance(inputs, dict):
        a = [':'.join(map(str, i)) for i in inputs.items()]
        output = ','.join(map(str, a))
    else:
        output = ''
        return status, output
    status = True
    return status, output


def decode_strings(inputs):
    '''Convert special-format string to list or directory'''
    status = False
    if isinstance(inputs, str):
        if ':' in inputs:
            output = {}
            a = inputs.split(',')
            for i in a:
                b = i.split(':')
                output[b[0]] = b[1]
        else:
            output = inputs.split(',')
    else:
        print("The input is not string!")
        output = []
        return status, output
    status = True
    return status, output


def compress_buf(data, source='file'):
    '''Data compression for storing in database
    The data source can be file,gzip,str'''
    status = False
    zbuf = io.BytesIO()

    if source == 'file' and os.path.exists(data):
        with gzip.GzipFile(mode='wb', fileobj=zbuf) as zfile:

            if os.path.isfile(data):
                with open(data, 'rb') as f_in:
                    buf = f_in.read()
                    zfile.write(buf)

            elif os.path.isdir(data):
                for file in os.listdir(data):
                    with open(os.path.join(data, file), 'rb') as f_in:
                        buf = f_in.read()
                        zfile.write(buf)

    elif source == 'gzip' and os.path.isfile(data):
        with open(data, 'rb') as f_in:
            shutil.copyfileobj(f_in, zbuf)

    elif source == 'str' and isinstance(data, str):
        buf = data.encode()
        with gzip.GzipFile(mode='wb', fileobj=zbuf) as zfile:
            zfile.write(buf)
    else:
        print("Invalid data source!")
        return status, zbuf.getvalue()
    status = True
    return status, zbuf.getvalue()


def decompress_buf(buf, out, des='file'):
    '''Data decompression to retrieve from database'''
    status = False
    if isinstance(buf, bytes):
        zbuf = io.BytesIO(buf)
        if des == 'file':
            with gzip.GzipFile(fileobj=zbuf) as f_in:
                with open(out, 'wb') as f_out:
                    f_out.write(f_in.read())
        elif des == 'buf':
            with gzip.GzipFile(fileobj=zbuf) as f_in:
                out = f_in.read()
                out = out.decode()
        else:
            print("Unknown output type!")
            return status
        status = True
        return status, out
    else:
        print("Invalid input data!")
        return status


def evlt(fun, inputs, action=sys.exit):
    '''Evaluate the specified function'''
    try:
        outputs = fun(*inputs)
        if isinstance(outputs, tuple):
            num = len(outputs)
        else:
            num = 1
        if outputs is None:
            num = 0

        if num == 0:
            pass
        elif num == 1:
            status = outputs
            if not status:
                action()
        elif num == 2:
            status = outputs[0]
            output = outputs[1]
            if status:
                return output
            else:
                action()
    except:
        print(traceback.print_exc())
        return


def condor_logger():
    '''
    Prepares a logger for job on HTCondor. It splits the levels to stdout
    and stderr, and disables module level logging.

    DEBUG, INFO go to stdout
    WARNING, ERROR go to stderr
    '''

    # disable module level logging of pysixdesk
    logger = logging.getLogger('pysixdesk')
    logger.setLevel(logging.CRITICAL)

    formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s: %(message)s',
                                  datefmt='%H:%M:%S')
    # enable local logging with stdout and stderr split
    logger = logging.getLogger('preprocess_job')
    h1 = logging.StreamHandler(sys.stdout)
    h1.setFormatter(formatter)
    h1.setLevel(logging.DEBUG)
    h1.addFilter(lambda record: record.levelno <= logging.INFO)

    h2 = logging.StreamHandler(sys.stderr)
    h2.setFormatter(formatter)
    h2.setLevel(logging.WARNING)

    logger.addHandler(h1)
    logger.addHandler(h2)
    logger.setLevel(logging.DEBUG)
    return logger


def diff(file1, file2, logger=None, **kwargs):
    '''
    Prints the diff of file1 and file2.
    file1/file2: either path to file, 'str', or 'list'
    if 'str' assumes it is the contents of the file e.i. from:
        file1 = f.read()
    if 'list' assumes it is the contents of the file e.i. from:
        file1 = f.readlines()
    if path to existing file then open and read the contents to
    make the diff.
    Any **kwargs are passed to difflib.unified_diff
    '''
    if logger is not None and isinstance(logger, logging.Logger):
        display = logger.info
    else:
        display = print

    def get_lines(file):
        if os.path.isfile(file):
            with open(file) as f:
                f_lines = f.read().split('\n')
        elif isinstance(file, str):
            f_lines = file.split('\n')
        elif isinstance(file, list):
            f_lines = file
        else:
            raise TypeError('"file" Must be either "str", "list" or "file path".')
        return f_lines

    file1 = get_lines(file1)
    file2 = get_lines(file2)

    for line in difflib.unified_diff(file1, file2, **kwargs):
        display(line)
