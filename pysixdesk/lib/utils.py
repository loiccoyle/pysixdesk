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
PYSIXDESK_ABSPATH = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


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
    if not os.path.isdir(dest):
        os.makedirs(dest, 0o755)

    for filename in filenames:
        if not os.path.isfile(filename):
            raise FileNotFoundError("The file %s doesn't exist, download failed!" % filename)
        if os.path.isfile(filename):
            if zp:
                out_name = os.path.join(dest, filename + '.gz')
                with open(filename, 'rb') as f_in, gzip.open(out_name, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            else:
                shutil.copy(filename, dest)


def replace(patterns, replacements, source, dest):
    '''Reads a source file and writes the destination file.
    In each line, replaces patterns with repleacements.

    TODO: maybe it's best to return the the lines and not write the file,
    so that more parsing can happen in memory without reopening and writing files.
    Also might be best to take as input the contents itself for the same
    reason.
    '''
    if not os.path.isfile(source):
        raise FileNotFoundError("The file %s doesn't exist!" % source)

    with open(source, 'r') as fin:
        fin_lines = fin.readlines()

    with open(dest, 'w') as fout:
        for line in fin_lines:
            for pat, rep in zip(patterns, replacements):
                if rep is not None:
                    line = re.sub(f'{pat}', f'{rep}', line)
            fout.write(line)


def sandwich(in_file, out_file, path_prefix='', logger=None):
    '''
    Looks for any patterns matching '^%FILE:.*' in in_file, then replaces the match
    with the content of the file following the match.
    A path prefix can be specified to look for matched file in another directory.
    If the matched file is not found, comment out the pattern.

    Example:
        contents of in_file:
            aaaaaaaaa
            %FILE:insert.txt
            aaaaaaaaa

        contents of insert.txt:
            bbbbbbbbb
            bbbbbbbbb

        writes to out_file:
            aaaaaaaaa
            bbbbbbbbb
            bbbbbbbbb
            aaaaaaaaa

    TODO: maybe it's best to return the the sandwiched lines and not write the file,
    so that more parsing can happen in memory without reopening and writing files.
    Also might be best to take as input the contents itself for the same
    reason.
    '''

    if logger is not None:
        display = logger.warning
    else:
        display = print

    with open(in_file, 'r') as f:
        in_lines = f.read()

    reg = re.compile(r'^%FILE:.*', re.MULTILINE)
    for m in re.finditer(reg, in_lines):
        m_str = m.group()
        try:
            with open(os.path.join(path_prefix, m_str.split(':')[1].lstrip()), 'r') as f:
                file_lines = f.read()
            in_lines = re.sub(f'{m_str}', file_lines, in_lines)
        except FileNotFoundError as e:
            display(e)
            display(f'Commenting out {m_str} for {out_file}')
            in_lines = re.sub(f'{m_str}', f'/{m_str}', in_lines)

    with open(out_file, 'w') as out:
        out.write(in_lines)


def comment_block(block, in_file, out_file, selection=None):
    '''
    Comments a fort.3 type block.
    in_file: file to comment
    block: str containing the title of the block to comment out
    out_file: output file with comments
    selection: integer or slice, a used to comment only a subselection of occurances,
    if none provided, comment all occurances.

    Example:
    to comment out the first occurance of the BEAM block:
        comment_block('fort.3', 'BEAM', fort.3.commented, selection=0)
    to comment out all but the fist occurance of the BEAM block:
        comment_block('fort.3', 'BEAM', fort.3.commented, selection=slice(1, None))

    TODO: maybe it's best to return the the lines and not write the file,
    so that more parsing can happen in memory without reopening and writing files.
    Also might be best to take as input the contents itself for the same
    reason.
    '''
    reg = fr"(?=^{block}).*?(?<=NEXT)"
    with open(in_file, 'r') as f_in:
        lines = f_in.read()

    results = list(re.finditer(reg, lines, re.MULTILINE | re.DOTALL))
    if selection is not None:
        results = results[selection]
        if not isinstance(results, list):
            results = [results]

    offset = 0
    for i, m in enumerate(results):
        matched_block = m.group()
        commented, n_subs = re.subn('\n', '\n/', '/' + matched_block)
        lines = ''.join([lines[:m.start() + offset], commented, lines[m.end() + offset:]])
        offset += n_subs + 1

    with open(out_file, 'w') as f_out:
        f_out.write(lines)


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