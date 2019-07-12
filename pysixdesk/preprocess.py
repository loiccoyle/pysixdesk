#!/usr/bin/env python3
import os
import sys
import time
import copy
import shutil
import configparser

# need to check these imports
from .pysixdb import SixDB
from . import resultparser as rp
from . import utils

logging.basicConfig(format='%(asctime)s-%(name)s-%(levelname)s: %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)


def run(wu_id, input_info):
    cf = configparser.ConfigParser()
    cf.optionxform = str  # preserve case
    cf.read(input_info)
    db_info = {}
    db_info.update(cf['db_info'])
    dbtype = db_info['db_type']
    db = SixDB(db_info)
    wu_id = str(wu_id)
    where = 'wu_id=%s' % wu_id
    outputs = db.select('preprocess_wu', ['input_file'], where)
    db.close()
    if not outputs[0]:
        content = "There isn't input file for preprocess job %s!" % wu_id
        logger.error(content)
        return 0
    input_buf = outputs[0][0]
    input_file = utils.evlt(utils.decompress_buf, [input_buf, None, 'buf'])
    cf.clear()
    cf.read_string(input_file)
    madx_config = cf['madx']
    mask_config = cf['mask']
    oneturn = cf['oneturn']
    status = madxjob(madx_config, mask_config)

    if not status and dbtype.lower() == 'sql':
        content = "The madx job failed!"
        logger.warning(content)

        return status

    otpt = madx_config["output_files"]
    output_files = utils.evlt(utils.decode_strings, [otpt])

    if status:
        sixtrack_config = cf['sixtrack']
        fort3_config = cf._sections['fort3']
        status = sixtrackjobs(sixtrack_config, fort3_config)

    if dbtype.lower() == 'mysql':
        dest_path = './result'
    else:
        dest_path = madx_config["dest_path"]
    if not os.path.isdir(dest_path):
        os.makedirs(dest_path)

    # Download the requested files.
    down_list = list(output_files.values())
    down_list.append('madx_in')
    down_list.append('madx_stdout')
    down_list.append('oneturnresult')
    status = utils.download_output(down_list, dest_path)

    if status:
        logger.info("All requested results have been stored in %s" % dest_path)
    else:
        logger.error("Job failed!")
    if dbtype.lower() == 'sql':
        return status

    # reconnect after jobs finished
    try:
        db = SixDB(db_info)
        where = "wu_id=%s" % wu_id
        task_id = db.select('preprocess_wu', ['task_id'], where)
        task_id = task_id[0][0]
        job_table = {}
        task_table = {}
        oneturn_table = {}
        task_table['status'] = 'Success'
        job_path = dest_path
        rp.parse_preprocess(wu_id, job_path, output_files, task_table,
                            oneturn_table, list(oneturn.keys()))
        where = "task_id=%s" % task_id
        db.update('preprocess_task', task_table, where)
        # where = "mtime=%s and wu_id=%s"%(task_table['mtime'], wu_id)
        # task_id = db.select('preprocess_task', ['task_id'], where)
        # task_id = task_id[0][0]
        oneturn_table['task_id'] = task_id
        db.insert('oneturn_sixtrack_result', oneturn_table)
        if task_table['status'] == 'Success':
            where = "wu_id=%s" % wu_id
            job_table['status'] = 'complete'
            job_table['mtime'] = int(time.time() * 1E7)
            db.update('preprocess_wu', job_table, where)
            content = "Preprocess job %s has completed normally!" % wu_id
            logger.info(content)
        else:
            where = "wu_id=%s" % wu_id
            job_table['status'] = 'incomplete'
            job_table['mtime'] = int(time.time() * 1E7)
            db.update('preprocess_wu', job_table, where)
            content = "This is a failed job!"
            logger.warning(content)
        return status
    except Exception as e:
        where = "wu_id=%s" % wu_id
        job_table['status'] = 'incomplete'
        job_table['mtime'] = int(time.time() * 1E7)
        db.update('preprocess_wu', job_table, where)
        logger.error(e)
        return False
    finally:
        db.close()


def madxjob(madx_config, mask_config):
    '''MADX job to generate input files for sixtrack'''
    logger = logging.getLogger(__name__)

    madx_status = 1
    madxexe = madx_config["madx_exe"]
    source_path = madx_config["source_path"]
    mask_name = madx_config["mask_file"]
    output_files = madx_config["output_files"]
    status, output_files = utils.decode_strings(output_files)
    if not status:
        logger.error("Wrong setting of madx output!")
        madx_status = 0
        return madx_status
    if 'mask' not in mask_name:
        mask_name = mask_name + '.mask'
    mask_file = os.path.join(source_path, mask_name)
    shutil.copy2(mask_file, mask_name)
    dest_path = madx_config["dest_path"]
    if not os.path.isdir(dest_path):
        os.mkdir(dest_path)

    # Generate the actual madx file from mask file
    patterns = ['%' + a for a in mask_config.keys()]
    values = list(mask_config.values())
    madx_in = 'madx_in'
    status = utils.replace(patterns, values, mask_name, madx_in)
    if not status:
        logger.error("Failed to generate actual madx input file!")
        madx_status = 0
        return madx_status

    # Begin to execute madx job
    command = madxexe + " " + madx_in
    logger.info("Calling madx %s" % madxexe)
    logger.info("MADX job is running...")
    output = os.popen(command)
    outputlines = output.readlines()
    with open('madx_stdout', 'w') as mad_out:
        mad_out.writelines(outputlines)
    if 'finished normally' not in outputlines[-2]:
        logger.error("MADX has not completed properly!")
        madx_status = 0
        return madx_status
    else:
        logger.info("MADX has completed properly!")

    # Check the existence of madx output
    status = utils.check(output_files)
    if not status:
        return status  # The required files aren't generated normally,we need to quit
    return madx_status


def sixtrackjobs(config, fort3_config):
    '''Manage all the one turn sixtrack job'''

    logger = logging.getLogger(__name__)

    sixtrack_status = 1
    sixtrack_exe = config['sixtrack_exe']
    source_path = config["source_path"]
    status, temp_files = utils.decode_strings(config["temp_files"])
    if not status:
        logger.error("Wrong setting of oneturn sixtrack templates!")
        sixtrack_status = 0
        return sixtrack_status
    for s in temp_files:
        source = os.path.join(source_path, s)
        shutil.copy2(source, s)
    logger.info('Calling sixtrack %s' % sixtrack_exe)
    first_status = sixtrackjob(config, fort3_config, 'first_oneturn',
                               dp1='.0', dp2='.0')
    if not first_status:
        return first_status
    second_status = sixtrackjob(config, fort3_config, 'second_oneturn')
    if not second_status:
        return second_status
    # Calculate and write out the requested values
    chrom_eps = fort3_config['chrom_eps']
    first = open('fort.10_first_oneturn')
    a = first.readline()
    valf = a.split()
    first.close()
    second = open('fort.10_second_oneturn')
    b = second.readline()
    vals = b.split()
    tunes = [chrom_eps, valf[2], valf[3], vals[2], vals[3]]
    chrom1 = (float(vals[2]) - float(valf[2])) / float(chrom_eps)
    chrom2 = (float(vals[3]) - float(valf[3])) / float(chrom_eps)
    mychrom = [chrom1, chrom2]

    chrom_status = sixtrackjob(config, fort3_config, 'beta_oneturn',
                               dp1='.0', dp2='.0')
    if not chrom_status:
        return chrom_status
    f_in = open('fort.10_beta_oneturn', 'r')
    beta_line = f_in.readline()
    f_in.close()
    beta = beta_line.split()
    beta_out = [beta[4], beta[47], beta[5], beta[48], beta[2], beta[3],
                beta[49], beta[50], beta[52], beta[53], beta[54], beta[55],
                beta[56], beta[57]]
    if fort3_config['CHROM'] == '0':
        beta_out[6] = chrom1
        beta_out[7] = chrom2
    beta_out = beta_out + mychrom + tunes
    lines = ' '.join(map(str, beta_out))
    with open('oneturnresult', 'w') as f_out:
        f_out.write(lines)
        f_out.write('\n')
    return sixtrack_status


def sixtrackjob(config, config_re, jobname, **kwargs):
    '''One turn sixtrack job'''

    logger = logging.getLogger(__name__)

    sixtrack_status = 1
    sixtrack_config = config
    fort3_config = copy.deepcopy(config_re)
    # source_path = sixtrack_config["source_path"]
    sixtrack_exe = sixtrack_config["sixtrack_exe"]
    status, temp_files = utils.decode_strings(sixtrack_config["temp_files"])
    if not status:
        logger.error("Wrong setting of oneturn sixtrack templates!")
        sixtrack_status = 0
        return sixtrack_status
    status, input_files = utils.decode_strings(sixtrack_config["input_files"])
    if not status:
        logger.error("Wrong setting of oneturn sixtrack input!")
        sixtrack_status = 0
        return sixtrack_status
    fc3aux = open('fort.3.aux', 'r')
    fc3aux_lines = fc3aux.readlines()
    fc3aux_2 = fc3aux_lines[1]
    c = fc3aux_2.split()
    lhc_length = c[4]
    fort3_config['length'] = lhc_length
    fort3_config.update(kwargs)

    # Create a temp folder to excute sixtrack
    if os.path.isdir('junk'):
        shutil.rmtree('junk')
    os.mkdir('junk')
    os.chdir('junk')

    logger.info("Preparing the sixtrack input files!")

    keys = list(fort3_config.keys())
    patterns = ['%' + a for a in keys]
    values = [fort3_config[key] for key in keys]
    output = []
    for s in temp_files:
        dest = s + ".t1"
        source = os.path.join('../', s)
        status = utils.replace(patterns, values, source, dest)
        if not status:
            logger.error("Failed to generate input file for oneturn sixtrack!")
            sixtrack_status = 0
            return sixtrack_status
        output.append(dest)
    temp1 = input_files['fc.3']
    temp1 = os.path.join('../', temp1)
    if os.path.isfile(temp1):
        output.insert(1, temp1)
    else:
        logger.error("The %s file doesn't exist!" % temp1)
        sixtrack_status = 0
        return sixtrack_status
    concatenate_files(output, 'fort.3')

    # prepare the other input files
    if os.path.isfile('../fort.2') and os.path.isfile('../fort.16'):
        os.symlink('../fort.2', 'fort.2')
        os.symlink('../fort.16', 'fort.16')
        if not os.path.isfile('../fort.8'):
            open('fort.8', 'a').close()
        else:
            os.symlink('../fort.8', 'fort.8')

    # actually run
    logger.info('Sixtrack job %s is running...' % jobname)
    six_output = os.popen(sixtrack_exe)
    outputlines = six_output.readlines()
    output_name = '../' + jobname + '.output'
    with open(output_name, 'w') as six_out:
        six_out.writelines(outputlines)
    if not os.path.isfile('fort.10'):
        logger.error("The %s sixtrack job for chromaticity FAILED!" % jobname)
        logger.info("Check the file %s which contains the SixTrack fort.6 output." % output_name)
        sixtrack_status = 0
        return sixtrack_status
    else:
        result_name = '../fort.10' + '_' + jobname
        shutil.move('fort.10', result_name)
        logger.info('Sixtrack job %s has completed normally!' % jobname)

    # Get out the temp folder
    os.chdir('../')
    return sixtrack_status


def concatenate_files(source, dest):
    '''Concatenate the given files'''
    f_out = open(dest, 'w')
    if type(source) is list:
        for s_in in source:
            f_in = open(s_in, 'r')
            f_out.writelines(f_in.readlines())
            f_in.close()
    else:
        f_in = open(source, 'r')
        f_out.writelines(f_in.readlines())
        f_in.close()
    f_out.close()


if __name__ == '__main__':

    logger = utils.condor_logger()

    args = sys.argv
    num = len(args[1:])
    if num == 0 or num == 1:
        logger.error("The input file is missing!")
        sys.exit(1)
    elif num == 2:
        wu_id = args[1]
        db_name = args[2]
        run(wu_id, db_name)
        sys.exit(0)
    else:
        logger.error("To many input arguments!")
        sys.exit(1)
