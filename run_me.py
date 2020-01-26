#!/usr/bin/env python

import itertools
import os
import socket  # get host name
import sys
import time

from datetime import datetime
from operator import itemgetter  # sort list of dic by dic values
from optparse import OptionParser  # command line parser
from multiprocessing import Pool
from subprocess import call, check_output


class Format:
    PINK = '\033[95m'
    CYAN = '\033[96m'
    DARKCYAN = '\033[36m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'


# we assume the following dir structure
# ximulator
# -- run_me.py (this script)
# -- /xim_YYYYmmdd_${other_subfix} ('master_dir', to store ximulation results)
# -- /src
# -- /trace
# -- others
# reconfigure src and trace) dir with SRC_DIR and TRACE_DIR
SRC_DIR = 'src/'
TRACE_DIR = 'trace/'
GUROBI_DIR = 'gurobi'
EXE_NAME = 'ximulator_main'
# dir where experiment is run.
# One dir is allocated to each expriment.
EXP_EXE_DIR = os.path.abspath('./') + '/experiments/'
MAX_NUM_PROCESS = 12
# detect local machine
local_machine_name = socket.gethostname()
print "Running on machine " + Format.BOLD + local_machine_name + Format.END
print "Using simulation dir " + Format.BOLD + EXP_EXE_DIR + Format.END

# Step 1/5:
master_dir = 'xim_' + time.strftime("%Y%m%d") + '/'
# Step 2/5:
local_exe_dir = EXP_EXE_DIR + '/running'

# [benchmark]
ximulator_opts = {
    'scheduler': ['varysImpl'],
    'traffic': [('fbplay', 'fbtrace-1hr.txt', 1.0, 1.0)],
    'rate': [1e9], # link rate
    'zero_comp': [True]
}


def input_to_link_rate(user_link_rate_input):
    if ("1Gbps" == user_link_rate_input):
        return ("01Gbps", 1 * 10 ** 9)
    if ("1.33Gbps" == user_link_rate_input):
        return ("01_33Gbps", 1.33 * 10 ** 9)
    elif ("10Gbps" == user_link_rate_input):
        return ("10Gbps", 10 * 10 ** 9)
    elif ("40Gbps" == user_link_rate_input):
        return ("40Gbps", 40 * 10 ** 9)
    elif ("100Gbps" == user_link_rate_input):
        return ("100Gbps", 100 * 10 ** 9)
    else:
        print "I need link rate (e.g. 10Gbps)"
        return ("", 0)


def input_to_traffic_inflate(user_play_load_input, link_rate_bps):
    link_rate_Gbps = link_rate_bps / 10 ** 9
    if ('X1' == user_play_load_input):
        return ('X1', 1.0)
    elif ('X0.05' == user_play_load_input):
        return ('X005p', 0.05 * link_rate_Gbps)
    elif ('X0.1' == user_play_load_input):
        return ('X010p', 0.1 * link_rate_Gbps)
    elif ('X0.20' == user_play_load_input):
        return ('X020p', 0.2 * link_rate_Gbps)
    elif ('X0.25' == user_play_load_input):
        return ('X025p', 0.25 * link_rate_Gbps)
    elif ('X0.30' == user_play_load_input):
        return ('X030p', 0.3 * link_rate_Gbps)
    elif ('X0.40' == user_play_load_input):
        return ('X040p', 0.4 * link_rate_Gbps)
    elif ('X0.50' == user_play_load_input):
        return ('X050p', 0.50 * link_rate_Gbps)
    elif ('X0.75' == user_play_load_input):
        return ('X075p', 0.75 * link_rate_Gbps)
    elif ('X1.25' == user_play_load_input):
        return ('X125p', 1.25 * link_rate_Gbps)
    elif ('X1.50' == user_play_load_input):
        return ('X150p', 1.50 * link_rate_Gbps)
    elif ('X1.75' == user_play_load_input):
        return ('X175p', 1.75 * link_rate_Gbps)
    elif ('X2.00' == user_play_load_input):
        return ('X200p', 2.00 * link_rate_Gbps)
    elif ('X2.25' == user_play_load_input):
        return ('X225p', 2.25 * link_rate_Gbps)
    else:
        return ('', 0)


def preset_mode(opts):
    # configure experiments to run by overwriting options in ximulator_opts
    # Will fall back to default settings if mode is not valid.
    preset_master_dir = master_dir
    preset_local_exe_dir = local_exe_dir
    preset_options = ximulator_opts
    mode = opts.mode

    if mode == 'weaver':
        preset_options['rate'] = [1e9]
        net_configs = [
            '100varys', '10varys_90varys', '20varys_80varys',
            '30varys_70varys', '40varys_60varys', '50varys_50varys',
            '60varys_40varys', '70varys_30varys', '80varys_20varys',
            '90varys_10varys']
        preset_options['scheduler'] = [
            'weaver_' + net for net in net_configs]
        preset_options['traffic'] = [
            ('fb1by1', 'fbtrace-1hr.txt', 1.0, 1.0),
            ('fbplay', 'fbtrace-1hr.txt', 1.0, 1.0),
        ]
    elif mode == 'prandom':
        preset_options['rate'] = [1e9]
        net_configs = ['10varys_90varys', '20varys_80varys', '30varys_70varys',
                       '40varys_60varys', '50varys_50varys', ]
        preset_options['scheduler'] = [
            'prandom{}_{}'.format(run, net)
            for net in net_configs for run in range(11, 50)]
        preset_options['traffic'] = [
            ('fb1by1', 'fbtrace-1hr.txt', 1.0, 1.0),
            ('fbplay', 'fbtrace-1hr.txt', 1.0, 1.0),
        ]
    elif mode == 'infocom':
        preset_options['rate'] = [1e9]
        net_configs = ['100varys', '50varys_50varys', '40varys_60varys',
                       '60varys_40varys', '30varys_70varys', '70varys_30varys',
                       '20varys_80varys', '80varys_20varys', '10varys_90varys',
                       '90varys_10varys']
        preset_options['scheduler'] = ['infocom_' + net for net in net_configs]
        preset_options['traffic'] = [
            ('fb1by1', 'fbtrace-1hr.txt', 1.0, 1.0),
            # ('fbplay', 'fbtrace-1hr.txt', 1.0, 1.0),
        ]
    elif mode == 'weaversorted':
        preset_options['rate'] = [1e9]
        preset_options['traffic'] = [('fb1by1', 'fbtrace-1hr.txt', 1.0, 1.0),
                                     ('fbplay', 'fbtrace-1hr.txt', 1.0, 1.0)]
        net_configs = ['10varys_90varys', '20varys_80varys', '30varys_70varys',
                       '40varys_60varys', '50varys_50varys', ]
        schedulers = ['weaverSortedFlowInc', 'weaverSortedFlowDec',
                      'weaverSortedSrcDstIdx', 'weaverSortedRandom']
        preset_options['scheduler'] = [scheduler + '_' + net_config
                                       for scheduler in schedulers
                                       for net_config in net_configs]
    elif mode == 'weavernoncritical':
        preset_options['rate'] = [1e9]
        preset_options['traffic'] = [('fb1by1', 'fbtrace-1hr.txt', 1.0, 1.0),
                                     ('fbplay', 'fbtrace-1hr.txt', 1.0, 1.0)]
        net_configs = ['100', '10_90', '20_80', '30_70', '40_60', '50_50']
        schedulers = ['weaverNonCRatioLB', 'weaverNonCRandom',
                      'weaverNonCMinBn']
        preset_options['scheduler'] = ['{}_{}net_varys_{}'.format(
            scheduler, len(net_config.split('_')), net_config)
            for scheduler in schedulers for net_config in net_configs]
    elif mode == 'aalo_weaver':
        preset_options['rate'] = [1e9]
        net_configs = ['100', '10_90', '20_80', '30_70', '40_60', '50_50']

        preset_options['scheduler'] = ['weaver_{}net_varys_{}'.format(
            len(net.split('_')), net) for net in net_configs]
        preset_options['traffic'] = [
            ('fb1by1', 'fbtrace-1hr.txt', 1.0, 1.0),
            ('fbplay', 'fbtrace-1hr.txt', 1.0, 1.0),
        ]
    elif '3net' in mode:
        preset_options['rate'] = [1e9]
        preset_options['traffic'] = [
            ('fb1by1', 'fbtrace-1hr.txt', 1.0, 1.0),
            ('fbplay', 'fbtrace-1hr.txt', 1.0, 1.0)]
        net_configs = ['10_10_80', '10_20_70', '10_30_60', '10_40_50',
                       '20_20_60', '20_30_50', '20_40_40', '30_30_40']
        if mode == '3net_weaver':
            preset_options['scheduler'] = [
                'weaver_3net_varys_' + net for net in net_configs]
        elif mode == '3net_infocom':
            preset_options['scheduler'] = [
                'infocom_3net_varys_' + net for net in net_configs]
        elif mode == '3net_random':
            preset_options['scheduler'] = [
                'prandom{}_3net_varys_{}'.format(run, net)
                for net in net_configs for run in range(0, 50)]
    elif '4net' in mode:
        preset_options['rate'] = [1e9]
        preset_options['traffic'] = [
            ('fb1by1', 'fbtrace-1hr.txt', 1.0, 1.0),
            ('fbplay', 'fbtrace-1hr.txt', 1.0, 1.0)]
        net_configs = ['10_10_10_70', '10_10_20_60', '10_10_30_50',
                       '10_10_40_40', '10_20_20_50', '10_20_30_40',
                       '10_30_30_30', '20_20_20_40', '20_20_30_30']
        if mode == '4net_weaver':
            preset_options['scheduler'] = [
                'weaver_4net_varys_' + net for net in net_configs]
        elif mode == '4net_infocom':
            preset_options['scheduler'] = [
                'infocom_4net_varys_' + net for net in net_configs]
        elif mode == '4net_random':
            preset_options['scheduler'] = [
                'prandom{}_4net_varys_{}'.format(run, net)
                for net in net_configs for run in range(0, 50)]
    else:
        print '{} Illegal mode {} {}'.format(Format.RED, mode, Format.END)
        sys.exit(-1)

    # set up direcotry
    preset_master_dir = 'xim_{}_preset-{}/'.format(
        time.strftime("%Y%m%d"), mode)
    preset_local_exe_dir = '{}/running-preset-{}'.format(EXP_EXE_DIR, mode)
    print "{}Mode {} {}".format(Format.BLUE, mode, Format.END)

    print 'master_dir {}{}{}'.format(
        Format.BOLD, preset_master_dir, Format.END)
    print 'local_exe_dir {}{}{}'.format(
        Format.BOLD, preset_local_exe_dir, Format.END)
    return (preset_master_dir, preset_local_exe_dir, preset_options)


class SimItem:

    def __init__(self, elec, scheduler, traffic,
                 ftrace, inflate, speedup, zero_comp):
        self.elec = elec
        self.scheduler = scheduler
        self.traffic = traffic
        self.ftrace = ftrace
        self.inflate = inflate
        self.speedup = speedup
        self.zero_comp = zero_comp
        # valid only for sunflow scheduler.
        self.sunflow_shuffle_random = 'false'
        self.sunflow_shuffle_sort = 'false'
        #####################################
        # simulation detials
        # a unique name assigned for this experiment
        self.xim_name = ""


def get_all_sim_items(options):
    '''Return a list of SimItem, each item is one experiment.

    Pre process options to add specific configs.
    Calculate all option combinations based on options.
    Filter out experiment with invalid configs.
    '''
    product = [x for x in apply(itertools.product, options.values())]
    configs_list = [dict(zip(options.keys(), p)) for p in product]
    result = []
    for configs in configs_list:
        # Okay, this experiment is valid to run
        new_sim = SimItem(configs['rate'], configs['scheduler'],
                          configs['traffic'][0],  # traffic generator
                          configs['traffic'][1],  # ftrace
                          configs['traffic'][2],  # inflate
                          configs['traffic'][3],  # speedup
                          configs['zero_comp'])
        new_sim.xim_name = 'xim_{:03d}'.format(len(result))
        result.append(new_sim)
    return result


def run_experiment(sim_item):
    '''run each unit experiment specified by sim_item

    we assume the following directory structure
    running{-preset01_}   (= local_exe_dir)
    |--- trace      (local_trace_dir)
    |--- src        (local_src_dir )
    |--- build      (local_build_dir)
    |--- xim_001    (local_sim_sub_dir)
         |--- ximulator_main (executable)
         |--- trace.txt
         |--- output.txt
         |--- others

    '''

    print '{}[{}] Running {}/{} {}: {}'.format(
        Format.DARKCYAN, datetime.now().strftime('%Y-%m-%d %H:%M'),
        sim_item.xim_name, len(all_experiments), Format.END,
        [value for key, value in sim_item.__dict__.items()
         if not key.startswith('__') and not callable(key)])

    local_src_dir = local_exe_dir + '/' + SRC_DIR
    local_trace_dir = local_exe_dir + '/' + TRACE_DIR
    local_build_dir = local_exe_dir + '/build/'
    local_sim_sub_dir = local_exe_dir + '/' + sim_item.xim_name
    # make dir
    call('mkdir {}'.format(local_sim_sub_dir), shell=True)
    # copy executable and trace
    call('cp {}/{} {} && cp {}/{} {}'.format(
        local_build_dir, EXE_NAME, local_sim_sub_dir,
        local_trace_dir, sim_item.ftrace, local_sim_sub_dir), shell=True)
    # kick start ximulator!
    cmd = ('./{} -s {} -elec {} '
           '-traffic {} -ftrace {} -inflate {} -speedup {} '
           '-cctaudit ximout_cct.txt '
           # TODO: remove useless output files
           '-fctaudit ximout_fct.txt '
           '-compaudit ximout_comp_time.txt '
           '-zc {} > ximout_output.txt').format(
        EXE_NAME, sim_item.scheduler, sim_item.elec,
        sim_item.traffic, sim_item.ftrace, sim_item.inflate, sim_item.speedup,
        sim_item.zero_comp)
    call('cd {} && {}'.format(local_sim_sub_dir, cmd), shell=True)

    # collect the output
    master_sim_sub_dir = master_dir + '/' + sim_item.xim_name
    call('mkdir {}'.format(master_sim_sub_dir), shell=True)
    call('cp {}/ximout_*.txt {}'.format(local_sim_sub_dir,
                                        master_sim_sub_dir), shell=True)

    audit_last_line = check_output(
        'tail -1 {}/ximout_cct.txt'.format(master_sim_sub_dir),
        shell=True).rstrip()
    print '{}[{}] Finished {}{}{} : {}'.format(
        Format.BLUE, datetime.now().strftime('%Y-%m-%d %H:%M'),
        Format.BOLD, sim_item.xim_name, Format.END, audit_last_line)


if __name__ == '__main__':

    parser = OptionParser(usage='run_me.py [options]')
    parser.add_option('-m', '--mode',
                      help='Index of preset experiemnt mode to run')
    parser.add_option('-l', '--link_rate', help='Link rate')
    parser.add_option('-d', '--delay', help='Reconfigure delay')
    parser.add_option('-i', '--inflate', help='Factor of traffic size')

    (opts, args) = parser.parse_args()

    if not opts.mode:
        print '{}Only preset mode is allowed.{}'.format(Format.RED, Format.END)
        sys.exit(-1)

    start_time = datetime.now()
    print "Start at " + start_time.strftime('%Y-%m-%d %H:%M:%S')
    # Use preset global variables if needed.
    (master_dir, local_exe_dir, ximulator_opts) = preset_mode(opts)
    print '*' * 70

    # Create or overwrite any exisiting master_dir, which stores experiment
    # results.
    call('rm -rf ' + master_dir, shell=True)
    call('mkdir -p ' + master_dir, shell=True)

    # back up src in master_dir
    call('cp -r ' + SRC_DIR + ' ' + master_dir, shell=True)

    # clean up dir to run experiments
    call('rm -rf ' + local_exe_dir, shell=True)
    call('mkdir ' + local_exe_dir, shell=True)

    # move src, trace and the Gurobi library, and build the ximulator binary
    call('cp -r {} {}'.format(SRC_DIR, local_exe_dir), shell=True)
    call('cp -r {} {}'.format(GUROBI_DIR, local_exe_dir), shell=True)
    call('cp -r {} {}'.format(TRACE_DIR, local_exe_dir), shell=True)
    call(('cd {} && rm -rf build && mkdir build && '
          'cd build && cmake ../src && make')
         .format(local_exe_dir), shell=True)
    print '{} {}Paper work done. Going to run experiments.{}{}'.format(
        '*' * 20, Format.DARKCYAN, Format.END, '*' * 7)

    all_experiments = get_all_sim_items(ximulator_opts)
    pool = Pool(processes=MAX_NUM_PROCESS)
    pool.map(run_experiment, all_experiments)

    print '*' * 70
    print 'All experiments done'
    print '     starts at ' + start_time.strftime('%Y-%m-%d %H:%M:%S')
    print '       ends at ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S')
