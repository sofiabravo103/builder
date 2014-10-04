#!/usr/bin/python2

import os
import sys
import getopt
import random
import logging
import time
import numpy
import math
import linecache
import resource
from kosmann_splitter import KosmannSplitter
from os.path import dirname, abspath

TIME = time.time()
GENERATOR = 'generador.cpp'
MAX_KOSSMAN_DATA_LIST_SIZE = 1000
MAX_INTERMEDIATE_FILE_SIZE_GB = 5

# Dataset will be generated with CONS_DATASET
# times the number of rows of the testcase
CONS_DATASET = 2

help_text = '''Usage:
builder [options] -o <outputfile> -s <size> -d <dimentions>
builder  -o <outputfile> --autodataset
builder  -o <outputfile> --autotiny

Options:
 -v                     verbose output

 --time                 specify the simulation time. Output may exceed this
                        parameter. By default is a random number between 0 and
                        1800.

 --interval             interval of time in which poisson parameter is defined.
                        example: --poissparameter 2 --interval 120 means 3
                        arrivals every two minutes. Default is five minutes.

 --arrivals <num>       sepcify how many arrivals per tuple will be generated
                        by default poisson distribution is used

 --poissparameter       poisson parameter to use when generating arrival events.
  <num>                 The parameter specifies average arrival events per time
                        interval (defaut is a random number between 1 and 10)

 --poissarray <string>  specify a string containg poisson parameters separated
                        with an '%' (used when different poisson parameters
                        will be used to generate arrival events)
                        example: --poissarray 5%2%10 for 3 dimentions.

 --testcases <num>      generate different testcases according to the same file

 --leavereport          leave a report file describing the parameters used to
                        create the testcase(s)

 --leavesettings        leave a report similar to that generated with
   <file>               leavereport but adjusted to the Dynasty Algorithm input.
                        The report will be appended to the file specified.

 --expirations          specify when a value expires for each dimention. This
                        value will not affect testcases generation, but will be
                        used for report and written to settings if
                        --leavesettings option is indicated. Input is a string
                        containg expiration values separated with an '%'.
                        example: --expirations 5%2%10 for 3 dimentions.

 --resume  <file>       resume execution from specified tmp file

 --events_per_line      in some cases it is better to have many events per line
   <num>                rather then just one. That way many events can be written
                        to memory per each disk reading. To adjust to just one event
                        per line set this to 1. By default 5000 events per line
                        are used.


 --dontdelete           keep kossmann tmp file (default is delete the file)

 --autodataset          generate medium to small random parameters for the
                        entire dataset (to generate a quick and dirty dataset
                        for tests)

  --autotiny            like --autodataset but generating a tiny dataset
                        (to generate a quick and dirty tiny dataset for control)

  --anticorrelated      use anticorrelated data distribution
  --correlated          use correlated data distribution
  --uniform             use uniform data distribution (default)

  --distributearr       data distribution refers to arrivals (default)
  --distributedim       data distribution refers to dimentions

 --independentdims      if specified dimentions will have actualizations indepently,
                        by default all dimentions arrive in a single event.
'''
def get_path(filename):
    return dirname(abspath(__file__)) + '/' + filename


def print_verbose_message(msg):
    if G_VERBOSE:
        sys.stdout.write(msg)
        sys.stdout.flush()


def get_options(argv):
    long_options =  [\
                     'autodataset', \
                     'autotiny',\
                     'anticorrelated', \
                     'correlated', \
                     'uniform', \
                     'leavereport', \
                     'distributearr', \
                     'distributedim', \
                     'dontdelete',\
                     'independentdims',\
                     'poissarray=',\
                     'poissparameter=',\
                     'arrivals=',\
                     'testcases=',\
                     'leavesettings=', \
                     'resume=',\
                     'expirations=',\
                     'interval=',
                     'time=',\
                     'events_per_line=']

    try:
        options, args = getopt.getopt(argv,"o:s:d:v",long_options)
        if not options:
            print help_text
            sys.exit(1)
    except getopt.GetoptError:
        print help_text
        sys.exit(2)
    return options


def check_dimentions(arg):
    if '.' in arg or '-' in arg:
        raise Exception('Error: dimention value (-d) value must be a positive'+\
                            'integer.')
    try:
        d = int(arg)
    except ValueError:
        raise Exception('Error: dimention value (-d) is not a number')
    if d < 3 or d > 10:
        raise Exception('Error: dimention value (-d) must be between 3 and 10')


def check_arrivals(arg):
    if '.' in arg or '-' in arg:
        raise Exception('Error: arrival value (-a) value must be a positive'+\
                            'integer.')
    try:
        int(arg)
    except ValueError:
        raise Exception('Error: arrival value (-a) is not a number')


def check_outputfile(arg):
    if os.path.isfile(get_path(arg)) or os.path.isfile(get_path(arg) + '_0'):
        ans = raw_input('Warning: file "{0}" exists, continue? [Y/n]: '.format(arg))
        while not (ans == 'y' or ans == 'n' or ans == ''):
            ans = raw_input("Please type 'y' or 'n': ")
        if ans == 'n':
            sys.exit()

def check_file_existence(arg, name, path_included=False):
    if arg[0] == '.':
            print('WARNING: Non absolute paths are not supported. Check that settings '+\
                'file was created in the correct location.')
    if not path_included:
        if not os.path.isfile(get_path(arg)):
            raise Exception('Error: '+ name +' file {0} does not exist'.format(get_path(arg)))
    else:
        if not os.path.isfile(arg):
            raise Exception('Error: '+ name +' file {0} does not exist'.format(arg))


def check_not_arrivals(options):
    opt_list = [x for x,y in options]
    if '--arrivals' in opt_list:
        raise Exception('Error: --arrivals cannot be selected along with '+\
                        '--poissparameter or --poissarray')

def check_num_parameter(arg,name,option):
    if '-' in arg or '.' in arg:
        raise Exception('Error: '+ name +' parameter(s) must be a positive '+\
                            'integer number')
    try:
        f = int(arg)
    except ValueError:
        raise Exception('Error: '+ name +' parameter('+ option \
                        +') is not a number.')
    if f == 0:
        raise Exception('Error: '+ name +' parameter ('+ option \
                        +') must be greater than zero.')


def check_poiss_array(arg,options):
    poiss_array = arg.split('%')
    dimentions = int([y for x,y in options if x == '-d'][0])
    if len(poiss_array) != dimentions:
        raise Exception('Error: --poissarray has bad format (is the number of '+\
                            'values in the array equal to the number of '+\
                            'dimentions?)')
    for num in poiss_array:
        check_num_parameter(num,'poisson','--poissarray')

    opt_list = [x for x,y in options]
    if '--poissparameter' in opt_list:
        raise Exception('Error: --poissparameter and --poissarray cannot be '+\
                            'seleceted together')

def check_expirations_array(arg,options):
    exp_array = arg.split('%')
    dimentions = int([y for x,y in options if x == '-d'][0])
    if len(exp_array) != dimentions:
        raise Exception('Error: --expirations has bad format (is the number'+\
                        'of values in the array equal to the number of '+\
                        'dimentions?)')
    for num in exp_array:
        check_num_parameter(num,'expirations','--expirations')


def check_auto(options):
    opt_list = [x for x,y in options]

    if len(options) > 2:
        counter = len(options)

        if '-v' in opt_list:
            counter = counter - 1

        if '--events_per_line' in opt_list:
            counter = counter - 1

        if '--testcases' in opt_list:
            counter = counter - 1

        if '--leavereport' in opt_list:
            counter = counter - 1

        if '--resume' in opt_list:
            counter = counter - 1

        if '--dontdelete' in opt_list:
            counter = counter - 1

        if '--leavesettings' in opt_list:
            counter = counter - 1

        if counter > 2:
            raise Exception('Error: options --autodataset or '+\
                            '--autotiny cannot be selected with '+\
                            'any other option')

def check_everything_set(auto, opt_list):
    if not auto:
        if '-s' not in opt_list:
            raise Exception('Error: unless --autodataset or --autotiny are '+\
                                'selected size value (-s) must be specified')
        if '-d' not in opt_list:
            raise Exception('Error: unless --autodataset or --autotiny are '+\
                                'selected dimentions value (-d) must be specified')
def check_interval(arg, options):
    opt_list = [x for x,y in options]
    if '--time' not in opt_list:
        raise Exception('Error: in order to use --interval the option '+\
            '--time must be set')
    else:
       for opt, val in options:
           if opt == '--time' and val < arg:
               raise Exception('Error: interval value must be smaller '+\
                'than simulation time value')

def check_options(options):
    auto = False
    opt_list = [x for x,y in options]
    if '-o' not in opt_list:
        raise Exception('Error: no output file selected')
    for opt, arg in options:
        if opt == '-s':
            check_num_parameter(arg,'size',opt)
        elif opt == '-d':
            check_dimentions(arg)
        elif opt == '-o':
            check_outputfile(arg)
        elif opt == '--expirations':
            check_expirations_array(arg,options)
        elif opt == '--leavesettings':
            check_file_existence(arg, 'settings', path_included=True)
        elif opt == '--time':
            check_num_parameter(arg,'simulation time',opt)
        elif opt == '--resume':
            check_file_existence(arg, 'tmp')
        elif opt == '--poissparameter':
            check_num_parameter(arg,'poisson',opt)
            check_not_arrivals(options)
        elif opt == '--testcases':
            check_num_parameter(arg,'testcases',opt)
        elif opt == '--poissarray':
            check_poiss_array(arg,options)
            check_not_arrivals(options)
        elif opt == '--interval':
            check_num_parameter(arg,'interval',opt)
            check_interval(arg, options)
        elif opt == '--events_per_line':
            check_num_parameter(arg,'events per line',opt)
        elif opt == '--arrrivals':
            check_arrivals(arg)
        elif opt == '--autodataset' or opt == '--autotiny':
            check_auto(options)
            auto = True

    check_everything_set(auto,opt_list)

def set_defaults():
    global G_DATA_DIST_APPLICATION
    global G_DATA_DIST
    global G_POISS_PARAMETER
    global G_ARRIVALS
    global G_AUTO
    global G_TINY
    global G_TESTCASES
    global G_LEAVE_REPORT
    global G_SETTINGS_FILE
    global G_VERBOSE
    global G_DELETE_TMP
    global G_RESUME
    global G_SIMULATION_TIME
    global G_POISS_ARRAY
    global G_EXPIRATIONS
    global G_LEAVE_SETTINGS
    global G_INTERVAL
    global G_INDEPENDENT_DIMS
    global MAX_ACTUALIZATIONS_LIST_SIZE

    MAX_ACTUALIZATIONS_LIST_SIZE = 5000

    G_INDEPENDENT_DIMS = False
    G_LEAVE_SETTINGS  = None
    G_SETTINGS_FILE = None
    G_RESUME = None
    G_DELETE_TMP = True
    G_VERBOSE = False
    G_POISS_ARRAY = None
    G_LEAVE_REPORT = False
    G_DATA_DIST_APPLICATION = 'arrivals'
    G_DATA_DIST = 'E'
    G_POISS_PARAMETER = random.randint(2,5)
    G_INTERVAL = 300
    G_SIMULATION_TIME = random.randint(100,600)
    G_ARRIVALS = None
    G_TESTCASES = 1
    G_AUTO = False
    G_TINY = False
    G_EXPIRATIONS = []

def parse_probability_options(options):
    global G_POISS_PARAMETER
    global G_POISS_ARRAY
    global G_ARRIVALS
    global G_INTERVAL

    for opt, arg in options:
        if opt == '--arrivals':
            G_ARRIVALS = int(arg)
            G_POISS_PARAMETER = None

        elif opt == '--poissparameter':
            G_POISS_PARAMETER = int(arg)

        elif opt == '--poissarray':
            G_POISS_PARAMETER = None
            G_POISS_ARRAY = [int(x) for x in arg.split('%')]

        elif opt == '--interval':
            G_INTERVAL = int(arg)

def parse_datadist_options(options):
    global G_DATA_DIST

    for opt, arg in options:
      if opt == '--anticorrelated':
          raise NotImplementedError
          G_DATA_DIST = 'A'
      elif opt == '--correlated':
          raise NotImplementedError
          G_DATA_DIST = 'C'
      elif opt == '--distributedim':
          raise NotImplementedError
          global G_DATA_DIST_APPLICATION
          G_DATA_DIST_APPLICATION = 'dimentions'


def parse_short_options(options):
    for opt, arg in options:
      if opt == '-s':
          global G_SIZE
          G_SIZE = int(arg)
      elif opt == '-d':
          global G_DIMENTIONS
          G_DIMENTIONS = int(arg)

def set_autodataset_values():
    global G_DIMENTIONS
    global G_SIZE
    global G_ARRIVALS
    global G_POISS_PARAMETER
    G_POISS_PARAMETER = None

    if G_TINY:
        max_dim = 3

        min_size = 4
        max_size = 7

        min_arr = 2
        max_arr = 2

    else:
        max_dim = 5

        min_size = 10000
        max_size = 100000

        min_arr = 2
        max_arr = 5

    G_DIMENTIONS = random.randint(3,max_dim)
    G_SIZE = random.randint(min_size,max_size)
    G_ARRIVALS = (random.randint(min_arr,max_arr) * G_SIMULATION_TIME) / G_INTERVAL

def parse_auto(options):
    global G_AUTO
    global G_TINY
    G_AUTO = False

    for opt, arg in options:
        if opt == '--autodataset':
            G_AUTO = True
            G_TINY = False
            set_autodataset_values()
        elif opt == '--autotiny':
            G_AUTO = True
            G_TINY = True
            set_autodataset_values()
    return G_AUTO

def set_expirations_default():
    global G_EXPIRATIONS
    if G_EXPIRATIONS == []:
        G_EXPIRATIONS = [random.randint(int(G_SIMULATION_TIME * 0.2)\
            ,int(G_SIMULATION_TIME * 0.6))\
        for x in range(0,G_DIMENTIONS)]


def parse_input(options):
    global G_VERBOSE
    global G_OUTPUTFILE
    global G_TESTCASES
    global G_LEAVE_REPORT
    global G_SETTINGS_FILE
    global G_RESUME
    global G_DELETE_TMP
    global G_SIMULATION_TIME
    global G_INDEPENDENT_DIMS
    global MAX_ACTUALIZATIONS_LIST_SIZE

    set_defaults()

    for opt, arg in options:
        if opt == '-v':
            G_VERBOSE = True
        elif opt == '-o':
            G_OUTPUTFILE = arg
        elif opt == '--testcases':
            G_TESTCASES = int(arg)
        elif opt == '--leavereport':
            G_LEAVE_REPORT = True
        elif opt == '--leavesettings':
            G_SETTINGS_FILE = arg
            G_LEAVE_SETTINGS = True
        elif opt == '--expirations':
            G_EXPIRATIONS = [int(x) for x in arg.split('%')]
        elif opt == '--events_per_line':
            MAX_ACTUALIZATIONS_LIST_SIZE = int(arg)
        elif opt == '--resume':
            G_RESUME = arg
        elif opt == '--dontdelete':
            G_DELETE_TMP = False
        elif opt == '--time':
            G_SIMULATION_TIME = float(arg)
        elif opt == '--independentdims':
            G_INDEPENDENT_DIMS = True

    if not parse_auto(options):
        parse_short_options(options)
        parse_datadist_options(options)
        parse_probability_options(options)

    set_expirations_default()

def report_settings():
    if G_SETTINGS_FILE is not None:
        of_settings = open(G_SETTINGS_FILE,'a')
        of_settings.write('\n')
        ommited = [\
                   'TESTCASES','POISS_ARRAY', 'VERBOSE','RESUME','ARRIVALS',\
                   'SETTINGS_FILE', 'DATA_DIST_APPLICATION','DATA_DIST',
                   'LEAVE_SETTINGS','POISS_PARAMETER','TINY','DELETE_TMP',
                   'LEAVE_REPORT','AUTO']

        for variable in globals():
            if 'G_' in variable:
                r_value = str(variable[2:])
                l_value = str(eval(variable))

                if r_value in ommited:
                    continue

                if r_value == 'SIZE':
                    r_value = 'TUPLES'

                if r_value == 'EXPIRATIONS':
                    r_value = 'EXPIRATION_TIMES'

                if r_value == 'OUTPUTFILE':
                    l_value = "'"+ str(eval(variable)) +"_output'"
                    path = str(abspath('.'))
                    of_settings.write('D_INPUTFILE = "'+ path + '/' + \
                                      str(eval(variable)) + '"\n')

                if r_value == 'SIMULATION_TIME':
                    r_value = 'SIMULATION_SECONDS'
                line = r_value + '\t=\t' + l_value
                of_settings.write('D_' + line + '\n')

        of_settings.close()

def report_input():
    if G_LEAVE_REPORT:
        of_report = open('{0}_report'.format(get_path(G_OUTPUTFILE)),'w')

    if G_VERBOSE or G_LEAVE_REPORT:
        print 'Generating dataset according to:'
        for variable in globals():
            if'G_' in variable:
                value = eval(variable)
                line = str(variable[2:]) +  ':\t' + str(value)
                if G_LEAVE_REPORT:
                    of_report.write(line + '\n')

                if G_VERBOSE:
                    print line

def get_kossman_filename():
    if G_RESUME is not None:
        return get_path('{0}'.format(G_RESUME))
    else:
        return get_path('tmp_{0}'.format(TIME))

def randomize_file():
    print_verbose_message('Randomizing kossman file...')
    file_name = get_kossman_filename()
    os.system('sort -R {0} -o {1}'.format(file_name, file_name))
    print_verbose_message(' done.\n')

def call_kossman():
    if G_RESUME is not None:
        return

    if not os.path.isfile(get_path(GENERATOR)):
        raise Exception('Error: kossman generator does exist')

    if not os.path.isfile(get_path('generador')):
        os.system('g++ {0} -o {1}'.format(get_path(GENERATOR),get_path('generator')))

    if G_DATA_DIST_APPLICATION == 'arrivals':
        if G_TESTCASES > 1:
            size = G_SIZE * G_DIMENTIONS * CONS_DATASET
        else:
            size = G_DIMENTIONS * G_SIZE

        dimentions = G_ARRIVALS
        if dimentions <= 1:
            dimentions = 2

        print_verbose_message('Creating tmp file with Kossmann generator...')
        os.system('{0} {1} {2} {3} {4} > /dev/null'.format(\
            get_path('generator'), dimentions, G_DATA_DIST,size,
            get_path('tmp_{0}_notail'.format(TIME))))
        os.system('tail -n +2 {0} > {1}'.format(\
            get_path('tmp_{0}_notail'.format(TIME)),\
            get_kossman_filename()))
        os.system('rm {0}'.format(get_path('tmp_{0}_notail'.format(TIME))))
        print_verbose_message(' done.\n')

    else:
        raise NotImplementedError

def create_poisson_arrival():
    arrivals = []
    max_arr = []

    if G_POISS_PARAMETER is not None:
            # Single poisson parameter for all dimentions
        for i in range(0,G_DIMENTIONS):
            numpy_arr = numpy.random.poisson(\
                (G_POISS_PARAMETER * G_SIMULATION_TIME) / float(G_INTERVAL)\
                ,G_SIZE)
            dim_poiss_events = numpy_arr.tolist()
            arrivals.append(dim_poiss_events)
            max_arr.append(max(dim_poiss_events))

    else:
            # A different poisson parameter for each dimetion
        for i in range(0,G_DIMENTIONS):
            numpy_arr = numpy.random.poisson(\
                (G_POISS_ARRAY[i] * G_SIMULATION_TIME) / float(G_INTERVAL)\
                ,G_SIZE)
            dim_poiss_events = numpy_arr.tolist()
            arrivals.append(dim_poiss_events)
            max_arr.append(max(dim_poiss_events))


    overall_max = max(max_arr)
    return (overall_max,arrivals)



def generate_poisson_arrivals():
    arrivals = []
    max_arr = []
    global G_ARRIVALS
    for i in range(0,G_TESTCASES):
        tpl = create_poisson_arrival()
        max_arr.append(tpl[0])
        arrivals.append(tpl[1])

    G_ARRIVALS = max(max_arr)
    return arrivals



def create_fixed_arrival():
    arrivals = []

    for i in range(0,G_DIMENTIONS):
        dim_list = []
        for j in range(0,G_SIZE):
            dim_list.append(G_ARRIVALS)

        arrivals.append(dim_list)

    return arrivals

def generate_fixed_arrivals():
    arrivals = []
    for i in range(0,G_TESTCASES):
        arr_list = create_fixed_arrival()
        arrivals.append(arr_list)

    return arrivals


def generate_timestamps(size,dim):

    if G_POISS_PARAMETER != None:
        numpy_arr = numpy.random.exponential(\
                    int(G_SIMULATION_TIME / \
                        (( float(G_POISS_PARAMETER) * G_SIMULATION_TIME) / G_INTERVAL)) \
                        ,size)

    elif G_POISS_ARRAY != None:
        numpy_arr = numpy.random.exponential(\
                    int(G_SIMULATION_TIME / \
                        (( float(G_POISS_ARRAY[dim]) * G_SIMULATION_TIME) / G_INTERVAL)) \
                        ,size)

    else:
        numpy_arr = numpy.random.exponential(\
                    int(G_SIMULATION_TIME / float(G_ARRIVALS)), size)


    timestamp_intervals = numpy_arr.tolist()

    time_counter = 0.0
    timestamps = []

    for interval in timestamp_intervals:
        time_counter = time_counter + interval
        timestamps.append(time_counter)

    return timestamps

def sort_file(intermediate_file_name):
    sorted_intermediate_file_name = intermediate_file_name +'_sorted'



    os.system('sort -T . -g {0} -o {1}'.format(intermediate_file_name, \
        sorted_intermediate_file_name))

    check_file_existence(sorted_intermediate_file_name,'sorted tmp file',path_included=True)

    return sorted_intermediate_file_name


def init_join_data(join_data):
    for id in range(0,G_SIZE):
        join_data[id] = {
            'current_event' : {'ts' : None, 'values' : dims_init()}
        }

def dims_init():
    dims = {}
    for i in range(0,G_DIMENTIONS):
        dims[i] = None
    return dims


def join_tuple_events(unmerged_file_name, act_count):
    print_verbose_message('Joining tuples into single events...\n')
    merged_file_name = get_kossman_filename() + '_merged'
    merged_file = open(merged_file_name,'w')
    unmerged_file = open(unmerged_file_name)
    perc_acum = 0
    perc_total = act_count
    perc = 0
    join_data = {}
    init_join_data(join_data)

    for tupl_str in unmerged_file:
        perc = (perc_acum * 100) / perc_total
        print_verbose_message('\r{0}%'.format(perc))

        tupl = eval('(' + tupl_str)
        ts_t, id_t, dim_t, val_t = tupl

        join_data[id_t]['current_event']['values'][dim_t] = val_t

        ready_tuple = {}
        cont = False
        for i in range(0,G_DIMENTIONS):
            if join_data[id_t]['current_event']['values'][i] == None:
                cont = True
                break
            else:
                ready_tuple[i+2] = join_data[id_t]['current_event']['values'][i]

        perc_acum += 1
        if cont:
            continue

        ready_tuple[0] = ts_t
        ready_tuple[1] = id_t
        tuple_event = tuple(ready_tuple.values())
        merged_file.write(str(tuple_event)[1:] + '\n')

        join_data[id_t]['current_event'] = {'ts' : None, 'values' : dims_init()}

    unmerged_file.close()


    merged_file.close()
    print_verbose_message('\r done\n')
    os.system('rm {0}'.format(unmerged_file_name))
    return merged_file_name

def write_outputfile(intermediate_file_name, testcase_num, act_count):
    if testcase_num != None:
        file_name = G_OUTPUTFILE + '_' + str(testcase_num)
    else:
        file_name = G_OUTPUTFILE

    print_verbose_message('Sorting dataset...')
    sorted_intermediate_file_name = sort_file(intermediate_file_name)
    print_verbose_message(' done.\n')

    if not G_INDEPENDENT_DIMS:
        sorted_intermediate_file_name = \
        join_tuple_events(sorted_intermediate_file_name,act_count)

    input_f = open(sorted_intermediate_file_name)
    output_f = open(file_name,'w')

    print_verbose_message('Writing output file...\n')
    line_counter = 0
    acum = 0
    of_line = ''
    end_of_file_reached = False

    while not end_of_file_reached:
        while line_counter < MAX_ACTUALIZATIONS_LIST_SIZE:
            try:
                line = input_f.next()
            except StopIteration:
                end_of_file_reached = True
                break
            of_line += '(' + line.rstrip('\n') + ', '
            line_counter += 1
            acum += 1
        p = (acum * 100) / act_count
        print_verbose_message('\r{0}%'.format(p))
        if end_of_file_reached:
            output_f.write(of_line[:-2])
        else:
            output_f.write(of_line[:-2] + '\n')
        line_counter = 0
        of_line = ''

    output_f.close()
    os.system('rm {0}'.format(sorted_intermediate_file_name))
    print_verbose_message('\r done.\n')

def intermediate_file_writer(result,intermediate_file):
    while result:
        tp = result.pop()

        #To improve lines could be longer
        intermediate_file.write(str(tp)[1:] + "\n")
        yield


def create_dataset(arrivals, splitter, testcase_num=None):
    kosmann_values = splitter.values_generator()
    tuples = G_SIZE
    dims = range(1,G_DIMENTIONS + 1)
    dim = dims.pop(0)
    result = []
    intermediate_file_name = "{0}_intermediate_file".format(get_kossman_filename())
    intermediate_file = open(intermediate_file_name,'w')
    file_writer = intermediate_file_writer(result, intermediate_file)

    print_verbose_message('Organizing dataset...\n')
    t = G_SIZE * G_DIMENTIONS
    s = 0
    act_count = 0
    for dim in range(0,G_DIMENTIONS):
        tuple_arr = arrivals.pop(0)
        for tuple_id in range(0,G_SIZE):
            values = kosmann_values.next()
            arr = tuple_arr.pop(0)
            timestamps = generate_timestamps(arr,dim)

            s = s + 1
            p = (s * 100) / t
            print_verbose_message('\r{0}%'.format(p))

            for i in range(0,arr):
                val = values.pop(0)
                ts = timestamps.pop(0)
                if ts <= G_SIMULATION_TIME:
                    result.append((ts, tuple_id, dim, float(val),))
                    act_count += 1
                    file_writer.next()

    print_verbose_message('\r done.\n')
    intermediate_file.close()

    write_outputfile(intermediate_file_name, testcase_num, act_count)
    os.system("rm {0}".format(intermediate_file_name))

def generate_datasets(arrival_arr):
    if G_TESTCASES != 1:
        testcase = 0
        for arrival in arrival_arr:
            splitter = KosmannSplitter(get_kossman_filename(),\
                MAX_INTERMEDIATE_FILE_SIZE_GB, G_VERBOSE)
            randomize_file()
            create_dataset(arrival, splitter, testcase)
            splitter.cleanup()
            testcase = testcase + 1
    else:
        splitter = KosmannSplitter(get_kossman_filename(),\
            MAX_INTERMEDIATE_FILE_SIZE_GB, G_VERBOSE)
        randomize_file()
        create_dataset(arrival_arr.pop(),splitter)
        splitter.cleanup()

    if G_DELETE_TMP:
            os.system('rm {0}'.format(get_kossman_filename()))


def main(argv):
    options = get_options(argv)
    check_options(options)
    parse_input(options)

    if not G_ARRIVALS:
        arrival_arr = generate_poisson_arrivals()
    else:
        arrival_arr = generate_fixed_arrivals()

    if G_VERBOSE or G_LEAVE_REPORT or G_LEAVE_SETTINGS:
        report_input()
        report_settings()

    call_kossman()

    generate_datasets(arrival_arr)

    if G_VERBOSE:
        print '\nEverything done!'

if __name__ == "__main__":
    main(sys.argv[1:])
