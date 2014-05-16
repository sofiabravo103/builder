#!/usr/bin/python

import os, sys, getopt, random, logging

help_text = '''Usage:
gen_dyn [options] -o <outputfile> -s <size> -d <dimentions> -a <arrivals>
gen_dyn  -o <outputfile> --autodataset
gen_dyn  -o <outputfile> --autotiny

Options:
 --expparameter <num>   exponential parameter to use when generating arrival 
                        timestamps (defaut is a random number between 1 and 20)

  --exparray <string>   specify a string containg exponential parameters separated
                        with an '%' (used when different exponential parameters 
                        will be used to generate arrival timestamps) 
                        example --exparray 0.5%2%4 for 3 dimentions.

  -v                    verbose output  
  --autodataset         generate medium to small random parameters for the 
                        entire dataset (to generate a quick and dirty dataset
                        for tests)

  --autotiny            like --autodataset but generating a tiny dataset
                        (to generate a quick and dirty tiny dataset for control)

  --anticorrelated      use anticorrelated data distribution
  --correlated          use correlated data distribution
  --uniform             use uniform data distribution (default)

 --distributearr        data distribution refers to arrivals 
 --distributedim        data distribution refers to dimentions (default)

  --rarrival            generate random arrival times, instead of exponential.
'''


def get_options(argv):
    try:
        long_options = ['exparray=', 'expparameter=','autodataset', 'autotiny',\
                            'anticorrelated', 'correlated', 'uniform',\
                            'distributearr', 'distributedim', 'rarrival']
        options, args = getopt.getopt(argv,"o:s:d:a:v",long_options)
        if not options:         
            print help_text
            sys.exit(1)
    except getopt.GetoptError:
        print help_text
        sys.exit(2)
    return options

def check_size(arg):
    if '.' in arg or '-' in arg:
        raise Exception('Error: size value (-s) value must be a positive'+\
                            'integer.')
    try: 
        int(arg)
    except ValueError:
        raise Exception('Error: size value (-s) is not a number')

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
    if os.path.isfile(arg):
        ans = raw_input('Warning: file "{0}" exists, continue? [Y/n]: '.format(arg))
        while not (ans == 'y' or ans == 'n' or ans == ''):
            ans = raw_input("Please type 'y' or 'n': ")
        if ans == 'n':
            sys.exit()

def check_exp_parameter(arg):
    pass

def check_exp_array(arg):
    pass

def check_options(options):
    auto = False
    opt_list = [x for x,y in options]
    if '-o' not in opt_list:
        raise Exception('Error: no output file selected')
    for opt, arg in options:
        if opt == '-s':
            check_size(arg)
        elif opt == '-d':
            check_dimentions(arg)
        elif opt == '-a':
            check_arrivals(arg)
        elif opt == '-o':
            check_outputfile(arg)
        elif opt == '--expparamneter':
            check_exp_parameter(arg)
        elif opt == '--exparray':
            check_exp_array(arg)
        elif opt == '--autodataset' or opt == '--autotiny':
            if len(options) >= 3:
                if not (len(options) == 3 and '-v' in opt_list):
                    raise Exception('Error: options --autodataset or '+\
                                        '--autotiny cannot be selected with '+\
                                        'anything else')
            auto = True

    if not auto:
        if '-s' not in opt_list:
            raise Exception('Error: unless --autodataset or --autotiny are '+\
                                'selected size value (-s) must be specified')
        if '-d' not in opt_list:
            raise Exception('Error: unless --autodataset or --autotiny are '+\
                                'selected dimentions value (-d) must be specified')
        if '-a' not in opt_list:
            raise Exception('Error: unless --autodataset or --autotiny are '+\
                                'selected arrivals value (-d) must be specified')

def set_defaults():
    global G_DATA_DIST_APPLICATION
    G_DATA_DIST_APPLICATION = 'dimentions'
        
    global G_DATA_DIST
    G_DATA_DIST = 'E'

    rand = random.random() * 20
    global G_EXP_PARAMETER
    G_EXP_PARAMETER = rand 
    
    global G_AUTO
    global G_TINY
    G_AUTO = False
    G_TINY = False

    global G_RANDOM_ARRIVALS
    G_RANDOM_ARRIVALS = False

def parse_exponential_options(options):
    global G_EXP_PARAMETER
    global G_EXP_ARRAY

    for opt, arg in options: 
      if opt == '--expparameter':
          G_EXP_PARAMETER = float(arg)
      elif opt == '--expparray':
          G_EXP_PARAMETER = None
          G_EXP_ARRAY = [float(x) for x in arg.split('%')]
      elif opt == '--rarrival':
          global G_RANDOM_ARRIVALS
          G_EXP_PARAMETER = None
          G_EXP_ARRAY = None
          G_RANDOM_ARRIVALS = True

def parse_datadist_options(options):
    global G_DATA_DIST

    for opt, arg in options:        
      if opt == '--anticorrelated':
          G_DATA_DIST = 'A'
      elif opt == '--correlated':
          G_DATA_DIST = 'C'
      elif opt == '--distributearr':
          global G_DATA_DIST_APPLICATION
          G_DATA_DIST_APPLICATION = 'arr'


def parse_short_options(options):
    for opt, arg in options:
      if opt == '-s':
          global G_SIZE
          G_SIZE = int(arg)
      elif opt == '-d':
          global G_DIMENTIONS
          G_DIMENTIONS = int(arg)
      elif opt == '-a':
          global G_ARRIVALS
          G_ARRIVALS = int(arg)

def parse_auto(options):
    global G_AUTO
    global G_TINY
    G_AUTO = False

    for opt, arg in options:
        if opt == '--autodataset':
            G_AUTO = True
            G_TINY = False
        elif opt == '--autotiny':
            G_AUTO = True
            G_TINY = True

    return G_AUTO

def parse_input(options):
    global G_VERBOSE
    global OUTPUTFILE
    G_VERBOSE = False
    for opt, arg in options:
        if opt == '-v':            
            G_VERBOSE = True
        elif opt == '-o':
            G_OUTPUTFILE = open(arg,'w')
        
    if not parse_auto(options):
        set_defaults()
        parse_short_options(options)
        parse_datadist_options(options)
        parse_exponential_options(options)

def report_input():
    print 'Generating dataset according to:'        
    for variable in globals():
        if 'G_' in variable:
            value = eval(variable)
            print variable[2:], ":\t", value


def main(argv):
    options= get_options(argv)
    check_options(options)
    parse_input(options)

    if G_VERBOSE:
        report_input()



if __name__ == "__main__":
    main(sys.argv[1:])
