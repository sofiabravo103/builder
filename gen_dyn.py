#!/usr/bin/python

import os, sys, getopt, random, logging, time
TIME = time.time()
GENERATOR = 'generador.cpp'

help_text = '''Usage:
gen_dyn [options] -o <outputfile> -s <size> -d <dimentions> 
gen_dyn  -o <outputfile> --autodataset
gen_dyn  -o <outputfile> --autotiny

Options:
 --arrivals <num>       sepcify how many arrivals per tuple will be generated
                        by default poisson distribution is used  

 --poissparameter <num> poisson parameter to use when generating arrival events.
                        The parameter specifies average arrival events per minute
                        (defaut is a random number between 1 and 10)

  --poissarray <string> specify a string containg poisson parameters separated
                        with an '%' (used when different poisson parameters 
                        will be used to generate arrival events) 
                        example: --poissarray 5%2%10 for 3 dimentions.

 --expparameter <num>   exponential parameter to use when generating arrival 
                        timestamps (defaut is a random number between 1 and 20)

  --exparray <string>   specify a string containg exponential parameters separated
                        with an '%' (used when different exponential parameters 
                        will be used to generate arrival timestamps) 
                        example: --exparray 0.5%2%4 for 3 dimentions.

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
                            'distributearr', 'distributedim', 'rarrival',\
                        'poissarray=','poissparameter=','arrivals=']
        options, args = getopt.getopt(argv,"o:s:d:v",long_options)
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
    if '-' in arg:
        raise Exception('Error: exponential parameter(s) must be a positive'+\
                            ' number')
    try: 
        f = float(arg)
    except ValueError:
        raise Exception('Error: exponential parameter(s) is not a number.')

    if f == 0.0:
        raise Exception('Error: exponential parameter(s) must be greater than'+\
                            ' zero.')


def check_exp_array(arg,options):
    exp_array = arg.split('%')
    dimentions = int([y for x,y in options if x == '-d'][0])
    if len(exp_array) != dimentions:
        raise Exception('Error: --exparray has bad format (is the number of '+\
                            'values in the array equal to the number of '+\
                            'dimentions?)')
    for num in exp_array:
        check_exp_parameter(num)

    opt_list = [x for x,y in options]
    if '--expparameter' in opt_list:
        raise Exception('Error: --expparameter and --exparray cannot be '+\
                            'seleceted together')

def check_not_arrivals(options):
    opt_list = [x for x,y in options]
    if '--arrivals' in opt_list:
        raise Exception('Error: --arrivals cannot be selected along with '+\
                        '--poissparameter or --poissarray')

def check_poiss_parameter(arg):
    if '-' in arg or '.' in arg:
        raise Exception('Error: poisson parameter(s) must be a positive '+\
                            'integer number')
    try: 
        f = int(arg)
    except ValueError:
        raise Exception('Error: poisson parameter(s) is not a number.')

    if f == 0:
        raise Exception('Error: poisson parameter(s) must be greater than'+\
                            ' zero.')


def check_poiss_array(arg,options):
    poiss_array = arg.split('%')
    dimentions = int([y for x,y in options if x == '-d'][0])
    if len(poiss_array) != dimentions:
        raise Exception('Error: --poissarray has bad format (is the number of '+\
                            'values in the array equal to the number of '+\
                            'dimentions?)')
    for num in poiss_array:
        check_poiss_parameter(num)

    opt_list = [x for x,y in options]
    if '--poissparameter' in opt_list:
        raise Exception('Error: --poissparameter and --poissarray cannot be '+\
                            'seleceted together')

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
        elif opt == '-o':
            check_outputfile(arg)
        elif opt == '--poissparameter':
            check_poiss_parameter(arg)
            check_not_arrivals(options)
        elif opt == '--poissarray':
            check_poiss_array(arg,options)
            check_not_arrivals(options)
        elif opt == '--arrrivals':
            check_arrivals(arg)
        elif opt == '--expparameter':
            check_exp_parameter(arg)
        elif opt == '--exparray':
            check_exp_array(arg,options)
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

def set_defaults():
    global G_DATA_DIST_APPLICATION
    global G_DATA_DIST
    global G_POISS_PARAMETER
    global G_EXP_PARAMETER
    global G_AUTO
    global G_TINY
    global G_RANDOM_ARRIVALS

    G_DATA_DIST_APPLICATION = 'dimentions'        

    G_DATA_DIST = 'E'

    G_EXP_PARAMETER = random.random() * 20
    
    G_POISS_PARAMETER = random.randint(1,10)
    
    G_AUTO = False
    G_TINY = False

    G_RANDOM_ARRIVALS = False
        
def parse_probability_options(options):
    global G_EXP_PARAMETER
    global G_EXP_ARRAY
    global G_POISS_PARAMETER
    global G_POISS_ARRAY
    global G_ARRIVALS

    for opt, arg in options: 
      if opt == '--expparameter':
          G_EXP_PARAMETER = float(arg)

      elif opt == '--exparray':
          G_EXP_PARAMETER = None
          G_EXP_ARRAY = [float(x) for x in arg.split('%')]

      elif opt == '--arrivals':  
          G_ARRIVALS = int(arg)
          G_POISS_PARAMETER = None

      elif opt == '--poissparameter':
          G_POISS_PARAMETER = int(arg)

      elif opt == '--poissarray':
          G_POISS_PARAMETER = None
          G_POISS_ARRAY = [int(x) for x in arg.split('%')]          

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

def set_autodataset_values():
    global G_DIMENTIONS
    global G_SIZE
    global G_ARRIVALS
    global G_POISS_PARAMETER

    G_POISS_PARAMETER = None

    if G_TINY:
        max_dim = 3

        min_size = 5
        max_size = 10

        min_arr = 3
        max_arr = 4

    else:
        max_dim = 5

        min_size = 10000
        max_size = 100000
        
        min_arr = 20
        max_arr = 50

    G_DIMENTIONS = random.randint(3,max_dim)
    G_SIZE = random.randint(min_size,max_size)
    G_ARRIVALS = random.randint(min_arr,max_arr)


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

def parse_input(options):
    global G_VERBOSE
    global OUTPUTFILE
    G_VERBOSE = False

    set_defaults()

    for opt, arg in options:
        if opt == '-v':            
            G_VERBOSE = True
        elif opt == '-o':
            G_OUTPUTFILE = open(arg,'w')
        
    if not parse_auto(options):
        parse_short_options(options)
        parse_datadist_options(options)
        parse_probability_options(options)


def report_input():
    print 'Generating dataset according to:'        
    for variable in globals():
        if 'G_' in variable:
            value = eval(variable)
            print variable[2:], ":\t", value

def call_kossman():
    if not os.path.isfile(GENERATOR):
        raise Exception('Error: kossman generator does exist')

    if not os.path.isfile('generador'):
        os.system('g++ {0} -o generator'.format(GENERATOR))

    if G_DATA_DIST_APPLICATION == 'dimentions':
        kossman_columns = G_ARRIVALS * G_DIMENTIONS
        os.system('./generator {0} {1} {2} tmp_{3} > /dev/null'\
                      .format(kossman_columns, G_DATA_DIST, G_SIZE,TIME))

    else:
        kossman_columns = G_ARRIVALS
        for i in range(0,G_DIMENTIONS - 1):
            os.system('./generator {0} {1} {2} tmp_{3}_{4} > /dev/null'\
                          .format(kossman_columns, G_DATA_DIST, G_SIZE,TIME,i))
            
def main(argv):
    options= get_options(argv)
    check_options(options)
    parse_input(options)

    if G_VERBOSE:
        report_input()

#    call_kossman()
    


if __name__ == "__main__":
    main(sys.argv[1:])