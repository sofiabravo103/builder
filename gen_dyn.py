#!/usr/bin/python

import os, sys, getopt

outputfile = None

help_text = '''Usage:
gen_dyn [options] -o <outputfile> -s <size> -d <dimentions> -a <arrivals>
gen_dyn  -o <outputfile> --autodataset
gen_dyn  -o <outputfile> --autotiny

Options:
 --expparameter <num>   exponential parameter to use when generating arrival 
                        timestamps (defaut is a random number between 0 and 10)

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
 --distributedim        data distribution refers to dimentions

  --rarrival            generate random arrival times, instead of exponentials
'''

def parse_input(options):
    pass

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
    print '-s bien'

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
    print '-d bien'


def check_arrivals(arg):
    if '.' in arg or '-' in arg:
        raise Exception('Error: arrival value (-a) value must be a positive'+\
                            'integer.')
    try: 
        int(arg)
    except ValueError:
        raise Exception('Error: arrival value (-a) is not a number')
    print '-a bien'


def check_outputfile(arg):
    if os.path.isfile(arg):
        ans = raw_input('Warning: file "{0}" exists, continue? [y/n]: '.format(arg))
        while not (ans == 'y' or ans == 'n'):
            ans = raw_input("Please type 'y' or 'n': ")
        if ans == 'n':
            sys.exit()

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
        elif opt == '--autodataset' or opt == '--autotiny':
            if len(options) >= 3:
                if not (len(options) == 3 and '-v' in opt_list):
                    raise Exception('Error: options --autodataset or '+\
                                        '--autotiny cannot be selected with '+\
                                        'anything else')
            auto = True
            print 'autodataset o autotiny bien'

    if not auto:
        if '-s' not in opt_list:
            raise Exception('Error: Unless --autodataset or --autotiny are '+\
                                'selected size value (-s) must be specified')
        if '-d' not in opt_list:
            raise Exception('Error: Unless --autodataset or --autotiny are '+\
                                'selected dimentions value (-d) must be specified')
        if '-a' not in opt_list:
            raise Exception('Error: Unless --autodataset or --autotiny are '+\
                                'selected arrivals value (-d) must be specified')


def main(argv):
    options= get_options(argv)
    check_options(options)
    parse_input(options)


if __name__ == "__main__":
   main(sys.argv[1:])
