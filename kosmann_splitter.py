import math, os

class KosmannSplitter():

    def __init__(self,kosmann_file_name, max_file_size_gb):
        self.kosmann_file_name = kosmann_file_name
        self.get_split_variables(max_file_size_gb)

        if self.split_info['slicing']:
            self.intermediate_file_names = \
                                    ['x'+ self.split_format_num(\
                                    self.split_info['slices'], i)\
                                    for i in range(0,self.split_info['slices'])]
    
            

            os.system('split -d -a {0} -n l/{1} {2}'.\
                      format(self.split_info['suffix_length'],\
                             self.split_info['slices'],\
                             self.kosmann_file_name))

    def cleanup(self):
        if self.split_info['slicing']:
            for file_name in self.intermediate_file_names:
                os.system('rm {0}'.format(file_name))

    def get_split_variables(self,max_file_size_gb):
        kosmann_file_size_bytes = os.stat(self.kosmann_file_name).st_size
        kosmann_max_size_bytes = int(max_file_size_gb * \
                                         (1024**3))

        if kosmann_file_size_bytes > kosmann_max_size_bytes:
            slices = int(math.floor( kosmann_file_size_bytes / \
                                         kosmann_max_size_bytes))
            if slices > 999: 
                raise NotImplementedError('File size is too big for slicing'+\
                                              ' with current implementation.')
    
            suffix_length = int(math.floor(math.log(slices,10))) + 1

            self.split_info = {
                'slicing' : True,
                'suffix_length': suffix_length,
                'slices' : slices            
            }
        else:
            self.split_info = {
                'slicing' : False,
            }


    # def check_linecache_memory():
    #     G_MAX_MEM = 4
    #     max_mem_kb = G_MAX_MEM * 1024 * 1024
    #     mem_used = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss    
    #     perc_mem_used = math.ceil((mem_used * 100.0) / max_mem_kb)
    
    # if perc_mem_used >= 99:
    #     linecache.clearcache()
    #     print_verbose_message('\n[warning] linecache cleared\n')
    
    @staticmethod
    def intermediate_file_reader(file_name):
        # for i in rows_index:
        #     # Rows are slided two positions, the first one beacuse 
        #     # the first line of the file is not used. 
        #     # The second because linecache cannot read line 0
        
        #     line = linecache.getline(file_name, i + 2)
        #     if line == '':
        #         raise Exception('Error: Problem with linecache with line {0}'\
            #                         .format(i))
        #     line_parsed = line.split()
        #     yield line_parsed
        #     check_linecache_memory()
        
        # linecache.clearcache()
        
        inter_file = open(file_name)

        for line in inter_file:
            yield line

        inter_file.close()

    def split_format_num(self, slices, num):
        return ("%0" + str(int(math.floor(math.log(slices,10))) + 1) + "i")%num



    def values_generator(self):
        
        if self.split_info['slicing']:

            for file_name in self.intermediate_file_names:
                file_reader = self.intermediate_file_reader(file_name)
                for value in file_reader:
                    yield value

        else:
            raise Exception(':(')
            file_reader = self.intermediate_file_reader(\
                                                self.split_info['file_name'])
            for value in file_reader:
                yield value
