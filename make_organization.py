##scripts to make file/directories organization
import argparse, os
from datetime import datetime as dt
from datetime import timedelta

parser = argparse.ArgumentParser(description="CNR Downloaded")
parser.add_argument("-m", "--mode", help="Mode", choices=["REMOVE_NR","MOVE","TEST"], required=True)
parser.add_argument("-i", "--input", help="Input file/directory")
parser.add_argument("-o", "--output", help="Ouput file/directory")
parser.add_argument("-script","--script_file",help="Prepare script file")
# parser.add_argument("-d", "--date", help="Date for a single date download")
parser.add_argument("-sd", "--start_date", help="Start date.")
parser.add_argument("-ed", "--end_date", help="End date.")

args = parser.parse_args()


def get_dates_from_arg():
    if not args.start_date:
        print('Start date is not defined')
        return None, None

    start_date = None
    end_date = None
    if args.start_date:
        try:
            start_date = dt.strptime(args.start_date, '%Y-%m-%d')
        except:
            try:
                tdelta = int(args.start_date)
                start_date = dt.now() + timedelta(days=tdelta)
                start_date = start_date.replace(hour=12, minute=0, second=0, microsecond=0)
            except:
                print(f'[ERROR] Start date {args.start_date} is not in the correct format: YYYY-mm-dd or integer')
    if args.end_date:
        try:
            end_date = dt.strptime(args.end_date, '%Y-%m-%d')
        except:
            try:
                tdelta = int(args.end_date)
                end_date = dt.now() + timedelta(days=tdelta)
                end_date = end_date.replace(hour=12, minute=0, second=0, microsecond=0)
            except:
                print(f'[ERROR] End date {args.end_date} is not in the correct format: YYYY-mm-dd or integer')
    if args.start_date and not args.end_date:
        end_date = start_date

    if start_date is not None and end_date is not None:
        if end_date < start_date:
            print(f'[ERROR] {end_date} must be greater or equal than {start_date}')
            return None, None

    return start_date, end_date

def get_output_path_date(output_path,date):
    path_year = createIfNotExist(os.path.join(output_path,date.strftime('%Y')))
    path_jday = createIfNotExist(os.path.join(path_year,date.strftime('%j'))) if path_year is not None else None
    return path_jday

def createIfNotExist(folder):
    if not os.path.exists(folder):
        try:
            os.mkdir(folder)
        except:
            return None

    return folder


def start_script_file(script_file):
    fw = open(script_file,'w')
    fw.write('#!/bin/bash')
    fw.write('\n')
    fw.write('#SBATCH --nodes=1')
    fw.write('\n')
    fw.write('#SBATCH --ntasks=1')
    fw.write('\n')
    fw.write('#SBATCH -p octac_rep')
    fw.write('\n')
    fw.write('#SBATCH --mail-type=BEGIN,END,FAIL')
    fw.write('\n')
    fw.write('#SBATCH --mail-user=luis.gonzalezvilas@artov.ismar.cnr.it')
    fw.write('\n')
    fw.write('\n')
    fw.write('\n')

    return fw


def do_test_1():
    from netCDF4 import Dataset
    work_date = dt(1997,9,1)
    end_date = dt(2023,12,31)
    dir_base = '/store/COP2-OC-TAC/arc/multi_temp'
    file_out_rrs = os.path.join(dir_base,'DatesRRS_New.csv')
    # file_out_chl = os.path.join(dir_base, 'DatesCHL.csv')
    # file_out_kd = os.path.join(dir_base, 'DatesKD.csv')
    frrs = open(file_out_rrs,'w')
    # fchl = open(file_out_chl, 'w')
    # fkd = open(file_out_kd, 'w')
    first_line = 'Date;TimeStamp'
    frrs.write(first_line)
    # fchl.write(first_line)
    # fkd.write(first_line)
    while work_date<=end_date:
        dir_date = os.path.join(dir_base,work_date.strftime('%Y'),work_date.strftime('%j'))
        if os.path.exists(dir_date):
            print(f'[INFO] Work date: {work_date.strftime("%Y-%m-%d")}')
            str_date = work_date.strftime('%Y%j')

            file_rrs = os.path.join(dir_date,f'C{str_date}_rrs-arc-4km.nc')
            if os.path.exists(file_rrs):
                dataset_rrs = Dataset(file_rrs)
                ts_rrs = float(dataset_rrs.variables['time'][0])
                frrs.write('\n')
                frrs.write(f'{work_date.strftime("%Y-%m-%d")};{ts_rrs}')
                dataset_rrs.close()

            # file_chl = os.path.join(dir_date,f'C{str_date}_chl-arc-4km.nc')
            # if os.path.exists(file_chl):
            #     dataset_chl = Dataset(file_chl)
            #     ts_chl = float(dataset_chl.variables['time'][0])
            #     fchl.write('\n')
            #     fchl.write(f'{work_date.strftime("%Y-%m-%d")};{ts_chl}')
            #     dataset_chl.close()
            #
            # file_kd = os.path.join(dir_date,f'C{str_date}_kd490-arc-4km.nc')
            # if os.path.exists(file_kd):
            #     dataset_kd = Dataset(file_kd)
            #     ts_kd = float(dataset_kd.variables['time'][0])
            #     fkd.write('\n')
            #     fkd.write(f'{work_date.strftime("%Y-%m-%d")};{ts_kd}')
            #     dataset_kd.close()

        work_date = work_date + timedelta(hours=24)
    frrs.close()
    # fchl.close()
    # fkd.close()

def do_test_2():
    file_dates = '/mnt/c/DATA_LUIS/OCTACWORK/DatesRRS.csv'
    file_out = '/mnt/c/DATA_LUIS/OCTACWORK/DatesRRS_OUT.csv'
    import pandas as pd
    import numpy as np
    df = pd.read_csv(file_dates,sep=';')
    timestamp = np.array(df['TimeStamp'])
    dates_1970 = []
    dates_1981 = []
    ref_1981 = dt(1981,1,1,0,0,0)
    for t in timestamp:
        dates_1981.append((ref_1981+timedelta(seconds=t)).strftime('%Y-%m-%d'))
        dates_1970.append(dt.utcfromtimestamp(t).strftime('%Y-%m-%d'))

    df['dates_1970'] = dates_1970
    df['dates_1981'] = dates_1981
    df.to_csv(file_out,sep=';')




def launch_test():
    do_test_1()
    #do_test_2()
def main():
    print('[INFO] Started organization')
    if args.mode == 'TEST':
        launch_test()
        return
    start_date, end_date = get_dates_from_arg()



    if args.mode == 'REMOVE_NR':
        if start_date is None or end_date is None: return
        input_path = args.input
        if not os.path.isdir(input_path):
            print(f'[ERROR] Input path {input_path} is not a valid directory')
            return
        work_date = start_date
        while work_date <= end_date:
            input_path_date = os.path.join(input_path, work_date.strftime('%Y'), work_date.strftime('%j'))
            if os.path.exists(input_path_date):
                print(f'[INFO] Input path date: {input_path_date} ')
                for name in os.listdir(input_path_date):
                    input_path_here = os.path.join(input_path_date, name)
                    index_nr = name.find('_NR_')
                    if index_nr > 0:
                        if name.endswith('.zip') and os.path.isfile(input_path_here):
                            os.remove(input_path_here)
                        if os.path.isdir(input_path_here):
                            for name_h in os.listdir(input_path_here):
                                os.remove(os.path.join(input_path_here, name_h))
                            os.rmdir(input_path_here)
            work_date = work_date + timedelta(hours=24)

    if args.mode == 'MOVE':
        if start_date is None or end_date is None: return
        input_path = args.input
        if not os.path.isdir(input_path):
            print(f'[ERROR] Input path {input_path} is not a valid directory')
            return
        output_path = args.output
        if not os.path.isdir(output_path):
            print(f'[ERROR] Output path {output_path} is not a valid directory')
            return

        work_date = start_date
        fw = None
        if args.script_file:
            fw = start_script_file(args.script_file)
        while work_date <= end_date:
            input_path_date = os.path.join(input_path, work_date.strftime('%Y'), work_date.strftime('%j'))

            if os.path.exists(input_path_date):
                print(f'[INFO] Input path date: {input_path_date} ')
                output_path_date = get_output_path_date(output_path, work_date)
                if os.path.exists(output_path_date):
                    for name in os.listdir(input_path_date):
                        input_path_here = os.path.join(input_path_date, name)
                        output_path_here = os.path.join(output_path_date,name)
                        print(f'[INFO] {input_path_here}-->{output_path_here}')
                        if fw is not None:
                            fw.write('\n')
                            fw.write(f'mv {input_path_here} {output_path_here}')
                        else:
                            os.rename(input_path_here,output_path_here)


            work_date = work_date + timedelta(hours=24)

        if fw is not None:
            fw.close()
if __name__ == '__main__':
    main()
