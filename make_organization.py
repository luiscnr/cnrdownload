##scripts to make file/directories organization
import argparse, os, subprocess
from datetime import datetime as dt
from datetime import timedelta

import pandas as pd
from eumdac_lois import EUMDAC_LOIS
import numpy as np

parser = argparse.ArgumentParser(description="CNR Downloaded")
parser.add_argument("-m", "--mode", help="Mode", choices=["REMOVE_NR","MOVE","TEST","CHECK_S3","TARA_META_OLCI_GRANULES","UNZIP_S3"], required=True)
parser.add_argument("-i", "--input", help="Input file/directory")
parser.add_argument("-o", "--output", help="Ouput file/directory")
parser.add_argument("-script","--script_file",help="Prepare script file")
# parser.add_argument("-d", "--date", help="Date for a single date download")
parser.add_argument("-sd", "--start_date", help="Start date.")
parser.add_argument("-ed", "--end_date", help="End date.")

args = parser.parse_args()


def get_olci_granules_from_tara_metadata(input_folder,output_file):
    fw = open(output_file,'w')
    fw.write('Date;Granule')
    for name in os.listdir(input_folder):
        if not name.endswith('.csv'):continue
        print(f'[INFO] Working with file name: {name}')
        file_csv = os.path.join(input_folder,name)
        df = pd.read_csv(file_csv,sep=',')
        time_array = df['time_sr']
        lat_array = df['lat']
        lon_array = df['lon']
        lat_min = np.min(lat_array)-0.1
        lat_max = np.max(lat_array)+0.1
        lon_min = np.min(lon_array)-0.1
        lon_max = np.max(lon_array)+0.1
        time_min = dt.strptime(time_array[0],'%Y-%m-%d %H:%M:%S')
        time_max = dt.strptime(time_array[len(time_array)-1], '%Y-%m-%d %H:%M:%S')

        edac = EUMDAC_LOIS(True, None)

        products, list_products, collection_id = edac.search_olci_by_bbox(time_min,'FR','L2',[lat_min,lat_max,lon_min,lon_max],time_min.hour,time_max.hour+1,'NT')

        if len(list_products)>0:
            for p in list_products:
                fw.write('\n')
                fw.write(f'{time_min.strftime("%Y-%m-%d")};{p}')


    fw.close()

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
    dir_base = '/store/COP2-OC-TAC/arc/multi'
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
    file_dates = '/mnt/c/DATA_LUIS/OCTACWORK/DatesRRS_New.csv'
    file_out = '/mnt/c/DATA_LUIS/OCTACWORK/DatesRRS_New_OUT.csv'
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




def do_test_3():
    file_nr = '/mnt/c/DATA_LUIS/OCTACWORK/FilesNRToDelete.slurm'
    files_nr_list = {}
    fr = open(file_nr,'r')
    for line in fr:
        if line.strip().startswith('rm'):
            file_here = line.split(' ')[1].strip()
            file_here_l = os.path.basename(file_here).split('_')
            start = dt.strptime(file_here_l[7],'%Y%m%dT%H%M%S')
            stop = dt.strptime(file_here_l[8],'%Y%m%dT%H%M%S')
            key = start.strftime('%Y-%m-%d')
            if key not in files_nr_list.keys():
                files_nr_list[key]={
                    'start':[start],
                    'stop': [stop]
                }
            else:
                files_nr_list[key]['start'].append(start)
                files_nr_list[key]['stop'].append(stop)

    fr.close()


    file_granules = '/mnt/c/DATA_LUIS/OCTACWORK/GranulesToDownload_UPDATED.txt'
    file_nonrt = '/mnt/c/DATA_LUIS/OCTACWORK/GranulesToDownload_UPDATED_WITH_NRT.txt'
    frg = open(file_granules,'r')
    fw = open(file_nonrt,'w')
    for line in frg:
        line_s = line.split(';')
        key = line_s[0].strip()
        file_here_l = line_s[1].strip().split('_')
        start_f = dt.strptime(file_here_l[7], '%Y%m%dT%H%M%S')
        stop_f = dt.strptime(file_here_l[8], '%Y%m%dT%H%M%S')
        available = False
        if key in files_nr_list:
            start_nr = files_nr_list[key]['start']
            stop_nr = files_nr_list[key]['stop']
            for start_nr_h,stop_nr_h in zip(start_nr,stop_nr):
                if start_nr_h <= start_f <= stop_nr_h:
                    available = True
                    break
                if start_nr_h <= stop_f <= stop_nr_h:
                    available = True
                    break
        if available:
            fw.write(line)

    frg.close()
    fw.close()


def do_test_4():
    file_dates = '/store3/OC/OLCI_BAL/org_scripts/dates_to_complete.txt'
    fw = open(file_dates,'w')
    dir_orig = '/store3/OC/OLCI_BAL/POLYMER_orig'
    dir_dest = '/store3/OC/OLCI_BAL/POLYMER_BAL202411'
    work_date = dt(2017, 1, 1)
    end_date = dt(2023, 12, 31)
    while work_date<=end_date:
        yyyy = work_date.strftime('%Y')
        jjj = work_date.strftime('%j')
        dir_orig_date = os.path.join(dir_orig,yyyy,jjj)
        dir_dest_date = os.path.join(dir_dest,yyyy,jjj)
        if os.path.isdir(dir_orig_date) and not os.path.exists(dir_dest_date):
            fw.write(work_date.strftime('%Y-%m-%d'))
            fw.write('\n')
        work_date = work_date + timedelta(hours=24)
    fw.close()


def do_test_5():
    dir_eum = '/mnt/c/DATA_LUIS/OCTACWORK/2023'
    file_granules = '/mnt/c/DATA_LUIS/OCTACWORK/GranulesToDownload2023_May.txt'
    fw = open(file_granules,'w')
    work_date = dt(2023,5,11)
    end_date = dt(2023,5,15)
    while work_date<=end_date:
        file_g = os.path.join(dir_eum,f'eum_filelist_bal_{work_date.strftime("%Y%m%d")}.txt')
        fr = open(file_g,'r')
        for line in fr:
            line_out = f'{work_date.strftime("%Y-%m-%d")};{line.strip()}'
            fw.write(line_out)
            fw.write('\n')
        fr.close()
        work_date = work_date + timedelta(hours=24)

    fw.close()
def launch_test():
    # do_test_1()
    # do_test_2()
    # do_test_3()
    # do_test_4()
    do_test_5()
def main():
    print('[INFO] Started organization')
    if args.mode == 'TEST':
        launch_test()
        return

    if args.mode == 'TARA_META_OLCI_GRANULES':
        input_path = args.input
        output_file = args.output
        if not os.path.isdir(input_path):
            print(f'[ERROR] Input path {input_path} must be a directory')
            return
        if not os.path.isdir(os.path.dirname(output_file)):
            print(f'[ERROR] Output path {os.path.dirname(output_file)} is not a valid directory')
            return
        if not output_file.endswith('.csv'):
            print(f'[ERROR] Output file {output_file} must be a csv file')
            return
        get_olci_granules_from_tara_metadata(input_path,output_file)
        return

    start_date, end_date = get_dates_from_arg()

    if args.mode == 'UNZIP_S3':
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
                    if not name.endswith('.zip'):continue
                    input_path_here = os.path.join(input_path_date, name)
                    if not os.path.isfile(input_path_here):continue
                    print(f'[INFO] Input path file to unzip: {input_path_here}')
                    cmd = f'unzip -o {input_path_here} -d {input_path_date}'
                    prog = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE)
                    out, err = prog.communicate()
                    if err:
                        print(err)

                    path_unzipped = input_path_here[:-4]
                    if os.path.exists(path_unzipped):
                        os.remove(input_path_here)
                        print(f'[INFO] Unzip process completed')
                    else:
                        print('f[WARNING] Unzip process incompleted, path was not found')

                file_browse = os.path.join(input_path_date,'browse.jpg')
                file_metadata = os.path.join(input_path_date,'EOPMetadata.xml')
                file_manifest = os.path.join(input_path_date,'manifest.xml')
                if os.path.exists(file_browse):os.remove(file_browse)
                if os.path.exists(file_metadata):os.remove(file_metadata)
                if os.path.exists(file_manifest):os.remove(file_manifest)

            work_date = work_date + timedelta(hours=24)

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

    if args.mode == 'CHECK_S3':
        if start_date is None or end_date is None: return
        input_path = args.input
        if not os.path.isdir(input_path):
            print(f'[ERROR] Input path {input_path} is not a valid directory')
            return

        file_list_dates = os.path.join(input_path,'DateList.csv')
        file_list_todownload = os.path.join(input_path,'GranulesToDownload.txt')
        fdownload = open(file_list_todownload,'w')
        file_list_nrtodelete = os.path.join(input_path,'FilesNRToDelete.slurm')
        fnr = open(file_list_nrtodelete,'w')
        fnr = start_slurm(fnr)
        fwl = open(file_list_dates,'w')
        fwl.write('Date;NGranules;NFilesAvailable;NFilesMissing;NFilesNR')
        work_date = start_date
        while work_date <= end_date:
            #print(f'[INFO] Date: {work_date}')
            eum_file_list = os.path.join(input_path,f'eum_filelist_bal_{work_date.strftime("%Y%m%d")}.txt')
            if not os.path.exists(eum_file_list):
                work_date = work_date + timedelta(hours=24)
                continue


            input_path_date = os.path.join(input_path, work_date.strftime('%Y'), work_date.strftime('%j'))

            granule_list = get_granule_list(eum_file_list)

            ngranules = len(granule_list)
            nfilesnr = 0
            nfilesavailable = 0
            nfilesmissing = 0
            if ngranules==0 or not os.path.isdir(input_path_date):
                fwl = add_line(fwl,f'{work_date.strftime("%Y-%m-%d")};{ngranules};{nfilesavailable};{nfilesmissing};{nfilesnr}')
                work_date = work_date + timedelta(hours=24)
                continue

            input_file_list  ={}
            for name in os.listdir(input_path_date):
                if not name.startswith('S3') or not name.endswith('.nc'): continue
                if name.find('_NR_')>0:
                    fnr = add_line(fnr,f'rm {os.path.join(input_path_date,name)}')
                    nfilesnr = nfilesnr + 1
                elif name.find('_NT_')>0:
                    start,stop = get_start_end_date_from_file_name(name)
                    input_file_list[os.path.join(input_path_date,name)]={
                        'start':start,
                        'stop':stop
                    }

            ninput = len(input_file_list)
            if ninput == 0:
                nfilesmissing = ngranules
                for g in granule_list:
                    fdownload = add_line(fdownload,f'{work_date.strftime("%Y-%m-%d")};{g}')
                fwl = add_line(fwl,
                               f'{work_date.strftime("%Y-%m-%d")};{ngranules};{nfilesavailable};{nfilesmissing};{nfilesnr}')
                work_date = work_date + timedelta(hours=24)
                continue



            for g in granule_list:

                available = False
                for f in input_file_list:

                    if granule_list[g]['start'] <= input_file_list[f]['start'] <= granule_list[g]['stop']:
                        available = True
                        break
                    if granule_list[g]['start'] <= input_file_list[f]['stop'] <= granule_list[g]['stop']:
                        available = True
                        break
                if available:
                    nfilesavailable = nfilesavailable + 1
                else:
                    nfilesmissing = nfilesmissing + 1
                    fdownload = add_line(fdownload, f'{work_date.strftime("%Y-%m-%d")};{g}')

            fwl = add_line(fwl,
                           f'{work_date.strftime("%Y-%m-%d")};{ngranules};{nfilesavailable};{nfilesmissing};{nfilesnr}')


            work_date = work_date + timedelta(hours=24)

        fdownload.close()
        fnr.close()
        fwl.close()

def get_granule_list(file_list):
    fr = open(file_list,'r')
    granules = {}
    for line in fr:
        if len(line.strip())>0:
            start_date,end_date = get_start_end_date_from_file_name(line.strip())
            granules[line.strip()]={
                'start':start_date,
                'stop':end_date
            }
    fr.close()
    return granules

def get_start_end_date_from_file_name(file_name):
    name = os.path.basename(file_name)
    name_l  = name.split('_')
    # print(name)
    # print(name_l)
    start_date = dt.strptime(name_l[7],'%Y%m%dT%H%M%S').timestamp()
    end_date = dt.strptime(name_l[8],'%Y%m%dT%H%M%S').timestamp()
    return start_date,end_date
def add_line(fw,line):
    fw.write('\n')
    fw.write(line)
    return fw

def start_slurm(fw):
    fw.write('#!/bin/bash')
    fw = add_line(fw, '#SBATCH --nodes=1')
    fw = add_line(fw, '#SBATCH --ntasks=1')
    fw = add_line(fw, '#SBATCH -p=octac_rep')
    fw = add_line(fw, '#SBATCH --mail-type=BEGIN,END,FAIL')
    fw = add_line(fw, '#SBATCH --mail-user=luis.gonzalezvilas@artov.ismar.cnr.it')
    fw = add_line(fw,'')
    return fw

if __name__ == '__main__':
    main()
