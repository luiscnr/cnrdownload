import argparse
import os.path
import shutil

parser = argparse.ArgumentParser(description="CNR Downloaded")
parser.add_argument("-m", "--mode", help="Mode",
                    choices=["CHECKPY", "CHECK", "LISTDOWNLOAD", "ARCDOWNLOAD", "BALDOWNLOAD", "MEDDOWNLOAD",
                             "BLKDOWNLOAD",
                             "AERONET_CHECK", "AERONET_DOWNLOAD", "CSV_DOWNLOAD", "REMOVE"], required=True)
parser.add_argument("-o", "--output", help="Ouput directory for downloads")
parser.add_argument("-d", "--date", help="Date for a single date download")
parser.add_argument("-sd", "--start_date", help="Start date for multiple donwload")
parser.add_argument("-ed", "--end_date", help="En date for multiple download")
parser.add_argument("-ilat", "--insitu_lat", help="In situ lat")
parser.add_argument("-ilong", "--insitu_long", help="In situ long")
parser.add_argument("-aoc", "--aeronetoc_file", help="Aeronet OC NetCDF File")
parser.add_argument("-exp", "--extracts_path", help="Extract path to check Aeronet Files")
parser.add_argument("-t", "--timeliness", help="Timeliness", choices=["NR", "NT"])
parser.add_argument("-res", "--resolution", choices=["FR", "RR"], help="Resolution. (FR or RR). Default: FR")
parser.add_argument("-l", "--level", choices=["L1B", "L2"], help="Level. (L1B or L2). Default: L2")
parser.add_argument("-c", "--config_file", help="Config file")
parser.add_argument("-cu", "--credentials_user", help="Credentials user from credentials.ini to be used")
parser.add_argument("-csv", "--csv_file", help="CSV File")
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
parser.add_argument("-check", "--check_param", help="Check param mode.", action="store_true")
parser.add_argument("-ol", "--only_list", help="Only list, no download.", action="store_true")
args = parser.parse_args()


def only_test():
    if not args.check_param:
        return False
    from eumdac_lois import EUMDAC_LOIS
    edac = EUMDAC_LOIS(True, args.credentials_user)
    limits = [58, 59, 17, 18]
    product, product_names, collection_id = edac.search_olci_by_bbox('2016-05-06', 'FR', 'L1B', limits, -1, -1, 'NT')
    print(product_names[0])
    outputpath = '/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_TEST'
    edac.download_product_byname(product_names[0], collection_id, outputpath, False)

    return True


def only_test_2():
    from eumdac_lois import EUMDAC_LOIS
    edac = EUMDAC_LOIS(True, args.credentials_user)
    edac.get_baltic_ocean()
    return True


def main():
    print('STARTED')

    # if only_test_2():
    #     return

    if args.mode == "CHECKPY":
        checkpy()
        return

    # if not checkpy():
    #     return

    resolution = 'FR'
    if args.resolution:
        resolution = args.resolution

    if args.mode == "CHECK":
        from eumdac_lois import EUMDAC_LOIS
        edac = EUMDAC_LOIS(True, args.credentials_user)
        outputdir = '/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_TEST'
        # products, product_names, collection_id = edac.search_olci_by_point('2023-01-19', 'FR', 'L2', 45.324091, 12.527398
        #                                                                , -1, -1)
        # edac.file_list_search = '/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_TEST/file_list.txt'
        # product, product_names, collection_id = edac.search_olci_by_bbox('2022-07-15', 'FR', 'L2',
        #                                                                  [65.0, 90.0, -180.0, 180.0], -1, -1)
        # pname = 'S3A_OL_2_WFR____20220715T232353_20220715T232653_20220717T115842_0179_087_301_1800_MAR_O_NT_003.SEN3'
        pname = 'S3A_OL_2_WFR____20240421T040424_20240421T040724_20240421T060542_0179_111_261_1800_MAR_O_NR_003.SEN3'
        collection_id = edac.get_olci_collection('2024-04-21', 'FR', 'L2', False, False)
        print(collection_id)
        edac.download_product_byname(pname, collection_id, '/mnt/c/DATA_LUIS/OCTACWORK/CHECK', True)

        # edac.download_product_byname(pname,collection_id,outputdir,False)
        # edac.download_product_from_product_list(products, outputdir)

    if args.mode == "LISTDOWNLOAD":  # two columns, date(as YYYY-mm-dd) and granule
        from eumdac_lois import EUMDAC_LOIS
        import pandas as pd
        from datetime import datetime as dt

        if not args.csv_file:
            print('[ERROR] CSV file is not defined')
            return
        if not args.output:
            print('[ERROR]Output directory is not defined')
            return
        file_csv = args.csv_file
        outputdir = get_output_dir()
        if outputdir is None:
            return

        df = pd.read_csv(file_csv, sep=';')

        if len(df.columns) == 2:

            date_array = df.iloc[:, 0]
            granule_array = df.iloc[:, 1]
            date_ref = 'YYYY-mm-dd'
            granules_donwload = {}
            for date, granule in zip(date_array, granule_array):
                if date != date_ref:
                    date_ref = date
                    granules_donwload[date] = [granule]
                else:
                    granules_donwload[date].append(granule)

            for date_h in granules_donwload:
                outputdir_date = get_output_dir_date(outputdir, dt.strptime(date_h, '%Y-%m-%d'))
                if outputdir_date is None:
                    print(f'[WARNING] Output dir for date {date_h} is not valid. Skipping...')
                
                if 3 <= dt.utcnow().hour <= 6:
                    print(f'[WARNING] Granules can not be downloaded from 3 to 6')
                    return
                print(f'[INFO]{date_h}->{len(granules_donwload[date_h])} granules to be downloaded to {outputdir_date}')
                edac = EUMDAC_LOIS(args.verbose, args.credentials_user)
                collection_id = edac.get_olci_collection(date_h, 'FR', 'L1B', False, False)
                edac.download_product_from_product_list_names(granules_donwload[date_h], collection_id, outputdir_date,
                                                              False)

    if args.mode == "REMOVE":
        from datetime import datetime as dt
        from datetime import timedelta
        if not args.output:
            print('Output directory is not defined')
            return
        if not args.start_date:
            print('Start date is not defined')
            return
        start_date, end_date = get_dates_from_arg()
        if start_date is None or end_date is None:
            return
        outputdir = args.output
        if not os.path.exists(outputdir):
            print(f'[ERROR] {outputdir} does not exist and could not be created')
        if end_date < start_date:
            print(f'[ERROR] {end_date} must be greater or equal than {start_date}')
            return
        print(f'[INFO] Remove start date: {start_date}')
        print(f'[INFO] Remove end date: {end_date}')
        timeliness = 'NT'

        if args.timeliness:
            timeliness = args.timeliness
            print(f'[INFO] Timeliness manually set to: {timeliness}')
        else:
            delta_t = dt.now().replace(hour=0, minute=0, second=0, microsecond=0) - end_date.replace(hour=0, minute=0,
                                                                                                     second=0,
                                                                                                     microsecond=0)
            ndays = delta_t.days
            if ndays < 8:
                timeliness = 'NR'
            print(f'[INFO] Days: {ndays} Timeliness automatically set to: {timeliness}')

        if args.check_param:
            return

        run_date = start_date

        while run_date <= end_date:
            output_folder = os.path.join(outputdir, run_date.strftime('%Y%m%d'))
            if os.path.exists(output_folder):
                list_folders = []
                for name in os.listdir(output_folder):
                    file = os.path.join(output_folder, name)
                    if os.path.isfile(file):
                        os.remove(file)
                    if os.path.isdir(file):
                        list_folders.append(file)
                        for fn in os.listdir(file):
                            os.remove(os.path.join(file, fn))
                if len(list_folders) > 0:
                    for folder in list_folders:
                        os.remove(folder)

            run_date = run_date + timedelta(hours=24)

    if args.mode == "ARCDOWNLOAD":
        from eumdac_lois import EUMDAC_LOIS
        from datetime import datetime as dt
        from datetime import timedelta
        import time

        if not args.output:
            print('Output directory is not defined')
            return
        if not args.start_date:
            print('Start date is not defined')
            return
        start_date, end_date = get_dates_from_arg()
        if start_date is None or end_date is None:
            return
        outputdir = args.output
        if not os.path.exists(outputdir):
            try:
                os.mkdir(outputdir)
            except:
                print(f'[ERROR] {outputdir} does not exist and could not be created')
                return
        if end_date < start_date:
            print(f'[ERROR] {end_date} must be greater or equal than {start_date}')
            return

        print(f'[INFO] Download start date: {start_date}')
        print(f'[INFO] Download end date: {end_date}')
        timeliness = 'NT'

        if args.timeliness:
            timeliness = args.timeliness
            print(f'[INFO] Timeliness manually set to: {timeliness}')
        else:
            delta_t = dt.now().replace(hour=0, minute=0, second=0, microsecond=0) - end_date.replace(hour=0, minute=0,
                                                                                                     second=0,
                                                                                                     microsecond=0)
            ndays = delta_t.days
            if ndays < 8:
                timeliness = 'NR'
            print(f'[INFO] Days: {ndays} Timeliness automatically set to: {timeliness}')

        if args.check_param:
            return

        run_date = start_date

        while run_date <= end_date:
            edac = EUMDAC_LOIS(args.verbose, args.credentials_user)
            run_date_str = run_date.strftime('%Y-%m-%d')

            if args.only_list:
                output_folder = outputdir
                run_date_str_file = run_date.strftime('%Y%m%d')
                if resolution == 'RR':
                    file_list = os.path.join(output_folder, f'eum_filelist_arc_rr_{run_date_str_file}.txt')
                else:
                    file_list = os.path.join(output_folder, f'eum_filelist_arc_{run_date_str_file}.txt')
            else:
                output_folder = os.path.join(outputdir, run_date.strftime('%Y%m%d'))
                if not os.path.exists(output_folder):
                    os.mkdir(output_folder)
                file_list = os.path.join(output_folder, 'eum_filelist.txt')
                if os.path.exists(file_list):
                    file_list_nrt = os.path.join(output_folder, 'eum_filelist_nrt.txt')
                    if os.path.exists(file_list_nrt):
                        os.remove(file_list)
                    else:
                        os.rename(file_list, file_list_nrt)

            edac.file_list_search = file_list

            ntimes = 1
            nfiles = 0
            while nfiles == 0 and ntimes <= 5:
                products, product_names, collection_id = edac.search_olci_by_bbox(run_date_str, resolution, 'L2',
                                                                                  [65.0, 90.0, -180.0, 180.0], -1, -1,
                                                                                  timeliness)
                nfiles = len(product_names)
                if nfiles == 0:
                    time.sleep(10)
                ntimes = ntimes + 1

            if args.only_list:
                run_date = run_date + timedelta(hours=24)
                continue

            ndownload = edac.download_product_from_product_list(products, output_folder, False)
            if args.verbose:
                print(f'[INFO] NDownload {ndownload} / {nfiles}')

            if os.path.exists(edac.file_list_search) and ndownload < nfiles:
                ntimes = 1
                while ndownload < nfiles and ntimes <= 5:
                    f1 = open(edac.file_list_search, 'r')
                    ndownload = 0
                    for line in f1:
                        pname = line.strip()
                        foutput = os.path.join(output_folder, f'{pname}.zip')
                        if os.path.exists(foutput):
                            ndownload = ndownload + 1
                            if args.verbose:
                                print(f'[INFO] Product {pname} already downloaded. Skipping...')
                            continue
                        b = edac.download_product_byname(pname, collection_id, output_folder, False)
                        if b:
                            ndownload = ndownload + 1
                    f1.close()
                    if args.verbose:
                        print(f'[INFO] Attempt: {ntimes} NDownload {ndownload} / {nfiles}')
                    if ndownload < nfiles:
                        time.sleep(60)
                    ntimes = ntimes + 1

            run_date = run_date + timedelta(hours=24)

    if args.mode == "MEDDOWNLOAD":
        from eumdac_lois import EUMDAC_LOIS
        from datetime import datetime as dt
        from datetime import timedelta
        import time

        if not args.output:
            print('Output directory is not defined')
            return
        if not args.start_date:
            print('Start date is not defined')
            return
        start_date, end_date = get_dates_from_arg()
        if start_date is None or end_date is None:
            return
        if end_date < start_date:
            print(f'[ERROR] {end_date} must be greater or equal than {start_date}')
            return

        outputdir = args.output
        if not os.path.exists(outputdir):
            try:
                os.mkdir(outputdir)
            except:
                print(f'[ERROR] {outputdir} does not exist and could not be created')
                return

        print(f'[INFO] Download start date: {start_date}')
        print(f'[INFO] Download end date: {end_date}')
        timeliness = 'NT'

        if args.timeliness:
            timeliness = args.timeliness
            print(f'[INFO] Timeliness manually set to: {timeliness}')
        else:
            delta_t = dt.now().replace(hour=0, minute=0, second=0, microsecond=0) - end_date.replace(hour=0, minute=0,
                                                                                                     second=0,
                                                                                                     microsecond=0)
            ndays = delta_t.days
            if ndays < 8:
                timeliness = 'NR'
            print(f'[INFO] Days: {ndays} Timeliness automatically set to: {timeliness}')

        if args.check_param:
            return

        run_date = start_date
        while run_date <= end_date:
            edac = EUMDAC_LOIS(args.verbose, args.credentials_user)
            run_date_str = run_date.strftime('%Y-%m-%d')

            if args.only_list:
                output_folder = outputdir
                run_date_str_file = run_date.strftime('%Y%m%d')
                if resolution == 'RR':
                    file_list = os.path.join(output_folder, f'eum_filelist_med_rr_{run_date_str_file}.txt')
                else:
                    file_list = os.path.join(output_folder, f'eum_filelist_med_{run_date_str_file}.txt')
            else:
                output_folder = os.path.join(outputdir, run_date.strftime('%Y%m%d'))
                if not os.path.exists(output_folder):
                    os.mkdir(output_folder)
                file_list = os.path.join(output_folder, 'eum_filelist.txt')
                if os.path.exists(file_list):
                    file_list_nrt = os.path.join(output_folder, 'eum_filelist_nrt.txt')
                    if os.path.exists(file_list_nrt):
                        os.remove(file_list)
                    else:
                        os.rename(file_list, file_list_nrt)
            edac.file_list_search = file_list

            ntimes = 1
            nfiles = 0

            while nfiles == 0 and ntimes <= 5:
                products, product_names, collection_id = edac.search_olci_by_bbox(run_date_str, resolution, 'L2',
                                                                                  [30.0, 46.0, -6.0, 36.5], -1, -1,
                                                                                  timeliness)
                nfiles = len(product_names)
                if nfiles == 0:
                    time.sleep(10)
                ntimes = ntimes + 1

            if args.only_list:
                run_date = run_date + timedelta(hours=24)
                continue

            ndownload = edac.download_product_from_product_list(products, output_folder, False)
            if args.verbose:
                print(f'[INFO] NDownload {ndownload} / {nfiles}')

            if os.path.exists(edac.file_list_search) and ndownload < nfiles:
                ntimes = 1
                while ndownload < nfiles and ntimes <= 5:
                    f1 = open(edac.file_list_search, 'r')
                    ndownload = 0
                    for line in f1:
                        pname = line.strip()
                        foutput = os.path.join(output_folder, f'{pname}.zip')
                        if os.path.exists(foutput):
                            ndownload = ndownload + 1
                            if args.verbose:
                                print(f'[INFO] Product {pname} already downloaded. Skipping...')
                            continue
                        b = edac.download_product_byname(pname, collection_id, output_folder, False)
                        if b:
                            ndownload = ndownload + 1
                    f1.close()
                    if args.verbose:
                        print(f'[INFO] Attempt: {ntimes} NDownload {ndownload} / {nfiles}')
                    if ndownload < nfiles:
                        time.sleep(60)
                    ntimes = ntimes + 1

            run_date = run_date + timedelta(hours=24)

    if args.mode == "BLKDOWNLOAD":
        from eumdac_lois import EUMDAC_LOIS
        from datetime import datetime as dt
        from datetime import timedelta
        import time

        if not args.output:
            print('Output directory is not defined')
            return
        if not args.start_date:
            print('Start date is not defined')
            return
        start_date, end_date = get_dates_from_arg()
        if start_date is None or end_date is None:
            return
        outputdir = args.output
        if not os.path.exists(outputdir):
            try:
                os.mkdir(outputdir)
            except:
                print(f'[ERROR] {outputdir} does not exist and could not be created')
                return
        if end_date < start_date:
            print(f'[ERROR] {end_date} must be greater or equal than {start_date}')
            return

        print(f'[INFO] Download start date: {start_date}')
        print(f'[INFO] Download end date: {end_date}')
        timeliness = 'NT'

        if args.timeliness:
            timeliness = args.timeliness
            print(f'[INFO] Timeliness manually set to: {timeliness}')
        else:
            delta_t = dt.now().replace(hour=0, minute=0, second=0, microsecond=0) - end_date.replace(hour=0, minute=0,
                                                                                                     second=0,
                                                                                                     microsecond=0)
            ndays = delta_t.days
            if ndays < 8:
                timeliness = 'NR'
            print(f'[INFO] Days: {ndays} Timeliness automatically set to: {timeliness}')

        if args.check_param:
            return

        run_date = start_date
        while run_date <= end_date:
            edac = EUMDAC_LOIS(args.verbose, args.credentials_user)
            run_date_str = run_date.strftime('%Y-%m-%d')
            if args.only_list:
                output_folder = outputdir
                run_date_str_file = run_date.strftime('%Y%m%d')
                if resolution == 'RR':
                    file_list = os.path.join(output_folder, f'eum_filelist_blk_rr_{run_date_str_file}.txt')
                else:
                    file_list = os.path.join(output_folder, f'eum_filelist_blk_{run_date_str_file}.txt')
            else:
                output_folder = os.path.join(outputdir, run_date.strftime('%Y%m%d'))
                if not os.path.exists(output_folder):
                    os.mkdir(output_folder)
                file_list = os.path.join(output_folder, 'eum_filelist.txt')
                if os.path.exists(file_list):
                    file_list_nrt = os.path.join(output_folder, 'eum_filelist_nrt.txt')
                    if os.path.exists(file_list_nrt):
                        os.remove(file_list)
                    else:
                        os.rename(file_list, file_list_nrt)
            edac.file_list_search = file_list

            ntimes = 1
            nfiles = 0
            while nfiles == 0 and ntimes <= 5:
                products, product_names, collection_id = edac.search_olci_by_bbox(run_date_str, resolution, 'L2',
                                                                                  [40.0, 48.0, 36.5, 42.0], -1, -1,
                                                                                  timeliness)
                nfiles = len(product_names)
                if nfiles == 0:
                    time.sleep(10)
                ntimes = ntimes + 1

            if args.only_list:
                run_date = run_date + timedelta(hours=24)
                continue

            ndownload = edac.download_product_from_product_list(products, output_folder, False)
            if args.verbose:
                print(f'[INFO] NDownload {ndownload} / {nfiles}')

            if os.path.exists(edac.file_list_search) and ndownload < nfiles:
                ntimes = 1
                while ndownload < nfiles and ntimes <= 5:
                    f1 = open(edac.file_list_search, 'r')
                    ndownload = 0
                    for line in f1:
                        pname = line.strip()
                        foutput = os.path.join(output_folder, f'{pname}.zip')
                        if os.path.exists(foutput):
                            ndownload = ndownload + 1
                            if args.verbose:
                                print(f'[INFO] Product {pname} already downloaded. Skipping...')
                            continue
                        b = edac.download_product_byname(pname, collection_id, output_folder, False)
                        if b:
                            ndownload = ndownload + 1
                    f1.close()
                    if args.verbose:
                        print(f'[INFO] Attempt: {ntimes} NDownload {ndownload} / {nfiles}')
                    if ndownload < nfiles:
                        time.sleep(60)
                    ntimes = ntimes + 1

            run_date = run_date + timedelta(hours=24)

    if args.mode == "BALDOWNLOAD":
        from eumdac_lois import EUMDAC_LOIS
        from datetime import datetime as dt
        from datetime import timedelta
        import time

        outputdir = get_output_dir()
        if outputdir is None:
            return

        start_date, end_date = get_dates_from_arg()
        if start_date is None or end_date is None:
            return

        print(f'[INFO] Download start date: {start_date}')
        print(f'[INFO] Download end date: {end_date}')

        timeliness = get_timeliness(end_date)

        if args.check_param:
            return
        # bbox =  [53.25, 65.85, 9.25, 30.25]
        lat_points = [53.25, 62, 66.25, 66.25, 64.6, 61.20, 61.20, 58, 53.25, 53.25]
        lon_points = [9.25, 9.25, 21.8, 26.75, 26.75, 23, 30.25, 30.25, 20.3, 9.25]
        bbox = [lat_points, lon_points]

        run_date = start_date
        while run_date <= end_date:
            edac = EUMDAC_LOIS(args.verbose, args.credentials_user)
            # output_year = os.path.join(outputdir, run_date.strftime('%Y'))
            # output_folder = os.path.join(output_year, run_date.strftime('%j'))
            # if not os.path.exists(output_year):
            #     os.mkdir(output_year)
            # output_folder = os.path.join(outputdir, run_date.strftime('%Y%m%d'))
            # if not os.path.exists(output_folder):
            #     os.mkdir(output_folder)
            run_date_str = run_date.strftime('%Y-%m-%d')
            if args.only_list:
                output_folder = outputdir
                run_date_str_file = run_date.strftime('%Y%m%d')
                if resolution == 'RR':
                    file_list = os.path.join(output_folder, f'eum_filelist_bal_rr_{run_date_str_file}.txt')
                else:
                    file_list = os.path.join(output_folder, f'eum_filelist_bal_{run_date_str_file}.txt')
            else:
                output_folder = os.path.join(outputdir, run_date.strftime('%Y%m%d'))
                if not os.path.exists(output_folder):
                    os.mkdir(output_folder)
                file_list = os.path.join(output_folder, 'eum_filelist.txt')
                if os.path.exists(file_list):
                    file_list_nrt = os.path.join(output_folder, 'eum_filelist_nrt.txt')
                    if os.path.exists(file_list_nrt):
                        os.remove(file_list)
                    else:
                        os.rename(file_list, file_list_nrt)
            # file_list = os.path.join(output_folder, 'eum_filelist.txt')
            # if os.path.exists(file_list):
            #     file_list_nrt = os.path.join(output_folder, 'eum_filelist_nrt.txt')
            #     if os.path.exists(file_list_nrt):
            #         os.remove(file_list)
            #     else:
            #         os.rename(file_list, file_list_nrt)
            edac.file_list_search = file_list

            ntimes = 1
            nfiles = 0
            while nfiles == 0 and ntimes <= 5:
                products, product_names, collection_id = edac.search_olci_by_bbox(run_date_str, resolution, 'L1B',
                                                                                  bbox, 3, 18,
                                                                                  timeliness)
                nfiles = len(product_names)
                if nfiles == 0:
                    time.sleep(10)
                ntimes = ntimes + 1

            if args.only_list:
                run_date = run_date + timedelta(hours=24)
                continue

            ndownload = edac.download_product_from_product_list(products, output_folder, False)
            if args.verbose:
                print(f'[INFO] NDownload {ndownload} / {nfiles}')

            if os.path.exists(edac.file_list_search) and ndownload < nfiles:
                ntimes = 1
                while ndownload < nfiles and ntimes <= 5:
                    f1 = open(edac.file_list_search, 'r')
                    ndownload = 0
                    for line in f1:
                        pname = line.strip()
                        foutput = os.path.join(output_folder, f'{pname}.zip')
                        if os.path.exists(foutput):
                            ndownload = ndownload + 1
                            if args.verbose:
                                print(f'[INFO] Product {pname} already downloaded. Skipping...')
                            continue
                        b = edac.download_product_byname(pname, collection_id, output_folder, False)
                        if b:
                            ndownload = ndownload + 1
                    f1.close()
                    if args.verbose:
                        print(f'[INFO] Attempt: {ntimes} NDownload {ndownload} / {nfiles}')
                    if ndownload < nfiles:
                        time.sleep(60)
                    ntimes = ntimes + 1

            run_date = run_date + timedelta(hours=24)

    if args.mode == 'AERONET_CHECK':
        ##CHECK THE DATES AND GRANULES AVAILABLE FOR AERONET OC

        from eumdac_lois import EUMDAC_LOIS
        from datetime import datetime as dt
        from datetime import timedelta

        if not args.aeronetoc_file:
            print('[ERROR] Option -aoc (--aeronetoc_file) is compulsory for AERONET_CHECK mode')
            return
        file_aeronet = args.aeronetoc_file
        if not os.path.exists(file_aeronet):
            print(f'[ERROR] File {file_aeronet} is not available. ')
            return
        name = file_aeronet.split('/')[-1]
        site = name[name.find('_') + 1:name.find('.')]
        site = site[site.find('_') + 1:len(site)]
        if args.verbose:
            print(f'[INFO] Site is: {site}')
        limits = get_limits_site(site)
        if args.verbose:
            print(f'[INFO] Site limits: {limits}')

        from netCDF4 import Dataset
        import numpy as np
        dataset = Dataset(file_aeronet)
        time_array = np.array(dataset.variables['Time'])
        time_list = []
        for time in time_array:
            time_here = (dt(1970, 1, 1) + timedelta(seconds=time)).replace(hour=0, minute=0, second=0, microsecond=0)
            if time_here not in time_list:
                time_list.append(time_here)
        dataset.close()

        from eumdac_lois import EUMDAC_LOIS
        edac = EUMDAC_LOIS(True, args.credentials_user)
        extracts_path = None
        if args.extracts_path:
            extracts_path = args.extracts_path
        info_extracts = get_info_from_extract_path(extracts_path)
        for this_date in info_extracts['S3A']:
            print(this_date, info_extracts['S3A'][this_date])

        lines = []
        # time_limit = dt(2018,8,24)
        level = 'L2'
        resolution = 'FR'
        if args.level:
            level = args.level
        if args.resolution:
            resolution = args.resolution
        for time in time_list:
            # if time<time_limit:
            #    continue
            time_str = time.strftime('%Y-%m-%d')
            product, product_names, collection_id = edac.search_olci_by_bbox(time_str, resolution, level, limits, -1,
                                                                             -1, 'NT')

            for namep in product_names:
                platform, datestr, hours_start, hours_end = get_datestr_and_hours(namep)
                append = True
                if info_extracts is not None and platform is not None and platform in info_extracts:
                    if datestr in info_extracts[platform]:
                        if hours_start <= info_extracts[platform][datestr] <= hours_end:
                            append = False
                            print(f'There is already a extract for: {namep}')
                if append:
                    line_here = f'{time_str};{namep}'
                    print(line_here)
                    lines.append(line_here)

        file_out = 'ListGranules.csv'
        if args.output:
            file_out = args.output
        f1 = open(file_out, 'w')
        for line in lines:
            f1.write(line)
            f1.write('\n')
        f1.close()

    if args.mode == 'AERONET_DOWNLOAD':
        # WORKS WITH CONFIG FILE (args.config_file)
        # IF GRANULES ARE AVAILABLE IN SOURCE FOLDERS, THEY ARE DIRECTLY COPYIED TO OUTPUT
        # IF GANULES ARE NOT AVAILABLE, THEY'RE DOWNLAODED
        from eumdac_lois import EUMDAC_LOIS
        from datetime import datetime as dt
        if not args.config_file:
            print('[ERROR] Argument config_file is compulsory with AERONET_DOWNLOAD mode')
            return
        config_file = args.config_file
        if not os.path.exists(config_file):
            print(f'[ERROR] Config file {config_file} does not exist')
            return
        import configparser
        options = configparser.ConfigParser()
        options.read(config_file)
        section = 'AERONET_DOWNLOAD'
        if not options.has_section(section):
            print(f'[ERROR] {section} section is not available in config. file: {config_file}')
            return
        file_granules = None
        if options.has_option(section, 'file_granules'):
            file_granules = options[section]['file_granules']
        else:
            print(f'[ERROR] Option file_granules is not available in section {section} in config file {config_file}')
        if not os.path.exists(file_granules):
            print(f'[ERROR] file_granules: {file_granules} does not exist')
            return
        output_path = None
        if options.has_option(section, 'output_path'):
            output_path = options[section]['output_path']
        else:
            print(f'[ERROR] Option output_path is not available in section {section} in config file {config_file}')
        if not os.path.exists(output_path):
            print(f'[ERROR] output_path: {output_path} does not exist')
            return

        source_folders = []
        if options.has_option(section, 'source_folders'):
            sf = options[section]['source_folders']
            source_folders = sf.strip().split(',')
        if len(source_folders) > 0:
            for source_folder in source_folders:
                if not os.path.exists(source_folder):
                    print(f'[ERROR] Source folder: {source_folder} does not exist')

        resolution = 'FR'
        level = 'L2'
        if options.has_option(section, 'resolution'):
            resolution = options[section]['resolution']
        if options.has_option(section, 'level'):
            level = options[section]['level']

        ntodownload = 0
        ndownload = 0
        edac = EUMDAC_LOIS(True, args.credentials_user)
        f1 = open(file_granules, 'r')
        for line in f1:
            lines = line.split(';')
            datestr = lines[0].strip()
            datehere = dt.strptime(datestr, '%Y-%m-%d')

            granule = lines[1].strip()
            granule_ref = granule.strip()[0:32]
            make_download = True
            for source_folder in source_folders:
                fgranule = get_fgranule(source_folder, datehere, granule_ref)
                if fgranule is not None:
                    name_granule = fgranule.split('/')[-1]
                    fout = os.path.join(output_path, name_granule)
                    if os.path.exists(fout):
                        print(f'[INFO] {name_granule} is already available in {output_path}. Skipping...')
                    else:
                        print(f'[INFO] Copying {name_granule} to {output_path}')
                        shutil.copy(fgranule, fout)
                    make_download = False
                    break
            if make_download:
                print(f'[INFO] Granule {granule} was not found in source folders. Preparing download....')
                foutput = os.path.join(output_path, f'{granule}.zip')
                if os.path.exists(foutput):
                    ndownload = ndownload + 1
                    if args.verbose:
                        print(f'[INFO] Product {granule} already downloaded. Skipping...')
                    continue
                print(f'[INFO] Donwloading granule: {granule}')
                ntodownload = ntodownload + 1
                collection_id = edac.get_olci_collection(datehere, resolution, level, False, False)
                b = edac.download_product_byname(granule.strip(), collection_id, output_path, False)
                if b:
                    ndownload = ndownload + 1
        f1.close()
        print(f'[INFO] Granules to be downloaded: {ntodownload}')
        print(f'[INFO] Granules downloaded: {ndownload}')

    if args.mode == 'CSV_DOWNLOAD':
        csv_file = args.csv_file
        if not os.path.isfile(csv_file):
            print(f'[ERROR] CSV file {csv_file} is not a valid file or does not exist')
            return
        import pandas as pd
        from datetime import datetime as dt
        dset = pd.read_csv(csv_file, sep=';')
        dates_limits = {}
        for index, row in dset.iterrows():
            date_here_str = row['Date']
            # date_here = dt.strptime(date_here_str,'%Y-%m-%d')
            time_here_str = row['Time']
            time_here_str = f'{date_here_str}T{time_here_str}'
            try:
                time_here = dt.strptime(time_here_str, '%Y-%m-%dT%H:%M')
            except:
                time_here = dt.strptime(date_here_str, '%Y-%m-%d')
            lat = float(row['Lat'])
            lon = float(row['Long'])
            if date_here_str not in dates_limits.keys():
                dates_limits[date_here_str] = {
                    'time_list': [time_here],
                    'lat_min': lat,
                    'lat_max': lat,
                    'lon_min': lon,
                    'lon_max': lon
                }
            else:

                dates_limits[date_here_str]['time_list'].append(time_here)
                if lat < dates_limits[date_here_str]['lat_min']:
                    dates_limits[date_here_str]['lat_min'] = lat
                if lat > dates_limits[date_here_str]['lat_max']:
                    dates_limits[date_here_str]['lat_max'] = lat
                if lon < dates_limits[date_here_str]['lon_min']:
                    dates_limits[date_here_str]['lon_min'] = lon
                if lon > dates_limits[date_here_str]['lon_max']:
                    dates_limits[date_here_str]['lon_max'] = lon

        for date_str in dates_limits:
            nvalues = len(dates_limits[date_str]['time_list'])
            print(date_str, nvalues, dates_limits[date_str]['lat_min'], dates_limits[date_str]['lat_max'],
                  dates_limits[date_str]['lon_min'], dates_limits[date_str]['lon_max'])


def get_fgranule(source_folder, datehere, granule_ref):
    yearstr = datehere.strftime('%Y')
    jjjstr = datehere.strftime('%j')
    path = os.path.join(source_folder, yearstr, jjjstr)
    fgranule = None
    if os.path.exists(path):
        for name in os.listdir(path):
            if name.startswith(granule_ref):
                fgranule = os.path.join(path, name)
                break
    return fgranule


def get_info_from_extract_path(extract_path):
    if extract_path is None:
        return None
    if not os.path.exists(extract_path):
        return None
    info = {key: {} for key in ['S3A', 'S3B']}
    for name in os.listdir(extract_path):
        if not name.startswith('S3'):
            continue
        platform, start_date, end_date = get_dates_and_platform_from_file_name(name)
        date_ref = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        date_str = start_date.strftime('%Y-%m-%d')
        hours = ((start_date - date_ref).total_seconds()) / 3600
        info[platform][date_str] = hours
    return info


def get_datestr_and_hours(name):
    platform, start_date, end_date = get_dates_and_platform_from_file_name(name)
    if platform is None or start_date is None or end_date is None:
        return platform, None, start_date, end_date

    datestr = start_date.strftime('%Y-%m-%d')
    start_date_ref = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date_ref = end_date.replace(hour=0, minute=0, second=0, microsecond=0)
    hours_start = ((start_date - start_date_ref).total_seconds()) / 3600
    hours_end = ((end_date - end_date_ref).total_seconds()) / 3600

    return platform, datestr, hours_start, hours_end


def get_dates_and_platform_from_file_name(name):
    from datetime import datetime as dt
    platform = None
    start_date = None
    end_date = None
    try:
        platform = name.split('_')[0]
        start_date = dt.strptime(name.split('_')[7], '%Y%m%dT%H%M%S')
        end_date = dt.strptime(name.split('_')[8], '%Y%m%dT%H%M%S')
    except:
        pass
    return platform, start_date, end_date


def get_limits_site(site):
    limits = None
    if site == 'Gustav_Dalen_Tower':
        limits = [58, 59, 17, 18]
    if site == 'Irbe_Lighthouse':
        limits = [57.25, 58.25, 21.25, 22.25]
    if site == 'Helsinki_Lighthouse':
        limits = [59.5, 60.5, 24.5, 25.5]

    return limits


def get_output_dir():
    if not args.output:
        print('Output directory is not defined')
        return None
    outputdir = args.output
    if not os.path.exists(outputdir):
        try:
            os.mkdir(outputdir)
        except:
            print(f'[ERROR] {outputdir} does not exist and could not be created')
            return None
    return outputdir


def get_output_dir_date(outputdir, date_here):
    outputdiryear = os.path.join(outputdir, date_here.strftime('%Y'))
    if not os.path.exists(outputdiryear):
        try:
            os.mkdir(outputdiryear)
        except:
            print(f'[ERROR] {outputdiryear} does not exist and could not be created')
            return None
    outputdirdate = os.path.join(outputdiryear, date_here.strftime('%j'))
    if not os.path.exists(outputdirdate):
        try:
            os.mkdir(outputdirdate)
        except:
            print(f'[ERROR] {outputdirdate} does not exist and could not be created')
            return None
    return outputdirdate


def get_timeliness(end_date):
    from datetime import datetime as dt
    timeliness = 'NT'

    if args.timeliness:
        timeliness = args.timeliness
        print(f'[INFO] Timeliness manually set to: {timeliness}')
    else:
        delta_t = dt.now().replace(hour=0, minute=0, second=0, microsecond=0) - end_date.replace(hour=0, minute=0,
                                                                                                 second=0,
                                                                                                 microsecond=0)
        ndays = delta_t.days
        if ndays < 8:
            timeliness = 'NR'
        print(f'[INFO] Days: {ndays} Timeliness automatically set to: {timeliness}')
    return timeliness


def get_dates_from_arg():
    if not args.start_date:
        print('Start date is not defined')
        return None, None

    from datetime import datetime as dt
    from datetime import timedelta
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


def checkpy():
    valid = True
    try:
        import shutil
    except:
        print(f'[ERROR] Package shutil is not available')
        valid = False
    try:
        import os
    except:
        print(f'[ERROR] Package os is not available')
        valid = False
    try:
        from datetime import datetime as dt
        from datetime import timedelta
    except:
        print(f'[ERROR] Package datetime is not available')
        valid = False

    try:
        import eumdac
    except:
        print(f'[ERROR] Package eumdac is not available')
        valid = False
    try:
        from eumdac_lois import EUMDAC_LOIS

        EUMDAC_LOIS(True, args.credentials_user)

    except:
        print(f'[ERROR] EUMDAC_LOIS could not be started. Check authorization file')
        valid = False

    return valid


if __name__ == '__main__':
    main()
