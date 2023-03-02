import argparse
import time
import os.path

parser = argparse.ArgumentParser(description="Artic resampler")
parser.add_argument("-m", "--mode", help="Mode", choices=["CHECKPY", "CHECK", "DOWNLOAD", "ARCDOWNLOAD"], required=True)
parser.add_argument("-o", "--output", help="Ouput directory for downloads")
parser.add_argument("-d", "--date", help="Date for a single date download")
parser.add_argument("-sd", "--start_date", help="Start date for multiple donwload")
parser.add_argument("-ed", "--end_date", help="En date for multiple download")
parser.add_argument("-ilat", "--insitu_lat", help="In situ lat")
parser.add_argument("-ilong", "--insitu_long", help="In situ long")
parser.add_argument("-r", "--resolution", choices=["FR", "RR"], help="Resolution. (FR or RR). Default: FR")
parser.add_argument("-l", "--level", choices=["L1B", "L2"], help="Level. (L1B or L2). Default: L2")
parser.add_argument("-cu", "--credentials_user", help="Credentials user from credentials.ini to be used")
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
args = parser.parse_args()


def main():
    print('STARTED')

    if args.mode == "CHECKPY":
        checkpy()
        return

    if not checkpy():
        return

    if args.mode == "CHECK":
        from eumdac_lois import EUMDAC_LOIS
        edac = EUMDAC_LOIS(True, args.credentials_user)
        outputdir = '/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_TEST'
        # products, product_names, collection_id = edac.search_olci_by_point('2023-01-19', 'FR', 'L2', 45.324091, 12.527398
        #                                                                , -1, -1)
        edac.file_list_search = '/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_TEST/file_list.txt'
        product, product_names, collection_id = edac.search_olci_by_bbox('2022-07-15', 'FR', 'L2',
                                                                         [65.0, 90.0, -180.0, 180.0], -1, -1)
        # pname = 'S3A_OL_2_WFR____20220715T232353_20220715T232653_20220717T115842_0179_087_301_1800_MAR_O_NT_003.SEN3'
        # edac.download_product_byname(pname,collection_id,outputdir,False)
        # edac.download_product_from_product_list(products, outputdir)

    if args.mode == "ARCDOWNLOAD":
        from eumdac_lois import EUMDAC_LOIS
        from datetime import datetime as dt
        from datetime import timedelta

        if not args.output:
            print('Output directory is not defined')
            return
        if not args.start_date:
            print('Start date is not defined')
            return
        start_date_str = args.start_date
        if args.end_date:
            end_date_str = args.end_date
        else:
            end_date_str = args.start_date
        outputdir = args.output
        if not os.path.exists(outputdir):
            try:
                os.mkdir(outputdir)
            except:
                print(f'[ERROR] {outputdir} does not exist and could not be created')
                return
        try:
            start_date = dt.strptime(start_date_str, '%Y-%m-%d')
            end_date = dt.strptime(end_date_str, '%Y-%m-%d')
        except:
            print(f'[ERROR] {start_date_str} and/or {end_date_str} are not in the correct format: YYYY-mm-dd')
            return
        if end_date < start_date:
            print(f'[ERROR] {end_date} must be greater or equal than {start_date}')
            return
        run_date = start_date
        while run_date <= end_date:
            edac = EUMDAC_LOIS(args.verbose, args.credentials_user)
            output_folder = os.path.join(outputdir, run_date.strftime('%Y%m%d'))
            if not os.path.exists(output_folder):
                os.mkdir(output_folder)
            run_date_str = run_date.strftime('%Y-%m-%d')
            edac.file_list_search = os.path.join(output_folder, 'eum_filelist.txt')

            ntimes = 1
            nfiles = 0
            while nfiles == 0 and ntimes <= 5:
                products, product_names, collection_id = edac.search_olci_by_bbox(run_date_str, 'FR', 'L2',
                                                                                  [65.0, 90.0, -180.0, 180.0], -1, -1)
                nfiles = len(product_names)
                if nfiles == 0:
                    time.sleep(60)
                ntimes = ntimes + 1

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
                    if ndownload<nfiles:
                        time.sleep(60)
                    ntimes = ntimes + 1

            run_date = run_date + timedelta(hours=24)


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
