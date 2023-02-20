import argparse

parser = argparse.ArgumentParser(description="Artic resampler")
parser.add_argument("-m", "--mode", help="Mode", choices=["CHECKPY", "CHECK", "DOWNLOAD"], required=True)
parser.add_argument("-d", "--date", help="Date for a single date download")
parser.add_argument("-ilat", "--insitu_lat", help="In situ lat")
parser.add_argument("-ilong", "--insitu_long", help="In situ long")
parser.add_argument("-r", "--resolution", choices=["FR", "RR"], help="Resolution. (FR or RR). Default: FR")
parser.add_argument("-l", "--level", choices=["L1B", "L2"], help="Level. (L1B or L2). Default: L2")
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
args = parser.parse_args()

def main():
    print('STARTED')

    if args.mode=="CHECKPY":
        checkpy()
        return

    if not checkpy():
        return

    if args.mode=="CHECK":
        from eumdac_lois import EUMDAC_LOIS
        edac = EUMDAC_LOIS(True)
        outputdir = '/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_TEST'
        products, product_names, collection_id = edac.search_olci_by_point('2023-01-19', 'FR', 'L2', 45.324091, 12.527398
                                                                       , -1, -1)
        #edac.download_product_from_product_list(products, outputdir)

def checkpy():
    valid = True
    try:
        import shutil
    except:
        print(f'[ERROR] Package shutil is not available')
        valid=False
    try:
        import os
    except:
        print(f'[ERROR] Package os is not available')
        valid=False
    try:
        from datetime import datetime as dt
        from datetime import timedelta
    except:
        print(f'[ERROR] Package datetime is not available')
        valid=False

    try:
        import eumdac
    except:
        print(f'[ERROR] Package eumdac is not available')
        valid=False

    return valid
if __name__ == '__main__':
    main()
