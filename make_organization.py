##scripts to make file/directories organization
import argparse, os
from datetime import datetime as dt
from datetime import timedelta

parser = argparse.ArgumentParser(description="CNR Downloaded")
parser.add_argument("-m", "--mode", help="Mode", choices=["REMOVE_NR","MOVE"], required=True)
parser.add_argument("-i", "--input", help="Input file/directory")
parser.add_argument("-o", "--output", help="Ouput file/directory")
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



def main():
    print('[INFO] Started organization')
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
                        #os.rename(input_path_here,output_path_here)
if __name__ == '__main__':
    main()
