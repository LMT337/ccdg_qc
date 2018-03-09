import os
import shutil
import subprocess
import csv
import datetime
import argparse
import webbrowser

# from .classes.woid import Woid

# declare global var, list, dictionaries
aligned_samples = dict()
compute_header_fields = list()
woid = ''

# current mmddyy
mm_dd_yy = datetime.datetime.now().strftime("%m%d%y")


def main():
    desc_str = """
        Update qcstatus file with qc'd samples for tracking, run qc scripts.
    """

    parser = argparse.ArgumentParser(description=desc_str)

    group = parser.add_mutually_exclusive_group()

    group.add_argument("-f", type=str, help='Input compute workflow file with all sample alignments')
    group.add_argument("-c", help='collaborator name', type=str)
    group.add_argument('-m', help='Manual QC.', action='store_true')
    group.add_argument('-l', help='Link to links compute workflow file', action='store_true')

    args = parser.parse_args()

    if args.l:
        woid = os.path.basename(os.getcwd())
        firefox_path = '/gapp/ia32linux/bin/firefox %s'
        compute_workflow_url = 'https://imp-lims.gsc.wustl.edu/entity/compute-workflow-execution?setup_wo_id=' + woid
        open_url = input("open compute workflow url in firefox? (y/n)")
        if open_url == 'y':
            webbrowser.get(firefox_path).open(compute_workflow_url)
        print('\ncompute workflow url: \n{}\n'.format(compute_workflow_url))
        print('Save compute workflow file as: \n{}.computeworkflow.{}.tsv\n'.format(woid, mm_dd_yy))
        print('create compute workflow with cat:\ncat > {}.computeworkflow.{}.tsv\n\n'.format(woid, mm_dd_yy))
        quit()

    if args.m:
        while True:
            woid = input('----------\nWork order id (enter to exit):\n').strip()
            # if not woid:
            if (len(woid) == 0):
                print('Exiting ccdg launcher.')
                break
            try:
                val = int(woid)
            except ValueError:
                print("\nwoid must be a number.")
                continue

            user_decision = str(input('\n1)Print sample link and create file?\n2)Input file name?\n3)(enter to exit)\n'))
            if user_decision == '1':
                computeworkflow_file = user_make_computeworkflow(woid)
                print('{} created'.format(computeworkflow_file))
            elif user_decision == '2':
                computeworkflow_file = input('Enter computeworkflow file:\n')
                if not os.path.exists(computeworkflow_file):
                    print('{} file not found\n')
                    quit()
                if os.path.exists(computeworkflow_file):
                    print('File found, using {} for QC.\n'.format(computeworkflow_file))
            elif len(user_decision) == 0:
                print('Exiting ccdg launcher.')
                break
            else:
                print('Please enter 1 or 2\n')
                continue

    if (args.c and not args.f):
        print('Input computeworkflow file with -f.')
        quit()

    if (args.f and not args.c):
        print('Input collaborator with -c')
        quit()

    if (args.f and args.c):
        print('cool')


def user_make_computeworkflow(woid):
    outfile = woid + '.computeworkflow.'+mm_dd_yy+'.tsv'
    print('\nComputeworkflow link:\nhttps://imp-lims.gsc.wustl.edu/entity/compute-workflow-execution?setup_wo_id={}\nEnter samples:'.format(woid))
    compute_file_samples = []
    while True:
        sample_line = input()
        if sample_line:
            compute_file_samples.append(sample_line)
        else:
            break
    with open(outfile, 'w') as computecsv:
        computewrite = csv.writer(computecsv, delimiter='\n')
        computewrite.writerows([compute_file_samples])
    return outfile

# make qc directory for samples qc'd
def make_dir(directory_in):
    i = 1
    control = 0
    while (directory_in):
        if not os.path.exists(directory_in):
            os.makedirs(directory_in)
            new_directory = directory_in
            control = 1
            break
        if control == 0:
            i += 1
            directory_in = 'qc.' + str(sample_number) + '.' + date_time + '.' + str(i)
            if not os.path.exists(directory_in):
                os.makedirs(directory_in)
                control = 1
                new_directory = directory_in
                break
    return new_directory


# create compute workflow file with only 'Aligned Bam To BQSR Cram And VCF' samples
def create_computeworkflow():

    with open(args.c) as computecsv, open(qc_computeworkflow_outfile, 'w') as outcsv, open(qc_dir_file, 'w') as dircsv:
        reader = csv.DictReader(computecsv, delimiter="\t")
        compute_header_fields = reader.fieldnames
        writer = csv.DictWriter(outcsv, compute_header_fields, delimiter="\t")
        writer.writeheader()
        dirwriter = csv.writer(dircsv)
        dirwriter.writerow(['Working Directory'])

        for line in reader:
            if not line['Sample Full Name']:
                continue
            if (line['Status'] == 'completed') and (line['Protocol'] == 'Aligned Bam To BQSR Cram And VCF') \
                    and (line['Work Order'] == woid):
                aligned_samples[line['Sample Full Name']] = line

if __name__ == '__main__':
    main()
