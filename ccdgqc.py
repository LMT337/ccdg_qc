import os
import shutil
import subprocess
import csv
import datetime
import argparse
import webbrowser

# current mmddyy
mm_dd_yy = datetime.datetime.now().strftime("%m%d%y")
#date mm-dd-yy
date = datetime.datetime.now().strftime("%m-%d-%y")

compute_header_fields = list()


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

            collection = assign_collections(woid)
            print('Using \'{}\' for collection'.format(collection))

            user_decision = str(input('\n1)Print sample link and create file\n2)Input file name\n3)(enter to exit)\n'))
            if user_decision == '1':
                computeworkflow_file = user_make_computeworkflow(woid)
                print('{} created'.format(computeworkflow_file))
            elif user_decision == '2':
                computeworkflow_file = input('Enter computeworkflow file:\n')
                if len(computeworkflow_file) == 0:
                    print()
                if not os.path.exists(computeworkflow_file):
                    print('{} file not found\n')
                    quit()
                elif os.path.exists(computeworkflow_file):
                    print('File found, using {} for QC.\n'.format(computeworkflow_file))

            elif len(user_decision) == 0:
                print('Exiting ccdg launcher.')
                break
            else:
                print('Please enter 1 or 2\n')
                continue

            aligned_samples = filter_computeworkflow(computeworkflow_file, woid)
            qc_status_update(woid, aligned_samples, collection)



    if (args.c and not args.f):
        print('-f <computeworkflow file required>.')
        quit()

    if (args.f and not args.c):
        print('-c <collection required>')
        quit()

    if (args.f and args.c):
        print('cool')


def assign_collections(woid):

    # get cod for woid
    admin_collections = subprocess.check_output(["wo_info", "--report", "billing", "--woid", woid]).decode(
        'utf-8').splitlines()

    collection = ''
    for ap in admin_collections:
        print(ap)
        if 'Administration Project' in ap:
            collection = ap.split(':')[1].strip()

    return collection

#command line create computeworkflow file with all statuses
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

# create compute workflow outfile and write header
#create dir file and write header
#populate alligned_samples with samples ready for QC
def filter_computeworkflow(computeworkflow_infile, woid):
    aligned_samples = dict()
    compute_header_fields = list()
    computeworkflow_outfile = woid +'.cw.aligned.'+ mm_dd_yy+'.tsv'
    dir_file = woid+'.working.directory.tsv'
    with open(computeworkflow_infile) as computecsv, open(computeworkflow_outfile, 'w') as outcsv, open(dir_file, 'w') as dircsv:
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
    return aligned_samples

#assign qc status to status csv, write computeworkflow for qc and directory file
def qc_ready(sample, woid, aligned_samples, collection):
    computeworkflow_outfile = woid + '.cw.aligned.' + mm_dd_yy + '.tsv'
    dir_file = woid + '.working.directory.tsv'

    with open(computeworkflow_outfile) as headercsv:
        header = csv.DictReader(headercsv, delimiter='\t')
        compute_header_fields = header.fieldnames

    if sample in aligned_samples:
        with open(computeworkflow_outfile, 'a') as outcsv, open(dir_file, 'a') as dircsv:
            writer = csv.DictWriter(outcsv, compute_header_fields, delimiter="\t")
            dirwriter = csv.writer(dircsv)
            dirwriter.writerow([aligned_samples[sample]['Working Directory'] + '/'])
            writer.writerow(aligned_samples[sample])
        qc_update = {'QC Status': 'QC Complete',
                     'QC Date': date,
                     'COD Collaborator': collection}
    else:
        qc_update = {'QC Status': 'NONE',
                     'QC Date': 'NONE',
                     'COD Collaborator': 'NONE'}

    return qc_update

def qc_status_update(woid, aligned_samples, collection):
    qc_status = woid + '.qcstatus.tsv'
    temp_status = woid + '.qcstatus.tmp.tsv'
    with open(qc_status, 'r') as qcstatuscsv, open(temp_status, 'w') as qcstatus_temp_csv:

        qc_status_reader = csv.DictReader(qcstatuscsv, delimiter="\t")
        header_fields = qc_status_reader.fieldnames

        qcs_temp_csv = csv.DictWriter(qcstatus_temp_csv, header_fields, delimiter="\t")
        qcs_temp_csv.writeheader()

        qc_update = {}
        for qc_status_line in qc_status_reader:
            if (qc_status_line['Launch Status'] == 'Launched' and  qc_status_line['QC Status'] == 'NONE'):
                qc_update = qc_ready(qc_status_line['QC Sample'], woid, aligned_samples, collection)
                master_qc_update = dict(list(qc_status_line.items()) + list(qc_update.items()))
                qcs_temp_csv.writerow(master_qc_update)
            else:
                qcs_temp_csv.writerow(qc_status_line)
    return

if __name__ == '__main__':
    main()
