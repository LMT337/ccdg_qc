import os
import shutil
import subprocess
import csv
import datetime
import argparse
import webbrowser

#command line input parser
parser = argparse.ArgumentParser()

parser.add_argument("compute", help='current compute workflow file from lims', type=str)
parser.add_argument("-c", help='collaborator name', type=str)
parser.add_argument("-l", help='url for compute workflow in lims', action='store_true')

args = parser.parse_args()

date = datetime.datetime.now().strftime("%m-%d-%y")
date_time = datetime.datetime.now().strftime("%m%d%y")

# add woid to tml.py and check to see if sample doesn't exist.

path = os.path.basename(os.getcwd())
cwd = os.getcwd()
qc_status = path + '.master.samples.qcstatus.tsv'
qc_temp_status = path +'.master.samples.qcstatus.temp.tsv'
qc_computeworkflow_outfile = path + '.compute.workflow.' + date_time + '.tsv'
qc_dir_file = path + '.working.directory.tsv'

firefox_path = '/gapp/ia32linux/bin/firefox %s'

if not args.c:
    print('\n-c <collection required>\n')
    exit()

if args.l:
    compute_workflow_url =  'https://imp-lims.gsc.wustl.edu/entity/compute-workflow-execution?setup_wo_id=' + path
    open_url = input("open compute workflow url in firefox? (y/n)")
    if open_url == 'y':
        webbrowser.get(firefox_path).open(compute_workflow_url)
    print('\ncompute workflow url: \n{}\n'.format(compute_workflow_url))
    print('Save compute workflow file as: \n{}.computeworkflow.{}.tsv\n'.format(path, date_time))
    print('create compute workflow with cat:\ncat > {}.computeworkflow.{}.tsv\n\n'.format(path, date_time))
    exit()


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
            directory_in = 'qc.'+str(sample_number)+'.'+date_time+'.'+str(i)
            if not os.path.exists(directory_in):
                os.makedirs(directory_in)
                control = 1
                new_directory = directory_in
                break
    return new_directory

aligned_samples = dict()

compute_header_fields = list()

with open(args.compute) as computecsv, open(qc_computeworkflow_outfile, 'w') as outcsv, open(qc_dir_file, 'w') as dircsv:
    reader = csv.DictReader(computecsv, delimiter="\t")
    compute_header_fields = reader.fieldnames
    writer = csv.DictWriter(outcsv, compute_header_fields, delimiter="\t")
    writer.writeheader()
    dirwriter = csv.writer(dircsv)
    dirwriter.writerow(['Working Directory'])

    for line in reader:
        if not line['DNA']:
            continue
        if (line['Status'] == 'completed') and (line['Protocol'] == 'Aligned Bam To BQSR Cram And VCF'):
            aligned_samples[line['DNA']] = line

def qc_ready(sample):
    """If sample has completed with an aligned to BQSR Cram and VCF status, 
        write path to directory file and sample info to filtered workflow file.
        Returns a dict with Status, date, and COD Collaborator"""
    if sample in aligned_samples:
        with open(qc_computeworkflow_outfile, 'a') as outcsv, open(qc_dir_file, 'a') as dircsv:
            writer = csv.DictWriter(outcsv, compute_header_fields, delimiter="\t")
            dirwriter = csv.writer(dircsv)
            dirwriter.writerow([aligned_samples[sample]['Working Directory'] + '/'])
            writer.writerow(aligned_samples[sample])
        qc_update = {'QC Status': 'QC Complete',
                     'QC Date': date,
                     'COD Collaborator': args.c}
    else:
        qc_update = {'QC Status': 'NONE',
                     'QC Date': 'NONE',
                     'COD Collaborator': 'NONE'}

    return qc_update

with open(qc_status, 'r') as qcstatuscsv, open(qc_temp_status, 'w') as qcstatus_temp_csv:

    qc_status_reader = csv.DictReader(qcstatuscsv, delimiter="\t")
    header_fields = qc_status_reader.fieldnames

    qcs_temp_csv = csv.DictWriter(qcstatus_temp_csv, header_fields, delimiter="\t")
    qcs_temp_csv.writeheader()

    test = {}
    qc_update = {}
    for qc_status_line in qc_status_reader:
        if (qc_status_line['Launch Status'] == 'Launched' and  qc_status_line['QC Status'] == 'NONE'):
            qc_update = qc_ready(qc_status_line['QC Sample'])
            master_qc_update = dict(list(qc_status_line.items()) + list(qc_update.items()))
            qcs_temp_csv.writerow(master_qc_update)
        else:
            qcs_temp_csv.writerow(qc_status_line)

num_lines = sum(1 for line in open(qc_dir_file))
sample_number = num_lines - 1

if num_lines == 1:
    print('\nNo samples found to QC.\n')
    os.remove(qc_temp_status)
    os.remove(qc_computeworkflow_outfile)
    os.remove(qc_dir_file)
    exit()

os.rename(qc_temp_status, qc_status)

def metrics_add(sample_name):
    with open(qc_directory + '/' + path + '.' + str(sample_number) + '.' + date_time + '.build38.all.tsv', 'r') as qcallcsv:
        qc_metrics_reader = csv.DictReader(qcallcsv, delimiter="\t")
        metrics_results = {}
        for line in qc_metrics_reader:
            if (sample_name == line['DNA']):
                if line['QC Failed Metrics'] == 'NA':
                    metrics_results['QC Failed Metrics'] = 'QC_PASS'
                    metrics_results['QC Directory'] = cwd + '/' + qc_directory
                else:
                    metrics_results['QC Failed Metrics'] = 'QC_FAIL: ' + line['QC Failed Metrics'] 
                    metrics_results['QC Directory'] = cwd + '/' + qc_directory
    return metrics_results

if num_lines > 1:

    directory = 'qc.'+str(sample_number)+'.'+date_time
    qc_directory = make_dir(directory)

    old_qco = cwd + '/' + qc_computeworkflow_outfile
    new_qco = qc_directory + '/' + qc_computeworkflow_outfile
    shutil.move(old_qco, new_qco)

    old_wd = cwd + '/' + qc_dir_file
    new_wd = qc_directory + '/' + qc_dir_file
    shutil.move(old_wd, new_wd)

    yaml_dir = cwd + '/' + qc_directory + '/yaml'

    if not os.path.exists(yaml_dir):
        os.makedirs(yaml_dir)

    #yaml create
    subprocess.run(["/gscuser/zskidmor/bin/python3",  "/gscuser/awollam/aw/yamparse.topmed.py", new_wd, yaml_dir + '/'])

    #qc.build38.topmed.py
    subprocess.run(["/gscuser/zskidmor/bin/python3", "/gscuser/awollam/aw/qc.build38.topmed.py", "--tm", "--dir", yaml_dir + '/',
                    new_qco, qc_directory+'/'+path+'.'+str(sample_number)+'.'+date_time])

    #qc.build38.topmed.reportmaker.py
    subprocess.run(["/gscuser/zskidmor/bin/python3", "/gscuser/awollam/aw/qc.build38.topmed.reportmaker.py", "--tm",
                    qc_directory+'/'+path+'.'+str(sample_number)+'.'+date_time+'.build38.all.tsv', qc_directory+'/'+path+'.'+str(sample_number)+'.'+date_time+'.report'])

    #topmetrics.py
    subprocess.run(["/gscuser/zskidmor/bin/python3", "/gscuser/awollam/aw/topmetrics.py", "--c", args.c,
                    qc_directory+'/'+path+'.'+str(sample_number)+'.'+date_time+'.build38.qcpass.tsv', qc_directory+'/'+path+'.'+str(sample_number)+'.'+date_time])

    #need to add QC failed metrics column and also qc directories and results files

    ##open qc status file again read only
    # open temp file to write everything to
    # open metrics file,
    with open(qc_status, 'r') as qcstatuscsv, open(qc_temp_status, 'w') as qcstatus_temp_csv:

        qc_status_reader = csv.DictReader(qcstatuscsv, delimiter="\t")
        header_fields = qc_status_reader.fieldnames

        qcs_temp_csv = csv.DictWriter(qcstatus_temp_csv, header_fields, delimiter="\t")
        qcs_temp_csv.writeheader()

        for qc_status_line in qc_status_reader:
                if (qc_status_line['QC Status'] == 'QC Complete') and (qc_status_line['QC Failed Metrics'] == 'NONE'):
                    results = metrics_add(qc_status_line['DNA'])
                    master_qc_update = dict(list(qc_status_line.items()) + list(results.items()))
                    qcs_temp_csv.writerow(master_qc_update)
                else:
                    qcs_temp_csv.writerow(qc_status_line)
    print('\nQC was run on {} samples '.format(sample_number))
    print('QC Directory is: {}/{}\n'.format(cwd, qc_directory))
    os.rename(qc_temp_status, qc_status)

exit()



