import os, subprocess, csv, datetime, argparse, webbrowser, glob, time
from shutil import copyfile

# current mmddyy
mm_dd_yy = datetime.datetime.now().strftime("%m%d%y")
# date mm-dd-yy
date = datetime.datetime.now().strftime("%m-%d-%y")
hour_min = datetime.datetime.now().strftime("%H%M")

qc_working_dir = '/gscmnt/gc2783/qc/CCDGWGS2018/'
os.chdir(qc_working_dir)

cwd = os.getcwd()

woid_dirs = glob.glob('285*')


def main():
    desc_str = """
        Update qcstatus file with qc'd samples for tracking, run qc scripts.
    """

    parser = argparse.ArgumentParser(description=desc_str)

    group = parser.add_mutually_exclusive_group()

    group.add_argument("-f", type=str, help='Input compute workflow file with all sample alignments')
    group.add_argument('-m', help='Manual QC.', action='store_true')
    group.add_argument('-l', help='Link to links compute workflow file', action='store_true')

    args = parser.parse_args()

    if args.l:

        woid = os.path.basename(os.getcwd())
        firefox_path = '/gapp/ia32linux/bin/firefox %s'
        compute_workflow_url = 'https://imp-lims.gsc.wustl.edu/entity/compute-workflow-execution?_show_result_set_' \
                               'definition=1&_result_set_name=&cwe_id=&setup_wo_id=&setup_wo_id=&sample_full_name=' \
                               '&woi_id=&working_directory=&current_job_id=&workflow_engine_id=&status=completed&' \
                               'protocol_id=&protocol_id=901&last_task=&last_task_timestamp='
        open_url = input("\nopen compute workflow url in firefox? (y/n)")
        if open_url == 'y':
            webbrowser.get(firefox_path).open(compute_workflow_url)
        print('\ncompute workflow url: \n{}\n'.format(compute_workflow_url))
        print('Save compute workflow file as: \ncomputeworkflow.all.{}.tsv\n'.format(mm_dd_yy))
        print('create compute workflow with cat:\ncat > computeworkflow.all.{}.tsv\n'.format(mm_dd_yy))
        quit()

    if args.m:

        while True:

            os.chdir(qc_working_dir)
            woid = input('----------\nWork order id (enter to exit):\n').strip()

            # if not woid:
            if len(woid) == 0:
                print('Exiting ccdg launcher.')
                break
            try:
                val = int(woid)
            except ValueError:
                print("\nwoid must be a number.")
                continue

            if woid in woid_dirs:

                collection = assign_collections(woid)
                print('Using \'{}\' for collection.'.format(collection))
                user_decision = str(input('\n1)Print sample link and create file\n2)Input file name\n3)'
                                          '(enter to exit)\n'))

                if user_decision == '1':

                    os.chdir(woid)
                    computeworkflow_file = user_make_computeworkflow(woid)
                    print('{} created'.format(computeworkflow_file))

                elif user_decision == '2':

                    computeworkflow_file = input('Enter computeworkflow file:\n')
                    if len(computeworkflow_file) == 0:
                        print()
                    if not os.path.exists(computeworkflow_file):
                        print('\n{} file not found\n'.format(computeworkflow_file))

                    elif os.path.exists(computeworkflow_file):
                        print('\nFile found, using {} for QC.'.format(computeworkflow_file))
                        header_fix(computeworkflow_file)
                        copyfile(computeworkflow_file, woid+'/'+computeworkflow_file)
                        os.chdir(woid)

                elif len(user_decision) == 0:

                    print('Exiting ccdg launcher.')
                    break

                else:

                    print('Please enter 1 or 2\n')
                    continue

                status_file = woid + '.qcstatus.tsv'
                if os.path.exists(status_file):

                    aligned_samples = filter_computeworkflow(computeworkflow_file, woid)
                    qc_status_update(woid, aligned_samples, collection)
                    qc_output, qc_dir = qc_run(woid)
                    os.remove(computeworkflow_file)

                    for line in qc_output:

                        if 'Attachments' in line:

                            subprocess.run(["/gscuser/awagner/bin/python3", "/gscuser/awollam/aw/ccdg_zero_restore.py",
                                            "-w", woid])

                            os.chdir(qc_dir)
                            print('-------\nRunning tkb.py\n-------\n')
                            subprocess.run(["/gscuser/zskidmor/bin/python3", "/gscuser/awollam/aw/tkb.py"])

                            attachments = 'attachments/'
                            os.makedirs('attachments')

                            qc_dir_name = os.path.basename(qc_dir).split('.')[1:]
                            qc_file_prefix = woid + '.' + qc_dir_name[0] + '.' + qc_dir_name[1]

                            os.rename(qc_file_prefix + '.build38.all.tsv', qc_file_prefix + '.build38.all.tsv.backup')
                            os.rename(qc_file_prefix + '.build38.totalBasesKB.tsv', qc_file_prefix + '.build38.all.tsv')

                            qc_files = [qc_file_prefix + '.build38.all.tsv', qc_file_prefix + '.report']

                            num_fail_lines = sum(1 for line in open(qc_file_prefix + '.build38.fail.tsv'))
                            if num_fail_lines > 1:
                                qc_files.append(qc_file_prefix + '.build38.fail.tsv')

                            num_samplemap_lines = sum(1 for line in open(qc_file_prefix + '.qcpass.samplemap.tsv'))
                            if num_samplemap_lines >= 1:
                                qc_files.append(qc_file_prefix + '.qcpass.samplemap.tsv')

                            print('Files copied to attachments directory:')
                            for file in qc_files:
                                print(file)
                                copyfile(file, attachments + file)

                            os.rename(qc_file_prefix + '.build38.all.tsv', qc_file_prefix + '.build38.totalBasesKB.tsv')
                            os.rename(qc_file_prefix + '.build38.all.tsv.backup', qc_file_prefix + '.build38.all.tsv')

                            os.chdir(attachments)
                            add_collections_allfile(qc_file_prefix + '.build38.all.tsv', collection)

                    print('QC FINISHED\n----------')

                else:

                    print('No {} found, skipping QC for {}.'.format(status_file, woid))
                    print('QC FINISHED\n----------')

    if args.f:

        if not os.path.exists(os.getcwd() + '/' + args.f):
            print('{}/{} file not found'.format(os.getcwd(),args.f))
            quit()

        qc_fieldnames = ['WOID', 'Collection', 'Sample QC', 'QC Directory', 'QC Date']

        qc_summary_outfile = 'qc.summary.' + mm_dd_yy + '.' + hour_min + '.tsv'
        qc_process_outfile = 'qc.process.' + mm_dd_yy + '.' + hour_min + '.tsv'
        computeworkflow_all_file = args.f
        header_fix(computeworkflow_all_file)

        with open(qc_summary_outfile, 'w') as qc_summary_outfilecsv, open(qc_process_outfile,'w') as \
                qc_process_outfilecsv:

            qcwrite = csv.DictWriter(qc_summary_outfilecsv, fieldnames=qc_fieldnames, delimiter='\t')
            qcwrite.writeheader()

            qcprocess_write = csv.DictWriter(qc_process_outfilecsv, fieldnames=qc_fieldnames, delimiter='\t')
            qcprocess_write.writeheader()

            for woid in filter(is_int, woid_dirs):

                qc_results = dict()
                qc_process = dict()

                status_file = woid + '.qcstatus.tsv'
                os.chdir(qc_working_dir)

                print('----------\n{} QC:'.format(woid))

                qc_results['WOID'] = woid
                qc_results['QC Date'] = date
                qc_results['Sample QC'] = 'NA'

                if os.path.exists(woid + '/' + status_file):

                    collection = assign_collections(woid)
                    qc_results['Collection'] = collection
                    print('Using \'{}\' for collection.'.format(collection))

                    print('{} exists, starting QC:'.format(status_file))
                    os.chdir(woid)
                    cwd = os.getcwd()

                    # run qcl
                    aligned_samples = filter_computeworkflow(qc_working_dir+'/'+computeworkflow_all_file, woid)
                    qc_status_update(woid, aligned_samples, collection)
                    qc_output, qc_dir = qc_run(woid)

                    qc_results['QC Directory'] = 'NA'

                    for line in qc_output:

                        if 'Total Samples QC\'ed' in line:
                            qc_results['Sample QC'] = line.split(':')[1].strip()
                            qc_process['Sample QC'] = line.split(':')[1].strip()

                        if 'Attachments' in line:
                            qc_process['WOID'] = woid
                            qc_process['Collection'] = collection
                            qc_process['QC Date'] = date

                            qc_results['QC Directory'] = cwd + '/' + qc_dir
                            qc_process['QC Directory'] = cwd + '/' + qc_dir

                            subprocess.run(["/gscuser/awagner/bin/python3", "/gscuser/awollam/aw/ccdg_zero_restore.py",
                                            "-w", woid])

                            os.chdir(qc_dir)
                            print('-------\nRunning tkb.py\n-------\n')
                            subprocess.run(["/gscuser/zskidmor/bin/python3", "/gscuser/awollam/aw/tkb.py"])

                            attachments = 'attachments/'
                            os.makedirs('attachments')

                            qc_dir_name = os.path.basename(qc_dir).split('.')[1:]
                            qc_file_prefix = woid + '.' + qc_dir_name[0] + '.' + qc_dir_name[1]

                            os.rename(qc_file_prefix + '.build38.all.tsv', qc_file_prefix + '.build38.all.tsv.backup')
                            os.rename(qc_file_prefix + '.build38.totalBasesKB.tsv', qc_file_prefix + '.build38.all.tsv')

                            qc_files = [qc_file_prefix + '.build38.all.tsv', qc_file_prefix + '.report']

                            num_fail_lines = sum(1 for line in open(qc_file_prefix + '.build38.fail.tsv'))
                            if num_fail_lines > 1:
                                qc_files.append(qc_file_prefix + '.build38.fail.tsv')

                            num_samplemap_lines = sum(1 for line in open(qc_file_prefix + '.qcpass.samplemap.tsv'))
                            if num_samplemap_lines > 1:
                                qc_files.append(qc_file_prefix + '.qcpass.samplemap.tsv')

                            print('Files copied to attachments directory:')
                            for file in qc_files:
                                print(file)
                                copyfile(file, attachments + file)

                            os.rename(qc_file_prefix + '.build38.all.tsv', qc_file_prefix + '.build38.totalBasesKB.tsv')
                            os.rename(qc_file_prefix + '.build38.all.tsv.backup', qc_file_prefix + '.build38.all.tsv')

                            os.chdir(attachments)
                            add_collections_allfile(qc_file_prefix + '.build38.all.tsv', collection)

                    qcwrite.writerow(qc_results)
                    if len(qc_process) != 0:
                        qcprocess_write.writerow(qc_process)

                    print('QC FINISHED\n----------')

                else:
                    print('No {} found, skipping QC for {}.'.format(status_file, woid))
                    qc_results['Collection'] = 'Analysis work order'
                    qc_results['QC Directory'] = 'NA'
                    qc_results['Sample QC'] = 'NA'
                    qcwrite.writerow(qc_results)
                    print('QC FINISHED\n----------')
                    time.sleep(0.25)


def is_int(string):
    try:
        int(string)
    except ValueError:
        return False
    else:
        return True


# query data base for collections id
def assign_collections(woid):

    # get cod for woid
    admin_collections = subprocess.check_output(["wo_info", "--report", "billing", "--woid", woid]).decode(
        'utf-8').splitlines()

    collection = ''
    for ap in admin_collections:
        if 'Administration Project' in ap:
            collection = ap.split(':')[1].strip()

    return collection


# replace 'Sample Full Name' in header with 'DNA'
def header_fix(compute_workflow_file):

    temp_file = 'cw.temp.tsv'
    with open(compute_workflow_file, 'r') as cwfcsv, open(temp_file, 'w') as tempcsv:
        cwfreader = csv.reader(cwfcsv, delimiter='\t')
        temp_writer = csv.writer(tempcsv, delimiter='\t')

        write_lines = []
        for line in cwfreader:
            if 'Sample Full Name' in line:
                line = ['DNA' if field == 'Sample Full Name' else field for field in line]
            write_lines.append(line)

        temp_writer.writerows(write_lines)
        os.rename(temp_file, compute_workflow_file)

        return


# command line create computeworkflow file with all statuses
def user_make_computeworkflow(woid):

    outfile = woid+'.computeworkflow.'+mm_dd_yy+'.tsv'
    print('\nComputeworkflow link:\nhttps://imp-lims.gsc.wustl.edu/entity/compute-workflow-execution?setup_wo_id={}'
          '\nEnter samples:'.format(woid))

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

    header_fix(outfile)

    return outfile


# create compute workflow outfile and write header
# create dir file and write header
# populate alligned_samples with samples ready for QC
def filter_computeworkflow(computeworkflow_infile, woid):

    aligned_samples = dict()
    compute_header_fields = list()
    computeworkflow_outfile = woid +'.cw.aligned.'+ mm_dd_yy+'.tsv'
    dir_file = woid+'.working.directory.tsv'

    with open(computeworkflow_infile) as computecsv, open(computeworkflow_outfile, 'w') as outcsv, \
            open(dir_file, 'w') as dircsv:
        reader = csv.DictReader(computecsv, delimiter="\t")
        compute_header_fields = reader.fieldnames
        writer = csv.DictWriter(outcsv, compute_header_fields, delimiter="\t")
        writer.writeheader()
        dirwriter = csv.writer(dircsv)
        dirwriter.writerow(['Working Directory'])

        for line in reader:
            if not line['DNA']:
                continue
            if (line['Status'] == 'completed') and (line['Protocol'] == 'Aligned Bam To BQSR Cram And VCF Without '
                                                                        'Genotype') and (line['Work Order'] == woid):
                aligned_samples[line['DNA']] = line

    return aligned_samples


# assign qc status to status csv, write computeworkflow for qc and directory file
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


# update status file for samples that meet qc criteria
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
            if qc_status_line['Launch Status'] == 'Launched' and  qc_status_line['QC Status'] == 'NONE':
                if qc_status_line['DNA'][0] == '0':
                    qc_status_line['DNA'] = qc_status_line['DNA'][1:]
                qc_update = qc_ready(qc_status_line['DNA'], woid, aligned_samples, collection)
                master_qc_update = dict(list(qc_status_line.items()) + list(qc_update.items()))
                qcs_temp_csv.writerow(master_qc_update)
            else:
                qcs_temp_csv.writerow(qc_status_line)
    return


# make qc directory for samples to have qc run on them
def make_dir(directory_in, sample_number):

    i = 1
    control = 0

    while directory_in:
        if not os.path.exists(directory_in):
            os.makedirs(directory_in)
            new_directory = directory_in
            control = 1
            break
        if control == 0:
            i += 1
            directory_in = 'qc.' + str(sample_number) + '.' + mm_dd_yy + '.' + str(i)
            if not os.path.exists(directory_in):
                os.makedirs(directory_in)
                control = 1
                new_directory = directory_in
                break

    return new_directory


# add qc directory and sample pass/fail status to samples that have been qc'd
def metrics_add(sample_name, qc_directory, woid, sample_number):
    with open(qc_directory + '/' + woid + '.' + str(sample_number) + '.' + mm_dd_yy + '.build38.all.tsv', 'r') as qcallcsv:
        qc_metrics_reader = csv.DictReader(qcallcsv, delimiter="\t")
        metrics_results = {}
        for line in qc_metrics_reader:
            if sample_name == line['DNA']:
                if line['QC Failed Metrics'] == 'NA':
                    metrics_results['QC Failed Metrics'] = 'QC_PASS'
                    metrics_results['QC Directory'] = cwd + '/' + qc_directory
                else:
                    metrics_results['QC Failed Metrics'] = 'QC_FAIL: ' + line['QC Failed Metrics']
                    metrics_results['QC Directory'] = cwd + '/' + qc_directory
    return metrics_results


# run qc on samples ready for qc
def qc_run(woid):

    computeworkflow_outfile = woid + '.cw.aligned.' + mm_dd_yy + '.tsv'
    dir_file = woid + '.working.directory.tsv'
    qc_status = woid + '.qcstatus.tsv'
    temp_status = woid + '.qcstatus.tmp.tsv'
    qc_dir = ''

    num_lines = sum(1 for line in open(woid + '.working.directory.tsv'))
    sample_number = num_lines - 1

    if num_lines == 1:
        print('\nNo samples found to QC.\n')
        os.remove(temp_status)
        os.remove(computeworkflow_outfile)
        os.remove(dir_file)
        qc_report = 'No samples found to QC.'

    if os.path.exists(temp_status):
        os.rename(temp_status, qc_status)

    if num_lines > 1:
        directory = 'qc.' + str(sample_number) + '.' + mm_dd_yy
        qc_dir = make_dir(directory, sample_number)

        # old_qco = woid + '/' + computeworkflow_outfile
        new_qco = qc_dir + '/' +computeworkflow_outfile
        copyfile(computeworkflow_outfile, new_qco)

        # old_wd = cwd + '/' + dir_file
        new_wd = qc_dir + '/' + dir_file
        copyfile(dir_file, new_wd)

        yaml_dir = qc_working_dir + '/' + woid +  '/' + qc_dir + '/yaml'

        if not os.path.exists(yaml_dir):
            os.makedirs(yaml_dir)

        # yaml create
        subprocess.run(["/gscuser/zskidmor/bin/python3", "/gscuser/awollam/aw/yamparse.py", new_wd, yaml_dir + '/'])

        # qc.build38.ccdgnew.py
        subprocess.run(["/gscuser/zskidmor/bin/python3", "/gscuser/awollam/aw/qc.build38.ccdgnew.py", "--ccdg", "--dir",
                        yaml_dir + '/', new_qco, qc_dir + '/' + woid + '.' + str(sample_number) + '.' + mm_dd_yy])

        # qc.build38.topmed.reportmaker.py
        qc_report = subprocess.check_output(["/gscuser/zskidmor/bin/python3", "/gscuser/awollam/aw/qc.build38.reportmaker.py", "--ccdg",
                        qc_dir + '/' + woid + '.' + str(sample_number) + '.' + mm_dd_yy + '.build38.all.tsv',
                        qc_dir + '/' + woid + '.' + str(sample_number) + '.' + mm_dd_yy + '.report']).decode('utf-8').splitlines()
        for line in qc_report:
            print(line)

        # open qc status file again read only
        # open temp file to write everything to
        # open metrics file,
        with open(qc_status, 'r') as qcstatuscsv, open(temp_status, 'w') as qcstatus_temp_csv:

            qc_status_reader = csv.DictReader(qcstatuscsv, delimiter="\t")
            header_fields = qc_status_reader.fieldnames

            qcs_temp_csv = csv.DictWriter(qcstatus_temp_csv, header_fields, delimiter="\t")
            qcs_temp_csv.writeheader()

            for qc_status_line in qc_status_reader:
                if (qc_status_line['QC Status'] == 'QC Complete') and (qc_status_line['QC Failed Metrics'] == 'NONE'):
                    results = metrics_add(qc_status_line['DNA'], qc_dir, woid, sample_number)
                    master_qc_update = dict(list(qc_status_line.items()) + list(results.items()))
                    qcs_temp_csv.writerow(master_qc_update)
                else:
                    qcs_temp_csv.writerow(qc_status_line)
        print('\nQC was run on {} samples '.format(sample_number))
        print('QC Directory is: {}/{}/{}'.format(qc_working_dir,woid, qc_dir))
        os.rename(temp_status, qc_status)

    return qc_report, qc_dir


# add admin project to all file in attachment dir
def add_collections_allfile(all_file, collection):

    all_temp_file = all_file + '.temp'

    with open(all_file, 'r') as all_filecsv, open(all_temp_file, 'w') as all_temp_filecsv:
        all_file_reader = csv.DictReader(all_filecsv, delimiter='\t')
        header = all_file_reader.fieldnames
        header.append('Admin Project')

        all_temp_file_writer = csv.DictWriter(all_temp_filecsv, fieldnames=header, delimiter='\t')
        all_temp_file_writer.writeheader()

        for line in all_file_reader:
            line['Admin Project'] = collection
            all_temp_file_writer.writerow(line)

    os.rename(all_temp_file, all_file)

    return all_file


if __name__ == '__main__':
    main()
