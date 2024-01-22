 
import sys, getopt
import os
import random
import subprocess
from pathlib import Path
import zipfile
import uuid
import magic
import mimetypes
import json
from decode_barracuda import reencode_email
import threading
import time

source_folder = ''
working_folder = ''
working_folder_one_name = ''
working_folder_two_name = ''

def decompress_gzip(file_path, file_extension):
    with Path(os.devnull).open('w') as devnull_fp:
        result = subprocess.call(["gunzip", '-k', file_path, '-S', file_extension], stdout=devnull_fp, stderr=devnull_fp)
    return result

def dump_zip_files():
    # zip_file_set = [x.absolute() for x in Path(source_folder).iterdir() if x.is_file() and x.suffix == '.zip' and int(x.stem) >= starting_zip and int(x.stem) <= ending_zip]
    zip_file_set = [x.absolute() for x in Path(source_folder).iterdir() if x.is_file() and x.suffix == '.zip']
    zip_file_set.sort()
    # Unzip all of the zip files in the list
    for zip_file in zip_file_set:
        print("extracting %s to %s" % (zip_file, working_folder_one_name))
        with zipfile.ZipFile(zip_file, 'r') as zip_object:
            zip_object.extractall(working_folder_one_name)

def find_files_to_extract(path_name):
    print('finding files to extract')
    return [file.absolute() for file in Path(path_name).rglob('*') if file.is_file() and not file.suffix.lower() == '.zip']

def create_new_file_info(filename, parent_destination, folder=None, subfolder=None):
    random_uuid = str(uuid.uuid4())
    target_file = Path(filename)

    f = magic.Magic(mime=True)
    mime = f.from_file(filename)
    extension = '.eml'
    if mime != 'message/rfc822' or mime != 'text/html':
        extension = mimetypes.guess_extension(mime)
    reencode_result = None
    if extension is None:
        extension = ''
    if extension == '.bin' or extension == '.txt' or \
        extension == '' or extension == '.html' or extension == '.csv':
        reencode_result = reencode_email(filename)
        extension = '.eml'
    # print(mime, extension, filename)
    destination_folder = Path(parent_destination)
    if folder is not None and subfolder is None:
        destination_folder = Path(parent_destination) / Path(folder)
    elif folder is not None and subfolder is not None:
        destination_folder = Path(parent_destination) / Path(folder) / Path(subfolder)
    destination_file = destination_folder / Path(random_uuid + extension)
    destination_folder.mkdir(parents=True, exist_ok=True)
    return target_file, destination_file, extension, reencode_result


def attempt_gzip_decompress(files_to_extract):
    for file_to_extract in files_to_extract:
        tmp_folder = Path(file_to_extract.parent / Path('extracting'))
        tmp_folder.mkdir(parents=True, exist_ok=True)
        #print(file_to_extract)
        Path(file_to_extract).rename(tmp_folder / Path(file_to_extract).name)
        ret = decompress_gzip(Path(tmp_folder / Path(file_to_extract).name).absolute(), Path(file_to_extract).suffix)
        # print(ret, file_to_extract)
        if ret != 0:
            Path(tmp_folder / Path(file_to_extract).name).rename(file_to_extract)
            yield ret, file_to_extract, Path(file_to_extract).parent.absolute()
        else:
            yield ret, Path(tmp_folder / Path(file_to_extract).name).absolute(), Path(file_to_extract).parent.absolute()


def extract(files_to_extract):
    thread_uuid = str(uuid.uuid4())
    moved_count = moved_renamed_count =0
    extensions_found = {}
    folder_mapping = {
        'original-folder-path': {
            'guid': '',
            'destination': 'parent-path'
        }
    }

    curr_folder_subdir_count = 0
    curr_folder_nb = 0
    reencoded_count = 0
    unknown_count = 0

    renamed_files = []
    reencoded_files = []
    unknown_files = []
    files_to_extract_count = len(files_to_extract)
    start = time.time()
    for ret, filename, original_parent in attempt_gzip_decompress(files_to_extract):
        start = time.time()
        #print(len(files_to_extract))
        #print(original_parent)
        if str(original_parent) in folder_mapping:
            parent_guid = folder_mapping[original_parent]['guid']
            parent_destination = folder_mapping[original_parent]['destination']
        else:
            parent_guid = str(uuid.uuid4())
            if curr_folder_subdir_count >= 99:
                curr_folder_nb += 1
                curr_folder_subdir_count = 0

                total_count = moved_count + moved_renamed_count + reencoded_count
                end = time.time()
                print('thread [%s] renamed %s extracted %s reencoded %s unknown %s | %s of at least %s | %s' % (thread_uuid, moved_renamed_count, moved_count, reencoded_count, unknown_count, total_count, files_to_extract_count, str(extensions_found)))
                print('[%s] execution time: {}'.format(end-start) % thread_uuid)
                start = time.time()
            else:
                curr_folder_subdir_count += 1
            parent_destination = Path(Path(working_folder_two_name) / Path(thread_uuid + str(curr_folder_nb))).absolute()
            parent_destination.mkdir(parents=True, exist_ok=True)
            folder_mapping[original_parent]= {}
            folder_mapping[original_parent]['guid'] = parent_guid
            folder_mapping[original_parent]['destination'] = parent_destination
        if ret == 0:
            #print('extracted gzip %s' % filename)
            parent = Path(filename).parent
            Path(filename).unlink(missing_ok=True)
            files_to_move = [x.absolute() for x in parent.iterdir() if x.is_file() and x.absolute() != filename]
            folder_guid = str(uuid.uuid4())
            for extracted_file in files_to_move:
                target_file, destination_file, extension, reencoded_res = create_new_file_info(extracted_file, parent_destination, parent_guid, folder_guid if len(files_to_move) > 1 else None)
                if reencoded_res == 'reencoded':
                    reencoded_count += 1
                    reencoded_files.append(Path(destination_file).absolute())
                elif reencoded_res == 'renamed':
                    moved_renamed_count += 1
                    renamed_files.append(Path(destination_file).absolute())
                else:
                    moved_count += 1
                if extension == '':
                    unknown_files.append(Path(destination_file).absolute())
                    unknown_count += 1
                Path(target_file).rename(destination_file)
                extensions_found[extension] = extensions_found.get(extension, 0) + 1
        elif ret == 1:
            # print('not gzip %s' % filename)
            target_file, destination_file, extension, reencoded_res = create_new_file_info(filename, parent_destination, parent_guid)
            if reencoded_res == 'reencoded':
                reencoded_count += 1
                reencoded_files.append(Path(destination_file).absolute())
            elif reencoded_res == 'renamed':
                moved_renamed_count += 1
                renamed_files.append(Path(destination_file).absolute())
            else:
                moved_count += 1
            if extension == '':
                unknown_files.append(Path(destination_file).absolute())
                unknown_count += 1
            Path(target_file).rename(destination_file)
            extensions_found[extension] = extensions_found.get(extension, 0) + 1
    print('thread [%s] renamed %s extracted %s reencoded %s unknown %s' % (thread_uuid, moved_renamed_count, moved_count, reencoded_count, unknown_count))
    print('thread [%s] %s' % (thread_uuid, str(json.dumps(extensions_found, indent=4, sort_keys=True))))
    print('thread [%s] total: %s' % (thread_uuid, moved_count + moved_renamed_count + reencoded_count))

    Path(thread_uuid).mkdir(parents=True, exist_ok=True)
    if len(renamed_files) > 0:
        with open(Path(thread_uuid) / Path('renamed.txt'), 'w') as f:
            f.write('\n'.join(str(x) for x in renamed_files))
    if len(reencoded_files) > 0:
        with open(Path(thread_uuid) / Path('reencoded.txt'), 'w') as f:
            f.write('\n'.join(str(x) for x in reencoded_files))
    if len(unknown_files) > 0:
        with open(Path(thread_uuid) / Path('unknown.txt'), 'w') as f:
            f.write('\n'.join(str(x) for x in unknown_files))

def extract_wrapper():
    dump_zip_files()
    files_to_extract = find_files_to_extract(working_folder_one_name)

    workers_nb = 8
    size = len(files_to_extract)

    chunk_size, rem = divmod(size, workers_nb)
    chunks = []
    print(len(files_to_extract))
    for i in range(workers_nb):
        if i == workers_nb - 1:
            chunks.append(files_to_extract[i * chunk_size:len(files_to_extract)])
        else:
            chunks.append(files_to_extract[i * chunk_size:(i + 1) * chunk_size])

    workers_list = []

    start = time.time()

    print('starting %s workers' % workers_nb)
    for r in range(0, workers_nb):
        workers_list.append(threading.Thread(target=extract, args=(chunks[r],)))
    for r in range(0, workers_nb):
        workers_list[r].start()
    for r in range(0, workers_nb):
        workers_list[r].join()
    end = time.time()
    print('Execution Time: {}'.format(end-start))

def main(argv):
    global source_folder
    global working_folder
    global working_folder_one_name
    global working_folder_two_name

    directory = None
    output = None

    try:
        opts, args = getopt.getopt(argv,"hd:o:")
    except getopt.GetoptError:
        print('usage: -h (help) -d <directory> -o <output directory>')
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print('usage: -h (help) -d <directory> -o <output directory>')
            sys.exit()
        elif opt == '-d':
            directory = arg
        elif opt == '-o':
            output = arg

    if directory is None or output is None:
        print('usage: -h (help) -d <directory> -o <output directory>')
        sys.exit(2)
    if not Path(directory).exists() or not Path(directory).is_dir():
        print('directory does not exist')
        sys.exit(2)

    Path(output).mkdir(parents=True, exist_ok=True)
    working_folder = Path(output).absolute()
    source_folder = Path(directory).absolute()


    random_seed_for_eml_folder = random.randint(1, 1000)
    working_folder_one_name = os.path.join(working_folder, "working_folder")
    working_folder_two_name = os.path.join(working_folder, 'Emails_%s' % random_seed_for_eml_folder)
    Path(working_folder_two_name).mkdir(parents=True, exist_ok=True)
    Path(working_folder_one_name).mkdir(parents=True, exist_ok=True)

    extract_wrapper()

if __name__ == "__main__":
    main(sys.argv[1:])