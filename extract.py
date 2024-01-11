 
import os
import random
import subprocess
from pathlib import Path
import zipfile
import uuid
import magic
import mimetypes
import json

# Specify source and working folders, as well as report file variables
source_folder = "/home/cameron/Documents/New Folder/mail/data/active"
working_folder = "/home/cameron/Documents/New Folder/extracted"
random_seed_for_eml_folder = random.randint(1, 1000)
working_folder_one_name = os.path.join(working_folder, "WorkingFolder1")
working_folder_two_name = os.path.join(working_folder, f'Emails_{random_seed_for_eml_folder}')
Path(working_folder_two_name).mkdir(parents=True, exist_ok=True)
Path(working_folder_one_name).mkdir(parents=True, exist_ok=True)

'''
# Ask for starting file number and ending file number
starting_zip = int(input("Enter Number of First Zip File to Process: "))
ending_zip = int(input("Enter Number of Last Zip File to Process: "))
'''
def decompress_gzip(file_path, file_extension):
    result = subprocess.call(["gunzip", '-k', file_path, '-S', file_extension])
    return result

#def dump_zip_files(start, end):
def dump_zip_files():
    # zip_file_set = [x.absolute() for x in Path(source_folder).iterdir() if x.is_file() and x.suffix == '.zip' and int(x.stem) >= starting_zip and int(x.stem) <= ending_zip]
    zip_file_set = [x.absolute() for x in Path(source_folder).iterdir() if x.is_file() and x.suffix == '.zip']
    # Unzip all of the zip files in the list
    for zip_file in zip_file_set:
        print("extracting %s to %s" % (zip_file, working_folder_one_name))
        with zipfile.ZipFile(zip_file, 'r') as zip_object:
            zip_object.extractall(working_folder_one_name)

def find_files_to_extract(path_name):
    return [file.absolute() for file in Path(path_name).rglob('*') if file.is_file() and not file.suffix.lower() == '.zip']

def create_new_file_info(filename):
    random_uuid = str(uuid.uuid4())
    target_file = Path(filename)

    f = magic.Magic(mime=True)
    mime = f.from_file(filename)
    extension = '.eml'
    if mime != 'message/rfc822':
        extension = mimetypes.guess_extension(mime)
    if extension is None:
        extension = ''
    print(mime, extension, filename)
    destination_file = Path(working_folder_two_name) / Path(random_uuid + extension)
    return target_file, destination_file, extension


def attempt_gzip_decompress(files_to_extract):
    for file_to_extract in files_to_extract:
        tmp_folder = Path(file_to_extract.parent / Path('extracting'))
        tmp_folder.mkdir(parents=True, exist_ok=True)
        Path(file_to_extract).rename(tmp_folder / Path(file_to_extract).name)
        ret = decompress_gzip(Path(tmp_folder / Path(file_to_extract).name).absolute(), Path(file_to_extract).suffix)
        # print(ret, file_to_extract)
        if ret != 0:
            Path(tmp_folder / Path(file_to_extract).name).rename(file_to_extract)
            yield ret, file_to_extract
        else:
            yield ret, Path(tmp_folder / Path(file_to_extract).name).absolute()



# dump_zip_files(starting_zip, ending_zip)

dump_zip_files()
files_to_extract = find_files_to_extract(working_folder_one_name)
moved_count = moved_renamed_count =0
extensions_found = {}

for ret, filename in attempt_gzip_decompress(files_to_extract):
    if ret == 0:
        #print('extracted gzip %s' % filename)
        parent = Path(filename).parent
        Path(filename).unlink(missing_ok=True)
        files_to_move = [x.absolute() for x in parent.iterdir() if x.is_file() and x.absolute() != filename]
        for extracted_file in files_to_move:
            target_file, destination_file, extension = create_new_file_info(extracted_file)
            extensions_found[extension] = extensions_found.get(extension, 0) + 1
            # print('moving %s to %s' % (target_file, destination_file))
            Path(target_file).rename(destination_file)
            moved_count += 1
    elif ret == 1:
        # print('not gzip %s' % filename)
        target_file, destination_file, extension = create_new_file_info(filename)
        extensions_found[extension] = extensions_found.get(extension, 0) + 1
        Path(target_file).rename(destination_file)
        moved_renamed_count += 1

print('renamed %s extracted %s' % (moved_renamed_count, moved_count))
print(json.dumps(extensions_found, indent=4, sort_keys=True))
print('total: %s' % (moved_count + moved_renamed_count))
