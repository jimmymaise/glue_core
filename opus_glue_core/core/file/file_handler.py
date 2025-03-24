import datetime
import filecmp
import glob
import gzip
import json
import os
import os.path
import pathlib
import shutil
import stat


class FileHandler:

    @staticmethod
    def make_json_file_from_dict(dict_data, saved_file_path):
        file_content = json.dumps(dict_data)
        with open(saved_file_path, 'w+') as write_file:
            write_file.write(file_content)

    @staticmethod
    def load_json_file_to_dict(file_path):
        with open(file_path, 'r') as read_file:
            dict_data = json.load(read_file)
        return dict_data

    @staticmethod
    def compress_file(file_path, is_remove_file=False):
        compress_file_path = file_path + '.gz'
        with open(file_path, 'rb') as f_in:
            with gzip.open(compress_file_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        if is_remove_file:
            os.remove(file_path)
        return compress_file_path

    @staticmethod
    def get_most_recent_file_from_folder(folder_path):
        list_of_files = glob.glob(f'{folder_path}/*')  # * means all if need specific format then *.csv
        return max(list_of_files, key=os.path.getctime)

    @staticmethod
    def get_sub_folders_in_current_dir(directory, top_level_only=False):
        if top_level_only:
            return {
                x[0].replace(f'{directory}/', '').split('/')[0] for x in os.walk(directory) if x[0] != directory}
        return {x[0].replace(f'{directory}/', '') for x in os.walk(directory) if x[0] != directory}

    @classmethod
    def compress_and_upload_file_to_s3(cls, file_path, is_remove_file, s3_handler, s3_upload_folder_path, bucket_name):
        gzip_file_path = cls.compress_file(file_path, is_remove_file)
        s3_file_url = s3_handler.upload(gzip_file_path, bucket_name,
                                        s3_upload_folder_path)
        if is_remove_file:
            os.remove(gzip_file_path)
        print('Upload file successfully with url {}'.format(s3_file_url))
        return s3_file_url

    @staticmethod
    def copy_file(src, dst):
        shutil.copyfile(src, dst)

    @classmethod
    def copy_tree(cls, src, dst, symlinks=False, ignore=None, ignore_paths=None):
        print(
            f'=====Copy from {src} to {dst} {"but ignore path {}".format(ignore_paths) if ignore_paths else ""}')

        def convert_ignore_path(paths):
            def ignore_func(directory, contents):
                return (f for f in contents if
                        os.path.abspath(os.path.join(directory, f)).startswith(tuple(paths)))

            return ignore_func

        if not ignore and ignore_paths:
            ignore = convert_ignore_path(ignore_paths)

        if not os.path.exists(dst):
            os.makedirs(dst)
            shutil.copystat(src, dst)
        lst = os.listdir(src)
        if ignore:
            excl = ignore(src, lst)
            lst = [x for x in lst if x not in excl]
        for item in lst:
            s = os.path.join(src, item)
            d = os.path.join(dst, item)
            if symlinks and os.path.islink(s):
                if os.path.lexists(d):
                    os.remove(d)
                os.symlink(os.readlink(s), d)
                st = os.lstat(s)
                mode = stat.S_IMODE(st.st_mode)
                os.lchmod(d, mode)

            elif os.path.isdir(s):
                cls.copy_tree(s, d, symlinks, ignore)
            else:
                shutil.copy2(s, d)

    @classmethod
    def delete_all_contents_in_folder(cls, folder_path, exclude_paths=None):
        if not cls.check_path_exist(folder_path):
            print(f'Nothing to delete. Folder {folder_path} not exist')
            return
        if not exclude_paths:
            exclude_paths = []
        for file_object in os.listdir(folder_path):
            file_object_path = os.path.join(folder_path, file_object)
            if cls.check_path_in_list(file_object_path, exclude_paths):
                continue
            if os.path.isfile(file_object_path):
                os.unlink(file_object_path)
            else:
                shutil.rmtree(file_object_path)

    @classmethod
    def delete_folder_path(cls, folder_path):
        if not cls.check_path_exist(folder_path):
            return
        shutil.rmtree(folder_path)

    @classmethod
    def delete_file_path(cls, file_path):
        if not cls.check_path_exist(file_path):
            return
        os.remove(file_path)

    @staticmethod
    def check_two_path_equal(path1, path2):
        return pathlib.Path(path1) == pathlib.Path(path2)

    @staticmethod
    def check_path_in_list(path, path_list):
        path_list = [pathlib.Path(item) for item in path_list]
        return pathlib.Path(path) in path_list

    @staticmethod
    def create_folder_path(paths, is_file_path=False):
        if not isinstance(paths, list):
            paths = [paths]
        for path in paths:
            if is_file_path:
                path = str(pathlib.Path(path).resolve().parents[0])
            os.makedirs(path, exist_ok=True)

    @staticmethod
    def check_empty_file(file_path):
        return os.stat(file_path).st_size == 0

    @staticmethod
    def check_path_exist(path):
        return os.path.exists(path)

    @staticmethod
    def check_is_file(file_path):
        return os.path.isfile(file_path)

    @staticmethod
    def check_is_folder(folder_path):
        return os.path.isdir(folder_path)

    @staticmethod
    def count_total_line_in_file(file_path):
        with open(file_path) as file:
            return sum(1 for line in file)

    @staticmethod
    def get_file_meta_data(file_path):
        return FileMetaData(file_path)

    @classmethod
    def is_expected_file_ext(cls, file_path, expected_file_ext_list):
        file_ext = cls.get_file_meta_data(file_path).get_file_ext()[1]
        return file_ext in expected_file_ext_list

    @staticmethod
    def get_list_file_from_path(folder_path, hook_file_filter_function=None, hook_file_filter_params=None):
        list_file = []
        if not hook_file_filter_params:
            hook_file_filter_params = {}
        for root, dirs, files in os.walk(folder_path):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                if not hook_file_filter_function or hook_file_filter_function(file_path=file_path,
                                                                              **hook_file_filter_params) is True:
                    list_file.append(file_path)
        return list_file

    @classmethod
    def is_two_dirs_the_same(cls, dir1, dir2):
        """
        Compare two directory trees content.
        Return False if they differ, True is they are the same.
        """
        compared = filecmp.dircmp(dir1, dir2)
        if (compared.left_only or compared.right_only or compared.diff_files
                or compared.funny_files):
            return False
        for subdir in compared.common_dirs:
            if not cls.is_two_dirs_the_same(os.path.join(dir1, subdir), os.path.join(dir2, subdir)):
                return False
        return True


class FileMetaData:
    def __init__(self, file_path):
        self.file_path = file_path
        if not FileHandler.check_is_file(file_path):
            raise Exception(f'{file_path} is not a valid file path')
        if not FileHandler.check_path_exist(file_path):
            raise FileNotFoundError(f'{file_path} does not exist')

    def get_last_modified(self, get_timestamp=False):
        timestamp = os.path.getmtime(self.file_path)
        if get_timestamp:
            return timestamp
        return datetime.datetime.fromtimestamp(timestamp)

    def get_last_changed(self, get_timestamp=False):
        timestamp = os.path.getctime(self.file_path)
        if get_timestamp:
            return timestamp
        return datetime.datetime.fromtimestamp(timestamp)

    def get_last_accessed(self, get_timestamp=False):
        timestamp = os.path.getatime(self.file_path)
        if get_timestamp:
            return timestamp
        return datetime.datetime.fromtimestamp(timestamp)

    def get_file_size(self):
        return os.path.getsize(self.file_path)

    def get_file_ext(self):
        return os.path.splitext(self.file_path)

    def get_dict_file_meta(self):
        return {
            'last_modified': self.get_last_modified(),
            'last_changed': self.get_last_changed(),
            'last_accessed': self.get_last_accessed(),
            'file_size': self.get_file_size(),
            'file_ext': self.get_file_ext()
        }
