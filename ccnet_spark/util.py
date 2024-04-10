import os
def convert_to_absolute_path(file_path):
    if not os.path.isabs(file_path):
        file_path = os.path.abspath(file_path)
    return file_path
