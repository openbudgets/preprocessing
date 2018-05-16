# /user/bin/python3
from os import listdir, path, mkdir
import csv
import yaml


def load_conf(path_to_config="../config.yml"):
    """
    load configuration file
    :param path_to_config:
    :return:
    """
    try:
        with open(path_to_config, 'r') as config:
            return yaml.load(config)
    except IOError:
        print('Failed to load %s' % path_to_config)


def clean_column_value(value):
    """
    clean values in the csv file in case the generation of invalid .nt file for pipelines
    :param value: value to be clean
    :return: cleaned value
    """
    return value.replace('"', ' ').strip()


def extract_columns(file, columns_name, id_column_name):
    """
    extract values from csv file with corresponding column name
    :param file:
    :param columns_name:
    :param id_column_name:
    :return: dictionary, consists of columns name and corresponding value
    """
    columns = {}
    for column_name in columns_name:
        columns[column_name] = []
    with open(file) as csv_file:
        reader = csv.DictReader(csv_file)
        for index, row in enumerate(reader):
            columns[id_column_name].append(index)
            for column_name in columns.keys():
                if column_name != id_column_name:
                    try:
                        columns[column_name].append(clean_column_value(row[column_name]))
                    except ValueError:
                        raise 'Key {} does not exist in %s'.format(column_name, file)
    return columns


def write_file(output_folder, file, columns):
    """
    write processed file to output
    :param output_folder: output folder for transformed file
    :param file: file name to write to
    :param columns: generated columns with corresponding values
    :return:
    """
    with open(output_folder + file, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(columns.keys())
        rows = zip(*columns.values())
        for row in rows:
            writer.writerow(row)


def transform(output_folder, columns_name, id_column_name, file):
    """
    transform input csv file with valid format as RDF
    :param output_folder: output folder for transformed file
    :param columns_name: name of columns to be transformed
    :param id_column_name: identifier column name
    :param file: file to be transformed
    :return:
    """
    columns = extract_columns(file=file, columns_name=columns_name, id_column_name=id_column_name)
    write_file(output_folder, path.basename(file), columns)


if __name__ == '__main__':
    cfg = load_conf()
    input_folder, output_folder, columns_name, id_column_name, files =\
        cfg['InputFolder'], cfg['OutputFolder'], cfg['Columns'], cfg['Column_ID_Name'], None
    try:
        files = [input_folder + file for file in listdir(input_folder) if file.endswith('.csv')]
        mkdir(output_folder)
    except Exception as e:
        print(e)
    for file in files:
        transform(output_folder=output_folder, columns_name=columns_name, id_column_name=id_column_name, file=file)
