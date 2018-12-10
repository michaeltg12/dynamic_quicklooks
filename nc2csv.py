#!/apps/base/python3/bin/python3

"""
Author: Michael Giansiracusa
Email: giansiracumt@ornl.gov

Purpose:
    This module processes NetCDF files into ascii csf format.

Arguments
    required arguments
        in_dir : str
            fully qualified path to files that will be processed.
        out_dir : str
            fully qualified path where to put output files.
        var_list : str
            fully qualified path to a file that contains all
            variables to extract each on it's own line.
        file_list : str
            fully qualified path to a file that contains all
            files in the in_dir to process.
    optional arguments
        merged_output : bool flag
            output files merged into one if provided
        replaceMissing : bool flag
            replace missing values (-9999, -9995) with ""/None
        DQRfilter : str
            filter data by variable using dqr status.
            should be suspect, incorrect, or suspect,incorrect


Output:
    One cdf file for each file in input directory is put in the out_dir or
    one merged file with all rows and columns converted from input cdf files.
    The first row of each file will be the column names; the first 3 column
    names will always be time_stamp, basetime, and time_offset. The time_stamp
    column is calculated by adding the basetime and time_offset and converting
    to a calendar date of the form YYYY-mm-dd HH:MM:SS
"""

# Input parsing
import argparse

# Multiprocessing
import multiprocessing
from functools import partial

# Operations
import logging.config
import os
import re
import requests
import sys
import time
import warnings
import yaml
from urllib.request import urlopen
from glob import glob

sys.path.insert(0, "/apps/base/python3.5/lib/python3.5/site-packages")

# NetCDF parsing
from netCDF4 import Dataset

# Data manipulation
from datetime import datetime
import pandas as pd
import numpy as np
np.seterr(invalid='ignore')

# modifying path to include site packages and local packages
sys.path.insert(0, "/apps/base/python3.5/lib/python3.5/site-packages")
sys.path.insert(1, "{}/packages".format(os.getcwd()))

# communications
try:
    import comms
except Exception:
    print("Could not import comms/config. No monitoring messages will be sent.")

# load yaml config file for logging
global_config = yaml.load(open("packages/logging_config.yaml"))
logging.config.dictConfig(global_config['logging'])
nc2csv_logger = logging.getLogger("nc2csv_logger")

# testing
import unittest

class Setup():
    def __init__(self, main_args):
        self.main_args = main_args
        self.in_dir = main_args.in_dir
        self.start = time.time()

    def validate_inputs(self):
        # create and validate output directories
        self.out_dir = self.validate_output(self.main_args.out_dir)
        self.variables = self.validate_var_list(self.main_args.var_list)
        self.files = self.validate_file_list(self.main_args.file_list, self.in_dir)
        if self.files == FileNotFoundError:
            raise FileNotFoundError
        self.datastream = self.parse_datastream(self.files)
        self.dqr_filter = self.validate_dqr_filter(self.main_args.DQRfilter)
        if self.dqr_filter:
            self.dqr_ranges = self.parse_dqrs(self.datastream, self.variables, self.out_dir, self.dqr_filter)
        else:
            nc2csv_logger.info("validate_inputs: no dqr filtering.")
            self.dqr_ranges = dict()
        self.processes = self.main_args.maxProcesses

    def __str__(self):
        str_list = list()
        for arg in vars(self.main_args):
            str_list.append("{} = {}".format(arg, getattr(main_args, arg)))
        return_str = "\n".join(str_list)
        return return_str

    @staticmethod
    def validate_output(out_dir):
        if not out_dir:
            raise NotADirectoryError
        # Check if out_dir exists
        if not os.path.isdir((out_dir)):
            # if not then create it
            os.makedirs(out_dir)
            nc2csv_logger.debug("created path {}".format(out_dir))

        # change out_dir to include the csv directory
        out_dir = os.path.join(out_dir, 'ascii-csv')
        nc2csv_logger.debug("changed directory to {}".format(out_dir))
        # check if the new out_dir exists (it should not)

        if not os.path.isdir(out_dir):
            # if not then create it
            os.makedirs(out_dir)
            nc2csv_logger.debug("created path {}".format(out_dir))

        # return new out_dir
        nc2csv_logger.debug("output directory = {}".format(out_dir))
        return out_dir

    @staticmethod
    def validate_var_list(var_list):
        # create empty list for variables
        variables = list()

        if var_list:
            if not os.path.isfile(var_list):
                nc2csv_logger.debug("{} DNE, extracting 'all' variables".format(var_list))
                variables.append("all")
            elif os.stat(var_list).st_size == 0:
                nc2csv_logger.debug("{} is empty, extracting 'all' variables".format(var_list))
                variables.append("all")
            # if the file exists and is not empty then open and populate list
            elif os.stat(var_list).st_size > 0:
                # open list of variables to extract and store them in a list
                with open(var_list) as open_var_list:
                    for line in open_var_list:
                        # for line in file, remove carriage return
                        variables.append(line.replace("\n", ""))
                    nc2csv_logger.debug("variables = {}".format(variables))
        else:
            # if the file does not exist then drop a flag in the list for all variables
            variables.append("all")
            nc2csv_logger.debug("extracting 'all' variables = {}".format(variables))
        return variables

    @staticmethod
    def validate_file_list(file_list, in_dir):
        # create list to hold files from loop
        files = list()
        if os.path.isfile(file_list):
            nc2csv_logger.debug("reading file_list = {}".format(file_list))
            # fixme this open file causes ResourceWarning: unclosed file
            for file_name in open(file_list, "r"):
                infile_path = os.path.join(in_dir, file_name.strip("\n"))
                if os.path.isfile(infile_path):
                    files.append(infile_path)
                else:
                    return FileNotFoundError
            nc2csv_logger.debug("all files in file_list exist")
            files.sort()
            nc2csv_logger.debug("all files sorted")
            return files
        else:
            return FileNotFoundError

    @staticmethod
    def parse_datastream(files):
        # parsing the datastream from the files, assumption that they are all the same datastream
        # todo check that they are all the same, maybe?
        for f in files:
            if os.path.isfile(f):
                datastream = ".".join((f.split('/')[-1].split('.')[:2]))
                nc2csv_logger.info("datastream = {}".format(datastream))
                return datastream
            # todo add notification that file's don't exist
        nc2csv_logger.info("ERROR: could not parse datastream from files.")
        return None

    @staticmethod
    def validate_dqr_filter(DQRfilter):
        pattern = re.compile("suspect|incorrect")
        result = pattern.findall(DQRfilter.lower())
        nc2csv_logger.info("{} - DQRfilter = {}".format(pattern, DQRfilter))
        suspect_filter, incorrect_filter = False, False
        if "suspect" in result:
            suspect_filter = True
        if "incorrect" in result:
            incorrect_filter = True
        if suspect_filter and incorrect_filter:
            nc2csv_logger.info("DQRfilter = incorrect,suspect")
            return "incorrect,suspect"
        elif suspect_filter:
            nc2csv_logger.info("DQRfilter = suspect")
            return "suspect"
        elif incorrect_filter:
            nc2csv_logger.info("DQRfilter = incorrect")
            return "incorrect"
        else:
            nc2csv_logger.info("DQRfilter = None")
            return ""

    # unused method
    @staticmethod
    def validate_delimiter(delimiter):
        if delimiter.lower() == "tab" or delimiter == "\\t":
            delimiter = "\t"
        return delimiter

    # unused method
    @staticmethod
    def validate_eol_char(eol):
        temp_eol = ""
        if re.search("cr", eol):
            temp_eol += "\r"
        if re.search("lf", eol):
            temp_eol += "\n"
        if temp_eol == "":
            eol = "\n"
        else:
            eol = temp_eol
        return eol

    @staticmethod
    def parse_dqrs(datastream, variables, out_dir, dqr_filter):
        # query the web service and parse response into an array of arrays
        def get_timeblocks(url):
            # query web service https://www.archive.arm.gov/dqrws/
            response = requests.get(url) #FIXME change this to use requests module
            # list to hold parsed response
            timeblocks = list()
            # if response was valid
            if response.status_code == 200:
                # for line in the response, each line is a dqr record
                for line in response.readlines():
                    # decode line, parse line and append an array to timeblocks array
                    timeblocks.append(line.decode().replace('\r\n', '').split('|')) # todo modify to work with requests response format, remove the decode and replace maybe?
            # if bad response from web server
            elif response.status_code == 500:
                # try and send a slack message based on config file
                try:
                    comms.slack_send_direct("nc2csv Error: DQRWS response: {}".format(response.__dict__))
                except NameError:
                    nc2csv_logger.error("Name Error: No comms sent.")
                raise ConnectionError

            return timeblocks

        # create result dictionary to hold dqr ranged by key = variable name
        dqr_results = dict()

        url = "".join(("https://adc.arm.gov/dqrws/ARMDQR?datastream=", datastream,
                           "&responsetype=delimited&searchmetric=", dqr_filter,
                           "&timeformat=YYYYMMDD.hhmmss&dqrfields=starttime,endtime,dqrid,metric,subject"))

        if variables[0] == 'all':
            # file_list was None, DNE or was empty.
            nc2csv_logger.warning("NO DQR FILTERING FOR THIS ORDER.")
            nc2csv_logger.warning("Error with db insert or file_list creation.")
            dqr_results['all'] = True
        else:
            dqr_results['all'] = False
            for var in variables:
                var_url = "".join((url, "&varname=", var))
                nc2csv_logger.info("getting dqr timeblocks for {}\n\t{}".format(var, var_url))
                try:
                    timeblocks = get_timeblocks(var_url)
                except ConnectionError:
                    # if dqrws is down then mark for all variables and break out of loop
                    nc2csv_logger.error("Error with dqr web service.")
                    dqr_results['all'] = True
                    break
                dqr_results[var] = timeblocks

        # write the dqr results to a file for reference
        if not dqr_results['all']:
            dqr_output = os.path.join(out_dir, 'dqr_results.txt')
            nc2csv_logger.info("writing dqr_results to {}".format(dqr_output), end="")
            with open(dqr_output, 'w') as of:
                for key in dqr_results.keys():
                    if key != 'all':
                        for element in dqr_results[key]:
                            of.write("{} - {}\n".format(key, element))
            nc2csv_logger.info(" -- finished writing dqr_results.")
        return dqr_results


class ProcessManager:

    def __init__(self, setup):

        self.files = setup.files
        self.variables = setup.variables
        self.out_dir = setup.out_dir
        self.datastream = setup.datastream
        self.dqr_ranges = setup.dqr_ranges
        self.dqr_filter = setup.dqr_filter
        self.merged_output = setup.main_args.merged_output
        self.processes = setup.processes

    def process_files(self):
        # add date_time to variables, this is where we will add the calculated calendar date_time
        self.variables.insert(0, "date_time")
        nc2csv_logger.info("added date_time to variables at index 0")
        # add base_time column to index 1, after trying to remove that variable if it was already selected
        try:
            self.variables.remove("base_time")
        except Exception:
            pass
        finally:
            self.variables.insert(1, "base_time")
            nc2csv_logger.info("added base_time to variables at index 1")
        # add time_offset column to index 2, after trying to remove that variable if it was already selected
        try:
            self.variables.remove("time_offset")
        except Exception:
            pass
        finally:
            self.variables.insert(2, "time_offset")
            nc2csv_logger.info("added time_offset to variables at index 2")

        pool = multiprocessing.Pool(processes=self.processes)
        func = partial(self.process_one_file, self.variables, self.datastream, self.out_dir,
                       self.dqr_filter, self.dqr_ranges)
        pool.map(func, self.files)
        pool.close()
        pool.join()

    @staticmethod
    def process_one_file(variables, datastream, out_dir, dqr_filter, dqr_ranges, file_name):

        # make netCDF4.Dataset from netcdf file, read only
        nc2csv_logger.info("Begin processing {}".format(file_name))
        rootgrp = Dataset(file_name, 'r')

        # if the flag for all variables is set
        if "all" in variables:
            # populate a temp var_list with all variables
            nc2csv_logger.info("populating list will all variables in current netCDF file")
            variables.remove("all")
            for key in rootgrp.variables.keys():
                if key not in variables:
                    variables.append(key)
            nc2csv_logger.info("Variables = {}".format(variables))

        # create Pandas Dataframe to hold the columns of new file
        df = pd.DataFrame()
        nc2csv_logger.info("created empty dataframe")
        for var in variables:
            dimension = None

            # create datetime column
            if var == "date_time":
                temp_datetimes = []
                for val in rootgrp["time_offset"][:]:
                    temp_timestamp = np.asscalar(val + rootgrp["base_time"][0])
                    temp_datetime = datetime.utcfromtimestamp(temp_timestamp).strftime("%Y-%m-%d %H:%M:%S")
                    temp_datetimes.append(temp_datetime)
                df["date_time"] = temp_datetimes
                df.set_index("date_time", inplace=True)
                nc2csv_logger.info("created date_time column")

                # remove all variables not in this file's variable keys
            # this occurs if a var is selected and it changes name (both names are included in var_list)
            elif var not in rootgrp.variables.keys():
                nc2csv_logger.info("Variable {} not found in file {}".format(var, file_name))
            else:
                dim_len, dim = len(rootgrp.variables[var].dimensions), rootgrp.variables[var].dimensions

                # if the length of dimensions is 0 then it is a constant
                if dim_len == 0:
                    # create list of correct length and write the list to
                    # pandas.Dataframe with column name of current var
                    nc2csv_logger.debug("{} is constant".format(var))
                    df[var] = [rootgrp.variables[var][0]] * len(rootgrp.variables["time_offset"])
                # if the len of dimension is 1 then it's a time series
                elif dim_len == 1 and dim[0] == "time":
                    # write the list to pandas.Dataframe with column name of current var
                    nc2csv_logger.debug("{} is 1 dimension".format(var))
                    df[var] = rootgrp.variables[var][:]
                elif dim_len == 2 and dim[0] == "time":
                    # if length of the dimensions is 2 then it's a 2d var with time
                    nc2csv_logger.warning("{} is 2 dimensional".format(var))
                elif dim_len > 2:
                    # if length of dimensions > 2 then... I've not seen this yet - May 4th 2017
                    nc2csv_logger.warning("{} > 2 dimensional".format(var))
        nc2csv_logger.info("finished variable extraction")

        # pass pandas.DataFrame, dict of dqr ranges and netCDF4.Dataset
        if dqr_filter:
            nc2csv_logger.info("filtering dqr time ranges for status = {}".format(dqr_filter))

            def dqr_filtering(df, dqr_ranges, rootgrp):
                try:
                    missing_num = rootgrp.getncattr('missing-data')
                except AttributeError:
                    missing_num = -9999
                df_start = df.first_valid_index()
                df_end = df.last_valid_index()
                for var in dqr_ranges.keys():
                    if var in df.keys():
                        for record in dqr_ranges[var]:
                            dqr_start = str(datetime.strptime(record[0], '%Y%m%d.%H%M%S'))
                            dqr_end = str(datetime.strptime(record[1], '%Y%m%d.%H%M%S'))
                            if dqr_start > df_end or dqr_end < df_start:
                                pass
                            else:
                                nc2csv_logger.info("Filtering {} from {} to {}".format(var, dqr_start, dqr_end))
                                df.loc[dqr_start:dqr_end, var] = missing_num
                return df

            # then filter by dqr ranges by variable
            df = dqr_filtering(df, dqr_ranges, rootgrp)
            nc2csv_logger.info("finished dqr filtering")

        nc2csv_logger.info("Finished processing {}".format(file_name))
        # replace elements of the start time datetime object to get in new format for file name
        start_time = df.first_valid_index()
        nc2csv_logger.info('first_valid_index = {}'.format(start_time))

        to_replace = [('-', ''), (':', ''), (' ', '.')]
        for item in to_replace:
            start_time = start_time.replace(item[0], item[1])
        nc2csv_logger.info("converted first_valid_index = {}".format(start_time))

        # get the first 4 parts of the first file and join them on a period and add new ending
        outbase = "{}.{}.custom.csv".format(datastream, start_time)
        # make fully qualified path to outfile using out_dir and base file name
        output_file = os.path.join(out_dir, outbase)
        nc2csv_logger.info("full output_file = {}".format(output_file))

        # write pandas dataframe to file
        nc2csv_logger.info("writing result to output file ... ")
        df.to_csv(output_file, index=True, encoding="ascii")
        nc2csv_logger.info("finished processing {}\n".format(output_file))


class PostProc():
    def __init__(self, setup, main_args):
        self.main_args = main_args
        self.in_dir = setup.in_dir
        self.start = setup.start
        self.out_dir =setup.out_dir
        self.variables = setup.variables
        self.files = setup.files
        self.datastream = setup.datastream

    @staticmethod
    def file_search(search_exp):
        try:
            tmpfiles = glob(os.path.join(search_exp))
            nc2csv_logger.info("tmpfiles aquired")
        except NameError as ne:
            nc2csv_logger.info("could not get files list, undefined variables. {}".format(ne))
        else:
            if len(tmpfiles) == 0:
                nc2csv_logger.info("empty file list for merging.")
            return tmpfiles

    @staticmethod
    def csv_2merged_df(files):
        dataframes = list()
        nc2csv_logger.info("creating list of dataframes")
        for f in files:
            dataframes.append(pd.read_csv(f))
        return dataframes

    @staticmethod
    def create_output_path(out_dir, files):
        files.sort()
        out_array = files[0].split('/')[-1].split(".")[0:3] # end at 3 if including end date
        out_array.append(files[-1].split(".")[2])  # this is the date in file YYYYMMDD
        # out_aray.append(tmpfiles[-1].split(".")[4]) # this is the time on file hhmmss
        nc2csv_logger.info("building output file from {}".format(out_array))

        # join them on a period and add new ending
        outfile_name = "{}.custom.merged.csv".format(".".join(out_array))

        # make fully qualified path to outfile using out_dir and base file name
        output_path = os.path.join(out_dir, outfile_name)
        nc2csv_logger.info("{} ".format(output_path))

        return output_path

    @staticmethod
    def remove_files(files):
        nc2csv_logger.info("removing files...")
        for f in files:
            try:
                os.remove(f)
            except FileNotFoundError as fnfe:
                warn_msg = 'PostProc.remove_files; could not remove files; {}'.format(fnfe)
                warnings.warn(warn_msg, RuntimeWarning, stacklevel=2)
        nc2csv_logger.info("done removing files")

    def merge_output(self):
        nc2csv_logger.info("merging output...")
        merge_start = time.time()

        file_search_exp = "{}*custom.csv".format(self.datastream)
        search_exp = os.path.join(self.out_dir, file_search_exp)
        nc2csv_logger.info("*** search exp *** {}".format(search_exp))
        tmpfiles = self.file_search(search_exp)
        # nc2csv_logger.debug("*** tmpfiles *** {}".format(tmpfiles))

        dataframes = self.csv_2merged_df(tmpfiles)
        concat_frames = pd.concat(dataframes)

        outfile_path = self.create_output_path(self.out_dir, tmpfiles)
        nc2csv_logger.info("output file --> {} -- {} lines".format(outfile_path, len(concat_frames)))

        concat_frames.sort_values("date_time", inplace=True, ascending=True)
        concat_frames.to_csv(outfile_path, index=False, encoding="ascii")

        if os.path.isfile(outfile_path):
            # if output sucsessful then delete the individual csv files
            self.remove_files(tmpfiles)
        nc2csv_logger.info("Done merging output")

    def dump_header(self):
        nc2csv_logger.info("dumping header")
        for i, f in enumerate(self.files):
            ds = Dataset(f)
            if i == 0:
                head_attrs = ds.ncattrs()
                base_name = ".".join(os.path.basename(f).split(".")[:4])
                output_header_name = "{}.header.txt".format(base_name)
                output_header_path = os.path.join(self.out_dir, output_header_name)
                cmd = "ncdump -h {} > {}".format(f, output_header_path)
                nc2csv_logger.info(cmd)
                os.system(cmd)
                nc2csv_logger.info("first header - cmd = {}".format(cmd))
            else:
                if head_attrs != ds.ncattrs():
                    head_attrs = ds.ncattrs()
                    base_name = ".".join(os.path.basename(f).split(".")[:4])
                    output_header_name = "{}.header.txt".format(base_name)
                    output_header_path = os.path.join(self.out_dir, output_header_name)
                    cmd = "ncdump -h {} > {}".format(f, output_header_path)
                    nc2csv_logger.info(cmd)
                    os.system(cmd)
                    nc2csv_logger.info("additional header - cmd = {}".format(cmd))
                else:
                    pass
        nc2csv_logger.info("done dumping headers")


class UnitTests(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.nc2csv_test_logger = logging.getLogger("nc2csv_test_logger")
        self.nc2csv_test_logger.debug("Setup UnitTest class")
        self.directory = os.path.join(os.getcwd(), "test_data")
        self.input = os.path.join(self.directory, "test_in")
        self.output = os.path.join(self.directory, "test_out")
        self.file_list = os.path.join(self.directory, "test_file_list.txt")
        self.var_list = os.path.join(self.directory, "test_var_list.txt")

    def test_validate_output(self):
        self.nc2csv_test_logger.debug("Testing Setup.test_validate_output")
        self.test_output = f"{self.output}/ascii-csv"
        self.assertEqual(Setup.validate_output(self.output), self.test_output)

    def test_validate_var_list(self):
        self.nc2csv_test_logger.debug("Testing Setup.test_validate_var_list")
        self.assertEqual(Setup.validate_var_list(''), ['all'])
        self.assertEqual(Setup.validate_var_list(self.var_list), ['all'])
        with open(self.var_list, "w") as of:
            lines = ["base_time", "time_offset", "time", "atmos_pressure", "qc_atmos_pressure", "temp_mean"]
            for element in lines:
                of.write("{}\n".format(element))
        self.assertEqual(Setup.validate_var_list(self.var_list), lines)

    def test_validate_file_list(self):
        self.nc2csv_test_logger.debug("Testing Setup.validate_file_list")
        self.assertEqual(Setup.validate_file_list(self.file_list, self.input), FileNotFoundError)
        test_files = ["test_file1.cdf", "test_file2.cdf", "test_file3.cdf"]
        os.makedirs(os.path.join(self.input), exist_ok=True)
        of = open(os.path.join(self.directory, self.file_list), "w")
        for element in test_files:
            of.write("{}\n".format(element))
        of.close()
        self.assertEqual(Setup.validate_file_list(self.file_list, self.input), FileNotFoundError)
        for element in test_files:
            open(os.path.join(self.input, element), "a").close()
        test_files = [os.path.join(self.input, f) for f in test_files]
        self.assertEqual(Setup.validate_file_list(self.file_list, self.input), test_files)

    def test_parse_datastream(self):
        self.nc2csv_test_logger.debug("Testing Setup.parse_datastream")
        self.assertIsNone(Setup.parse_datastream([]))
        self.assertIsNone(Setup.parse_datastream(""))
        test_file_list = [os.path.join(self.input,"sgptestC1.00.19830101.000000.cdf"),
                          os.path.join(self.input,"sgptestC1.00.19830102.000000.cdf"),
                          os.path.join(self.input,"sgptestC1.00.19830103.000000.cdf")]
        self.assertIsNone(Setup.parse_datastream(test_file_list))
        os.makedirs(self.input, exist_ok=True)
        for element in test_file_list:
            open(element, "a").close()
        self.assertEqual(Setup.parse_datastream(test_file_list), "sgptestC1.00")

    def test_validate_dqr_filter(self):
        self.nc2csv_test_logger.debug("Testing Setup.validate_dqr_filter")
        self.assertEqual(Setup.validate_dqr_filter("foo bar"), "")
        self.assertEqual(Setup.validate_dqr_filter("/\SusPect#WIERD-test* "), "suspect")
        self.assertEqual(Setup.validate_dqr_filter("INCORRECT"), "incorrect")
        self.assertEqual(Setup.validate_dqr_filter("suspect, incorrect"), "incorrect,suspect")
        self.assertEqual(Setup.validate_dqr_filter("suspect"), "suspect")
        self.assertEqual(Setup.validate_dqr_filter("incorrect"), "incorrect")

    def test_validate_delimiter(self):
        self.nc2csv_test_logger.debug("Testing Setup.validate_delimiter")
        self.assertEqual(Setup.validate_delimiter("tab"), "\t")
        self.assertEqual(Setup.validate_delimiter("\t"), "\t")
        self.assertEqual(Setup.validate_delimiter("Tab"), "\t")
        self.assertEqual(Setup.validate_delimiter("\\t"), "\t")
        self.assertEqual(Setup.validate_delimiter(","), ",")

    # def test_parse_dqrs(self):
    #     TODO WIP here

    @classmethod
    def tearDownClass(self):
        self.nc2csv_test_logger.debug("Teardown UnitTest class")
        import shutil
        print("removing tree {}".format(self.directory))
        shutil.rmtree(self.directory)


def send_email(setup, message):
    try:
        import smtplib
        from email.mime.text import MIMEText
        executable = os.path.basename(__file__)
        host = os.uname().__getattribute__('nodename')
        sender = '{}@{}'.format(executable, host)
        recipients = 'giansiracumt@ornl.gov'

        msg = "Sent from {}. Variable extraction.\n\n" \
              "\t\t *** {} ***\n\n" \
              "\t *** Total process time = {}\n\n" \
              "\t *** Main Arguments *** \n{}\n\n".format(host, message, setup.total_time, setup.main_args)
        msg = MIMEText(msg)

        msg['Subject'] = "***  ***"
        msg['From'] = sender
        msg['To'] = recipients
        s = smtplib.SMTP('localhost')
        s.sendmail(sender, recipients, msg.as_string())
    except Exception:
        nc2csv_logger.info("Email not sent to {}".format(recipients))


help_description = """
This program converts netcdf files into csv files.
"""

example = """
EXAMPLE: py3 netcdf2ascii.py
            --in_dir "/path_to/input"
            --out_dir "/path_to/output"
            --var_list "/path_to/var_list.txt"
            --file_list "/path_to/file_list.txt"
"""

""" setup_arguments summary

Author: Michael Giansiracusa
Email: giansiracumt@ornl.gov

Purpose:
    Parse arguments from command line.

Args:
    None

Returns:
    args object with attributes
            in_dir : str
            out_dir : str
            var_list : str
            file_list : str
"""

def parse_arguments():
    parser = argparse.ArgumentParser(description=help_description, epilog=example,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    requiredArguments = parser.add_argument_group("required arguments")

    requiredArguments.add_argument("-i", "--in_dir", type=str, dest="in_dir", help="The directory where input files are.")
    requiredArguments.add_argument("-o", "--out_dir", type=str, dest="out_dir",
                                   help="The directory to put output file in.")
    requiredArguments.add_argument("-v", "--var_list", type=str, dest="var_list", default=None,
                                   help="The name of a file that contains the names of variables to extract.")
    requiredArguments.add_argument("-f", "--file_list", type=str, dest="file_list",
                                   help="The name of a file that contains the names of files to convert.")

    parser.add_argument("--merged_output", action="store_true", dest="merged_output",
                        help="merge files into one, possibly with max size")
    parser.add_argument("--DQRfilter", type=str, dest="DQRfilter", default="",
                        help="filter out data with dqr, <suspect> or <incorrect> or <suspect,incorrect>")
    parser.add_argument("-mp", type=int, dest="maxProcesses", default=6,
                        help="number of processes in multiprocessing pool")

    # Debug logging moved to log file
    #parser.add_argument("-D", "--Debug", action="store_true", dest="debug", help="enables debug nc2csv_logger.infoing")

    args = parser.parse_args()

    return args

def nc2csv(main_args):
    # setup and verify inputs
    nc2csv_logger.info("\n{0}\nnc2csv: instantiating setup class\n{0}".format("*"*100))
    setup = Setup(main_args)
    nc2csv_logger.info("\n{0}\nnc2csv: validating inputs\n{0}".format("*" * 100))
    setup.validate_inputs()

    # this process manager creates up to maxProcesses (based on number of files)
    nc2csv_logger.info("\n{0}\nnc2csv: instatiating ProcessManager\n{0}".format("*"*100))
    process_manager = ProcessManager(setup)

    nc2csv_logger.info("\n{0}\nnc2csv: Converting files...\n{0}".format("*" * 100))
    # do the work
    process_manager.process_files()
    nc2csv_logger.info("\n{0}\nnc2csv: Finished processing files\n{0}".format("*" * 100))

    # PostProc has inherited constructor from Setup
    post_proc = PostProc(setup, main_args)

    # Dump the header to a separate file
    post_proc.dump_header()
    nc2csv_logger.info("\n{0}\nnc2csv: Dumped header\n{0}".format("*" * 100))

    # merge output files into one
    if main_args.merged_output:
        nc2csv_logger.info("\n{0}\nnc2csv: Merging output...\n{0}".format("*" * 100))
        post_proc.merge_output()
        nc2csv_logger.info("\n{0}\nMerged output\n{0}".format("*" * 100))

    # get the total time to process files
    setup.total_time = time.time() - setup.start
    nc2csv_logger.info("\n{0}\nprocessed {1} files in {2}s\n{0}".format("*" * 100, len(setup.files), round(setup.total_time, 1)))

    return 0

if __name__ == "__main__":
    if len(sys.argv) > 1:
        main_args = parse_arguments()

        nc2csv(main_args)
