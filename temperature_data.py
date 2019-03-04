import glob
import pandas as pd
import pickle
import time
from tqdm import tqdm
import numpy as np
from datetime import datetime
import os


def find_las_files(walk=False):
    if walk is True:
        # walk through directory to find las files
        las_files = glob.glob("I:\AGSStatDat\LAS_2007\*\*.las", recursive=True)
        upper_case_las_files = glob.glob("I:\AGSStatDat\LAS_2007\*\*.LAS", recursive=True)

        las_files += upper_case_las_files

        pickle.dump(las_files, open("las_file_list.pkl", "wb"))
    else:
        las_files = pickle.load(open("las_file_list.pkl", "rb"))
    return las_files


def load_temperature_data(las_files, dataframe):
    """
    open each las file and look for:

    UWI, GL (ground level), GL units, TDD (total depth driller), TDD units, BHT (bottom hole temperature), BHT units
    """
    count = 0
    for i in tqdm(las_files):
        time.sleep(0.1)
        with open(i) as las:
            line = las.readline()
            uwi = np.NaN
            gl = np.NaN
            tdd = np.NaN
            bht = np.NaN
            timc = np.NaN
            dlab = np.NaN
            t = np.NaN
            while line:

                # very unprofessional, I know...
                try:
                    # print(line)
                    if "UWI" in line:
                        uwi = line.split()[2]
                    elif "GL" in line and "Ground" in line:
                        try:
                            gl = float(line.split()[2])
                        except ValueError:
                            try:
                                gl = line.split()[2]
                                gl = float(gl[:-1])
                            except ValueError:
                                pass

                        gl_units = line.split()[1]
                        if gl_units == ".FT":
                            try:
                                gl *= 0.3048
                            except TypeError:
                                pass
                    elif "TDD" in line and "Driller" in line:
                        try:
                            tdd = float(line.split()[2])
                        except ValueError:
                            try:
                                tdd = line.split()[2]
                                tdd = float(tdd[:-1])
                            except ValueError:
                                pass
                        tdd_units = line.split()[1]
                        if tdd_units == ".FT":
                            try:
                                tdd *= .3048
                            except TypeError:
                                pass
                    elif "BHT" in line and "Temperature" in line:
                        try:
                            bht = float(line.split()[2])
                        except ValueError:
                            try:
                                bht = line.split()[2]
                                bht = float(bht[:-1])
                            except ValueError:
                                pass

                        bht_units = line.split()[1]
                        if bht_units == ".DEGF":
                            if not np.isnan(bht):
                                try:
                                    bht = (bht - 32) * 5 / 9.
                                    if bht < 0:
                                        bht = np.NaN
                                except TypeError:
                                    pass
                    elif "TIMC" in line and "Circulation" in line:
                        timc = line.split()[1] + " " + line.split()[2]
                    elif "DLAB" in line and "Logging Tool" in line:
                        dlab = line.split()[1] + " " + line.split()[2]
                    if isinstance(timc, str) and isinstance(dlab, str):
                        try:
                            timc = datetime.strptime(timc, "%d-%b-%Y %H:%M")
                            dlab = datetime.strptime(dlab, "%d-%b-%Y %H:%M")
                            t = dlab - timc
                        except ValueError:
                            pass

                    # no need to read data any lower than this.
                    if "~A" in line:
                        try:
                            if not np.isnan(bht):
                                dataframe[os.path.basename(i)] = {
                                    "UWI": uwi,
                                    "GL": gl,
                                    "TDD": tdd,
                                    "BHT": bht,
                                    "Time": t
                                }
                        except TypeError:
                            pass
                except IndexError:
                    pass


                line = las.readline()
    df = pd.DataFrame.from_dict(dataframe)
    try:
        df = df.T
        df.to_csv("temperature_data.csv")
    except PermissionError:
        print("Close temp_data.csv if you want to save data to this file.")
    pickle.dump(dataframe, open("temp_dataframe.pkl", "wb"))


if __name__ == "__main__":
    las_files = find_las_files()
    load_pkl_data = False
    dataframe = dict()

    if load_pkl_data is True:
        dataframe = pickle.load(open("temp_dataframe.pkl", "rb"))
        completed_files = dataframe.keys()
        print(completed_files)

        to_do_list = list()
        for i in las_files:
            if i not in completed_files:
                to_do_list.append(i)
        print(len(to_do_list), len(las_files))
        las_files = to_do_list

    load_temperature_data(las_files, dataframe)




