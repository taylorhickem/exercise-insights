"""this module conditions and converts the data into different
formats such as from google sheets to sqlite
"""
#-----------------------------------------------------------
#dependencis
#-----------------------------------------------------------
import os
import datetime as dt
import pandas as pd
from sqlgsheet import database as db

FIELDS = {
    'mydate': 'date',
    'belongsession': 'session',
    'ename': 'lift',
    'record': 'orm_kg',
    'logs': 'sets'
}


EXERCISE_LOGS_FILE = 'exercise_logs.csv'
BEGIN_DELIM = 'EXERCISE LOGS'
END_DELIM = '\n'
#-----------------------------------------------------------
#dynamic variables
#-----------------------------------------------------------
DATASETS = {}

#-----------------------------------------------------------
#load
#-----------------------------------------------------------
def update():
    global DATASETS
    db.SQL_DB_NAME = 'sqlite:///exercise.db'
    db.load()
    logs_path = logs_file_in_folder()
    lifts = read_logs(logs_path)
    db.update_table(lifts, 'lifts', False)
    DATASETS['lifts'] = lifts


def read_logs(logs_path: str) -> pd.DataFrame:
    exercise_logs_extract(logs_path)
    lifts = pd.read_csv(EXERCISE_LOGS_FILE)
    if len(lifts) > 0:
        lifts = lifts[list(FIELDS.keys())].rename(columns=FIELDS)
        lifts['volume_kg'], lifts['weight_kg'], lifts['reps'] = zip(*lifts['sets'].map(metrics_from_sets))
        del lifts['sets']
    return lifts


def metrics_from_sets(sets_str):
    sets = [(float(s.split('x')[0]), int(s.split('x')[1])) for s in sets_str.split(',')]
    reps = sum([x[1] for x in sets])
    volume = sum([x[0]*x[1] for x in sets])
    weight = volume/reps
    return volume, weight, reps


def exercise_logs_extract(logs_path):
    lines = []
    logs = open(logs_path, 'r')
    section_start = False
    section_end = False
    while not section_end:
        line = logs.readline()
        if section_start:
            section_end = line == END_DELIM
            if not section_end:
                lines.append(line)
        else:
            section_start = BEGIN_DELIM in line
            if section_start:
                line = logs.readline()
    logs.close()
    exercise_logs_str = ''.join(lines)
    with open(EXERCISE_LOGS_FILE, 'w') as f:
        f.write(exercise_logs_str)
        f.close()


def logs_file_in_folder() -> str:
    logs_path = 'jefit_20230622.csv'
    logs_files = [f for f in os.listdir() if ('.csv' in f and f != EXERCISE_LOGS_FILE)]
    if logs_files:
        logs_path = logs_files[0]
    return logs_path
# -----------------------------------------------------
# Command line
# -----------------------------------------------------
if __name__ == "__main__":
    update()
