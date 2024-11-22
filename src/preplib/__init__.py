# preplib.py tries to collect some functions for data preparation
# before fhir building

# methods:

# gen_method_name
# jsonff
# prune
# rename
# stdprep

import pandas as pd
import json
import yaml
from datetime import datetime

# stdprep does the standard prepping steps:
# renaming columns via mapping, throwing out columns not in mapping, parsing date, inserting methodname and sender column
# only the operations for given arguments are done
def stdprep(df, mapjson:str=None, mapyaml:str=None, sender=None, method=None, datefmt=None, methodname_prefix=None):
    # wir vereinheitlichen die column names und behalten nur die columns im mapping.
    # support json and yaml
    if mapjson is not None:
        map = jsonff(mapjson)
    if mapyaml is not None:
        with open(mapyaml, "r") as file:
            map = yaml.safe_load(file)
    if mapjson is None and mapyaml is None:
        print("error: either json or yaml mapping needed")
    df = rename(df, map)
    df = prune(df, map)

    # the following code assumes the standard column names as given in fhirbuild/csvtofhirobs.py
    
    # eine neue column fuer den sender
    if sender is not None:
        df["sender"] = sender
    # eine neue column fuer das messprofil sender
    if method is not None:
        df["method"] = method
    # wir parsen die date strings.
    if datefmt is not None:
        df["effective_date_time"] = pd.to_datetime(df["effective_date_time"], format=datefmt)
    # wir generieren method names (messbefund namen) aus prefix, sampleid und datum.
    if methodname_prefix is not None:
        df["methodname"] = df.apply(lambda row: gen_method_name(methodname_prefix, row["id_SAMPLEID"], row["effective_date_time"]), axis=1)
        
    return df


# rename renames df column names from mapping
# first the mapping was given in to-from direction with the num-names first, since they stay the same
# but for sending mapping suggestions around it seems to be more natural to put the column names first and than the num-names, the direction in which the mapping is done.
def rename(df, map):
    #fromtomap = {}
    #for k in tofrommap:
    #    fromtomap[tofrommap[k]] = k
        
    return df.rename(map, axis="columns")

# prune keeps only the dataframe columns that are a value in map
def prune(df, map):
    dfcols = list(df)
    keepcols = list(map.values())
    dropcols = list(filter(lambda x: x not in set(keepcols), dfcols))
    print(f"dropping columns {dropcols}")
    notindf = list(filter(lambda x: x not in set(dfcols), keepcols))
    if len(notindf) > 0:
        raise Exception(f"error: these columns should be kept but not in df: {notindf}")

    return df[keepcols]

# jsonff ('json-from-file') returns a json object read from file
def jsonff(filename):
    with open(filename, "r") as f:
        return json.load(f)

# gen_method_name generates a method name from prefix, sampleid and date
def gen_method_name(prefix, sampleid, date:datetime):
    return prefix + '_' + str(sampleid).strip() + '_' + datetime.strftime(date, "%d.%m.%Y %H:%M:%S")
