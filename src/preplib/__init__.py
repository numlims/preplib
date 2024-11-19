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
from datetime import datetime

# stdprep does the standard prepping steps:
# renaming columns via mapping, throwing out columns not in mapping, parsing date, inserting methodname and sender column
# only the operations for given arguments are done
def stdprep(df, mapfile=None, sender=None, method=None, datefmt=None, methodname_prefix=None):
    # wir vereinheitlichen die column names und behalten nur die columns im mapping.
    if mapfile is not None:
        m = jsonff("mapping.json")
        df = rename(df, m)
        df = prune(df, m)

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


# rename renames df column names from to-from mapping
#
# warum to-from als parameter? die to-namen bleiben gleich, es scheint
# angenehmer, sie in der ersten spalte zu haben.
def rename(df, tofrommap):
    fromtomap = {}
    for k in tofrommap:
        fromtomap[tofrommap[k]] = k
        
    return df.rename(fromtomap, axis="columns")

# prune keeps only the dataframe columns that are a key in map
def prune(df, tfmap):
    allcols = list(df)
    keepcols = list(tfmap.keys())
    dropcols = list(filter(lambda x: x not in set(keepcols), allcols))
    print(f"dropping columns {dropcols}")
    return df[keepcols]

# jsonff ('json-from-file') returns a json object read from file
def jsonff(filename):
    with open(filename, "r") as f:
        return json.load(f)

# gen_method_name generates a method name from prefix, sampleid and date
def gen_method_name(prefix, sampleid, date:datetime):
    return prefix + '_' + str(sampleid).strip() + '_' + datetime.strftime(date, "%d.%m.%Y %H:%M:%S")
