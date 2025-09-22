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
import tr
import re
from dip import dig, dis

def stdprep(df, mapjson:str=None, mapyaml:str=None, sender=None, method=None, datefmt=None, methodname_prefix=None):
    """stdprep does the standard prepping steps: renaming columns via mapping, throwing out columns not in mapping, parsing date, inserting methodname and sender column. only the steps for which arguments are given are done."""

    # wir vereinheitlichen die column names und behalten nur die columns im mapping.
    # support json and yaml
    if mapjson is not None:
        map = _jsonff(mapjson)
    if mapyaml is not None:
        with open(mapyaml, "r") as file:
            map = yaml.safe_load(file)
    if mapjson is None and mapyaml is None:
        print("error: either json or yaml mapping needed")

    # strip surrounding blanks in column names
    
    df = rename(df, map)
    #print(df.iloc[0])
    df = prune(df, map)
    #print(df.iloc[0])    

    # the following code assumes the standard column names as given in fhirbuild/csvtofhirobs.py
    
    # eine neue column fuer den sender
    if sender is not None:
        df["sender"] = sender
    # eine neue column fuer das messprofil sender
    if method is not None:
        df["method"] = method
    # wir parsen die date strings.
    if datefmt is not None and "effective_date_time" in df:
        df["effective_date_time"] = pd.to_datetime(df["effective_date_time"], format=datefmt)
    # wir generieren method names (messbefund namen) aus prefix, sampleid und datum.
    if methodname_prefix is not None:
        df["methodname"] = df.apply(lambda row: gen_method_name(methodname_prefix, row["idcs_SAMPLEID"], row["effective_date_time"]), axis=1)

    # insert a subject_idcontainer column if there is none
    if not "subject_idcontainer" in map.keys():
        df["subject_idcontainer"] = "LIMSPSN"

    return df

def stdcheck(df, db, outfile=None):
    """stdcheck does data checks against the db. it assumes that stdprep has run before.
    for now it checks that the samples are there."""
    # the out string is printed and optionally also written to outfile
    out = ""
    # start traction from the db connection
    trac = tr.traction(db)
    # fish out the sampleids that are not in db
    for i, row in df.iterrows():
        sampleid = df.at[i, "idcs_SAMPLEID"]
        if len(trac.sample(sampleids=[sampleid])) == 0:
            out += sampleid + "\n"

    if out != "":
        # print missing ids
        print("sampleids not in db:")
        print(out)
        # write them to file if given
        if outfile:
            f = open(outfile, "w")
            f.write(out)
            f.close


def rename(df, outtoin):
    """rename renames df column names from mapping.
    give the mapping in the direction of output-col: input-col
    """
    
    # first the mapping was given in to-from direction with the num-names first, since they stay the same but for sending mapping suggestions around it seems to be more natural to put the column names first and than the num-names, the direction in which the mapping is done."""

    #fromtomap = {}
    #for k in tofrommap:
    #    fromtomap[tofrommap[k]] = k

    # check if the names are there
    # the colunms of the dataframe
    dfcols = list(df)
    # the expected column names from the mapping
    expectcols = list(outtoin.values())
    # which of expectcols is not in the dataframe cols?
    notindf = list(filter(lambda x: x not in set(dfcols), expectcols))
    if len(notindf) > 0:
      raise Exception(f"error: these columns should be in df according to the mapping: {notindf}")    

    # flip the map so now the direction is df-columns to csv-columns
    intoout = _flip(outtoin)
    return df.rename(intoout, axis="columns")

def prune(df, outtoin):
    """prune keeps only the dataframe columns that are a key in map"""
    dfcols = list(df)
    keepcols = list(outtoin.keys())
    dropcols = list(filter(lambda x: x not in set(keepcols), dfcols))
    print(f"dropping columns {dropcols}")
    notindf = list(filter(lambda x: x not in set(dfcols), keepcols))
    if len(notindf) > 0:
        raise Exception(f"error: these columns should be kept but not in df: {notindf}")

    return df[keepcols]

def insaqg_auto(df):
    """insaqg_auto inserts aliquotgroups automatically. it puts aliquots that have both the same parent and the same material into the same aliquotgroup (should be the standard case). it assumes fhirbuild-ready columns, namely parent_sampleid, and type. the aqtgroups also get the organization_unit, received_date, and subject_id of their respective aliquots.  it updates the fhirids along the way."""
    
    # group the aliquots by parent and material
    byparmat = {}
    nonaliquots = []
    for i, row in df.iterrows():
        # not an aliquot? save for later.
        if df.at[i, "category"] != "DERIVED":
            nonaliquots.append(row)
            continue
        
        parent = df.at[i, "parent_sampleid"]
        # create new parent group if new
        if not parent in byparmat:
            byparmat[parent] = {}
        material = df.at[i, "type"]
        # create new aliquot array if new
        if not material in byparmat[parent]:
            byparmat[parent][material] = []

        # append the aliquot
        byparmat[parent][material].append(row)

    
    # make groups from iterate byparmat
    groups = []
    for parentid in byparmat:
        for material in byparmat[parentid]:
            group = [parentid] # parent is the first element in group
            for aqt in byparmat[parentid][material]:
                group.append(aqt["idcs_SAMPLEID"])
            groups.append(group)

    # pass to insaqg
    return insaqg(df, groups)


    
def insaqg(df, groups):
    """insaqg inserts the aliquotgroups given in groups. groups is an array of arrays, each member array holding the parent sampleid as first element and the aliquot ids that go into the group as subsequent elements. there can be multiple aliquotgroups with the same parent. it assumes fhirbuild column names. the aqtgroup get the organization_unit, received_date, and subject_id of their respective aliquots.  it updates the fhirids along the way."""

    nonaliquots = []
    aqtbyid = {} # aliquots by their sampleid

    for i, row in df.iterrows():
        # not an aliquot? save for later.
        if row["category"] != "DERIVED":
            nonaliquots.append(row)
        # safe aliquots by id for group info
        elif row["category"] == "DERIVED":
            aqtbyid[row["idcs_SAMPLEID"]] = row
        
    # create a new list of output rows
    out = []

    # first put in the non-aliquots that were filtered out.  is that smart?
    
    # count up the fhirid
    fhirid = 1

    for nonaqt in nonaliquots:
        nonaqt["fhirid"] = str(fhirid)
        fhirid += 1
        out.append(nonaqt)

    # insert rows for aliquot groups
    for group in groups:
        parentid = group[0] # the first array element is the parentid
        # take the values for the aqtgroup from the first aliquot
        firstaqtid = group[1]
        firstaqt = dict(aqtbyid[firstaqtid])
        # make a new aliquotgroup
        aqtgrp = {
            "category": "ALIQUOTGROUP",
            "fhirid": str(fhirid),
            "organization_unit": dig(firstaqt, "organization_unit"),
            "parent_sampleid": parentid,
            "received_date": dig(firstaqt, "received_date"),
            "subject_id": dig(firstaqt, "subject_id"),
            "type": dig(firstaqt, "type")
        }

        # append the aliquotgroup to out
        out.append(aqtgrp)

        # the aliquotgroup references its parent sample by sampleid, the aliquots in turn reference their parent aliquotgroup by fhirid.
        fhirid_aqtgrp = str(fhirid)
            
        # count up the fhirid for the next aliquot
        fhirid += 1

        # go through the aliquots (ignore the first element of the array, the parent) and link them to the aliquot group
        for aqtid in group[1:]:

            # get the aliquot
            aqt = aqtbyid[aqtid]
            
            # link to the aliquot group
            aqt["parent_fhirid"] = fhirid_aqtgrp
                
            # the parent sampleid is taken care of now by the aliquot group
            aqt["parent_sampleid"] = None

            # give the aliquot a fhirid
            aqt["fhirid"] = str(fhirid)
            fhirid += 1

            # append the aliquot to out
            out.append(aqt)
            
    # return one dataframe with both aliquotgroups and aliquots, each aliquotgroup followed by the aliquots it contains.
    return pd.DataFrame.from_dict(out)

def _flip(map):
    """_flip flips the map's keys and values"""
    out = {}
    for k in map:
        v = map[k]
        if v in out:
            print("error: can't add key {v} a second time to map.")
            return None
        out[v] = k
    return out

def _jsonff(filename):
    """jsonff ('json-from-file') returns a json object read from file"""
    with open(filename, "r") as f:
        return json.load(f)


def gen_method_name(prefix, sampleid, date:datetime=None):
    """gen_method_name generates a method name from prefix, sampleid and date"""
    name = prefix + '_' + str(sampleid).strip() 
    if date:
        name += '_' + datetime.strftime(date, "%d.%m.%Y %H:%M:%S")
    return name


def messparam_yaml_stub(map:dict) -> str:
    """messparam_yaml_stub returns boilerplate yaml for messparameter or messprofil building with masterblaster from the codes in the map"""

    # prepare an array of objects holding a code
    params = []
    for value in map.values():
        # if the value is not a messparam
        if not re.match(r"^cmp_", value):
            continue
        # remove the cmp_ prefix
        value = value.replace("cmp_", "")
        # put the value in an entry object, 
        param = {}
        param["code"] = value
        params.append(param)

    # return as yaml string
    return yaml.dump(params)
