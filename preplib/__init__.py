import pandas as pd
import json
import yaml
from datetime import datetime
import tr
import re
from dip import dig, dis
def stdprep(df, mapjson:str=None, mapyaml:str=None, sender=None, method=None, datefmt=None, methodname_prefix=None):
    if mapjson is not None:
        map = _jsonff(mapjson)
    if mapyaml is not None:
        with open(mapyaml, "r") as file:
            map = yaml.safe_load(file)
    if mapjson is None and mapyaml is None:
        print("error: either json or yaml mapping needed")
    df = rename(df, map)
    #print(df.iloc[0])
    df = prune(df, map)
    #print(df.iloc[0])    
    if sender is not None:
        df["sender"] = sender
    if method is not None:
        df["method"] = method
    if datefmt is not None and "effective_date_time" in df:
        df["effective_date_time"] = pd.to_datetime(df["effective_date_time"], format=datefmt)
    if methodname_prefix is not None:
        df["methodname"] = df.apply(lambda row: gen_method_name(methodname_prefix, row["idcs_SAMPLEID"], row["effective_date_time"]), axis=1)
    if not "subject_idcontainer" in map.keys():
        df["subject_idcontainer"] = "LIMSPSN"
    return df
def stdcheck(df, db, outfile=None):
    out = ""
    trac = tr.traction(db)
    for i, row in df.iterrows():
        sampleid = df.at[i, "idcs_SAMPLEID"]
        if len(trac.sample(sampleids=[sampleid])) == 0:
            out += sampleid + "\n"
    if out != "":
        print("sampleids not in db:")
        print(out)
        # write them to file if given
        if outfile:
            f = open(outfile, "w")
            f.write(out)
            f.close
def rename(df, outtoin):
    # the colunms of the dataframe
    dfcols = list(df)
    # the expected column names from the mapping
    expectcols = list(outtoin.values())
    notindf = list(filter(lambda x: x not in set(dfcols), expectcols))
    if len(notindf) > 0:
      raise Exception(f"error: these columns should be in df according to the mapping: {notindf}")    
    intoout = _flip(outtoin)
    return df.rename(intoout, axis="columns")
def prune(df, outtoin):
    dfcols = list(df)
    keepcols = list(outtoin.keys())
    dropcols = list(filter(lambda x: x not in set(keepcols), dfcols))
    print(f"dropping columns {dropcols}")    
    notindf = list(filter(lambda x: x not in set(dfcols), keepcols))
    if len(notindf) > 0:
        raise Exception(f"error: these columns should be kept but not in df: {notindf}")
    return df[keepcols]
def insaqg_auto(df):
    byparmat = {}
    nonaliquots = []
    for i, row in df.iterrows():
        if df.at[i, "category"] != "DERIVED":
            nonaliquots.append(row)
            continue
        parent = df.at[i, "parent_sampleid"]
        if not parent in byparmat:
            byparmat[parent] = {}
        material = df.at[i, "type"]
        if not material in byparmat[parent]:
            byparmat[parent][material] = []
        byparmat[parent][material].append(row)
    groups = []
    for parentid in byparmat:
        for material in byparmat[parentid]:
            group = [parentid] # parent is the first element in group
            for aqt in byparmat[parentid][material]:
                group.append(aqt["idcs_SAMPLEID"])
            groups.append(group)
    return insaqg(df, groups)
def insaqg(df, groups):
    nonaliquots = []
    aqtbyid = {} 
    for i, row in df.iterrows():
        # not an aliquot? save for later.
        if row["category"] != "DERIVED":
            nonaliquots.append(row)
        # safe aliquots by id for group info
        elif row["category"] == "DERIVED":
            aqtbyid[row["idcs_SAMPLEID"]] = row
    out = []
    fhirid = 1
    for nonaqt in nonaliquots:
        nonaqt["fhirid"] = str(fhirid)
        fhirid += 1
        out.append(nonaqt)
    for group in groups:
        parentid = group[0]
        firstaqtid = group[1]
        firstaqt = dict(aqtbyid[firstaqtid])

        aqtgrp = {
            "category": "ALIQUOTGROUP",
            "fhirid": str(fhirid),
            "organization_unit": dig(firstaqt, "organization_unit"),
            "parent_sampleid": parentid,
            "received_date": dig(firstaqt, "received_date"),
            "subject_id": dig(firstaqt, "subject_id"),
            "type": dig(firstaqt, "type")
        }
        out.append(aqtgrp)
        fhirid_aqtgrp = str(fhirid)
        fhirid += 1
        for aqtid in group[1:]:
            aqt = aqtbyid[aqtid]
            aqt["parent_fhirid"] = fhirid_aqtgrp
            aqt["parent_sampleid"] = None
            aqt["fhirid"] = str(fhirid)
            fhirid += 1
            out.append(aqt)
    return pd.DataFrame.from_dict(out)
def messparam_yaml_stub(map:dict) -> str:
    params = []
    for value in map.values():
        if not re.match(r"^cmp_", value):
            continue
        value = value.replace("cmp_", "")

        param = {}
        param["code"] = value
        params.append(param)
    return yaml.dump(params)
def gen_method_name(prefix, sampleid, date:datetime=None):
    name = prefix + '_' + str(sampleid).strip() 
    if date:
        name += '_' + datetime.strftime(date, "%d.%m.%Y %H:%M:%S")
    return name
def _flip(map):
    out = {}
    for k in map:
        v = map[k]
        if v in out:
            print("error: can't add key {v} a second time to map.")
            return None
        out[v] = k
    return out
def _jsonff(filename):
    with open(filename, "r") as f:
        return json.load(f)
