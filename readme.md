# preplib

csv prepping before fhirbuild.

```
df = stdprep(mapyaml="mapping.yaml", sender="NUM_FRA", method="NUM_VIR_METADATA", datefmt="%d.%m.%Y", methodname_prefix="VIR")
```

docs [here](https://numlims.github.io/preplib/).

## methods

```
stdprep
rename
prune
gen_method_name
```

## dev

assemble code from the .ct files with [ct](https://github.com/tnustrings/ct).

build and install:

```
make install
```
