# __main__ holds the command line interface

import sys
import argparse
import yaml
from preplib import messparam_yaml_stub

def main():
  """main starts the command line interface"""
  parser = argparse.ArgumentParser(description="generate stammdaten xml")
  parser.add_argument("--messparam-yaml-stub", help="yaml mapping file", required=False)
  args = parser.parse_args()

  # get messparam yaml from mapping yaml if wished
  if args.messparam_yaml_stub:
    yamlfile = args.messparam_yaml_stub
    with open(yamlfile, "r") as file:
      mapping = yaml.safe_load(file)
      print(messparam_yaml_stub(mapping))

sys.exit(main())