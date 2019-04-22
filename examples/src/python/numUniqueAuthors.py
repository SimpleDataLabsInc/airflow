# script to do simple processing of reddit data
import json
from sets import Set
import sys

filename = sys.argv[1]
authorSet = Set()

with open(filename) as f:
	for i, line in enumerate(f):
		jsonLine = json.loads(line)
		if 'artistID' in jsonLine:
			authorSet.add(jsonLine['artistID'])
print len(authorSet)
