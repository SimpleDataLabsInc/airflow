import json
import sys

filename = sys.argv[1]

count = 0
total = 0
with open(filename) as f:
    for i, line in enumerate(f):
        jsonLine = json.loads(line)
        if 'weight' in jsonLine:
            count += 1
            total += jsonLine['weight']
print total/count
