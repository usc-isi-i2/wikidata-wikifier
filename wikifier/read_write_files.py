import json
#write files
b = {} #some dictionary
f1 = open("identifier_nodes_v2_small.json", "w+")
js = json.dumps(b, indent=4)
f1.write(js)
f1.close()

#read files
with open('identifier_nodes_v2_small.json') as f4:
	d = json.load(f4)