from SPARQLWrapper import SPARQLWrapper, JSON,XML
import re,json

#get all identifiers, i.e.[P1,P2,P3, ...]
def get_identifiers():
	global P_nodes
	#sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
	sparql = SPARQLWrapper("http://sitaware.isi.edu:8080/bigdata/namespace/wdq/sparql")
	#sparql.setTimeout(1000)
	sparql.setQuery("""select ?p where {
		?p wikibase:propertyType wikibase:ExternalId .
		}
		""")
	sparql.setReturnFormat(JSON)
	results = sparql.query().convert()
	res = results["results"]["bindings"]
	for r in res:
		try:
			P_node = re.search(r'[A-Za-z][0-9]+',r["p"]["value"]).group()
			#update global varibale P_nodes
			P_nodes.append(P_node)
		except:
			print('get_identifiers Error()', r)

#get properties for a given P_node, return {'01101':'Q1234567'}
def get_properties(P_ID):
	cur_json = {}
	#sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
	sparql = SPARQLWrapper("http://sitaware.isi.edu:8080/bigdata/namespace/wdq/sparql")
	#sparql.setTimeout(1000)
	sparql.setQuery("""
		SELECT ?x ?v WHERE { ?x wdt:""" + P_ID + """ ?v. }
	""")
	sparql.setReturnFormat(JSON)
	try:
		results = sparql.query().convert()
		for result in results["results"]["bindings"]:
			value = result["v"]["value"]
			Q = re.search(r'Q[0-9]+',result["x"]["value"])
			if Q:
				Q_node = Q.group(0)
				cur_json[value] = Q_node
				#if the key is a number and has leading zeros, add a new key in the dict
				if(value[0]=="0" and value.isdigit()):
					rm_leading0 = str(int(value))
					cur_json[rm_leading0] = Q_node
	except:
		print('get_properties() Error:',P_ID)
	return cur_json

#do traversal on prop_idents dict, build identifier_nodes_dict, i.e. {'1101': [P882/Q123456,PP881/Q123]}
def convert_dict(prop_idents):
    identifier_nodes_dict = {}
    count = 0
    keys = set()
    for P_number,p_entity in prop_idents.items():
        count += 1
        print(P_number,count)
        for identi,Q_number in p_entity.items():
            #if identi in identifier_nodes_dict.keys():
            if identi in keys:
                identifier_nodes_dict[identi].append(P_number + '/' + Q_number)
            else:
                keys.add(identi)
                identifier_nodes_dict[identi] = [P_number + '/' + Q_number]
    return identifier_nodes_dict

if __name__ == "__main__":
	P_nodes = []
	prop_idents ={}
	get_identifiers()
	for idx,P_node in enumerate(P_nodes):
		print(idx,P_node)
		prop_idents[P_node] = get_properties(P_node)

	identifier_nodes = convert_dict(prop_idents)
	js = json.dumps(identifier_nodes)
	# could be changed to any file path
	f = open("identifier_nodes.json","w+")
	f.write(js)
	f.close()
