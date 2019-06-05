from collections import Counter
import json
import requests
from SPARQLWrapper import SPARQLWrapper, JSON
import typing


P_blacklist = ["P5736", "P3984"]

class FindIdentity:
    def __init__(self):
        pass

    # input: strings, output dictionary of string and Qnode in wikidate
    @staticmethod
    def get_identtifier(strings):
        id_nodes_dict = FindIdentity.call_redis(strings)
        keys = set(id_nodes_dict.keys())
        output = {}  # key string, value:Q123
        print(len(keys))
        P_list = []
        for s in strings:
            if s in keys:
                P_list.extend([P_Q.split('/')[0] for P_Q in id_nodes_dict[s]])
        P_predict = Counter(P_list).most_common(1)[0][0]  # [('P932', 8), ('P1566', 6), ('P698', 2)]
        for s in strings:
            if s in keys:
                for P_Q in id_nodes_dict[s]:  # for P_Q in P_Qlist
                    P_Q_splited = P_Q.split('/')
                    if P_Q_splited[0] == P_predict:
                        output[s] = P_Q_splited[1]
            else:
                output[s] = ''
        return output

    @staticmethod
    def get_identifier_3(strings:typing.List[str], column_name: str=None, target_p_node: str=None):

        id_nodes_dict = FindIdentity.call_redis(strings)
        result = []
        P_list = []
        appeared_threshold = 0.5

        keys = set(id_nodes_dict.keys())

        for s in strings:
            if s in keys:
                # update 2019.5.31: it seems sometimes duplicate relationship will appeared!
                temp = [P_Q.split('/')[0] for P_Q in set(id_nodes_dict[s])]
                temp1 = list(set(temp))
                P_list.extend(temp1)

        if target_p_node is not None:
            P_predicts = [target_p_node]
            print("User-defined P node is " + P_predicts[0])
        else:
            P_predicts = [x[0] for x in Counter(P_list).most_common(5)]  # [('P932', 8), ('P1566', 6), ('P698', 2)]

            for each in P_blacklist:
                try:
                    P_predicts.remove(each)
                except:
                    pass
            if len(P_predicts) == 0:
                print("[ERROR] No candidate P nodes found for input column : [" + column_name + "]")
                return result
            print("The best matching P node is " + P_predicts[0])

        """
        # use edit distance to find best candidate
        P_edit_distance = {}
        for each in P_predicts:
            P_edit_distance[each] = FindIdentity.min_distance(FindIdentity.get_node_name(each), column_name)
        smallest_dist = 1000
        for key, val in P_edit_distance.items():
            if val < smallest_dist:
                best = key
                smallest_dist = val

        import pdb
        pdb.set_trace()
        # best_predicts =
        """
        best_predicts = [P_predicts[0]]

        # print('Top 3 possible properties:')
        # print(P_predicts)

        for P_predict in best_predicts:
            output = {}  # key string, value:Q123
            for s in strings:
                output[s] = ''
                if s in keys:
                    for P_Q in id_nodes_dict[s]:  # for P_Q in P_Qlist
                        P_Q_splited = P_Q.split('/')
                        if P_Q_splited[0] == P_predict:
                            output[s] = P_Q_splited[1]
                            continue
            result.append((P_predict, output))
        return result

    @staticmethod
    def call_redis(qnodes):
        payload = json.dumps({"ids": qnodes})
        search_headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
        r = requests.post("http://minds03.isi.edu:4444/get_identifiers", data=payload, headers=search_headers)
        if r:
            val = r.json()
            return val
        else:
            return {}

    @staticmethod
    def get_node_name(node_code):
        """
        Inner function used to get the properties(P nodes) names with given P node
        :param node_code: a str indicate the P node (e.g. "P123")
        :return: a str indicate the P node label (e.g. "inception")
        """
        sparql_query = "SELECT DISTINCT ?x WHERE \n { \n" + \
                       "wd:" + node_code + " rdfs:label ?x .\n FILTER(LANG(?x) = 'en') \n} "
        try:
            sparql = SPARQLWrapper("http://sitaware.isi.edu:8080/bigdata/namespace/wdq/sparql")
            sparql.setQuery(sparql_query)
            sparql.setReturnFormat(JSON)
            results = sparql.query().convert()
            return results['results']['bindings'][0]['x']['value']
        except:
            return ""

    @staticmethod
    def min_distance(word1, word2):
        """
        :type word1: str
        :type word2: str
        :rtype: int
        """
        dp = [[0] * (len(word2) + 1) for _ in range(len(word1) + 1)]
        for i in range(len(word2)):
            dp[0][i + 1] = i + 1
        for i in range(len(word1)):
            dp[i + 1][0] = i + 1
        for i in range(len(word1)):
            for j in range(len(word2)):
                if word1[i] == word2[j]:
                    dp[i + 1][j + 1] = dp[i][j]
                else:
                    dp[i + 1][j + 1] = min(dp[i][j], dp[i + 1][j], dp[i][j + 1]) + 1
        return dp[-1][-1]


if __name__ == "__main__":
    test1 = ['5333265', '5333264', '5333267', '5333266', '5333261', '5333260', '5333263', '5333262']
    test2 = ['XX1007441', 'XX1012281', 'XX1018804', 'XX1033111', 'XX1041185', 'XX1041305', 'XX1044878', 'XX1049656', 'XX1069173']
    test3 = ['0', '1', '1212', '1323', '2112', '2113', '212', '2141', '2142', '2144', '2145', '2147', '215']

    print('Testcase1, Truth P932:')
    for idx, res in enumerate(FindIdentity.get_identifier_3(test1)):
        print('Top', idx+1, res[0])
        print(res[1])
    print('-'*30)
    print('Testcase2, Truth P950')
    for idx, res in enumerate(FindIdentity.get_identifier_3(test2)):
        print('Top', idx+1, res[0])
        print(res[1])
    print('-'*30)
    print('Testcase3, Truth P952')
    for idx, res in enumerate(FindIdentity.get_identifier_3(test3)):
        print('Top', idx+1, res[0])
        print(res[1])
    print('-'*30)
