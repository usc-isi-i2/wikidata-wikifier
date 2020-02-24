import json
import queue
import pandas as pd

class CTA(object):
    def __init__(self, type_of_dict, super_class_closure_path='wikifier/caches/wikidata_super_classes_closure.json',
                 direct_children_dict_path='wikifier/caches/wikidata_direct_children.json',
                 graph_root='Q35120'):  # Q35120 is the class Entity
        self.super_class_closure = json.load(open(super_class_closure_path))
        self.direct_children_dict = json.load(open(direct_children_dict_path))
        self.graph_root = graph_root
        self.type_of_dict = type_of_dict

    def evaluate_class_closure(self, items, current_node):
        matches = 0
        for item in items:
            if item in self.type_of_dict:
                instances = self.type_of_dict.get(item, [])
                for instance in instances:
                    if current_node in self.super_class_closure.get(instance, []):
                        matches += 1
                        break

        score = matches / len(items)
        return score

    def find_class(self, items, threshold):
        q = queue.Queue()
        q.put(self.graph_root)

        matched_classes = []
        max_score = -1
        seen_nodes = {}
        while (q.qsize() > 0):
            superclass = q.get()
            score = self.evaluate_class_closure(items, superclass)
            if score >= threshold:
                if score >= max_score:
                    if superclass not in matched_classes:
                        matched_classes.append(superclass)
                    max_score = score
                subclasses = self.direct_children_dict[superclass]
                for subclass in subclasses:
                    if subclass not in seen_nodes:
                        seen_nodes[subclass] = 1
                        q.put(subclass)

        print(matched_classes)
        return matched_classes

    def process(self, items, threshold=0.508):
        if len(items) == 0:
            return ""
        matched_classes = self.find_class(items, threshold)
        return " ".join(matched_classes)

    def process_frequency_match(self, qnodes):
        if len(qnodes) == 0:
            return ""

        class_list = list()
        for qnode in qnodes:
            _list = self.type_of_dict.get(qnode, [])
            class_list.extend(_list)
        wiki_class = pd.Series(list(class_list), name='Class')
        value_counts = wiki_class.value_counts(normalize=True, dropna=False)
        frequency = pd.DataFrame(value_counts.rename_axis('Class').reset_index(name='Frequency'))
        return frequency['Class'].tolist()[0]
