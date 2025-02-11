import ast
import copy
from collections import Counter

class PrefixSpan:
    def __init__(self, min_support=2):
        if min_support < 0:
            raise ValueError("Minimum support (min_support) must be greater than or equal to 0.")
        self.min_sup = min_support
        self.out = {}
        self.frequent_patterns = []
        
    def _get_frequent_single_items(self, db):
        freq_single_items = Counter()
        for seq in db:
            seen = set()  
            for itemset in seq:
                for item in itemset:
                    if item not in seen:
                        seen.add(item)
                        freq_single_items[str([[item]])] += 1
        freq_single_items = {k: v for k, v in freq_single_items.items() if v >= self.min_sup}
        return freq_single_items
    
    def _add_new_frequent_patterns(self, prefix, db):
        freq_elements = Counter()
        for seq in db:
            unique_elements = set()
            for item in seq:
                if item[0] == '_':
                    unique_elements.update(f'_{elem}' for elem in item[1:])
                else:
                    unique_elements.update(item)
            freq_elements.update(unique_elements)
        freq_elements = {k: v for k, v in freq_elements.items() if v >= self.min_sup}
        index = len(self.frequent_patterns)
        for key, value in freq_elements.items():
            literal_prefix = ast.literal_eval(prefix)
            if '_' not in key:
                literal_prefix.append([key])
            else:
                literal_prefix[-1].append(key[1:])
            self.frequent_patterns.append((str(literal_prefix), value))
        return index, len(freq_elements)

    def _project_database(self, database, prefix):
        with_underscore = prefix.startswith('_')
        if with_underscore:
            prefix = prefix[1:]
        projected_db = []
        database_copy = copy.deepcopy(database)
        for sequence in database_copy:
            projected_sequence = []
            first_occurrence_removed = False
            for item in sequence:
                if prefix in item and not first_occurrence_removed:
                    index = item.index(prefix)
                    if item[0] == '_':
                        if not with_underscore:
                            continue
                        del item[index]
                        modified_item = item
                    else:
                        item[index] = '_'
                        modified_item = item[index:]
                    if len(modified_item) > 1:
                        projected_sequence.append(modified_item)
                    first_occurrence_removed = True
                elif first_occurrence_removed:
                    projected_sequence.append(item)
            if projected_sequence:
                projected_db.append(projected_sequence)
        return projected_db

    def _prefixspan(self, db, index=0):
        considered_patterns = self.frequent_patterns[index:] if index > 0 else self.frequent_patterns
        for key, value in considered_patterns:
            if key not in self.out:
                self.out[key] = value
                literal_key = ast.literal_eval(key)
                last_item = literal_key[-1]
                last_element = last_item[-1] if len(last_item) == 1 else f'_{last_item[-1]}'
                projected_db = self._project_database(db, last_element)
                if projected_db:
                    new_index, length = self._add_new_frequent_patterns(key, projected_db)
                    for i in range(length):
                        self._prefixspan(projected_db, new_index + i)

    def run(self, db):
        if self.min_sup <= 1:
            self.min_sup *= len(db)
        self.frequent_patterns = list(self._get_frequent_single_items(db).items())
        self._prefixspan(db)
        return self.out
        
