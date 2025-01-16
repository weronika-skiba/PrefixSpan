class SPMFParser:

    @staticmethod
    def parse_line(line):
        sequence = []
        itemset = []
        
        for token in line.strip().split():
            if token == "-1":
                if itemset:
                    sequence.append(itemset)
                    itemset = []
            elif token == "-2":
                break
            else:
                itemset.append(token)
                
        return sequence
    
    @staticmethod
    def parse_file(filepath, for_spark=False):
        try:
            with open(filepath, "r") as f:
                sequences = [SPMFParser.parse_line(line) for line in f]
                
            if for_spark:
                return [(idx, seq) for idx, seq in enumerate(sequences, start=1)]
            return sequences
            
        except FileNotFoundError:
            raise FileNotFoundError(f"Could not find file: {filepath}")
        except Exception as e:
            raise Exception(f"Error parsing file {filepath}: {str(e)}")

