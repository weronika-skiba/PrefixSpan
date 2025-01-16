from time import perf_counter
from prefixspan import PrefixSpan
import matplotlib.pyplot as plt
import random
from pyspark.ml.fpm import PrefixSpan as PS
from pyspark.sql import SparkSession

class SequenceAnalyzer:
    def __init__(self):
        self.spark = None

    def parse_spmf_line(self, line):
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

    def parse_database(self, file, for_spark=False):
        with open(file, "r") as f:
            lines = f.readlines()
        sequences = [self.parse_spmf_line(line) for line in lines]
        if for_spark:
            return [(idx, sequence) for idx, sequence in enumerate(sequences, start=1)]
        return sequences

    def plot_results(self, x_data, y_data, x_label, y_label, title):
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.set_xlabel(x_label)
        ax.set_ylabel(y_label)
        ax.scatter(x_data, y_data, color="mediumblue")
        plt.title(title)
        ax.legend(loc='upper right')
        plt.tight_layout()
        plt.show()

    def get_spark_session(self):
        if not self.spark:
            self.spark = SparkSession.builder \
                .appName("PrefixSpan-Analysis") \
                .master("local[*]") \
                .getOrCreate()
        return self.spark

    def analyze_min_support(self, dataset, min_support_range, use_spark=False):
        database = self.parse_database(f"datasets/{dataset}.txt", use_spark)
        elapsed_times = []
        min_supports = []

        if use_spark:
            spark = self.get_spark_session()
            df = spark.createDataFrame(database, ["id", "sequence"])

        for support in min_support_range:
            print(f'Searching for sequences of min support = {support}')
            
            if use_spark:
                prefixspan = PS(minSupport=support, maxPatternLength=1000)
                start = perf_counter()
                patterns = prefixspan.findFrequentSequentialPatterns(df)
            else:
                prefixspan = PrefixSpan(support)
                start = perf_counter()
                patterns = prefixspan.run(database)
            
            stop = perf_counter()
            elapsed_times.append(stop - start)
            min_supports.append(support)

        self.plot_results(
            min_supports, 
            elapsed_times,
            "Wartość współczynnika wsparcia",
            "Czas wykonania (s)",
            "Wpływ wartości parametru minimalnego wsparcia na czas działania algorytmu"
        )

    def analyze_sequence_length(self, dataset, base_min_support, use_spark=False):
        org_database = self.parse_database(f"datasets/{dataset}.txt", use_spark)
        elapsed_times = []
        sequence_numbers = []

        for i in range(1, 100):
            subset_size = int(len(org_database) * (i / 100))
            shuffled_database = org_database[:]
            random.shuffle(shuffled_database)
            smaller_database = shuffled_database[:subset_size]
            print(f'Searching for sequences of size = {subset_size}')

            if use_spark:
                spark = self.get_spark_session()
                df = spark.createDataFrame(smaller_database, ["id", "sequence"])
                prefixspan = PS(minSupport=base_min_support, maxPatternLength=1000)
                start = perf_counter()
                patterns = prefixspan.findFrequentSequentialPatterns(df)
            else:
                prefixspan = PrefixSpan(base_min_support)
                start = perf_counter()
                patterns = prefixspan.run(smaller_database)

            stop = perf_counter()
            elapsed_times.append(stop - start)
            sequence_numbers.append(subset_size)

        self.plot_results(
            sequence_numbers,
            elapsed_times,
            "Liczba sekwencji",
            "Czas wykonania (s)",
            "Wpływ liczby sekwencji na czas działania algorytmu"
        )

def get_min_support_range(dataset):
    if dataset == "FIFA":
        return [0.35 + i * 0.004 if i < 25 else (i-1)/50 for i in range(51)]
    else:  # Retail
        return [0.2 + i * 0.01 for i in range(60)]

def main():
    analyzer = SequenceAnalyzer()
    
    def showMinSupImpactFIFA():
        analyzer.analyze_min_support("FIFA", get_min_support_range("FIFA"))

    def showMinSupImpactRetail():
        analyzer.analyze_min_support("OnlineRetail", get_min_support_range("OnlineRetail"))

    def showLengthImpactRetail():
        analyzer.analyze_sequence_length("OnlineRetail", 0.25)

    def showLengthImpactFIFA():
        analyzer.analyze_sequence_length("FIFA", 0.4)

    def showMinSupImpactFIFASpark():
        analyzer.analyze_min_support("FIFA", get_min_support_range("FIFA"), use_spark=True)

    def showMinSupImpactRetailSpark():
        analyzer.analyze_min_support("OnlineRetail", get_min_support_range("OnlineRetail"), use_spark=True)

    def showLengthImpactRetailSpark():
        analyzer.analyze_sequence_length("OnlineRetail", 0.25, use_spark=True)

    def showLengthImpactFIFASpark():
        analyzer.analyze_sequence_length("FIFA", 0.4, use_spark=True)

    # Uncomment the function you want to run:
    showMinSupImpactFIFA()
    # showMinSupImpactRetail()
    # showLengthImpactRetail()
    # showLengthImpactFIFA()
    # showMinSupImpactFIFAspark()
    # showMinSupImpactRetailspark()
    # showLengthImpactRetailspark()
    # showLengthImpactFIFAspark()

if __name__ == "__main__":
    main()