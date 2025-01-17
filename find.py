from pyspark.ml.fpm import PrefixSpan as PrefixSpanSpark
from pyspark.sql import SparkSession
import argparse
import csv
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional
from pathlib import Path
from parser import SPMFParser
from prefixspan import PrefixSpan

@dataclass
class Config:
    min_support: float
    dataset: Optional[str]
    use_spark: bool = False
    output_dir: Path = Path("output")
    
    @classmethod
    def from_args(cls, args: argparse.Namespace) -> 'Config':
        return cls(
            min_support=float(args.min_support),
            dataset=args.dataset,
            use_spark=args.spark
        )

class DataLoader:
    '''SMALL_DATABASE = [
        [list("ab"), list("c"), list("a")],
        [list("ab"), list("b"), list("c")],
        [list("b"), list("c"), list("d")],
        [list("b"), list("ab"), list("c")],
    ]'''

    SMALL_DATABASE = [
        [list("a"), list("abc"), list("ac"), list("d"), list("cf")],
        [list("ad"), list("c"), list("bc"), list("ae")],
        [list("ef"), list("ab"), list("df"), list("c"), list("b")],
        [list("e"), list("g"), list("af"), list("c"), list("b"), list("b")],
    ]
    
    @staticmethod
    def load_data(config: Config) -> List:
        if config.dataset:
            dataset_path = Path("datasets") / f"{config.dataset}.txt"
            if config.use_spark:
                return SPMFParser.parse_file(str(dataset_path), for_spark=True)
            return SPMFParser.parse_file(str(dataset_path))
        
        if config.use_spark:
            return [(idx, seq) for idx, seq in enumerate(DataLoader.SMALL_DATABASE, start=1)]
        return DataLoader.SMALL_DATABASE

class SparkPatternMiner:
    def __init__(self, config: Config):
        self.config = config
        self.spark = None
    
    def setup_spark(self):
        self.spark = SparkSession.builder \
            .appName("PrefixSpan-Spark") \
            .master("local[*]") \
            .getOrCreate()
    
    def mine_patterns(self, database: List[Tuple]) -> List:
        self.setup_spark()
        
        df = self.spark.createDataFrame(database, ["id", "sequence"])
        prefix_span = PrefixSpanSpark(
            minSupport=self.config.min_support,
            maxPatternLength=1000
        )
        
        patterns = prefix_span.findFrequentSequentialPatterns(df)
        rows = patterns.collect()

        patterns_count = patterns.count()
        
        self.spark.stop()
        return rows, patterns_count

class StandardPatternMiner:
    def __init__(self, config: Config):
        self.config = config
    
    def mine_patterns(self, database: List) -> Dict:
        prefix_span = PrefixSpan(min_support=self.config.min_support)
        patterns = prefix_span.run(database)
        return {key: int(value) for key, value in patterns.items()}

class ResultWriter:
    @staticmethod
    def write_spark_results(filename: Path, rows: List, count: int):
        with open(filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['id', 'sequence'])
            for row in rows:
                writer.writerow(row)
        print(f"{count} patterns have been written to {filename}.")
    
    @staticmethod
    def write_standard_results(filename: Path, patterns: Dict):
        with open(filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['id', 'sequence'])
            for key, value in patterns.items():
                writer.writerow([key, value])
        print(f"{len(patterns)} patterns have been written to {filename}.")

def setup_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Mine frequent patterns in sequential data.')
    parser.add_argument('min_support', help='Minimum support needed to consider a sequence frequent.')
    parser.add_argument('--dataset', default=None, choices=['FIFA', 'OnlineRetail'], 
                       help='Dataset to process.')
    parser.add_argument('--spark', type=bool, default=False, 
                       help='Indicator whether to use spark implementation.')
    return parser

def main():
    parser = setup_argument_parser()
    args = parser.parse_args()
    config = Config.from_args(args)
    
    config.output_dir.mkdir(exist_ok=True)
    
    database = DataLoader.load_data(config)
    
    if config.use_spark:
        miner = SparkPatternMiner(config)
        rows, count = miner.mine_patterns(database)
        output_file = config.output_dir / "patterns-spark.csv"
        ResultWriter.write_spark_results(output_file, rows, count)
    else:
        miner = StandardPatternMiner(config)
        patterns = miner.mine_patterns(database)
        output_file = config.output_dir / "patterns.csv"
        ResultWriter.write_standard_results(output_file, patterns)

if __name__ == "__main__":
    main()