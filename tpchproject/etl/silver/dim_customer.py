from datetime import datetime
from typing import Dict, List, Optional, Type

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

from etl.bronze.customer import CustomerBronzeETL
from etl.utils.base_table import ETLDataSet, TableETL

