import json
import os
import time
import string
from pathlib import Path
from typing import Iterable
import pyarrow as pa
from datafusion import SessionContext
from datafusion import substrait as ds
from datafusion import DataFrame
from src.substrait_consumer.execution_engine import ExecutionEngine
from src.substrait_producer.isthmus_producer import IsthmusProducer
from src.substrait_producer.datafusion_producer import DataFusionProducer
from src.substrait_producer.duckdb_producer import DuckDBProducer
from src.substrait_producer.isthmus_kit.tpch_schema import *
from src.substrait_producer.isthmus_kit.tpcds_schema import *
from src.substrait_producer.isthmus_kit.imdb_schema import *
from src.substrait_producer.isthmus_kit.stackoverflow_schema import *
from src.db_context import DBContext
from google.protobuf.json_format import Parse
from substrait.gen.proto.plan_pb2 import Plan
from typing import Optional
from src.errors import ConsumptionError
import logging
import re

logger = logging.getLogger()


class DataFusionEngine(ExecutionEngine):

    def __init__(self):
        self.ctx = SessionContext()
        self.table_mapping = {}
        self.tpch_schema_mapping = {
            'lineitem': lineitem_schema,
            'customer': customer_schema,
            'nation': nation_schema,
            'orders': orders_schema,
            'part': part_schema,
            'partsupp': partsupp_schema,
            'region': region_schema,
            'supplier': supplier_schema
        }
        self.tpcds_schema_mapping = {
            'call_center': call_center_schema_ds,
            'catalog_page': catalog_page_schema_ds,
            'catalog_returns': catalog_returns_schema_ds,
            'catalog_sales': catalog_sales_schema_ds,
            'customer': customer_schema_ds,
            'customer_address': customer_address_schema_ds,
            'customer_demographics': customer_demographics_schema_ds,
            'date_dim': date_dim_schema_ds,
            'household_demographics': household_demographics_schema_ds,
            'income_band': income_band_schema_ds,
            'inventory': inventory_schema_ds,
            'item': item_schema_ds,
            'promotion': promotion_schema_ds,
            'reason': reason_schema_ds,
            'ship_mode': ship_mode_schema_ds,
            'store': store_schema_ds,
            'store_returns': store_returns_schema_ds,
            'store_sales': store_sales_schema_ds,
            'time_dim': time_dim_schema_ds,
            'warehouse': warehouse_schema_ds,
            'web_page': web_page_schema_ds,
            'web_returns': web_returns_schema_ds,
            'web_sales': web_sales_schema_ds,
            'web_site': web_site_schema_ds,
        }
        self.imdb_schema_mapping = {
            'aka_name': aka_name_schema,
            'aka_title': aka_title_schema,
            'cast_info': cast_info_schema,
            'char_name': char_name_schema,
            'comp_cast_type': comp_cast_type_schema,
            'company_name': company_name_schema,
            'company_type': company_type_schema,
            'complete_cast': complete_cast_schema,
            'info_type': info_type_schema,
            'keyword': keyword_schema,
            'kind_type': kind_type_schema,
            'link_type': link_type_schema,
            'movie_companies': movie_companies_schema,
            'movie_info': movie_info_schema,
            'movie_info_idx': movie_info_idx_schema,
            'movie_keyword': movie_keyword_schema,
            'movie_link': movie_link_schema,
            'name': name_schema,
            'person_info': person_info_schema,
            'role_type': role_type_schema,
            'title': title_schema,
        }
        self.stackoverflow_schema_mapping = {
            'account': account_schema,
            'answer': answer_schema,
            'badge': badge_schema,
            'comment': comment_schema,
            'post_link': post_link_schema,
            'question': question_schema,
            'site': site_schema,
            'so_user': so_user_schema,
            'tag': tag_schema,
            'tag_question': tag_question_schema,
        }

    def register_table(self, table: str):
        self.table_mapping[table.split('.')[0].upper()] = table.split('.')[0]
        try:
            if isinstance(self.compodb.parser, DataFusionProducer):
                if table.endswith(".parquet"):
                    self.ctx.register_parquet(f"{table.split('.')[0]}", f"{DBContext.current_data_path}/{table}")
                else:
                    self.ctx.register_csv(f"{table.split('.')[0]}", f"{DBContext.current_data_path}/{table}")
            else:
                schema = None
                if DBContext.benchmark == 'tpch':
                    schema = self.tpch_schema_mapping.get(table.split('.')[0])
                elif DBContext.benchmark == 'tpcds':
                    schema = self.tpcds_schema_mapping.get(table.split('.')[0])
                elif DBContext.benchmark == 'j-o-b':
                    schema = self.imdb_schema_mapping.get(table.split('.')[0])
                elif DBContext.benchmark == 'stackoverflow':
                    schema = self.stackoverflow_schema_mapping.get(table.split('.')[0])
                else:
                    pass
                if table.endswith(".parquet"):
                    self.ctx.register_parquet(f"{table.split('.')[0]}", f"{DBContext.current_data_path}/{table}", schema=schema)
                else:
                    self.ctx.register_csv(f"{table.split('.')[0]}", f"{DBContext.current_data_path}/{table}", schema=schema)
        except Exception as e:
            if "already exists" in str(e):
                self.ctx.deregister_table(table.split('.')[0])
                if isinstance(self.compodb.parser, DataFusionProducer):
                    if table.endswith(".parquet"):
                        self.ctx.register_parquet(f"{table.split('.')[0]}", f"{DBContext.current_data_path}/{table}")
                    else:
                        self.ctx.register_csv(f"{table.split('.')[0]}", f"{DBContext.current_data_path}/{table}")
                else:
                    schema = None
                    if DBContext.benchmark == 'tpch':
                        schema = self.tpch_schema_mapping.get(table.split('.')[0])
                    elif DBContext.benchmark == 'tpcds':
                        schema = self.tpcds_schema_mapping.get(table.split('.')[0])
                    elif DBContext.benchmark == 'j-o-b':
                        schema = self.imdb_schema_mapping.get(table.split('.')[0])
                    elif DBContext.benchmark == 'stackoverflow':
                        schema = self.stackoverflow_schema_mapping.get(table.split('.')[0])
                    else:
                        pass
                    if table.endswith(".parquet"):
                        self.ctx.register_parquet(f"{table.split('.')[0]}", f"{DBContext.current_data_path}/{table}", schema=schema)
                    else:
                        self.ctx.register_csv(f"{table.split('.')[0]}", f"{DBContext.current_data_path}/{table}", schema=schema)



    def prep_isthmus_query(self, substrait_query: str) -> str:

        for upper_name, lower_name in self.table_mapping.items():
            substrait_query = re.sub(r'\b' + re.escape(upper_name) + r'\b', lower_name, substrait_query)

            column_df = self.ctx.sql(f"SHOW COLUMNS FROM {lower_name}").to_pandas()
            column_names = column_df["column_name"].tolist()

            for col in column_names:
                substrait_query = substrait_query.replace(col.upper(), col)

        return substrait_query


    def run_substrait(self, substrait_query) -> Optional[DataFrame]:

        if isinstance(self.compodb.parser, IsthmusProducer):
            substrait_query = self.prep_isthmus_query(substrait_query)

        try:
            substrait_json = json.loads(substrait_query)
            plan_proto = Parse(json.dumps(substrait_json), Plan())
            plan_bytes = plan_proto.SerializeToString()
            substrait_plan = ds.serde.deserialize_bytes(plan_bytes)
            logical_plan = ds.consumer.from_substrait_plan(
                self.ctx, substrait_plan
            )
            result = self.ctx.create_dataframe_from_logical_plan(logical_plan)
            result = result.collect()
            return result

        except Exception as e:
            raise ConsumptionError(repr(e))


    def test_sql(self, sql_query, q, sf):

        times = []
        try:
            for i in range(4):
                stCPU = time.process_time()

                query_result = self.ctx.sql(sql_query)

                etCPU = time.process_time()
                resCPU = (etCPU - stCPU) * 1000
                if (i == 1) | (i == 2) | (i == 3):
                    times.append(resCPU)
            timeAVG = (times[0] + times[1] + times[2]) / 3
            # print(query_result)
            res_obj = TestResult('TPC-H', 'SQL', 'DataFusion', 'Parquet', '-- SQL', sf, q.split('.')[0], query_result,
                                 times, timeAVG)
            print(f"TEST DataFusion\t\tSUCCESS")

            return res_obj

        except Exception as e:
            print(f"TEST DataFusion\t\tEXCEPTION: SQL not working: {repr(e)[:100]}")
            return None


    def get_name(self) -> str:
        return "DataFusion"