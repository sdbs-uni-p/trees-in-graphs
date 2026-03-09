# SPDX-License-Identifier: GPL-3.0-only

import copy
import json
from pathlib import Path
from typing import Tuple, List, Type

import pandas as pd
from tqdm import tqdm

from .ExecutorDefinitions import Executor, ApacheExecutor
from .CreateParametrizedQueries import Parametrizer


def get_all_graph_names(flag : str, metadata_path : Path) -> List[str]:
    """
    Extract graph names from metadata JSON files in the given directory.

    Args:
        flag: String to filter files (e.g., "_dewey")
        metadata_path: Path to directory containing graph metadata JSON files

    Returns:
        List of graph names extracted from matching filenames
    """
    graph_names = []
    metadata_path = Path(metadata_path)

    if not metadata_path.exists():
        print(f"Warning: Metadata path does not exist: {metadata_path}")
        return graph_names

    for filepath in metadata_path.iterdir():
        filename = filepath.name

        if flag not in filename:
            continue

        if filename.endswith(".json"):
            graph_names.append(filename[:-len(".json")])

    return graph_names


"""def load_queries(
        query_file: str | Path,
        query_base_path : Path
):
    query_path = query_base_path / Path(query_file)
    query_df = pd.read_csv(query_path, sep="|")
    return query_df"""

def load_queries_from_sql(
        query_base_path: Path
):
    baseline_path = query_base_path / Path("baseline")
    dewey_path = query_base_path / Path("dewey")
    prepost_path = query_base_path / Path("prepost")

    query_data = []

    for base_file in baseline_path.iterdir():
        if not base_file.name.endswith('.sql'):
            continue
        entry_dict = {
            "Description" : base_file.parts[-1][3:-4],
        }
        dewey_file = dewey_path / Path(base_file.name)
        prepost_file = prepost_path / Path(base_file.name)

        with base_file.open() as f1, dewey_file.open() as f2, prepost_file.open() as f3:
            entry_dict["baseline"] = f1.read()
            entry_dict["dewey"] = f2.read()
            entry_dict["prepost"] = f3.read()
        query_data.append(entry_dict)


    # print(query_data)
    return pd.DataFrame(query_data)


class Assessor:
    def __init__(self,
                 graph_name: str,
                 plain_executor: Executor,
                 dewey_executor: Executor,
                 prepost_executor: Executor,
                 save_logs: Path,
                 dewey_metadata_path: Path,
                 parametrizer_cls: Type[Parametrizer] = Parametrizer,
                 db_name: str = None,
                 metadata_name: str = None,
                 ):

        self.plain_executor = plain_executor
        self.dewey_executor = dewey_executor
        self.prepost_executor = prepost_executor

        self.graph_name = graph_name
        self.db_name = db_name if db_name is not None else graph_name
        meta_base = metadata_name if metadata_name is not None else graph_name

        self.save_logs = save_logs / Path(f"{graph_name}.json")

        self.parameter_generator = parametrizer_cls(
            base_meta_path=dewey_metadata_path,
            ex=dewey_executor,
            db_name=f"{self.db_name}_dewey",
        )
        self.parameter_generator.set_metadata(f"{meta_base}_dewey")

    def parametrize_query(self, query : str, param_dict : dict, gname_addendum : str):
        q_copy = copy.deepcopy(query)
        for k, v in param_dict.items():
            q_copy = q_copy.replace(k, str(v))
        q_copy = q_copy.replace("$GRAPHNAME", f"{self.db_name}_{gname_addendum}")
        return q_copy

    def run_query_n(self,
                    vanilla_p : Tuple[str, str],
                    dewey_p : Tuple[str, str],
                    prepost_p: Tuple[str, str],
                    heat=5, n=200
                    ):
        vanilla_dict = {
            "time" : [],
            "plans" : [],
            "estimated_costs" : [],
            "results": [],
            "queries": [],
        }

        dewey_dict = {
            "time": [],
            "plans": [],
            "estimated_costs" : [],
            "results": [],
            "queries": [],
        }

        prepost_dict = {
            "time": [],
            "plans": [],
            "estimated_costs": [],
            "results": [],
            "queries": [],
        }

        heat_params = self.parameter_generator.sample_n(heat)
        run_params = self.parameter_generator.sample_n(n)

        vanilla_name, vanilla_query = vanilla_p
        dewey_name, dewey_query = dewey_p
        prepost_name, prepost_query = prepost_p

        for heat_value in heat_params:
            _, _ = self.plain_executor.execute_query(
                self.parametrize_query(vanilla_query, heat_value, "plain")
            )
            _, _ = self.dewey_executor.execute_query(
                self.parametrize_query(dewey_query, heat_value, "dewey")
            )
            _, _ = self.prepost_executor.execute_query(
                self.parametrize_query(prepost_query, heat_value, "prepost")
            )

        for run_value in run_params:
            vanilla_q = self.parametrize_query(vanilla_query, run_value, "plain")
            vanilla_time, vanilla_plan, vanilla_est_cost, vanilla_results = self.plain_executor.collect_query_plan(vanilla_q)
            vanilla_dict['time'].append(vanilla_time)
            vanilla_dict['plans'].append(vanilla_plan)
            vanilla_dict['estimated_costs'].append(vanilla_est_cost)
            vanilla_dict['results'].append(vanilla_results)
            vanilla_dict['queries'].append(vanilla_q)

            self.dewey_executor.execute_command("SET enable_seqscan = off;")

            dewey_q = self.parametrize_query(dewey_query, run_value, "dewey")
            dewey_time, dewey_plan, dewey_est_cost, dewey_results = self.dewey_executor.collect_query_plan(dewey_q)
            dewey_dict['time'].append(dewey_time)
            dewey_dict['plans'].append(dewey_plan)
            dewey_dict['estimated_costs'].append(dewey_est_cost)
            dewey_dict['results'].append(dewey_results)
            dewey_dict['queries'].append(dewey_q)

            self.dewey_executor.execute_command("SET enable_seqscan = on;")

            prepost_q = self.parametrize_query(prepost_query, run_value, "prepost")
            prepost_time, prepost_plan, prepost_est_cost, prepost_results = self.prepost_executor.collect_query_plan(prepost_q)
            self.prepost_executor.execute_command("SET enable_seqscan = off;")
            prepost_dict['time'].append(prepost_time)
            prepost_dict['plans'].append(prepost_plan)
            prepost_dict['estimated_costs'].append(prepost_est_cost)
            prepost_dict['results'].append(prepost_results)
            prepost_dict['queries'].append(prepost_q)

            self.prepost_executor.execute_command("SET enable_seqscan = on;")

        return {
            vanilla_name : vanilla_dict,
            dewey_name : dewey_dict,
            prepost_name: prepost_dict,
            "run_info" : run_params,
        }


    def run_all_query_n(self, query_df : pd.DataFrame, heat=1, n=1):
        log_dict = {}
        for _, row in query_df.iterrows():
            description_p, vanilla_p, dewey_p, prepost_p = list(zip(query_df.columns, row.values))
            print(f"Processing {description_p[1]}")
            log_dict[description_p[1]] = self.run_query_n(vanilla_p, dewey_p, prepost_p, heat=heat, n=n)

        with open(self.save_logs, "w", encoding="utf-8") as f:
            json.dump(log_dict, f, indent=2, ensure_ascii=False)

        return log_dict

# Maps virtual discovery names â†’ actual database base names.
# Allows multiple parametrized runs on the same physical database.
"""VIRTUAL_TO_DB_MAP = {
    "s_all_comment":  "sf1",
    "s_all_place":    "sf1",
    "s_all_tagclass": "sf1",
}"""

VIRTUAL_TO_DB_MAP = {
    "s_all_comment":  "s_all",
    "s_all_place":    "s_all",
    "s_all_tagclass": "s_all",
}

# Maps virtual discovery names â†’ existing metadata base names.
# Avoids duplicating large metadata files for graphs sharing the same tree structure.
VIRTUAL_TO_METADATA_MAP = {
    "s_all_comment":  "s1",
    "s_all_place":    "s2",
    "s_all_tagclass": "s3",
}


def assess_db(plain_ex : Executor,
              dewey_ex : Executor,
              prepost_ex : Executor,
              result_log_base : Path,
              query_path : Path,
              metadata_path : Path,
              heat=5, n=200,
              parametrizer_cls: Type[Parametrizer] = Parametrizer
              ):
    query_df = load_queries_from_sql(
        query_base_path=query_path
    )

    graph_names_s = get_all_graph_names(flag="_dewey", metadata_path=metadata_path)

    # Skip raw DB graphs that are only accessed via virtual names (e.g. s_all_dewey
    # is the actual database; experiments run against s_all_comment/place/tagclass instead)
    _virtual_dbs = set(VIRTUAL_TO_DB_MAP.values())
    graph_names_s = [g for g in graph_names_s if g[:-len("_dewey")] not in _virtual_dbs]

    # Add virtual graph names (e.g. s_all_comment, s_all_place, s_all_tagclass) which
    # are not present as metadata files but map to real databases via VIRTUAL_TO_DB_MAP
    graph_names_s += [f"{name}_dewey" for name in VIRTUAL_TO_DB_MAP.keys()]

    if type(plain_ex) == ApacheExecutor:
        graph_names_s = [i for i in graph_names_s if "100000_" not in i]

    for graph_name in tqdm(graph_names_s, desc="Processing string graphs"):
        print(f"Processing {graph_name}")
        base_name = graph_name[:-len("_dewey")]
        actual_db = VIRTUAL_TO_DB_MAP.get(base_name, base_name)
        metadata_base = VIRTUAL_TO_METADATA_MAP.get(base_name, base_name)
        plain_ex.set_graph(f"{actual_db}_plain")
        dewey_ex.set_graph(f"{actual_db}_dewey")
        prepost_ex.set_graph(f"{actual_db}_prepost")
        ass = Assessor(
            graph_name=base_name,
            db_name=actual_db,
            metadata_name=metadata_base,
            plain_executor=plain_ex,
            dewey_executor=dewey_ex,
            prepost_executor=prepost_ex,
            save_logs=result_log_base,
            dewey_metadata_path=metadata_path,
            parametrizer_cls=parametrizer_cls,
        )

        ass.run_all_query_n(
            query_df=query_df,
            heat=heat,
            n=n
        )
