from graphviz import Digraph
import json
import uuid
from pathlib import Path
import os

class SubstraitVisualizer:

    @classmethod
    def parse_operator_dag(cls, node, graph, parent=None):

        if not node:  # Safeguard for empty nodes
            return

        operator = None
        input_node = None

        # Identify operator type and handle nested cases
        if "root" in node:
            operator = "Root"
            input_node = node["root"].get("input")
        elif "fetch" in node:
            operator = "Fetch (Limit: {})".format(node["fetch"].get("count", "Unknown"))
            input_node = node["fetch"].get("input")
        elif "sort" in node:
            operator = "Sort"
            input_node = node["sort"].get("input")
        elif "project" in node:
            operator = "Project"
            input_node = node["project"].get("input")
        elif "aggregate" in node:
            operator = "Aggregate"
            input_node = node["aggregate"].get("input")
        elif "join" in node or "cross" in node:
            if "join" in node:
                node_type = "join"
                join_type = node["join"].get("type", "Unknown")
            else:
                node_type = "cross"
                join_type = "Cross"

            operator = f"Join (Type: {join_type})"

            node_id = str(id(node))
            graph.node(node_id, operator)

            if parent:
                graph.edge(parent, node_id)

            left_input = node[node_type].get("left")
            right_input = node[node_type].get("right")

            if left_input:
                cls.parse_operator_dag(left_input, graph, node_id)
            if right_input:
                cls.parse_operator_dag(right_input, graph, node_id)
            return
        elif "filter" in node:
            operator = "Filter"
            input_node = node["filter"].get("input")
        elif "read" in node:
            table_name = node["read"].get("namedTable", {}).get("names", ["Unknown"])[0]
            if table_name == "Unknown":
                table_name = node["read"].get("localFiles", {}).get("items", {})[0].get("uriFile", ["Unknown"])
                if table_name != "Unknown":
                    table_name = table_name.split("/")[-1].split(".")[0]
            operator = f"Read ({table_name})"
            input_node = None
        else:
            operator = "Unknown"
            input_node = None

        if operator:
            # Create a unique node ID
            node_id = str(id(node))
            graph.node(node_id, operator)

            # Add an edge from the parent node, if applicable
            if parent:
                graph.edge(parent, node_id)

            # Recursively parse the next input
            if input_node:
                cls.parse_operator_dag(input_node, graph, node_id)


    @classmethod
    def generate_dag_image(cls, query_name, parser_name, json_plan):
        """Generate the query tree DAG and return the image path."""
        graph = Digraph(format="png")
        graph.attr(rankdir="TB")
        if json_plan is None: return
        try:
            json_plan = json.loads(json_plan)
        except Exception as e:
            print("JSON decoding failed:", e)
            json_plan = {}
        relations = json_plan.get("relations", [])
        if relations:
            root_relation = relations[0]
            cls.parse_operator_dag(root_relation, graph)
        else:
            raise ValueError(f"No relations found in the query plan for {query_name}.")
        output_filename = f"{query_name}_{parser_name}"
        output_path = f"static/{output_filename}"
        try:
            graph.render(output_path, cleanup=True)
        except Exception as e:
            print(f"Error rendering graph: {e}")
        return output_filename+".png"