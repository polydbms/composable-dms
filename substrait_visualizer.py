import json
from graphviz import Digraph
import argparse

def parse_operator_dag(node, graph, parent=None):
    """Recursively parse the query tree and add nodes/edges to the graph."""
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

        # Add the join node and process its left/right inputs
        node_id = str(id(node))
        graph.node(node_id, operator)

        if parent:
            graph.edge(parent, node_id)

        left_input = node[node_type].get("left")
        right_input = node[node_type].get("right")

        if left_input:
            parse_operator_dag(left_input, graph, node_id)
        if right_input:
            parse_operator_dag(right_input, graph, node_id)
        return  # No further input field to process for Join
    elif "filter" in node:
        operator = "Filter"
        input_node = node["filter"].get("input")
    elif "read" in node:
        table_name = node["read"].get("namedTable", {}).get("names", ["Unknown"])[0]
        operator = f"Read ({table_name})"
        input_node = None  # No further input for Read
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
            parse_operator_dag(input_node, graph, node_id)


def load_json_plan(file_path):
    """Load the JSON plan from a file."""
    with open(file_path, "r") as file:
        return json.load(file)


def visualize_query_tree_dag(json_plan, output_file="query_tree.png"):
    """Visualize the query tree as a DAG and save it to a PNG file."""
    graph = Digraph(format="png")
    graph.attr(rankdir="TB")  # Top-to-bottom layout

    # Parse the query tree starting from the root relation
    relations = json_plan.get("relations", [])
    if relations:
        root_relation = relations[0]
        parse_operator_dag(root_relation, graph)
    else:
        print("No relations found in the Substrait plan.")

    # Render the graph to a file
    graph.render(output_file, cleanup=True)
    print(f"Query tree DAG saved to {output_file}.png")


parser = argparse.ArgumentParser(description="Visualize Substrait query plan as a DAG.")
parser.add_argument("--i", type=str, required=True, help="Path to the JSON file containing the Substrait query plan.")
parser.add_argument("--o", type=str, default="query_tree", help="Output file name for the visualized DAG (without extension).")
args = parser.parse_args()

file_path = args.i
output_file = args.o

substrait_plan = load_json_plan(file_path)
visualize_query_tree_dag(substrait_plan, output_file)
