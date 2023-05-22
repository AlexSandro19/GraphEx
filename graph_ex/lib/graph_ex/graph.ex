defmodule GraphEx.Graph do

  alias GraphEx.Utils

  defstruct [:name, nodes: %{}, edges: [], labels: [], propertyKeys: [], relationshipTypes: []]

  def new(map) do
    struct(__MODULE__, map)
  end

  def get_graph(connection, graph_name) do
    query = ["GRAPH.QUERY", graph_name, "MATCH (n) RETURN count(n)"]
    {:ok, res} = Redix.command(connection, query)
    case Enum.at(res, 1) |> Enum.at(0) |> Enum.at(0) do
      0 -> {:error, "no such graph found"}
      _ -> {:ok, struct(__MODULE__, %{name: graph_name})}
    end
  end

  # def get_graphs() -- check how to get all graphs in db (but not sure if its secure)

  def delete_graph(connection, graph) do
    query = ["GRAPH.QUERY", graph.name, "MATCH (n) DETACH DELETE n"]
    {:ok, _res} = Redix.command(connection, query)
    {:ok}
  end

  def get_labels(connection, graph) do
    query = ["GRAPH.QUERY", graph.name, "CALL db.labels()"]
    {:ok, res} = Redix.command(connection, query)
    [_columns, labels, _statistics] = res
    labels = Enum.with_index(labels, fn [element | _], index -> {index, element} end)
    labels = Map.new(labels)
    labels
  end

  def get_propertyKeys(connection, graph) do
    query = ["GRAPH.QUERY", graph.name, "CALL db.propertyKeys()"]
    {:ok, res} = Redix.command(connection, query)
    [_columns, propertyKeys, _statistics] = res
    propertyKeys = Enum.with_index(propertyKeys, fn [element | _], index -> {index, element} end)
    propertyKeys = Map.new(propertyKeys)
    propertyKeys
  end

  def get_relationshipTypes(connection, graph) do
    query = ["GRAPH.QUERY", graph.name, "CALL db.relationshipTypes()"]
    {:ok, res} = Redix.command(connection, query)
    [_columns, relationshipTypes, _statistics] = res
    relationshipTypes = Enum.with_index(relationshipTypes, fn [element | _], index -> {index, element} end)
    relationshipTypes = Map.new(relationshipTypes)
    relationshipTypes
  end

  # "CALL db.propertyKeys()"
  # "CALL db.relationshipTypes()"
  # "CALL db.labels()"
end
# MATCH (n) RETURN n
