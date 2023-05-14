defmodule GraphEx.Graph do

  alias GraphEx.Utils

  defstruct [:name, nodes: %{}, edges: [], connection: ""]

  def new(map) do
    {:ok, conn} = Redix.start_link()
    map = Map.put(map, :connection, conn)
    struct(__MODULE__, map)
  end

  def get_graph(graph_name) do
    {:ok, conn} = Redix.start_link()
    query = ["GRAPH.QUERY", graph_name, "MATCH (n) RETURN count(n)"]
    {:ok, res} = Redix.command(conn, query)
    case Enum.at(res, 1) |> Enum.at(0) |> Enum.at(0) do
      0 -> {:error, "no such graph found"}
      _ -> {:ok, struct(__MODULE__, %{name: graph_name, connection: conn})}
    end
  end

  # def get_graphs() -- check how to get all graphs in db (but not sure if its secure)

  def delete_graph(graph) do
    query = ["GRAPH.QUERY", graph.name, "MATCH (n) DETACH DELETE n"]
    {:ok, _res} = Redix.command(graph.connection, query)
    {:ok}
  end

end
