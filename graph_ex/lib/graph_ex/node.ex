defmodule GraphEx.Node do

  alias GraphEx.Utils

  defstruct [:id, labels: [], properties: %{}]

  def new(map) do
    struct(__MODULE__, map)
  end

  def save_node(graph, node) do
    # Dont know if I should save the nodes locally or query the DB each time
    query = ["GRAPH.QUERY", graph.name, "CREATE (n#{Utils.labels_to_string(node.labels)} #{Utils.properties_to_string(node.properties)}) RETURN n"]
    {:ok, res} = Redix.command(graph.connection, query)
    {:ok, node_array } = extract_data(res)
    {:ok, List.first(node_array)}
  end

  def get_nodes(graph, labels \\ [], properties \\ %{}) do
    if is_pid(graph.connection) do
      inner_query = "MATCH (n#{Utils.labels_to_string(labels)} #{Utils.properties_to_string(properties)}) RETURN n"
      query = ["GRAPH.QUERY", graph.name, inner_query]
      {:ok, res} = Redix.command(graph.connection, query)
      extract_data(res)
    end
  end

  def update_node(graph, updated_node) do # how to make sure that the id is not changed
    # update the labels as well (or maybe not)
    inner_query = "MATCH (n) WHERE id(n) = #{updated_node.id} SET n = #{Utils.properties_to_string(updated_node.properties)} RETURN n"
    query = ["GRAPH.QUERY", graph.name, inner_query]
    {:ok, res} = Redix.command(graph.connection, query)
    {:ok, node_array } = extract_data(res)
    {:ok, List.first(node_array)}
  end

  def delete_node(graph, node_to_delete) do
    inner_query = "MATCH (n) WHERE id(n) = #{node_to_delete.id} DETACH DELETE n"
    query = ["GRAPH.QUERY", graph.name, inner_query]
    {:ok, _res} = Redix.command(graph.connection, query)
    {:ok, node_to_delete}
  end

  def extract_data(response) do
    data = Enum.at(response, 1)
      if length(data) > 0 do
        nodes = Enum.map(data,fn node ->
              nd = List.first(node)
                    |> Enum.map(
                        fn res ->
                            cond do
                              # maybe its better to make it more generic? (just dump everything into the map and it would figure out which key-value to take or some other way); but still need to identify the "properties" key
                              Enum.at(res, 0) == "id" -> {:id, Enum.at(res, 1)}
                              Enum.at(res, 0) == "labels" -> {:labels, Enum.at(res, 1)}
                              Enum.at(res, 0) == "properties" ->
                                                  properties = Enum.at(res, 1) |> Enum.map(fn property -> {:"#{Enum.at(property, 0)}", Enum.at(property, 1)} end) |> Map.new
                                                  {:properties, properties}
                              true -> nil # If its neither of those two, I dont want to ignore
                            end
                          #   # [head | [head2 | tail2]] -> IO.inspect(head); IO.inspect(head2); IO.inspect(tail2)
                          #   # nd |> IO.inspect
                          # end)
                            # IO.inspect(Enum.at(res, 0))
                            # if (Enum.at(res, 0) == "labels") do
                            #   {:labels, Enum.at(res, 1)}
                            # end
                            # if (Enum.at(res, 0) == "properties") do
                            #   properties = Enum.at(res, 1) |> Enum.map(fn property -> {:"#{Enum.at(property, 0)}", Enum.at(property, 1)} end) |> Map.new
                            #   {:properties, properties}
                            # end
                          # [head | [head2 | tail2]] -> IO.inspect(head); IO.inspect(head2); IO.inspect(tail2)
                          # nd |> IO.inspect
                        end)
              struct(__MODULE__, nd)
        end)
        {:ok, nodes}
      else
        {:ok, []}
      end
  end


end
