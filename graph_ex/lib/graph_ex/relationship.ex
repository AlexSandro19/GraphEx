defmodule GraphEx.Relationship do

  alias GraphEx.Utils

  defstruct [:id, :src_node, :dest_node, :type, properties: %{}]

  def new(map) do
    struct(__MODULE__, map)
  end

  def add_relationship(graph, relationship) do
    inner_query = "MATCH (n#{Utils.labels_to_string(relationship.src_node.labels)} #{Utils.properties_to_string(relationship.src_node.properties)}),
                         (m#{Utils.labels_to_string(relationship.dest_node.labels)} #{Utils.properties_to_string(relationship.dest_node.properties)})
                   CREATE (n)-[r#{Utils.type_to_string(relationship.type)} #{Utils.properties_to_string(relationship.properties)}]->(m)
                   RETURN r"
    query = ["GRAPH.QUERY", graph.name, inner_query]
    {:ok, res} = Redix.command(graph.connection, query) # decide what to do with res
    {:ok, relationship_array } = extract_data(res)
    {:ok, List.first(relationship_array)}
    # {:ok, relationship}
  end

  def get_relationships(graph, src_node \\ %GraphEx.Node{}, type \\ "", properties \\ %{}, dest_node \\ %GraphEx.Node{})  do # %Node{} -- doesn't work for some reason;
    # need to think about the order of the parameters again, and how to better do it
    IO.inspect(dest_node)
    if is_pid(graph.connection) do
      src_node_inner_query = "(n#{Utils.labels_to_string(src_node.labels)} #{Utils.properties_to_string(src_node.properties)})"
      relationship_inner_query = "[r#{Utils.type_to_string(type)} #{Utils.properties_to_string(properties)}]"
      dest_node_inner_query = "(m#{Utils.labels_to_string(dest_node.labels)} #{Utils.properties_to_string(dest_node.properties)})"
      inner_query = "MATCH #{src_node_inner_query}-#{relationship_inner_query}->#{dest_node_inner_query} RETURN r"

      query = ["GRAPH.QUERY", graph.name, inner_query]
      {:ok, res} = Redix.command(graph.connection, query)
      extract_data(res)
    end
  end

  def update_relationship(graph, upd_relationship) do # can I let people change the type, src_node and dest_node? and how to enforce it otherwise (through const)?
    inner_query = "MATCH (n)-[r]->(m) WHERE id(r) = #{upd_relationship.id} SET r = #{Utils.properties_to_string(upd_relationship.properties)} RETURN r"
    query = ["GRAPH.QUERY", graph.name, inner_query]
    {:ok, res} = Redix.command(graph.connection, query)
    {:ok, relationship_array } = extract_data(res)
    {:ok, List.first(relationship_array)}
  end

  def delete_relationship(graph, relationship_to_delete) do
    inner_query = "MATCH (n)-[r]->(m) WHERE id(r) = #{relationship_to_delete.id} DELETE r"
    query = ["GRAPH.QUERY", graph.name, inner_query]
    {:ok, _res} = Redix.command(graph.connection, query)
    {:ok, relationship_to_delete}
  end

  def extract_data(response) do
    data = Enum.at(response, 1)
      if length(data) > 0 do
        nodes = Enum.map(data,fn node ->
              nd = List.first(node)
                    |> Enum.map(
                        fn res ->
                            cond do
                              Enum.at(res, 0) == "id" -> {:id, Enum.at(res, 1)}
                              Enum.at(res, 0) == "type" -> {:type, Enum.at(res, 1)}
                              Enum.at(res, 0) == "src_node" -> {:src_node, Enum.at(res, 1)}
                              Enum.at(res, 0) == "dest_node" -> {:dest_node, Enum.at(res, 1)}
                              Enum.at(res, 0) == "properties" ->
                                                  properties = Enum.at(res, 1) |> Enum.map(fn property -> {:"#{Enum.at(property, 0)}", Enum.at(property, 1)} end) |> Map.new
                                                  {:properties, properties}
                              true -> nil # If its neither of those two, I dont want to ignore
                            end
                        end)
              struct(__MODULE__, nd)
        end)
        {:ok, nodes}
      else
        {:ok, []}
      end
  end

end
