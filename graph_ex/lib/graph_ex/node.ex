defmodule GraphEx.Node do

  alias GraphEx.Utils

  defstruct [:id, labels: [], properties: %{}]

  @column_type %{
    COLUMN_UNKNOWN: 0,
    COLUMN_SCALAR: 1,
    COLUMN_NODE: 2,      # Unused, retained for client compatibility.
    COLUMN_RELATION: 3,  # Unused, retained for client compatibility.

  }

  @value_type %{
    VALUE_UNKNOWN: 0,
    VALUE_NULL: 1,
    VALUE_STRING: 2,
    VALUE_INTEGER: 3,
    VALUE_BOOLEAN: 4,
    VALUE_DOUBLE: 5,
    VALUE_ARRAY: 6,
    VALUE_EDGE: 7,
    VALUE_NODE: 8,
    VALUE_PATH: 9,
    VALUE_MAP: 10,
    VALUE_POINT: 11
  }

  def new(map) do
    struct(__MODULE__, map)
  end

  def save_node(connection, graph, node) do
    # Dont know if I should save the nodes locally or query the DB each time
    query = ["GRAPH.QUERY", graph.name, "CREATE (n#{Utils.labels_to_string(node.labels)} #{Utils.properties_to_string(node.properties)}) RETURN n"]
    {:ok, res} = Redix.command(connection, query)
    {:ok, node_array } = extract_data_test(res)
    {:ok, List.first(node_array)}
  end

  def get_nodes(connection, graph, node, labels \\ [], properties \\ %{}) do
    if is_pid(connection) do
      inner_query = "MATCH (n#{Utils.labels_to_string(labels)} #{Utils.properties_to_string(properties)}) RETURN n"
      query = ["GRAPH.QUERY", graph.name, inner_query]
      get_labels = ["GRAPH.QUERY", graph.name, "CALL db.labels()", "--compact"]
      get_all = ["GRAPH.QUERY", graph.name, "MATCH (n)-[r]->(m)  RETURN n, r", "--compact"]
      get_nodes = ["GRAPH.QUERY", graph.name, "MATCH (n) RETURN n", "--compact"]
      # get_path = ["GRAPH.QUERY", graph.name, "MATCH p=(n#{Utils.labels_to_string(node.labels)} #{Utils.properties_to_string(node.properties)})-[*]->(m)  RETURN p", "--compact"]
      {:ok, res} = Redix.command(connection, get_nodes)
      IO.puts("in get_nodes")
      IO.inspect(res)
      # extract_data_test(res)
      extract_response(res, connection, graph)
    end
  end

  def update_node(connection, graph, updated_node) do # how to make sure that the id is not changed
    # update the labels as well (or maybe not)
    inner_query = "MATCH (n) WHERE id(n) = #{updated_node.id} SET n = #{Utils.properties_to_string(updated_node.properties)} RETURN n"
    query = ["GRAPH.QUERY", graph.name, inner_query]
    {:ok, res} = Redix.command(connection, query)
    {:ok, node_array } = extract_data_test(res)
    {:ok, List.first(node_array)}
  end

  def delete_node(connection, graph, node_to_delete) do
    # inner_query = "MATCH (n#{Utils.labels_to_string(node_to_delete.labels)} #{Utils.properties_to_string(node_to_delete.properties)}) DELETE n"
    # inner_query
    inner_query = "MATCH (n) WHERE id(n) = #{node_to_delete.id} DETACH DELETE n"

    query = ["GRAPH.QUERY", graph.name, inner_query]
    Redix.command(connection, query)

  end

  def extract_response(response, connection, graph) do
    res = case length(response) do
      1 -> extract_statistics(response)
      3 -> extract_data(response, connection, graph)
    end
  end

  def extract_statistics(response)do
    # TODO
    response
  end

  def extract_data(response, connection, graph) do
    # IO.puts("in extract_data")
    # IO.inspect(response)
    header = List.first(response)
    results = Enum.at(response, 1)
    # IO.puts("in extract_data-> header")
    # IO.inspect(header)
    # IO.puts("in extract_data -> results")
    # IO.inspect(results)
    results = extract_records(header, results, connection, graph)

  end

  def extract_records(header, results, connection, graph) do
    # IO.puts("in extract_records")
    # IO.inspect(header)
    # IO.inspect(results)
    for [header_column_type, _header_element] <- header, [ result_set | _ ] <- results do
      res = cond do
        header_column_type == @column_type[:COLUMN_SCALAR] -> extract_scalar_record(result_set, connection, graph)
        # header_column_type == @column_type[:COLUMN_SCALAR] -> result_set
        header_column_type == @column_type[:COLUMN_NODE] -> extract_node_record(result_set)
        header_column_type == @column_type[:COLUMN_RELATION] -> extract_relation_record(result_set)
        true -> {:error, "unknown header column type"}
      end
    end
  end

  def extract_scalar_record(result_set, connection, graph) do
    IO.puts("in extract_scalar_record > result_set length")
    IO.inspect(length(result_set))
    [value_type | [ value | _ ]] = result_set
    # IO.puts("in extract_scalar_record > result")
    # IO.puts("in extract_scalar_record > result > value_type")
    # IO.inspect(value_type)
    # IO.puts("in extract_scalar_record > result > value")
    # IO.inspect(value)
    value = cond do
      # value_type == @value_type[:VALUE_NULL] -> extract_null_value(value)
      # value_type == @value_type[:VALUE_STRING] -> extract_string_value(value)
      # value_type == @value_type[:VALUE_INTEGER] -> extract_integer_value(value)
      # value_type == @value_type[:VALUE_DOUBLE] -> extract_double_value(value)
      # value_type == @value_type[:VALUE_ARRAY] -> extract_array_value(value)
      # value_type == @value_type[:VALUE_EDGE] -> extract_edge_value(value)
      value_type == @value_type[:VALUE_NODE] -> extract_node_value(value, connection, graph)
      # value_type == @value_type[:VALUE_PATH] -> extract_path_value(value)
      # value_type == @value_type[:VALUE_MAP] -> extract_map_value(value)
      # value_type == @value_type[:VALUE_POINT] -> extract_point_value(value)
      true -> {:error, "unknown value type"}
    end
      IO.puts("in extract_scalar_record > value")
      IO.inspect(value)

    # IO.puts("in extract_scalar_record")
    # IO.inspect(res)
  end

  def extract_node_value(value, connection, graph) do
    [ id | [ labels | [ properties ]]] = value
    # IO.puts("extract_node_value")
    # IO.puts("extract_node_value > id")
    # IO.inspect(id)
    # IO.puts("extract_node_value > labels")
    # IO.inspect(labels)
    # IO.puts("extract_node_value > properties")
    # IO.inspect(properties)
    all_labels = GraphEx.Graph.get_labels(connection, graph)
    # IO.puts("extract_node_value > labels")
    # IO.inspect(labels)
    all_propertyKeys = GraphEx.Graph.get_propertyKeys(connection, graph)
    # IO.puts("extract_node_value > propertyKeys")
    # IO.inspect(propertyKeys)
    labels = Enum.map(labels, fn label_id -> Map.get(all_labels, label_id) end)
    # IO.puts("extract_node_value > new labels")
    # IO.inspect(labels)
    properties = Enum.map(properties, fn [property_id | [_valueType | [value]]] -> {:"#{Map.get(all_propertyKeys, property_id)}", value} end) |> Map.new
    # IO.inspect(properties)
    node = %{id: id, labels: labels, properties: properties}
    new(node)
    # IO.puts("extract_node_value > new node")
    # IO.inspect(node)
    # node = GraphEx.Node.new()
  end



  def extract_node_record(result) do
    # IO.puts("extract_node_record")
  end

  def extract_relation_record(result) do
    # IO.puts("extract_relation_record")
  end

  def extract_data_test(response) do
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
