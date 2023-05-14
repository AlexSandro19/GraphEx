defmodule GraphEx.Utils do

  def properties_to_string(properties) do
    props = cond do
      is_map(properties) and map_size(properties) > 0 -> Stream.map(properties, fn({k, x}) -> "#{k}: #{converted_value(x)}" end) |> Enum.join(", ")
      :true -> ""
    end
    IO.inspect(props)
    case String.length(props) do
      0 -> ""
      _ -> "{#{props}}"
    end
  end

  def labels_to_string(labels) do
    cond do
      is_list(labels) and length(labels) > 0 -> ":" <> Enum.join(labels, ":")
      :true -> ""
    end
  end

  def type_to_string(type) do # need to check if if can have multiple types
    cond do
      is_binary(type) and String.length(type) > 0 -> ":" <> type
      :true -> ""
    end
  end

  def converted_value(val) do
    case is_binary(val) do
      :true -> "'" <> val <> "'"
      :false -> val
    end
  end
end
