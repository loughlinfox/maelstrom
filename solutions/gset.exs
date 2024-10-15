#! /usr/bin/env elixir

defmodule Json do
  def encode(value), do: :json.encode(value) |> to_string()
  def decode(json_string), do: :json.decode(json_string)

  def atomify(json) when is_map(json) do
    Map.new(json, fn {k, v} ->
      key = if is_binary(k), do: String.to_atom(k), else: k
      value = atomify(v)
      {key, value}
    end)
  end

  def atomify(json) when is_list(json), do: Enum.map(json, &atomify/1)
  def atomify(json), do: json
end

defmodule Counter do
  @moduledoc """
  Simple counter Agent for keeping track of message IDs.
  """
  use Agent
  def start_link(init_value \\ 1), do: Agent.start_link(fn -> init_value end, name: __MODULE__)
  def next_msg_id(), do: Agent.get_and_update(__MODULE__, fn count -> {count, count + 1} end)
end

defmodule Stdio do
  def read do
    line = IO.gets("")
    json = Json.decode(line)
    # Convert all keys to atoms just for ease and prettyness.
    Json.atomify(json)
  end

  def write(message) do
    IO.puts(Json.encode(message))
  end

  def debug(debug_message) do
    IO.puts(:standard_error, "[#{debug_message}]")
  end

  def debug(debug_message, message) do
    IO.puts(:standard_error, "[#{debug_message}] #{inspect(message)}")
  end
end

defmodule Message do
  def type(m), do: m[:body][:type]
  def body(m), do: m[:body]
  def msg_id(m), do: m[:body][:msg_id]
  def src(m), do: m[:src]
  def in_reply_to(m), do: m[:body][:in_reply_to]
  def is_broadcast?(m), do: Message.type(m) == "broadcast"
end

defmodule Gossip do
  @period 500

  @doc """
  This process needs to broadcast `elements` to other nodes
  in the network.
  This means it needs to be able to
  - periodically send out a "broadcast" of the elements it knows about
  - receive an updated list of elements from other processes & update what it is broadcasting
  """
  def start(id, nodes) do
    Process.send_after(self(), :gossip, @period)
    loop(id, nodes, MapSet.new())
  end

  def loop(id, nodes, elements) do
    receive do
      {:elements, elements} ->
        # Essentially update our internal state.
        loop(id, nodes, elements)

      :gossip ->
        Stdio.debug("gossip recv :gossip", %{elements: elements})
        # Send to other nodes,
        # & schedule the next broadcast
        # & then loop
        gossip_elements(id, nodes, elements)
        Process.send_after(self(), :gossip, @period)
        loop(id, nodes, elements)

      _unexpected ->
        raise "Gossip process unexpected message type"
    end
  end

  defp gossip_elements(id, nodes, elements) do
    elements = MapSet.to_list(elements)

    Enum.map(nodes, fn node ->
      %{
        src: id,
        dest: node,
        body: %{
          type: "gossip",
          elements: elements
        }
      }
    end)
    |> Enum.each(fn msg ->
      Stdio.write(msg)
    end)
  end
end

defmodule Workload do
  def loop(state) do
    message = Stdio.read()
    # Stdio.debug("recv", message)
    type = Message.type(message)

    {state_updated, out_messages} =
      case type do
        "add" ->
          element = message[:body][:element]

          resp = %{
            src: state.id,
            dest: Message.src(message),
            body: %{
              type: "add_ok",
              # TODO Can optinally add a msg_id for response.
              in_reply_to: Message.msg_id(message)
            }
          }

          updated_elements = MapSet.put(state.elements, element)
          Process.send(state.gossip_pid, {:elements, updated_elements}, [])

          {
            %{state | elements: updated_elements},
            [resp]
          }

        "read" ->
          value = MapSet.to_list(state.elements)

          resp = %{
            src: state.id,
            dest: Message.src(message),
            body: %{
              type: "read_ok",
              value: value,
              # TODO Can optinally add a msg_id for response.
              in_reply_to: Message.msg_id(message)
            }
          }

          {state, [resp]}

        "gossip" ->
          elements = message[:body][:elements]
          updated_elements = MapSet.union(state.elements, MapSet.new(elements))
          Process.send(state.gossip_pid, {:elements, updated_elements}, [])

          {
            %{state | elements: updated_elements},
            []
          }

        _ ->
          raise "Unexpected message type: #{type}"
      end

    Enum.each(out_messages, fn m -> Stdio.debug("send", m) end)
    Enum.each(out_messages, &Stdio.write/1)

    loop(state_updated)
  end

  def start do
    # Init any state we need.
    Counter.start_link(1)

    # First get the init call.
    init = Stdio.read()

    node_id = init[:body][:node_id]
    node_ids = init[:body][:node_ids]

    init_resp = %{
      src: node_id,
      dest: init[:src],
      body: %{type: "init_ok", in_reply_to: init[:body][:msg_id]}
    }

    Stdio.write(init_resp)

    gossip_pid = spawn(fn -> Gossip.start(node_id, node_ids) end)

    loop(%{
      id: node_id,
      node_ids: node_ids,
      elements: MapSet.new(),
      gossip_pid: gossip_pid
    })
  end
end

Workload.start()
