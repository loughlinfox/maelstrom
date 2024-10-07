#! /usr/bin/env elixir

defmodule Json do
  def encode(value) do
    :json.encode(value) |> to_string()
  end

  def decode(json_string) do
    :json.decode(json_string)
  end

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

  def start_link(init_value \\ 1) do
    Agent.start_link(fn -> init_value end, name: __MODULE__)
  end

  def next_msg_id() do
    id = Agent.get_and_update(__MODULE__, fn count -> {count, count + 1} end)
    id
  end
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
  @doc """
  Helper to start a gaggle of gossips.
  Returns map of {msg_id => Gossip pid}.
  """
  def start_all(broadcast_messages) do
    Enum.map(broadcast_messages, fn m ->
      id = Message.msg_id(m)
      pid = Process.spawn(fn -> Gossip.start(m) end, [])
      {id, pid}
    end)
    |> Enum.into(%{})
  end

  @doc """
  Entry point Gossip process.
  Will keep retransmitting the message until we get a `:stop` signal
  from parent (that should be taking care of reading messages/requests from stdin).

  Note that `start` still have to actually
  1) send the message for first time; and
  2) schedule the first retry / looping.
  """
  def start(message) do
    Stdio.write(message)
    Process.send_after(self(), :retry, 1_000)
    loop(message, 1)
  end

  def loop(message, try_count) do
    receive do
      :retry ->
        Stdio.write(message)
        Process.send_after(self(), :retry, 1_000)
        loop(message, try_count + 1)

      :stop ->
        id = Message.msg_id(message)
        Stdio.debug("Gossip stopping", id)
    end
  end
end

defmodule Broadcast do
  def handle_read(state, message) do
    resp = %{
      src: state.id,
      dest: Message.src(message),
      body: %{
        type: "read_ok",
        msg_id: Counter.next_msg_id(),
        in_reply_to: Message.msg_id(message),
        messages: state.messages
      }
    }

    {state, [resp]}
  end

  def handle_topology(state, message) do
    topology_map = message[:body][:topology]
    # Rember that our keys in the json map will have been converted to
    # atoms for pretty 💫 💅 reasons
    neighbours = Map.get(topology_map, String.to_atom(state.id))
    Stdio.debug("neighbours", neighbours)

    reply =
      %{
        src: state.id,
        dest: Message.src(message),
        body: %{
          type: "topology_ok",
          msg_id: Counter.next_msg_id(),
          in_reply_to: Message.msg_id(message)
        }
      }

    state = %{state | neighbours: neighbours}

    {state, [reply]}
  end

  @doc """
  When handling broadcast messages we want to keep in mind
  - Always ack with `broadcast_ok` since other nodes need to know
    they can stop retries in precence of failure.
  - We do not need to process/add/gossip `broadcast` values if
    we already have the message in our node's state.
  - `msg_id` nullability: don't think we need to worry about this being
    nil anymore since we will always attach one so that
    neighbours can ack our broadcasts & we can stop retrying.
  """
  def handle_broadcast(state, message) do
    msg_value = message[:body][:message]

    # TODO Think we can remove this check.
    broadcast_ok_message =
      if is_nil(Message.msg_id(message)) do
        raise "msg_id is nil"
      else
        %{
          src: state.id,
          dest: Message.src(message),
          body: %{
            type: "broadcast_ok",
            msg_id: Counter.next_msg_id(),
            in_reply_to: Message.msg_id(message)
          }
        }
      end

    if Enum.member?(state.messages, msg_value) do
      {state, [broadcast_ok_message]}
    else
      # First filter out neighbour that send you message
      # since they've (obviously) already got it.
      gossip_messages =
        state.neighbours
        |> Enum.reject(fn n -> n == Message.src(message) end)
        |> Enum.map(fn n ->
          %{
            src: state.id,
            dest: n,
            body: %{
              type: "broadcast",
              # Want to attach id now so that neighbours can ack.
              msg_id: Counter.next_msg_id(),
              message: msg_value
            }
          }
        end)

      updated_state = %{state | messages: Enum.concat(state.messages, [msg_value])}

      {updated_state, [broadcast_ok_message | gossip_messages]}
    end
  end

  def handle_broadcast_ok(state, message) do
    msg_id = Message.in_reply_to(message)
    gossip_pid = Map.get(state.gossips, msg_id)
    send(gossip_pid, :stop)
    # Could potentially remove the gossip pid from the state
    # but we don't really need to I think.
    {state, []}
  end

  def broadcast_loop(state) do
    message = Stdio.read()
    type = Message.type(message)

    {state_updated, out_messages} =
      case type do
        "broadcast" ->
          handle_broadcast(state, message)

        "read" ->
          handle_read(state, message)

        "topology" ->
          handle_topology(state, message)

        "broadcast_ok" ->
          handle_broadcast_ok(state, message)

        _ ->
          raise "Unexpected message type: #{type}"
      end

    {broadcast_messages, responses} = Enum.split_with(out_messages, &Message.is_broadcast?/1)
    Enum.each(responses, fn m -> Stdio.write(m) end)
    new_gossips = Gossip.start_all(broadcast_messages)
    state_updated = %{state_updated | gossips: Map.merge(state_updated.gossips, new_gossips)}

    broadcast_loop(state_updated)
  end

  def main do
    # Init any state we need.
    Counter.start_link(1)

    # First get the init call.
    init = Stdio.read()

    node_id = init[:body][:node_id]

    init_resp = %{
      src: node_id,
      dest: init[:src],
      body: %{type: "init_ok", in_reply_to: init[:body][:msg_id]}
    }

    Stdio.write(init_resp)

    broadcast_loop(%{
      id: node_id,
      neighbours: [],
      messages: [],
      gossips: %{}
    })
  end
end

Broadcast.main()
