#! /usr/bin/env elixir

defmodule Json do
  def encode(value) do
    :json.encode(value) |> to_string()
  end

  def decode(json_string) do
    :json.decode(json_string)
  end
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
    Json.decode(line)
  end

  def write(message) do
    IO.puts(Json.encode(message))
  end
end

defmodule Message do
  def type(m) do
    m["body"]["type"]
  end

  def body(m) do
    m["body"]
  end

  def msg_id(m) do
    m["body"]["msg_id"]
  end

  def src(m) do
    m["src"]
  end
end

defmodule Broadcast do
  def handle_read(state, message) do
    body = %{
      type: "read_ok",
      msg_id: Counter.next_msg_id(),
      in_reply_to: Message.msg_id(message),
      messages: state.messages
    }

    m = %{src: state.id, dest: Message.src(message), body: body}

    {state, [m]}
  end

  def handle_topology(state, message) do
    topology_map = message["body"]["topology"]
    neighbours = Map.get(topology_map, state.id)

    reply =
      %{
        src: state.id,
        dest: Message.src(message),
        body: %{
          type: "topology_ok",
          msg_id: Counter.next_msg_id(),
          in_reply_to: message["body"]["msg_id"]
        }
      }

    state = %{state | neighbours: neighbours}

    {state, [reply]}
  end

  def handle_broadcast(state, message) do
    # If we have already seen this message
    # then we can ignore it.
    # Otherwise we should add to list.
    #
    # If there is a msg_id on the body
    # - add the message to our state
    # - gossip to our neighbours
    # Otherwise if there is no msg_id
    # - just save the message and respond broadcast_ok
    msg_value = message["body"]["message"]

    if Enum.member?(state.messages, msg_value) do
      {state, []}
    else
      # Nullable message given that we don't always
      # need to send one back.
      broadcast_ok_message =
        if is_nil(Message.msg_id(message)) do
          nil
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

      # First filter out neighbour that send you message
      # since they've (obviously) already got it.
      gossip_messages =
        Enum.reject(state.neighbours, fn n -> n == Message.src(message) end)
        |> Enum.map(fn n ->
          %{
            src: state.id,
            dest: n,
            body: %{
              type: "broadcast",
              # We shouldn't need to have a msg_id
              # on gossip broadcast messages.
              msg_id: nil,
              message: msg_value
            }
          }
        end)

      updated_state = %{state | messages: Enum.concat(state.messages, [msg_value])}

      out_messages =
        if is_nil(broadcast_ok_message),
          do: gossip_messages,
          else: [broadcast_ok_message | gossip_messages]

      {updated_state, out_messages}
    end
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
          # No action necessary.
          {state, []}

        _ ->
          raise "Unexpected message type: #{type}"
      end

    # TODO Actually send all responses and 'loop'.
    Enum.each(out_messages, fn m -> Stdio.write(m) end)
    broadcast_loop(state_updated)
  end

  def main do
    # Init any state we need.
    Counter.start_link(1)

    # First get the init call.
    line = IO.gets("")

    init = Json.decode(line)

    node_id = init["body"]["node_id"]

    init_resp = %{
      src: node_id,
      dest: init["src"],
      body: %{type: "init_ok", in_reply_to: init["body"]["msg_id"]}
    }

    IO.puts(Json.encode(init_resp))

    broadcast_loop(%{id: node_id, neighbours: [], messages: []})
  end
end

Broadcast.main()
