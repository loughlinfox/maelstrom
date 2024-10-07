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
  def start_link(init_value \\ 1), do: Agent.start_link(fn -> init_value end, name: __MODULE__)
  def next_msg_id(), do: Agent.get_and_update(__MODULE__, fn count -> {count, count + 1} end)
end

defmodule Echo do
  def echo_loop(node \\ %{}) do
    line = IO.gets("")
    # This message should be an echo
    # so we need to respond with an echo_ok.
    message = Json.decode(line)

    msg_id = Counter.next_msg_id()

    resp = %{
      src: node.id,
      dest: message["src"],
      body: %{
        type: "echo_ok",
        msg_id: msg_id,
        in_reply_to: message["body"]["msg_id"],
        echo: message["body"]["echo"]
      }
    }

    IO.puts(Json.encode(resp))
    echo_loop(node)
  end

  def main do
    # Init any state we need.
    Counter.start_link(1)

    # First get the init call.
    line = IO.gets("")
    IO.puts(:standard_error, "[recv] #{line}")

    init = Json.decode(line)

    node_id = init["body"]["node_id"]

    init_resp = %{
      src: node_id,
      dest: init["src"],
      body: %{type: "init_ok", in_reply_to: init["body"]["msg_id"]}
    }

    IO.puts(Json.encode(init_resp))

    echo_loop(%{id: node_id})
  end
end

Echo.main()
