# XEts

A partitioned ETS implementation.

This is a wrapper around the `shards` library as well as a key-value store
implementation.

See `XEts.KV` for the key-value store implementation.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `x_ets` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:x_ets, "~> 0.2.0"}
  ]
end
```

## Development

The checked-in `.tool-versions` selects Elixir 1.20.4 and Erlang/OTP 29.1.2.

```sh
mix deps.get
mix compile --warnings-as-errors
mix test --warnings-as-errors
mix dialyzer
mix coveralls
mix docs --warnings-as-errors
```

Both Access implementations distinguish a stored `:error` from a missing key and
support callbacks returning `:pop` from `get_and_update/3`.

Shards 1.1.1 omits its supported `:parallel` option from its exported option type.
The existing Dialyzer exception is narrowed to `XEts.KV.new/1`, whose default options
enable parallel mode; a regression test verifies that default path. Project analysis
otherwise passes without ignore filters. Clean development/test dependency builds
still emit upstream MixTestWatch/ExCoveralls compiler warnings; dependency source files
are not patched locally.

## Usage

### XEts

```elixir
iex> tab = XEts.new(:table_name, [:named_table])
iex> tab |> XEts.insert(x: 1, y: %{}) |> put_in([:y, :z], 1) |>
...> update_in([:y, :z], & &1 + 1) |> XEts.to_list()
[x: 1, y: %{z: 2}]
```

### XEts.KV

```elixir
iex> tab = XEts.KV.new(:table_name, [])
iex> tab |> XEts.KV.put(:x, 1) |> XEts.KV.put(:y, %{z: 1}) |> XEts.KV.get(:x)
1
iex> tab |> update_in([:y, :z], & &1 + 1) |> get_in([:y, :z])
2
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/x_ets>.

