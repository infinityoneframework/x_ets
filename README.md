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
    {:x_ets, "~> 0.1.0"}
  ]
end
```

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

