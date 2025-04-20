defmodule XEts.KV do
  @moduledoc """
  A module for working with ETS tables.

  This module is a wrapper around the `:shards` module which is provides
  a high level of concurrency and scalability without the need for locks.

  Supports the Access protocol for getting and setting values.
  """

  @behaviour Access

  defstruct [:tab]

  @type t() :: %__MODULE__{tab: atom()}

  @default_opts [:named_table, parallel: true, read_concurrency: true, write_concurrency: true]

  @dialyzer {:no_return, new: 2, new: 1}

  @doc """
  Delete an item from the table.

      iex> XEts.KV.new(:foo, []) |> XEts.KV.put(:k, :v) |> XEts.KV.delete(:k) |> XEts.KV.to_list()
      []
  """
  @spec delete(t(), any()) :: t()
  def delete(%{tab: tab} = t, key) do
    :shards.delete(tab, key)
    t
  end

  @doc """
  Delete all items from the table.

      iex> XEts.KV.new(:foo, []) |> XEts.KV.put(:k, :v) |> XEts.KV.delete_all() |> XEts.KV.to_list()
      []
  """
  @spec delete_all(t()) :: t()
  def delete_all(%{tab: tab} = t) do
    :shards.delete_all_objects(tab)
    t
  end

  @doc """
  Get an item from the table.

      iex> XEts.KV.new(:foo, []) |> XEts.KV.put(:k, :v) |> XEts.KV.fetch(:k)
      {:ok, :v}

      iex> XEts.KV.new(:foo, []) |> XEts.KV.fetch(:k)
      :error
  """
  @impl true
  @spec fetch(t(), any()) :: {:ok, any()} | :error
  def fetch(tab, key) do
    case get(tab, key, :error) do
      :error -> :error
      value -> {:ok, value}
    end
  end

  @doc """
  Get an item from the table.

      iex> XEts.KV.new(:foo, []) |> XEts.KV.put(:k, :v) |> XEts.KV.get(:k)
      :v
  """
  @spec get(t(), any(), any()) :: any()
  def get(tab, value, default \\ nil)

  def get(%{tab: tab}, key, default) do
    try do
      :shards.lookup_element(tab, key, 2)
    rescue
      _ -> default
    end
  end

  @doc """
  Get and update an item in the table.

      iex> tab = XEts.KV.new(:foo, []) |> XEts.KV.put(:k, 2)
      iex> XEts.KV.get_and_update(tab, :k, & { &1, &1 + 1 })
      {2, tab}
  """
  @impl true
  @spec get_and_update(t(), any(), any()) :: any()
  def get_and_update(tab, key, fun) do
    {old, updated} =
      case fetch(tab, key) do
        {:ok, value} -> fun.(value)
        :error -> fun.(nil)
      end

    put(tab, key, updated)
    {old, tab}
  end

  @doc """
  Get information about the table.

      iex> XEts.KV.new(:foo, []) |> XEts.KV.info() |> length()
      18
  """
  @spec info(t()) :: list()
  def info(%{tab: tab}) do
    :shards.info(tab)
  end

  @doc """
  Get information about a key in the table.

      iex> XEts.KV.new(:foo, []) |> XEts.KV.info(:name)
      :foo
  """
  @spec info(t()) :: list()
  def info(%{tab: tab}, key) do
    :shards.info(tab, key)
  end

  @doc """
  Get an item from the table.

      iex> XEts.KV.new(:foo, []) |> XEts.KV.put(:k, :v) |> XEts.KV.get(:k)
      :v
  """
  @spec match(t(), any(), any(), any()) :: any()
  def match(tab, key, wildcard \\ :"$2", default \\ [])

  def match(%{tab: tab}, key, wildcard, default)
      when wildcard in [:_, :"$1", :"$2", :"$3", :"$4", :"$5", :"$6"] do
    case :shards.match(tab, {key, wildcard}) do
      [] -> default
      value -> Enum.map(value, &List.to_tuple/1)
    end
  end

  def match(tab, key, wildcard, _default) do
    match(tab, key, :"$2", wildcard)
  end

  @doc """
  Delete an item from the table.

      iex> XEts.KV.new(:foo, []) |> XEts.KV.put(:k, :v) |> XEts.KV.match_delete(:"$1") |> XEts.KV.to_list()
      []
  """
  @spec match_delete(t(), any(), any()) :: t()
  def match_delete(%{tab: tab} = t, key_match, value_match \\ :"$2") do
    :shards.match_delete(tab, {key_match, value_match})
    t
  end

  @doc """
  Test if key is a member.

      iex> :foo |> XEts.KV.new([]) |> XEts.KV.put(:x, nil) |> XEts.KV.member?(:x)
      true

      iex> :bar |> XEts.KV.new([]) |> XEts.KV.member?(:x)
      false
  """
  @spec member?(t(), any()) :: boolean()
  def member?(%{tab: tab}, key) do
    :shards.member(tab, key)
  end

  @doc """
  Pop an item from the table.

      iex> tab = XEts.KV.new(:foo, [])
      iex> tab |> XEts.KV.put(:k, :v) |> XEts.KV.pop(:k)
      {:v, tab}
  """
  @impl true
  @spec pop(t(), any(), any()) :: {any(), t()}
  def pop(tab, key, default \\ nil) do
    case fetch(tab, key) do
      {:ok, value} ->
        delete(tab, key)
        {value, tab}

      :error ->
        {default, tab}
    end
  end

  @doc """
  Put an item into the table.

      iex> XEts.KV.new(:foo, []) |> XEts.KV.put(:k, :v) |> XEts.KV.to_list()
      [{:k, :v}]
  """
  @spec put(t(), any(), any()) :: t()
  def put(%{tab: tab} = t, key, value) do
    :shards.insert(tab, {key, value})
    t
  end

  @doc """
  Put one or more items into the table.

      iex> XEts.KV.new(:foo, []) |> XEts.KV.put({:k, :v}) |> XEts.KV.to_list()
      [{:k, :v}]

      iex> XEts.KV.new(:foo, []) |> XEts.KV.put(k: :v, k2: :v2) |> XEts.KV.to_list()
      [{:k, :v}, {:k2, :v2}]
  """
  @spec put(t(), any()) :: t()
  def put(%{tab: tab} = t, item_or_items) do
    :shards.insert(tab, item_or_items)
    t
  end

  @doc """
  Put an item into the table if it doesn't exist.

      iex> XEts.KV.new(:foo, []) |> XEts.KV.put_new(:k, :v) |> XEts.KV.to_list()
      [{:k, :v}]
  """
  @spec put_new(t(), any()) :: t()
  def put_new(%{tab: _} = tab, key, value) do
    put_new(tab.tab, key, value)
    tab
  end

  def put_new(tab, key, value) do
    :shards.insert_new(tab, {key, value})
  end

  @doc """
  Put one or more new items into the table.

      iex> XEts.KV.new(:foo, []) |>
      ...> XEts.KV.put({:k, :v}) |>
      ...> XEts.KV.put_new(:k, :v2) |>
      ...> XEts.KV.to_list()
      [{:k, :v}]

      iex> XEts.KV.new(:foo, [])  |>
      ...> XEts.KV.put(k: :v, k2: :v2) |>
      ...> XEts.KV.put_new([k: :v3, k2: :vv]) |>
      ...>XEts.KV.to_list()
      [{:k, :v}, {:k2, :v2}]
  """
  @spec put_new(t(), any()) :: t()
  def put_new(%{tab: _} = tab, item_or_items) do
    put_new(tab.tab, item_or_items)
    tab
  end

  def put_new(tab, item_or_items) do
    :shards.insert_new(tab, item_or_items)
  end

  @spec new(atom(), list()) :: t() | none()
  def new(tab, opts \\ @default_opts) do
    %__MODULE__{tab: :shards.new(tab, opts)}
  end

  @doc """
  Convert the table to a list.

      iex> XEts.KV.new(:foo, []) |> XEts.KV.put(:k, :v) |> XEts.KV.to_list()
      [{:k, :v}]
  """
  @spec to_list(t()) :: list()
  def to_list(%{tab: tab}) do
    :shards.tab2list(tab)
  end
end
