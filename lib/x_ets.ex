defmodule XEts do
  @moduledoc """
  A partitioned ETS implementation.

  This is an Elixir wrapper around the `shards` library.

  The Access protocol is implemented. To support the Access protocol, XEts.new/2
  returns a struct with the `tab` field set to the table name.

  Many of the functions support a struct or a shards tab() as the first argument.
  Those that take a struct (and the Access protocol callbacks) are chainable as
  illustrated in the example below.

  :shard functions that return a value are not chainable, like lookup, select, etc.

  ## Examples

      iex> XEts.new(:test) |>
      ...> XEts.insert(x: %{}) |>
      ...> put_in([:x, :y], 1) |>
      ...> update_in([:x, :y], & &1 + 1) |>
      ...> XEts.to_list()
      [{:x, %{y: 2}}]
  """

  @behaviour Access

  defstruct [:tab, keypos: 1]

  @type t() :: %__MODULE__{tab: atom(), keypos: integer()}
  @type meta :: :shards_meta.t()
  @type tab :: atom() | :ets.tid()
  @type continuation() :: :shards.continuation()
  @type match_spec() :: :ets.match_spec()
  @type limit() :: pos_integer()
  @type limit_or_meta() :: limit() | meta()
  @type opt() :: {:heir, :none} | {:heir, pid(), term()}
  @type opts() :: opt() | [opt()]
  @type filename() :: charlist() | binary() | atom()
  @type tabinfo_item() :: :shards.tabinfo_item()
  @type options() :: [option()] | option()
  @type option() :: {:n_objects, n_objects()} | {:traverse, traversal_method()}
  @type n_objects() :: :default | pos_integer()
  @type traversal_method() :: :first_next | :last_prev | :select | {:select, match_spec()}
  @type element_spec() :: {pos_integer(), term()} | [{pos_integer(), term()}]
  @type info_tuple() :: :shards.info_tuple()
  @type info_item() :: :shards.info_item()

  @doc """
  Returns a list of all tables at the node.

  Equivalent to :ets.all/0.
  """
  @spec all :: [tab()]
  def all, do: :shards.all()

  @doc """
  Deletes the entire table.

  Equivalent to :ets.delete/1.
  """
  @spec delete(t()) :: true
  def delete(%{tab: tab}), do: delete(tab)
  def delete(tab), do: :shards.delete(tab)

  @doc """
  Delete an item from the table.

  ## Examples

      iex> XEts.new(:table) |> XEts.insert(x: 1, y: 2) |> XEts.delete(:x) |> XEts.to_list()
      [{:y, 2}]

      iex> %{tab: tab} = XEts.new(:table1) |> XEts.insert(x: 1, y: 2)
      iex> XEts.delete(tab, :y)
      iex> XEts.to_list(tab)
      [{:x, 1}]
  """
  @spec delete(t(), any()) :: tab()
  def delete(%{tab: _} = tab, key) do
    delete(tab.tab, key)
    tab
  end

  @spec delete(tab(), any()) :: true
  def delete(tab, key) do
    :shards.delete(tab, key)
  end

  @doc """
  Delete a key given metadata.
  """
  @spec delete(t(), any(), meta()) :: t()
  def delete(%{tab: _} = tab, key, meta) do
    delete(tab.tab, key, meta)
    tab
  end

  @spec delete(tab(), any(), meta()) :: true
  def delete(tab, key, meta) do
    :shards.delete(tab, key, meta)
    tab
  end

  @doc """
  Delete all objects from the table.

  ## Examples

      iex> XEts.new(:table) |> XEts.insert(x: 1, y: 2) |>  XEts.delete_all_objects() |> XEts.to_list()
      []

      iex> %{tab: tab} = XEts.new(:table1) |> XEts.insert(x: 1, y: 2)
      iex> XEts.delete_all_objects(tab)
      iex> XEts.to_list(tab)
      []
  """
  @spec delete_all_objects(t()) :: t()
  def delete_all_objects(%{tab: _} = tab) do
    delete_all_objects(tab.tab)
    tab
  end

  @spec delete_all_objects(tab()) :: true
  def delete_all_objects(tab) do
    :shards.delete_all_objects(tab)
  end

  @doc """
  Delete all objects from the table given metadata.
  """
  @spec delete_all_objects(t(), meta()) :: t()
  def delete_all_objects(%{tab: _} = tab, meta) do
    delete_all_objects(tab.tab, meta)
    tab
  end

  @spec delete_all_objects(tab(), meta()) :: true
  def delete_all_objects(tab, meta) do
    :shards.delete_all_objects(tab, meta)
  end

  @doc """
  Delete an object from the table.

  ## Examples

      iex> XEts.new(:table) |> XEts.insert(x: 1, y: 2) |> XEts.delete_object({:x, 1}) |> XEts.to_list()
      [{:y, 2}]

      iex> %{tab: tab} = XEts.new(:table1) |> XEts.insert(x: 1, y: 2)
      iex> XEts.delete_object(tab, {:y, 2})
      iex> XEts.to_list(tab)
      [{:x, 1}]
  """
  @spec delete_object(t(), any) :: t()
  def delete_object(%{tab: _} = tab, key) do
    delete_object(tab.tab, key)
    tab
  end

  @spec delete_object(tab(), any()) :: boolean()
  def delete_object(tab, key) do
    :shards.delete_object(tab, key)
  end

  @doc """
  Delete an object from the table given metadata.

  ## Examples

      iex> tab = XEts.new(:table)
      iex> XEts.insert(tab, x: 1, y: 2)
      iex> meta = :shards_meta.get(tab.tab)
      iex> tab |> XEts.delete_object({:x, 1}, meta) |> XEts.to_list()
      [{:y, 2}]
  """
  @spec delete_object(t(), any(), meta()) :: t()
  def delete_object(%{tab: _} = tab, key, meta) do
    delete_object(tab.tab, key, meta)
    tab
  end

  @spec delete_object(tab(), any(), meta()) :: boolean()
  def delete_object(tab, key, meta) do
    :shards.delete_object(tab, key, meta)
  end

  @spec file2tab(charlist() | binary()) :: {:ok, tab()} | {:error, any()}
  def file2tab(filename) when is_binary(filename), do: file2tab(to_charlist(filename))
  def file2tab(filename), do: :shards.file2tab(filename)

  @spec file2tab(charlist() | binary(), keyword()) :: {:ok, tab()} | {:error, any()}
  def file2tab(filename, options) when is_binary(filename),
    do: file2tab(to_charlist(filename), options)

  def file2tab(filename, options),
    do: :shards.file2tab(filename, options)

  @doc """
  Equivalent to first(tab, :shards_meta.get(tab)).
  """
  @spec first(t() | tab()) :: term() | :"$end_of_table"
  def first(%{tab: tab}), do: first(tab)
  def first(tab), do: :shards.first(tab)

  @doc """
  Equivalent to `:ets.first/1`.

  However, the order in which results are returned might be not the same as the
  original ETS function, since it is a sharded table.

  See also: :ets.first/1.
  """
  @spec first(t() | tab(), meta()) :: term() | :"$end_of_table"
  def first(%{tab: tab}, meta), do: first(tab, meta)
  def first(tab, meta), do: :shards.first(tab, meta)

  @doc """
  Similar to `:ets.info/1` but extra information about the partitioned table is added.

  ## Extra Info:

  * `{:partitions, pos_integer()}` - The number of partitions in the table.
  * `{:keyslot_fun, :shards_meta.keyslot_fun()}` - Functions used to compute the keyslot.
  * `{:parallel, boolean()}` - Whether the parallel mode is enabled or not.

  See also :ets.info/1.
  """
  @spec info(t() | tab()) :: [info_tuple()] | :undefined
  def info(%{tab: tab}), do: :shards.info(tab)
  def info(tab), do: :shards.info(tab)

  @doc """
  Equivalent to `:ets.info/2`.

  See the added items by `XEts.info/1`.

  See also: :ets.info/2.
  """
  @spec info(t() | tab(), info_item()) :: term()
  def info(%{tab: tab}, key), do: info(tab, key)
  def info(tab, key), do: :shards.info(tab, key)

  @doc """
  Fetch a value from the table.

      iex> XEts.new(:foo) |> XEts.put(:k, :v) |> XEts.fetch(:k)
      {:ok, :v}

      iex> XEts.new(:foo, []) |> XEts.fetch(:k)
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
  Fold left over the table.

  ## Examples

      iex> %{tab: tab} = XEts.new(:table) |>  XEts.insert(y: 1, x: 2)
      iex> XEts.foldl(tab, [], fn {k, v}, acc -> [{k, v * 2} | acc] end)
      [x:  4, y:  2]
  """
  @spec foldl(t(), any(), function()) :: t()
  def foldl(%{tab: tab}, acc, fun) when is_function(fun, 2) do
    foldl(tab, acc, fun)
  end

  @spec foldl(tab(), any(), function()) :: any()
  def foldl(tab, acc, fun) when is_function(fun, 2) do
    :shards.foldl(fun, acc, tab)
  end

  @doc """
  Fold left over the table given metadata.

  ## Examples

      iex> tab = XEts.new(:table) |>  XEts.insert(y: 1, x: 2)
      iex> XEts.foldl(tab, [], XEts.get_meta(tab), fn {k, v}, acc -> [{k, v * 2} | acc] end)
      [x:  4, y:  2]


      iex> %{tab: tab} = XEts.new(:table) |>  XEts.insert(y: 1, x: 2)
      iex> XEts.foldl(tab, [], XEts.get_meta(tab), fn {k, v}, acc -> [{k, v * 2} | acc] end)
      [x:  4, y:  2]
  """
  @spec foldl(t(), any(), meta(), function()) :: t()
  def foldl(%{tab: tab}, acc, meta, fun) when is_function(fun, 2) do
    foldl(tab, acc, meta, fun)
  end

  @spec foldl(tab(), any(), meta(), function()) :: t()
  def foldl(tab, acc, meta, fun) when is_function(fun, 2) do
    :shards.foldl(fun, acc, tab, meta)
  end

  @doc """
  Fold right over the table.

  ## Examples

      iex> tab = XEts.new(:table) |>  XEts.insert(x: 1, y: 2)
      iex> XEts.foldr(tab, [], fn {k, v}, acc -> [{k, v * 2} | acc] end)
      [x:  2, y:  4]
  """
  @spec foldr(t(), any(), function()) :: t()
  def foldr(%{tab: tab}, acc, fun) when is_function(fun, 2) do
    foldr(tab, acc, fun)
  end

  @spec foldr(tab(), any(), function()) :: t()
  def foldr(tab, acc, fun) when is_function(fun, 2) do
    :shards.foldr(fun, acc, tab)
  end

  @doc """
  Fold right over the table given metadata.

  ## Examples

      iex> tab = XEts.new(:table) |>  XEts.insert(x: 1, y: 2)
      iex> XEts.foldr(tab, [], XEts.get_meta(tab), fn {k, v}, acc -> [{k, v * 2} | acc] end)
      [x:  2, y:  4]

      iex> %{tab: tab} = XEts.new(:table) |>  XEts.insert(x: 1, y: 2)
      iex> XEts.foldr(tab, [], XEts.get_meta(tab), fn {k, v}, acc -> [{k, v * 2} | acc] end)
      [x:  2, y:  4]
  """
  @spec foldr(t(), any(), meta(), function()) :: t()
  def foldr(%{tab: _} = tab, acc, meta, fun) when is_function(fun, 2) do
    foldr(tab.tab, acc, meta, fun)
  end

  @spec foldr(tab(), any(), meta(), function()) :: t()
  def foldr(tab, acc, meta, fun) when is_function(fun, 2) do
    :shards.foldr(fun, acc, tab, meta)
  end

  @doc """
  Get a value from the table.

  ## Examples

      iex> tab = XEts.new(:table) |>  XEts.insert(x: 1, y: 2)
      iex> XEts.get(tab, :x)
      1
      iex> XEts.get(tab, :foo)
      nil
      iex> XEts.get(tab, :foo, :bar)
      :bar
  """
  @spec get(t(), any(), any()) :: any()
  def get(tab, value, default \\ nil)

  def get(%{tab: tab, keypos: keypos}, key, default) do
    try do
      lookup_element(tab, key, keypos + 1)
    rescue
      _ -> default
    end
  end

  @doc """
  Get and update an item in the table.

      iex> tab = XEts.new(:foo)|> XEts.put(:k, 2)
      iex> XEts.get_and_update(tab, :k, & { &1, &1 + 1 })
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
  Get the metadata of the table.

  ## Examples

      iex> tab = XEts.new(:table)
      iex> {:meta, pid, _, _, fun, false, :infinity, []} = XEts.get_meta(tab)
      iex> is_pid(pid) && is_function(fun, 2)
      true

      iex> %{tab: tab} = XEts.new(:table)
      iex> {:meta, pid, _, _, fun, false, :infinity, []} = XEts.get_meta(tab)
      iex> is_pid(pid) && is_function(fun, 2)
      true
  """
  @spec get_meta(t()) :: tuple()
  def get_meta(%{tab: tab}), do: get_meta(tab)

  @spec get_meta(tab()) :: tuple()
  def get_meta(tab), do: :shards_meta.get(tab)

  @spec i :: any()
  def i, do: :shards.i()

  @doc """
  Insert a new item into the table.

  ## Examples

      iex> tab = XEts.new(:table) |> XEts.insert(x: 1, y: 2)
      iex> XEts.insert_new(tab, {:x, 3}) |> XEts.to_list()
      [{:y, 2}, {:x, 1}]
  """
  @spec insert_new(t(), any()) :: t()
  def insert_new(%{tab: _} = tab, item) do
    insert_new(tab.tab, item)
    tab
  end

  @spec insert_new(tab(), any()) :: boolean()
  def insert_new(tab, item) do
    :shards.insert_new(tab, item)
    tab
  end

  @doc """
  Insert a new item into the table given metadata.

  ## Examples

      iex> tab = XEts.new(:table) |> XEts.insert(x: 1, y: 2)
      iex> XEts.insert_new(tab, {:x, 3}, XEts.get_meta(tab)) |> XEts.to_list()
      [{:y, 2}, {:x, 1}]
  """
  @spec insert_new(t(), any(), meta()) :: t()
  def insert_new(%{tab: _} = tab, item, meta) do
    insert_new(tab.tab, item, meta)
    tab
  end

  @spec insert_new(tab(), any(), meta()) :: boolean()
  def insert_new(tab, item, meta) do
    :shards.insert_new(tab, item, meta)
  end

  @doc """
  Check if an item is a compiled match spec.

  ## Examples

      iex> XEts.is_compiled_ms({:x, 1})
      false
  """
  @spec is_compiled_ms(any()) :: boolean()
  def is_compiled_ms(item), do: :shards.is_compiled_ms(item)

  @doc """
  Get the last item in the table.

  ## Examples

      iex> tab = XEts.new(:table) |> XEts.insert(x: 1, y: 2)
      iex> XEts.last(tab)
      :y
  """
  @spec last(t()) :: any()
  def last(%{tab: tab}), do: last(tab)

  @spec last(tab()) :: any()
  def last(tab), do: :shards.last(tab)

  @doc """
  Lookup an item in the table.

  ## Examples

      iex> tab = XEts.new(:table) |> XEts.insert(x: 1, y: 2)
      iex> XEts.lookup(tab, :x)
      [x: 1]

      iex> %{tab: tab} = XEts.new(:table) |> XEts.insert(x: 1, y: 2)
      iex> XEts.lookup(tab, :y)
      [y: 2]
  """
  @spec lookup(t(), any()) :: any()
  def lookup(%{tab: tab}, key), do: lookup(tab, key)

  @spec lookup(tab(), any()) :: any()
  def lookup(tab, key), do: :shards.lookup(tab, key)

  @doc """
  Lookup an item in the table given metadata.

  ## Examples

      iex> tab = XEts.new(:table) |> XEts.insert(x: 1, y: 2)
      iex> XEts.lookup(tab, :x, XEts.get_meta(tab))
      [x: 1]
  """
  @spec lookup(t(), any(), meta()) :: any()
  def lookup(%{tab: tab}, key, meta), do: lookup(tab, key, meta)

  @spec lookup(tab(), any(), meta()) :: any()
  def lookup(tab, key, meta), do: :shards.lookup(tab, key, meta)

  @doc """
  Insert an item into the table.

  ## Examples

      iex> XEts.new(:table) |> XEts.insert({:x, 5}) |> XEts.to_list()
      [{:x, 5}]

      iex> tab = XEts.new(:table) |> Map.get(:tab)
      iex> XEts.insert(tab, {:x, 5})
      iex> XEts.to_list(tab)
      [{:x, 5}]
  """
  @spec insert(t(), any()) :: t()
  def insert(%{tab: _} = tab, item) do
    insert(tab.tab, item)
    tab
  end

  @spec insert(tab(), any()) :: any()
  def insert(tab, item) do
    :shards.insert(tab, item)
  end

  @doc """
  Insert an item into the table given metadata.

  ## Examples

      iex> tab = XEts.new(:table)
      iex> tab |> XEts.insert({:x, 5}, XEts.get_meta(tab)) |> XEts.to_list()
      [{:x, 5}]
  """
  @spec insert(t(), any(), meta()) :: t()
  def insert(%{tab: _} = tab, key, meta) do
    insert(tab.tab, key, meta)
    tab
  end

  @spec insert(tab(), any(), meta()) :: any()
  def insert(tab, key, meta) do
    :shards.insert(tab, key, meta)
  end

  @doc """
  Lookup an element in the table.

  ## Examples

      iex> tab = XEts.new(:table) |> XEts.insert(x: 1, y: 2)
      iex> XEts.lookup_element(tab, :x, 1)
      :x
      iex> XEts.lookup_element(tab, :x, 2)
      1

      iex> %{tab: tab} = XEts.new(:table) |> XEts.insert(x: 1, y: 2)
      iex> XEts.lookup_element(tab, :y, 2)
      2
  """
  @spec lookup_element(t(), any(), integer()) :: any()
  def lookup_element(%{tab: tab}, key, pos), do: lookup_element(tab, key, pos)

  @spec lookup_element(tab(), any(), integer()) :: any()
  def lookup_element(tab, key, pos), do: :shards.lookup_element(tab, key, pos)

  @doc """
  Lookup an element in the table given metadata.

  ## Examples

      iex> tab = XEts.new(:table) |> XEts.insert(x: 1, y: 2)
      iex> XEts.lookup_element(tab, :x, 1, XEts.get_meta(tab))
      :x
      iex> XEts.lookup_element(tab, :x, 2, XEts.get_meta(tab))
      1
  """
  @spec lookup_element(t(), any(), integer(), meta()) :: any()
  def lookup_element(%{tab: tab}, key, pos, meta), do: :shards.lookup_element(tab, key, pos, meta)

  @spec lookup_element(tab(), any(), integer(), meta()) :: any()
  def lookup_element(tab, key, pos, meta), do: :shards.lookup_element(tab, key, pos, meta)

  @spec match(any()) :: any()
  def match(continuation), do: :shards.match(continuation)

  @doc """
  Match an item in the table.

  ## Examples

      iex> tab = XEts.new(:table) |> XEts.insert(x: 1, y: 2)
      iex> XEts.match(tab, :"$1")
      [[y: 2], [x: 1]]

      iex> %{tab: tab} = XEts.new(:table) |> XEts.insert({{:item, 1}, 2})
      iex> XEts.match(tab, {{:item, :"$1"}, :"$2"})
      [[1, 2]]
  """
  @spec match(t(), any()) :: any()
  def match(%{tab: tab}, pattern), do: match(tab, pattern)

  @spec match(tab(), any()) :: any()
  def match(tab, pattern), do: :shards.match(tab, pattern)

  @doc """
  Match an item in the table.

  ## Examples

      iex> tab = XEts.new(:table) |> XEts.insert(x: 1, y: 2)
      iex> XEts.match(tab, :"$1", XEts.get_meta(tab))
      [[y: 2], [x: 1]]

      iex> %{tab: tab} = XEts.new(:table1) |> XEts.insert(x: 1, y: 2, z: 3)
      iex> XEts.match(tab, {:"$1", :"$2"}, 2) |> elem(0)
      [[:y, 2], [:z, 3]]
  """
  @spec match(t(), any(), any()) :: any()
  def match(%{tab: tab}, pattern, limit_or_meta),
    do: :shards.match(tab, pattern, limit_or_meta)

  @spec match(tab(), any(), any()) :: any()
  def match(tab, pattern, limit_or_meta),
    do: :shards.match(tab, pattern, limit_or_meta)

  @doc """
  Match an item in the table given metadata and limit.

  ## Examples

      iex> tab = XEts.new(:table) |> XEts.insert(x: 1, y: 2)
      iex> XEts.match(tab, :"$1", 2, XEts.get_meta(tab)) |> elem(0)
      [[x: 1], [y: 2]]

      iex> %{tab: tab} = XEts.new(:table1) |> XEts.insert(x: 1, y: 2, z: 3)
      iex> XEts.match(tab, {:"$1", :"$2"}, 2, XEts.get_meta(tab)) |> elem(0)
      [[:y, 2], [:z, 3]]
  """
  @spec match(t(), any(), any(), any()) :: any()
  def match(%{tab: tab}, pattern, limit, meta),
    do: :shards.match(tab, pattern, limit, meta)

  @spec match(tab(), any(), any(), any()) :: any()
  def match(tab, pattern, limit, meta),
    do: :shards.match(tab, pattern, limit, meta)

  @doc """
  Match an item in the table and delete it.

  ## Examples

      iex> tab = XEts.new(:table) |> XEts.insert(x: 1, y: 2)
      iex> tab |> XEts.match_delete({:x, :_}) |> XEts.to_list()
      [{:y, 2}]
      iex> tab |> XEts.match_delete({:_, 2}) |> XEts.to_list()
      []
  """
  @spec match_delete(t(), any()) :: t()
  def match_delete(%{tab: _} = tab, pattern) do
    :shards.match_delete(tab.tab, pattern)
    tab
  end

  @spec match_delete(tab(), any()) :: any()
  def match_delete(tab, pattern) do
    :shards.match_delete(tab, pattern)
  end

  @doc """
  Match an item in the table and delete it given metadata.

  ## Examples

      iex> tab = XEts.new(:table) |> XEts.insert(x: 1, y: 2)
      iex> tab |> XEts.match_delete({:x, :_}, XEts.get_meta(tab)) |> XEts.to_list()
      [{:y, 2}]
  """
  @spec match_delete(t(), any(), meta()) :: t()
  def match_delete(%{tab: _} = tab, pattern, meta) do
    match_delete(tab.tab, pattern, meta)
    tab
  end

  @spec match_delete(tab(), any(), meta()) :: any()
  def match_delete(tab, pattern, meta) do
    :shards.match_delete(tab, pattern, meta)
  end

  @doc """
  Match an item in the table.

  ## Examples

      iex> tab = XEts.new(:table) |> XEts.insert(x: 1, y: 2)
      iex> tab |> XEts.match_object({:x, :_})
      [{:x, 1}]
      iex> tab |> XEts.match_object({:_, 2})
      [{:y, 2}]
  """
  @spec match_object(t(), any()) :: any()
  def match_object(%{tab: tab}, pattern), do: match_object(tab, pattern)

  @spec match_object(tab(), any()) :: any()
  def match_object(tab, pattern), do: :shards.match_object(tab, pattern)

  @doc """
  Match an item in the table given limit or metadata.

  ## Examples

      iex> tab = XEts.new(:table) |> XEts.insert(x: 1, y: 2, z: 3)
      iex> tab |> XEts.match_object({:x, :_}, XEts.get_meta(tab))
      [{:x, 1}]
      iex> tab |> XEts.match_object(:_, 2) |> elem(0)
      [{:y, 2}, {:z, 3}]
  """
  @spec match_object(t(), any(), any()) :: any()
  def match_object(%{tab: tab}, pattern, limit_or_meta),
    do: match_object(tab, pattern, limit_or_meta)

  @spec match_object(tab(), any(), any()) :: any()
  def match_object(tab, pattern, limit_or_meta),
    do: :shards.match_object(tab, pattern, limit_or_meta)

  @doc """
  Match an item in the table given limit and metadata.

  ## Examples

      iex> tab = XEts.new(:table) |> XEts.insert(x: 1, y: 1, z: 1)
      iex> tab |> XEts.match_object({:_, 1}, 2, XEts.get_meta(tab)) |> elem(0)
      [{:y, 1}, {:z, 1}]
  """
  @spec match_object(t(), any(), any(), any()) :: any()
  def match_object(%{tab: tab}, pattern, limit, meta),
    do: match_object(tab, pattern, limit, meta)

  @spec match_object(tab(), any(), any(), any()) :: any()
  def match_object(tab, pattern, limit, meta),
    do: :shards.match_object(tab, pattern, limit, meta)

  @spec match_spec_compile(any()) :: any()
  def match_spec_compile(match_spec), do: :shards.match_spec_compile(match_spec)

  @spec match_spec_run(list(), any()) :: any()
  def match_spec_run(list, compiled_match_spec),
    do: :shards.match_spec_run(list, compiled_match_spec)

  @doc """
  Check if an item is a member of the table.

  ## Examples

      iex> tab = XEts.new(:table) |> XEts.insert(x: 1, y: 2)
      iex> XEts.member?(tab, :x)
      true
      iex> XEts.member?(tab, :z)
      false
  """
  @spec member?(t(), any()) :: boolean()
  def member?(%{tab: tab}, key), do: member?(tab, key)

  @spec member?(tab(), any()) :: boolean()
  def member?(tab, key), do: :shards.member(tab, key)

  @doc """
  Check if an item is a member of the table given metadata.

  ## Examples

      iex> tab = XEts.new(:table) |> XEts.insert(x: 1, y: 2)
      iex> XEts.member?(tab, :x, XEts.get_meta(tab))
      true
      iex> XEts.member?(tab, :z, XEts.get_meta(tab))
      false
  """
  @spec member?(t(), any(), meta()) :: boolean()
  def member?(%{tab: tab}, key, meta), do: member?(tab, key, meta)

  @spec member?(tab(), any(), meta()) :: boolean()
  def member?(tab, key, meta), do: :shards.member(tab, key, meta)

  @doc """
  Create a new table.

  ## Examples

      iex> %{tab: tab, keypos: 1} = XEts.new(:table)
      iex> is_reference(tab)
      true
  """
  @spec new(atom(), keyword()) :: t()
  def new(tab, opts \\ []) do
    keypos = opts |> Enum.filter(&is_tuple/1) |> Keyword.get(:keypos, 1)
    %__MODULE__{tab: :shards.new(tab, opts), keypos: keypos}
  end

  @doc """
  Get the next item in the table.

  ## Examples

      iex> tab = XEts.new(:table) |> XEts.insert(x: 1, y: 2)
      iex> tab |> XEts.next(:x, XEts.get_meta(tab))
      :"$end_of_table"
  """
  @spec next(t(), any(), meta()) :: any()
  def next(%{tab: tab}, key1, meta), do: next(tab, key1, meta)

  @spec next(tab(), any(), meta()) :: any()
  def next(tab, key1, meta), do: :shards.next(tab, key1, meta)

  @doc """
  Returns the partition PIDs associated with the given table `TabOrPid`.
  """
  @spec partition_owners(pid() | tab() | t()) :: [pid()]
  def partition_owners(%{tab: tab}), do: partition_owners(tab)
  def partition_owners(tab_or_pid), do: :shards.partition_owners(tab_or_pid)

  @doc """
  Pop an item from the table.

      iex> tab = XEts.KV.new(:foo, [])
      iex> tab |> XEts.KV.put(:k, :v) |> XEts.KV.pop(:k)
      {:v, tab}
  """
  @impl true
  @spec pop(t(), any(), any()) :: {any(), t()}
  def pop(%{tab: _} = tab, key, default \\ nil) do
    case fetch(tab, key) do
      {:ok, value} ->
        delete(tab, key)
        {value, tab}

      :error ->
        {default, tab}
    end
  end

  @doc """
  Returns the previous item in the table.

  Equivalent to `:ets.next/2`

  However, the order in which results are returned might not be the same as the
  original ETS function, since it's a sharded table.

  See also: :ets.pray/2.
  """
  @spec prev(t() | tab(), any()) :: term() | :"$end_of_table"
  def prev(%{tab: tab}, key1), do: prev(tab, key1)
  def prev(tab, key1), do: :shards.prev(tab, key1)

  @doc """
  Put an item into the table.

  ## Examples

      iex> XEts.new(:table) |> XEts.put(:x, 1) |> XEts.to_list()
      [x: 1]
  """
  @spec put(t(), any(), any()) :: t()
  def put(%{tab: _} = tab, key, value) do
    insert(tab, {key, value})
    tab
  end

  @doc """
  Put an item or items into the table.

  ## Examples

      iex> XEts.new(:table) |> XEts.put(x: 1, y: 2) |> XEts.to_list()
      [y: 2, x: 1]
  """
  @spec put(t(), any()) :: t()
  def put(%{tab: _} = tab, item) do
    insert(tab, item)
    tab
  end

  @doc """
  Put metadata into the table.

  Wrapper for `:shards_meta.put/3`.
  """
  @spec put_meta(t() | tab(), term(), term()) :: t() | :ok
  def put_meta(%{tab: _} = tab, key, value) do
    put_meta(tab.tab, key, value)
    tab
  end

  def put_meta(tab, key, value) do
    :shards.put_meta(tab, key, value)
  end

  @doc """
  Equivalent to rename(tab, name, :shards_meta.get(tab))
  """
  @spec rename(t() | tab(), atom()) :: t() | atom()
  def rename(%{tab: _} = tab, name) do
    rename(tab.tab, name)
    tab
  end

  def rename(tab, name) do
    :shards.rename(tab, name)
  end

  @doc """
  Equivalent to `:ets.rename/2`.

  Renames the table name and all its associated shard tables. If something
  unexpected occurs during the process, an exception will be raised.

  See also: :ets.rename/2.
  """
  @spec rename(t() | tab(), atom(), meta()) :: t() | atom()
  def rename(%{tab: _} = tab, name, meta) do
    rename(tab.tab, name, meta)
    tab
  end

  @spec rename(tab(), any(), meta()) :: any()
  def rename(tab, name, meta) do
    :shards.rename(tab, name, meta)
  end

  @doc """
  Equivalent to `:ets.safe_fixtable/2`.
  """
  @spec safe_fixtable(t() | tab(), any()) :: t() | term()
  def safe_fixtable(%{tab: _} = tab, fix) do
    safe_fixtable(tab.tab, fix)
    tab
  end

  def safe_fixtable(tab, fix) do
    :shards.safe_fixtable(tab, fix)
  end

  @doc """
  Equivalent to `:ets.select/1`.

  The order in which results are returned might be not the same as the original
  ETS function.

  See also: ets:select/1.
  """
  @spec select(continuation()) :: {[term()], continuation()} | :"$end_of_table"
  def select(continuation), do: :shards.select(continuation)

  @doc """
  Equivalent to `:ets.select/2`.

  See also: ets:select/2.
  """
  def select(%{tab: tab}, match_spec), do: select(tab, match_spec)
  def select(tab, match_spec), do: :shards.select(tab, match_spec)

  @doc """
  Select items from the table given a match spec and limit or metadata.

  If 3rd argument is pos_integer() this function behaves like ets:select/3, otherwise,
  the 3rd argument is assumed as shards_meta:t()` and it behaves like `ets:select/2.

  The order in which results are returned might be not the same as the original
  ETS function.

  See also: :ets.select/3.
  """
  @spec select(t() | tab(), match_spec(), limit_or_meta()) ::
          t() | {[term()], continuation()} | :"$end_of_table"
  def select(%{tab: tab}, match_spec, limit_or_meta),
    do: select(tab, match_spec, limit_or_meta)

  def select(tab, match_spec, limit_or_meta),
    do: :shards.select(tab, match_spec, limit_or_meta)

  @doc """
  Select items from the table given a match spec and limit or metadata.

  If 3rd argument is pos_integer() this function behaves like ets:select/3, otherwise,
  the 3rd argument is assumed as shards_meta:t()` and it behaves like `ets:select/2.

  The order in which results are returned might be not the same as the original
  ETS function.

  See also: :ets.select/3.
  """
  @spec select(t() | tab(), match_spec(), limit() | meta()) ::
          t() | {[term()], continuation()} | :"$end_of_table"
  def select(%{tab: tab}, match_spec, limit, meta), do: select(tab, match_spec, limit, meta)
  def select(tab, match_spec, limit, meta), do: :shards.select(tab, match_spec, limit, meta)

  @doc """
  Equivalent to select_count(tab, match_spec, :shards_meta.get(tab)).
  """
  @spec select_count(t() | tab(), match_spec()) :: non_neg_integer()
  def select_count(%{tab: tab}, match_spec), do: select_count(tab, match_spec)
  def select_count(tab, match_spec), do: :shards.select_count(tab, match_spec)

  @doc """
  Equivalent to `:ets.select_count/2`.

  See also: :ets.select_count/2.
  """
  @spec select_count(t() | tab(), match_spec(), meta()) :: non_neg_integer()
  def select_count(%{tab: tab}, match_spec, meta), do: select_count(tab, match_spec, meta)
  def select_count(tab, match_spec, meta), do: :shards.select_count(tab, match_spec, meta)

  @doc """
  Equivalent to select_delete(tab, match_spec, :shards_meta.get(tab)).
  """
  @spec select_delete(t() | tab(), match_spec()) :: num_deleted :: non_neg_integer()
  def select_delete(%{tab: tab}, match_spec), do: select_delete(tab, match_spec)
  def select_delete(tab, match_spec), do: :shards.select_delete(tab, match_spec)

  @doc """
  Equivalent to `:ets.select_delete/2`.

  See also: :ets.select_delete/2.
  """
  @spec select_delete(t() | tab(), match_spec(), meta()) :: num_deleted :: non_neg_integer()
  def select_delete(%{tab: tab}, match_spec, meta), do: select_delete(tab, match_spec, meta)
  def select_delete(tab, match_spec, meta), do: :shards.select_delete(tab, match_spec, meta)

  @doc """
  Equivalent to select_replace(tab, match_spec, :shards_meta.get(tab)).
  """
  @spec select_replace(t() | tab(), match_spec()) :: num_replaced :: non_neg_integer()
  def select_replace(%{tab: tab}, match_spec), do: select_replace(tab, match_spec)
  def select_replace(tab, match_spec), do: :shards.select_replace(tab, match_spec)

  @doc """
  Equivalent to `:ets.select_replace/2`.

  See also: :ets.select_replace/2.
  """
  @spec select_replace(t() | tab(), match_spec(), meta()) :: num_replaced :: non_neg_integer()
  def select_replace(%{tab: tab}, match_spec, meta), do: select_replace(tab, match_spec, meta)
  def select_replace(tab, match_spec, meta), do: :shards.select_replace(tab, match_spec, meta)

  @doc """
  Equivalent to `:ets.select_reverse/1`.

  The order in which results are returned might be not the same as the original ETS function.

  See also: :ets.select_reverse/1.
  """
  @spec select_reverse(continuation()) :: {[term()], continuation()} | :"$end_of_table"
  def select_reverse(continuation), do: :shards.select_reverse(continuation)

  @doc """
  Equivalent to select_reverse(tab, match_spec, :shards_meta.get(tab)).
  """
  @spec select_reverse(t() | tab(), match_spec()) ::
          {[term()], continuation()} | :"$end_of_table" | [term()]
  def select_reverse(%{tab: tab}, match_spec), do: select_reverse(tab, match_spec)
  def select_reverse(tab, match_spec), do: :shards.select_reverse(tab, match_spec)

  @doc """
  Select items in reverse order given a match spec and limit or metadata.

  If 3rd argument is pos_integer() this function behaves like ets:select_reverse/3,
  otherwise, the 3rd argument is assumed as `shards_meta:t()` and it behaves like
  :ets.select_reverse/2.

  The order in which results are returned might be not the same as the original
  ETS function.

  See also: :ets.select_reverse/3.
  """
  @spec select_reverse(t() | tab(), match_spec(), limit_or_meta()) ::
          {[term()], continuation()} | :"$end_of_table" | [term()]
  def select_reverse(%{tab: tab}, match_spec, limit_or_meta),
    do: select_reverse(tab, match_spec, limit_or_meta)

  def select_reverse(tab, match_spec, limit_or_meta),
    do: :shards.select_reverse(tab, match_spec, limit_or_meta)

  @doc """
  Equivalent to :ets.select_reverse/3.

  The order in which results are returned might be not the same as the original
  ETS function.

  See also: :ets.select_reverse/3.
  """
  @spec select_reverse(t() | tab(), match_spec(), limit(), meta()) ::
          {[term()], continuation()} | :"$end_of_table"
  def select_reverse(%{tab: tab}, match_spec, limit, meta),
    do: select_reverse(tab, match_spec, limit, meta)

  def select_reverse(tab, match_spec, limit, meta),
    do: :shards.select_reverse(tab, match_spec, limit, meta)

  @doc """
  Equivalent to setopts(tab, opts, :shards_meta.get(tab)).
  """
  def setopts(%{tab: tab}, opts), do: setopts(tab, opts)
  def setopts(tab, opts), do: :shards.setopts(tab, opts)

  @doc """
  Equivalent to `:ets.setopts/2`.

  Returns true if the function was applied successfully on each partition,
  otherwise, false is returned.

  See also: :ets.setopts/2.
  """
  @spec setopts(t() | tab(), opts(), meta()) :: boolean()
  def setopts(%{tab: tab}, opts, meta), do: setopts(tab, opts, meta)
  def setopts(tab, opts, meta), do: :shards.setopts(tab, opts, meta)

  @doc """
  Get the size of the table.
  """
  @spec size(t()) :: integer()
  def size(tab), do: info(tab, :size)

  @doc """
  Equivalent to tab2file(tab, filename, []).
  """
  @spec tab2file(t() | tab(), filename()) :: t() | :ok | {:error, term()}
  def tab2file(%{tab: _} = tab, filename) do
    tab2file(tab.tab, filename)
    tab
  end

  def tab2file(tab, filename) do
    :shards.tab2file(tab, filename)
  end

  @doc """
  Equivalent to `:ets.tab2file/3`.

  This function generates one file per partition using :ets.tab2file/3, and also
  generates a master file with the given Filename that holds the information of
  the created partition files so that they can be recovered by calling :ets.file2tab/1,2.

  See also: :ets.tab2file/3.
  """
  @spec tab2file(t() | tab(), filename(), keyword()) :: t() | :ok | {:error, term()}
  def tab2file(%{tab: _} = tab, filename, options) do
    tab2file(tab.tab, filename, options)
    tab
  end

  def tab2file(tab, filename, options) do
    :shards.tab2file(tab, filename, options)
  end

  @doc """
  Equivalent to tab2list(tab, :shards_meta.get(tab)).
  """
  @spec tab2list(t() | tab()) :: [tuple()]
  def tab2list(%{tab: tab}), do: tab2list(tab)
  def tab2list(tab), do: :shards.tab2list(tab)

  @doc """
  Equivalent to `:ets.tab2list/1`.

  See also: :ets.tab2list/1.
  """
  @spec tab2list(t() | tab(), meta()) :: [tuple()]
  def tab2list(%{tab: tab}, meta), do: tab2list(tab, meta)
  def tab2list(tab, meta), do: :shards.tab2list(tab, meta)

  @doc """
  Equivalent to `:ets.tabfile_info/1`.

  Adds extra information about the partitions.

  See also: :shards.tabfile_info/1.
  """
  @spec tabfile_info(filename()) :: {:ok, tabinfo_item()} | {:error, term()}
  def tabfile_info(filename), do: :shards.tabfile_info(filename)

  @doc """
  Equivalent to table(tab, []).
  """
  @spec table(t() | tab()) :: :qlc.query_handle()
  def table(%{tab: tab}), do: table(tab)
  def table(tab), do: :shards.table(tab)

  @doc """
  Equivalent to table(tab, options, :shards_meta.get(tab)).
  """
  @spec table(t() | tab(), options()) :: :qlc.query_handle()
  def table(%{tab: tab}, options) when is_list(options), do: table(tab, options)
  def table(tab, options) when is_list(options), do: :shards.table(tab, options)

  @doc """
  Similar to `:ets.table/2`, but it returns a list of :qlc.query_handle(); one
  per partition.

  See also: :ets.table/2.
  """
  @spec table(t() | tab(), meta(), options()) :: :qlc.query_handle()
  def table(%{tab: tab}, meta, options) when is_list(options), do: table(tab, meta, options)
  def table(tab, meta, options) when is_list(options), do: :shards.table(tab, options, meta)

  @doc """
  Returns the metadata associated with the given table.
  """
  @spec table_meta(t() | tab()) :: :shards_meta.t()
  def table_meta(%{tab: tab}), do: table_meta(tab)
  def table_meta(tab), do: :shards.table_meta(tab)

  @doc """
  Equivalent to take(tab, key, :shards_meta.get(tab)).
  """
  @spec take(t() | tab(), term()) :: [tuple()]
  def take(%{tab: tab}, key), do: take(tab, key)
  def take(tab, key), do: :shards.take(tab, key)

  @doc """
  Equivalent to `:ets.take/2`.

  See also: :ets.take/2.
  """
  @spec take(t() | tab(), term(), meta()) :: [tuple()]
  def take(%{tab: tab}, key, meta), do: take(tab, key, meta)
  def take(tab, key, meta), do: :shards.take(tab, key, meta)

  @doc """
  Equivalent to `:ets.test_ms/2`.
  """
  @spec test_ms(tuple(), match_spec()) :: boolean()
  def test_ms(tuple, match_spec), do: :shards.test_ms(tuple, match_spec)

  @doc """
  Convert a table to a list of tuples.

  See also: :ets.tab2list/1.
  """
  @spec to_list(tab()) :: [tuple()]
  def to_list(tab), do: tab2list(tab)

  @doc """
  Equivalent to update_counter(tab, key, update_op, :shards_meta.get(tab)).

  ## Examples

      iex> %{tab: tab} = :test |> XEts.new() |> XEts.insert({10, 10, 4, "description"})
      iex> XEts.update_counter(tab, 10, {3, 1})
      5
      iex> XEts.lookup(tab, 10)
      [{10, 10, 5, "description"}]
  """
  @spec update_counter(t() | tab(), term(), term()) :: t() | [integer()] | integer()
  def update_counter(%{tab: _} = tab, key, update_op) do
    update_counter(tab.tab, key, update_op)
    tab
  end

  def update_counter(tab, key, update_op) do
    :shards.update_counter(tab, key, update_op)
  end

  @doc """
  Equivalent to update_counter(tab, key, update_op, default_or_meta).

  ## Examples

      iex> %{tab: tab} = :test |> XEts.new()
      iex> XEts.update_counter(tab, 10, {3, 1}, {10, 10, 4, "description"})
      5
      iex> XEts.lookup(tab, 10)
      [{10, 10, 5, "description"}]
  """
  @spec update_counter(t() | tab(), term(), tuple() | meta()) :: t() | [integer()] | integer()
  def update_counter(%{tab: _} = tab, key, update_op, default_or_meta) do
    update_counter(tab.tab, key, update_op, default_or_meta)
    tab
  end

  def update_counter(tab, key, update_op, default_or_meta) do
    :shards.update_counter(tab, key, update_op, default_or_meta)
  end

  @doc """
  Equivalent to update_counter(tab, key, update_op, default, meta).

  ## Examples

      iex> %{tab: tab} = :test |> XEts.new() |> XEts.insert(x: 0, y: 1)
      iex> meta = XEts.get_meta(tab)
      iex> XEts.update_counter(tab, :x, {2, 3}, {:x, 0}, meta)
      3
      iex> XEts.update_counter(tab, :y, {2, 3}, {:y, 0}, meta)
      4
      iex> XEts.update_counter(tab, :z, {2, -1}, {:z, 5}, meta)
      4
      iex> XEts.to_list(tab) |> Enum.sort()
      [x: 3, y: 4, z: 4]
  """
  @spec update_counter(t() | tab(), term(), tuple(), meta()) :: t() | [integer()] | integer()
  def update_counter(%{tab: _} = tab, key, update_op, default, meta) do
    update_counter(tab.tab, key, update_op, default, meta)
    tab
  end

  def update_counter(tab, key, update_op, default, meta) do
    :shards.update_counter(tab, key, update_op, default, meta)
  end

  @doc """
  Equivalent to update_element(tab, key, element_spec, :shards_meta.get(tab)).

  ## Examples

      iex> %{tab: tab} = :test |> XEts.new() |> XEts.insert(x: 2, y: 4)
      iex> XEts.update_element(tab, :x, {2, 10})
      true
      iex> XEts.lookup(tab, :x)
      [{:x, 10}]
  """
  @spec update_element(t() | tab(), term(), element_spec()) :: t() | boolean()
  def update_element(%{tab: _} = tab, key, element_spec) do
    update_element(tab.tab, key, element_spec)
    tab
  end

  def update_element(tab, key, element_spec) do
    :shards.update_element(tab, key, element_spec)
  end

  @doc """
  Equivalent to `:ets.update_element/3`.

  See also: :ets.update_element/3.
  """
  @spec update_element(t() | tab(), term(), element_spec(), meta()) :: t() | boolean()
  def update_element(%{tab: _} = tab, key, element_spec, meta) do
    update_element(tab.tab, key, element_spec, meta)
    tab
  end

  def update_element(tab, key, element_spec, meta) do
    :shards.update_element(tab, key, element_spec, meta)
  end

  @doc """
  Equivalent to `:ets.whereis/1`.

  See also: :ets.whereis/1.

  ## Examples

      iex> XEts.new(:test, [:named_table])
      iex> XEts.whereis(:test) |> is_reference()
      true
      iex> XEts.whereis(:test2)
      :undefined
  """
  @spec whereis(t() | tab()) :: reference() | :undefined
  def whereis(%{tab: tab}), do: whereis(tab)
  def whereis(tab), do: :ets.whereis(tab)
end
