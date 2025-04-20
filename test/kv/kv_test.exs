defmodule XEts.KVTest do
  use ExUnit.Case

  alias XEts.KV

  doctest KV

  setup do
    tab = KV.new(:ets_test, write_concurrency: true, read_concurrency: true)
    %{tab: tab}
  end

  test "put/3", %{tab: tab} do
    KV.put(tab, :key, :value)
    assert KV.to_list(tab) == [{:key, :value}]

    KV.put(tab, {:info, 1}, 2)
    assert KV.to_list(tab) == [{:key, :value}, {{:info, 1}, 2}]
  end

  test "get/2", %{tab: tab} do
    KV.put(tab, :key, :value)
    assert KV.get(tab, :key) == :value
  end

  test "get/2 with tuple key", %{tab: tab} do
    KV.put(tab, {:key, "id"}, :value)
    assert KV.get(tab, {:key, "id"}) == :value
  end

  test "get/3 with default", %{tab: tab} do
    assert KV.get(tab, :key, :default) == :default
  end

  test "match/2", %{tab: tab} do
    KV.put(tab, {:key, "id"}, :value)
    assert KV.match(tab, {:key, :"$1"}) == [{"id", :value}]
    KV.put(tab, {:key, "id1"}, :other)
    assert KV.match(tab, {:key, :"$1"}) == [{"id", :value}, {"id1", :other}]
  end

  test "match/3 with default", %{tab: tab} do
    assert KV.match(tab, {:key, :"$1"}, :default) == :default
  end

  test "match/3 with wildcard", %{tab: tab} do
    assert KV.match(tab, {:key, :"$1"}, :"$2") == []
  end

  test "match/4", %{tab: tab} do
    # KV.put(tab, {:key, ""}, :one)
    assert KV.match(tab, {:key, :"$1"}, :"$2", :default) == :default
    KV.put(tab, {:key, "one"}, :one)
    assert KV.match(tab, {:key, :"$1"}, :"$2", :default) == [{"one", :one}]
    assert KV.match(tab, {:key2, :"$1"}, :"$2", :default) == :default
  end

  test "fetch/2", %{tab: tab} do
    assert KV.fetch(tab, :key) == :error
    KV.put(tab, :key, :value)
    assert KV.fetch(tab, :key) == {:ok, :value}
  end

  test "get_and_update/3", %{tab: tab} do
    KV.put(tab, :key, 1)
    assert KV.get_and_update(tab, :key, fn v -> {v, v + 1} end) == {1, tab}
    assert KV.get_and_update(tab, :key1, fn v -> {v, "#{v}test"} end) == {nil, tab}
  end

  test "pop/3", %{tab: tab} do
    KV.put(tab, :key, 1)
    assert KV.pop(tab, :key) == {1, tab}
    assert KV.pop(tab, :key1) == {nil, tab}
  end

  test "put_in/3", %{tab: tab} do
    KV.put(tab, :key, %{a: 1})
    assert tab |> put_in([:key, :a], 2) |> KV.to_list() == [{:key, %{a: 2}}]
    assert tab |> put_in([:key, :b], 3) |> KV.to_list() == [{:key, %{a: 2, b: 3}}]

    KV.put(tab, :key1, %{})
    assert tab |> put_in([:key1, :a], 5) |> KV.to_list() == [key: %{a: 2, b: 3}, key1: %{a: 5}]

    KV.put(tab, :key1, %{a: 5, b: %{}})
    assert tab |> put_in([:key1, :b, :c], 5) == tab
    assert KV.get(tab, :key1) == %{a: 5, b: %{c: 5}}
  end

  test "get_in/2", %{tab: tab} do
    assert get_in(tab, [:key, :a]) == nil
    KV.put(tab, :key, %{a: 1})
    assert get_in(tab, [:key, :a]) == 1
    assert get_in(tab, [:key, :b]) == nil
  end

  test "Kernel put_in/3", %{tab: tab} do
    KV.put(tab, :key, %{})
    assert put_in(tab, [:key, :a], 2) == tab
    assert put_in(tab, [:key, :b], 3) == tab
  end

  test "Kernel.put_in/3 tuple", %{tab: tab} do
    KV.put(tab, {:info, 1}, %{})
    assert put_in(tab, [{:info, 1}, :a], 3) == tab
    assert KV.get(tab, {:info, 1}) == %{a: 3}
  end

  test "update_in/3", %{tab: tab} do
    KV.put(tab, :key, %{a: 0})
    assert update_in(tab, [:key, :a], fn v -> (v || 0) + 1 end)
    assert KV.get(tab, :key) == %{a: 1}
    assert update_in(tab, [:key, :a], fn v -> (v || 0) + 1 end)
    assert KV.get(tab, :key) == %{a: 2}
  end

  test "get_and_update_in/3", %{tab: tab} do
    KV.put(tab, :key, %{})
    assert get_and_update_in(tab, [:key, :a], fn v -> {v, (v || 0) + 1} end) == {nil, tab}
    assert KV.get(tab, :key) == %{a: 1}
    assert get_and_update_in(tab, [:key, :a], fn v -> {v, v + 1} end) == {1, tab}
    assert KV.get(tab, :key) == %{a: 2}
  end

  test "delete/2", %{tab: tab} do
    KV.put(tab, :key, :value)
    assert KV.get(tab, :key) == :value
    assert KV.delete(tab, :key)
    assert KV.get(tab, :key) == nil
  end

  test "delete_match/2", %{tab: tab} do
    KV.put(tab, {:info, 1}, 2)
    KV.put(tab, {:info, 2}, 3)
    assert KV.match_delete(tab, {:info, :"$1"})
    assert KV.match(tab, :"$1") == []
  end

  test "delete_match/3", %{tab: tab} do
    KV.put(tab, {:info, 1, 5}, 2)
    KV.put(tab, {:info, 1, 6}, 5)
    KV.put(tab, {:info, 2, 6}, 3)
    assert KV.match_delete(tab, {:"$1", 1, :"$2"}, :"$3")
    assert KV.to_list(tab) == [{{:info, 2, 6}, 3}]
  end

  test "delete_all/1", %{tab: tab} do
    KV.put(tab, :key, :value)
    assert KV.delete_all(tab)
    assert KV.to_list(tab) == []
  end

  test "put_new/3", %{tab: tab} do
    assert KV.put_new(tab, :key, :value)
    assert KV.put_new(tab, :key, :other)
    assert KV.to_list(tab) == [{:key, :value}]
  end

  test "named tests" do
    tab = KV.new(:test_tab)
    assert KV.to_list(tab) == []
    tab = %{tab | tab: :test_tab}
    KV.put(tab, :x, :y)
    assert KV.to_list(tab) == [x: :y]
  end

  test "info/0", %{tab: tab} do
    keys =
      ~w(heir keypos name named_table protection read_concurrency size type write_concurrency)a

    assert KV.info(tab)
           |> Keyword.take(keys)
           |> Enum.sort() ==
             Enum.sort(
               heir: :none,
               keypos: 1,
               name: :ets_test,
               named_table: false,
               protection: :public,
               read_concurrency: true,
               size: 0,
               type: :set,
               write_concurrency: true
             )
  end

  test "info/1", %{tab: tab} do
    assert KV.info(tab, :keypos) == 1
    assert KV.info(tab, :name) == :ets_test
  end
end
