defmodule XEts.AccessTest do
  use ExUnit.Case, async: true

  for module <- [XEts, XEts.KV] do
    describe "#{module} Access callbacks" do
      setup do
        module = unquote(module)
        %{module: module, tab: module.new(:access_test, [])}
      end

      test "fetch distinguishes a stored :error value from a missing key", %{
        module: module,
        tab: tab
      } do
        module.put(tab, :key, :error)
        assert module.fetch(tab, :key) == {:ok, :error}
        assert tab[:key] == :error
        assert module.fetch(tab, :missing) == :error
      end

      test "get_and_update supports the :pop callback result", %{module: module, tab: tab} do
        module.put(tab, :key, :value)
        assert module.get_and_update(tab, :key, fn :value -> :pop end) == {:value, tab}
        assert module.fetch(tab, :key) == :error
        assert module.get_and_update(tab, :missing, fn nil -> :pop end) == {nil, tab}
      end

      test "get_and_update rejects invalid callback results without changing the table", %{
        module: module,
        tab: tab
      } do
        module.put(tab, :key, :value)

        assert_raise ArgumentError, ~r/expected a two-element tuple or :pop/, fn ->
          module.get_and_update(tab, :key, fn _ -> :invalid end)
        end

        assert module.fetch(tab, :key) == {:ok, :value}
      end
    end
  end

  test "XEts.fetch respects a nondefault key position" do
    tab = XEts.new(:keypos_test, keypos: 2)
    XEts.insert(tab, {:prefix, :key, :error})
    assert XEts.fetch(tab, :key) == {:ok, :error}
    assert XEts.fetch(tab, :missing) == :error
  end
end
