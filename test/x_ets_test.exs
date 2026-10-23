defmodule XEtsTest do
  use ExUnit.Case
  doctest XEts

  test "test_ms returns tagged results for valid and invalid match specifications" do
    assert XEts.test_ms({:key, :value}, [{{:"$1", :"$2"}, [], [:"$2"]}]) == {:ok, :value}

    assert {:error, [_ | _]} =
             XEts.test_ms({:key, :value}, [{:_, [{:unsupported_guard, :"$_"}], [true]}])
  end
end
