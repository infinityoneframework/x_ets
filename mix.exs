defmodule XEts.MixProject do
  use Mix.Project

  def project do
    [
      app: :x_ets,
      version: "0.1.0",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: [
        extras: ["README.md"]
      ],
      dialyzer: [
        plt_add_deps: :app_tree
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {XEts.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:shards, "~> 1.1"},
      {:mix_test_watch, "~> 1.0", only: :dev, runtime: false},
      {:dialyxir, "~> 1.1", only: [:dev], runtime: false, override: true},
      {:earmark, "1.4.15", override: true},
      {:ex_doc, "0.24.2", override: true, only: :dev}
    ]
  end
end
