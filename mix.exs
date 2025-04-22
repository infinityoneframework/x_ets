defmodule XEts.MixProject do
  use Mix.Project

  def project do
    [
      app: :x_ets,
      version: "0.2.0",
      elixir: "~> 1.12",
      description: "An extended ETS library for Elixir",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      docs: [
        extras: ["README.md", "LICENSE"]
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

  defp package do
    [
      files: ~w(lib LICENSE mix.exs README.md .formatter.exs),
      licenses: ["MIT"],
      links: %{
        "E-MetroTel" => "https://emetrol.com"
      }
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:shards, "~> 1.1"},
      {:mix_test_watch, "~> 1.0", only: :dev, runtime: false},
      {:dialyxir, "~> 1.1", only: [:dev], runtime: false},
      {:earmark, "1.4.15"},
      {:ex_doc, "0.24.2", only: :dev}
    ]
  end
end
