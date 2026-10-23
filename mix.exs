defmodule XEts.MixProject do
  use Mix.Project

  def project do
    [
      app: :x_ets,
      version: "1.0.0",
      elixir: "~> 1.20",
      description: "An extended ETS library for Elixir",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      test_coverage: [tool: ExCoveralls],
      docs: [
        extras: ["README.md", "LICENSE"]
      ],
      dialyzer: [
        plt_add_deps: :app_tree
      ]
    ]
  end

  def cli do
    [preferred_envs: [coveralls: :test, "coveralls.html": :test, "coveralls.json": :test]]
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
        "E-MetroTel" => "https://emetrotel.com"
      }
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:shards, "~> 1.1"},
      {:mix_test_watch, "~> 1.4", only: :dev, runtime: false},
      {:dialyxir, "~> 1.4.8", only: :dev, runtime: false},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false},
      {:excoveralls, "~> 0.18.5", only: :test}
    ]
  end
end
