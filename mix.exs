defmodule ETFs.MixProject do
  use Mix.Project

  def project do
    [
      app: :etfs,
      version: "0.1.4",
      elixir: "~> 1.6",
      description: description(),
      package: package(),
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  defp description do
    """
    A simple streamable binary-serialization container format based on Erlang's External Term Format
    """
  end

  defp package do [
    name: :etfs,
    files: ["config", "lib", "mix.exs", "LICENSE"],
    maintainers: ["Levi Aul"],
    licenses: ["MIT"],
    links: %{"GitHub" => "https://github.com/tsutsu/etfs"}
  ] end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  def deps do
    [
      {:ex_doc, ">= 0.0.0", only: :dev}
    ]
  end
end
