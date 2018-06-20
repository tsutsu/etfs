defmodule ETFs do
  def stream!(path, opts \\ []) do
    ETFs.Stream.open(path, opts)
  end
end
