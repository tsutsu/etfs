defmodule ETFs.Stream do
  defstruct path: nil, format: :v2

  @fourcc "ETFs"

  @doc false
  def open(path, opts \\ []) do
    format = Keyword.get(opts, :format, :v2)
    %__MODULE__{path: path, format: format}
  end

  def record_count(%__MODULE__{path: path, format: :v2}) do
    with {:ok, f} <- File.open(path, [:read]),
         @fourcc <- IO.binread(f, 4),
         <<record_count::integer-size(32)>> <- IO.binread(f, 4) do
      {:ok, record_count}
    else
      err -> {:error, err}
    end
  end

  def stream_records!(%__MODULE__{path: path, format: :v2}) do
    Stream.resource(
      fn ->
        {:ok, io} = File.open(path, [:read])
        @fourcc = IO.binread(io, 4)
        <<record_count::integer-size(32)>> = IO.binread(io, 4)
        {io, record_count}
      end,
      fn
        {_io, 0} ->
          {:halt, []}

        {io, records_left} ->
          with <<record_len::integer-size(32)>> <- IO.binread(io, 4),
               record when is_binary(record) <- IO.binread(io, record_len) do
            {[record], {io, records_left - 1}}
          else
            :eof -> {:halt, []}
            {:error, _} -> {:halt, []}
          end
      end,
      &File.close/1
    )
    |> Stream.map(&:erlang.binary_to_term/1)
  end

  def collect_into(%__MODULE__{path: path, format: :v2}) do
    {:ok, io} = File.open(path, [:write])

    IO.binwrite(io, [@fourcc, <<0::integer-size(32)>>])

    collector_fun = fn
      {io, record_count}, {:cont, record} ->
        record_bin = :erlang.term_to_binary(record)
        msg = [<<byte_size(record_bin)::integer-size(32)>>, record_bin]
        IO.binwrite(io, msg)
        {io, record_count + 1}

      {io, record_count}, :done ->
        :file.position(io, {:bof, 4})
        IO.binwrite(io, <<record_count::integer-size(32)>>)

      _set, :halt ->
        :ok
    end

    {{io, 0}, collector_fun}
  end
end

defimpl Enumerable, for: ETFs.Stream do
  def reduce(dumpfile, acc, fun) do
    s = ETFs.Stream.stream_records!(dumpfile)
    Enumerable.reduce(s, acc, fun)
  end

  def slice(dumpfile) do
    s = ETFs.Stream.stream_records!(dumpfile)
    Enumerable.slice(s)
  end

  def member?(dumpfile, element) do
    s = ETFs.Stream.stream_records!(dumpfile)
    Enumerable.member?(s, element)
  end

  def count(dumpfile) do
    ETFs.Stream.record_count(dumpfile)
  end
end

defimpl Collectable, for: ETFs.Stream do
  def into(dumpfile) do
    ETFs.Stream.collect_into(dumpfile)
  end
end
