defmodule ETFs.Stream do
  defstruct path: nil, format: :v3

  @fourcc "ETFs"

  @doc false
  def open(path, opts \\ []) do
    format = Keyword.get(opts, :format, :v3)
    %__MODULE__{path: path, format: format}
  end

  def record_count(%__MODULE__{path: path, format: :v3}) do
    with {:ok, f} <- File.open(path, [:read]),
         @fourcc <- IO.binread(f, 4),
         <<record_count::integer-size(32)>> <- IO.binread(f, 4) do
      {:ok, record_count}
    else
      err -> {:error, err}
    end
  end

  def stream_all_records!(%__MODULE__{path: path, format: :v3}) do
    Stream.resource(
      fn ->
        {:ok, io} = File.open(path, [:read])
        @fourcc = IO.binread(io, 4)
        <<record_count::integer-size(32)>> = IO.binread(io, 4)
        <<_toc_pos::integer-size(64)>> = IO.binread(io, 8)
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
      fn {io, _} -> File.close(io) end
    )
    |> Stream.map(&:erlang.binary_to_term/1)
  end

  def slice_records(%__MODULE__{path: path, format: :v3}) do
    {:ok, io} = File.open(path, [:read])

    @fourcc = IO.binread(io, 4)
    <<record_count::integer-size(32)>> = IO.binread(io, 4)
    <<toc_pos::integer-size(64)>> = IO.binread(io, 8)

    f = fn start, count ->
      first_record_pos_pos = toc_pos + (start * 8)
      {:ok, <<first_record_pos::integer-size(64)>>} = :file.pread(io, first_record_pos_pos, 8)
      :file.position(io, {:bof, first_record_pos})

      records = for _i <- (0..(count - 1)) do
        <<record_len::integer-size(32)>> = IO.binread(io, 4)
        IO.binread(io, record_len) |> :erlang.binary_to_term
      end

      File.close(io)

      records
    end

    {:ok, record_count, f}
  end

  def collect_into(%__MODULE__{path: path, format: :v3}) do
    {:ok, io} = File.open(path, [:write])

    IO.binwrite(io, [@fourcc, <<0::integer-size(32)>>, <<0::integer-size(64)>>])

    collector_fun = fn
      {io, pos, toc}, {:cont, record} ->
        record_bin = :erlang.term_to_binary(record, [:compressed, minor_version: 2])
        msg = [<<byte_size(record_bin)::integer-size(32)>>, record_bin]

        IO.binwrite(io, msg)

        msg_size = 4 + byte_size(record_bin)
        {io, pos + msg_size, [{pos, msg_size} | toc]}

      {io, toc_pos, toc}, :done ->
        toc
        |> Enum.reverse
        |> Enum.each(fn {pos, _msg_size} ->
          IO.binwrite(io, [
            <<pos::integer-size(64)>>
          ])
        end)

        :file.position(io, {:bof, 4})
        IO.binwrite(io, <<length(toc)::integer-size(32)>>)
        IO.binwrite(io, <<toc_pos::integer-size(64)>>)

      _set, :halt ->
        :ok
    end

    {{io, 16, []}, collector_fun}
  end
end

defimpl Enumerable, for: ETFs.Stream do
  def reduce(dumpfile, acc, fun) do
    s = ETFs.Stream.stream_all_records!(dumpfile)
    Enumerable.reduce(s, acc, fun)
  end

  def slice(dumpfile), do:
    ETFs.Stream.slice_records(dumpfile)

  def member?(dumpfile, element) do
    s = ETFs.Stream.stream_all_records!(dumpfile)
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
