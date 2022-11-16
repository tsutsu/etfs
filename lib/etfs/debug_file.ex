defmodule ETFs.DebugFile do
  @moduledoc false

  defstruct [opened: false, disk_log_name: nil, path: nil]

  @format_version {0, 1, 0}

  def open(disk_log_name, path) do
    open_async(disk_log_name, path)
    |> ensure_opened()
  end

  def open_async(disk_log_name, path) do
    %__MODULE__{path: path, disk_log_name: disk_log_name}
  end

  def ensure_opened(%__MODULE__{opened: true} = state), do: state
  def ensure_opened(%__MODULE__{opened: false, disk_log_name: disk_log_name, path: path} = state) do
    {:ok, _bytes_lost} = open_log(disk_log_name, path)
    %__MODULE__{state | opened: true}
  end

  def flush(%__MODULE__{opened: false}), do: :ok
  def flush(%__MODULE__{opened: true, disk_log_name: name}) do
    :disk_log.sync(name)
  end

  def close(%__MODULE__{opened: false}), do: :ok
  def close(%__MODULE__{opened: true, disk_log_name: name}) do
    :ok = :disk_log.sync(name)
    :disk_log.close(name)
  end

  def log_term(%__MODULE__{disk_log_name: disk_log_name} = state, term) do
    state = ensure_opened(state)
    case :disk_log.log(disk_log_name, term) do
      :ok -> {:ok, state}
      {:error, reason} -> {:error, reason, state}
    end
  end

  def log_term!(state, term) do
    case log_term(state, term) do
      {:ok, state} ->
        state

      {:error, reason, _state} ->
        raise RuntimeError, "error while logging term #{inspect(term)}: #{inspect(reason)}"
    end
  end

  def stream!(%__MODULE__{disk_log_name: disk_log_name} = state) do
    Stream.resource(
      fn -> {ensure_opened(state), nil} end,

      fn {state, cont} ->
        chunk_to_req = case cont do
          nil -> :start
          cont -> cont
        end

        case :disk_log.chunk(disk_log_name, chunk_to_req) do
          {:error, :no_such_log} ->
            {:halt, state}

          :eof ->
            {:halt, state}

          {cont, logged_terms} ->
            {strip_header(logged_terms), {state, cont}}
        end
      end,

      fn
        {state, _cont} -> close(state)
        state -> close(state)
      end
    )
  end

  ## Helpers

  defp open_log(disk_log_name, path, bytes_already_lost \\ 0) do
    path = path
    |> resolve_path()
    |> String.to_charlist()

    # ensure dir exists
    File.mkdir_p!(Path.dirname(path))

    opts = [
      name: disk_log_name,
      file: path,
      format: :internal,
      type: :wrap,
      repair: true,
      size: {1024*1024, 1000}, # max 1GB split into 1000 1MB files
      notify: false,
      head: {:vsn, @format_version},
      quiet: true,
      mode: :read_write
    ]

    case :disk_log.open(opts) do
      {:ok, ^disk_log_name} ->
        {:ok, bytes_already_lost}

      {:repaired, ^disk_log_name, {:recovered, _good_bytes}, {:badbytes, new_bad_bytes}} ->
        {:ok, new_bad_bytes + bytes_already_lost}

      {:error, {:need_repair, ^disk_log_name}} ->
        {:ok, new_bad_bytes} = repair_log(disk_log_name, opts)
        open_log(disk_log_name, path, new_bad_bytes + bytes_already_lost)

      {:error, {:arg_mismatch, :repair, true, false}} ->
        {:ok, new_bad_bytes} = repair_log(disk_log_name, opts)
        open_log(disk_log_name, path, new_bad_bytes + bytes_already_lost)

      {:error, {:name_already_open, _}} ->
        {:error, :name_already_open}

      {:error, {:size_mismatch, _}} ->
        {:error, :size_mismatch}

      {:error, {:invalid_header, {:vsn, old_version}}} ->
        upgrade_version(disk_log_name, old_version, @format_version)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp resolve_path(:default) do
    if Process.whereis(Mix.ProjectStack) do
      project_root = Mix.Project.deps_path() |> Path.dirname()
      Path.join([project_root, "debug"])
    else
      :filename.basedir(:user_log, "erlang-debug")
    end
  end
  defp resolve_path(path), do: path

  defp repair_log(log_name, log_options) do
    log_options = Keyword.drop(log_options, :size)

    bytes_lost = case :disk_log.open(log_options) do
      {:repaired, ^log_name, {:recovered, _good_bytes}, {:badbytes, bad_bytes}} -> bad_bytes
      _ -> 0
    end

    _ = :disk_log.close(log_name)

    {:ok, bytes_lost}
  end

  defp strip_header([{:vsn, _} | rest]), do: rest
  defp strip_header(logs), do: logs

  defp upgrade_version(_log_name, unknown_version, @format_version), do:
    {:error, {:upgrade_failed, :unknown_version, unknown_version}}
end
