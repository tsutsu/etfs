# ETFs

`ETFs` is a simple streamable binary-serialization container format based on Erlang's External Term Format (i.e. `erlang:term_to_binary/1`)

## Usage

The public interface to `ETFs` is `ETFs.stream!/1`.

`ETFs.stream!` produces a struct which works a lot like `File.stream!/1`, in the sense that it is both `Enumerable` and `Collectable`. But an `%ETFs.Stream{}` consumes and produces arbitrary Erlang terms, rather than `iodata`.

```elixir
iex> etfs_file = ETFs.stream!("foo.etfs")
%ETFs.Stream{path: "foo.etfs", format: :v2}

iex> (1..10
...> |> Stream.map(fn i ->
...>   [i, 3, "foo", %{arbitrary: :terms}, make_ref(), self()]
...> end)
...> |> Enum.into(etfs_file))

# then, later...

iex> (etfs_file
...> |> Stream.take(1)
...> |> Enum.to_list
...> |> List.first)
[
  0,
  3,
  "foo",
  %{arbitrary: :terms},
  #Reference<0.2532052800.3802660866.220063>,
  #PID<0.88.0>
]
# [i, 3, "foo", %{arbitrary: terms}, make_ref(), self()]
```

The `ETFs` format also embeds a Table of Contents chunk, and so supports O(1) slicing/element lookup (e.g. `Enum.slice`, `Enum.take`, `Enum.at`, etc.)

## Installation

The package can be installed by adding `etfs` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:etfs, "~> 0.1.3"}
  ]
end
```

