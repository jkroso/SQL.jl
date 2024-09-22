@use Dates: Date, DateTime, @dateformat_str, format
@use "." SQLFunction SQLReference SQLNode SQLQuery variables
@use "github.com/jkroso/DynamicVar.jl" @dynamic!
@use SQLite: DBInterface, DB, columns, tables

sqlvalue(m::Date) = format(m, dateformat"yyyy-mm-dd")
sqlvalue(m::DateTime) = format(m, dateformat"yyyy-mm-ddTHH:MM:SS.sssZ")
sqlvalue(::Missing) = missing
sqlvalue(::Nothing) = nothing
sqlvalue(n::Unsigned) = convert(Int, n)
sqlvalue(n::Integer) = n
sqlvalue(n::AbstractFloat) = n
sqlvalue(s::AbstractString) = s

"Add a value to the `db`"
save(db::DB, obj) = begin
  table = Symbol(table_name(typeof(obj)))
  names = columns(db, table)
  sql = "INSERT INTO \"$table\" ($(join(names, ','))) VALUES ($(join(fill('?', length(names)), ',')))"
  DBInterface.execute(db, sql, sqlrow(obj))
  nothing
end

"Determine which table a Datatype belongs in"
table_name(::Type{T}) where T = String(T.name.name)

"Convert a struct to a tuple of values that SQLite can store"
sqlrow(obj) = map(p->sqlvalue(getproperty(obj, p)), propertynames(obj))

"Update the `db` row associated with `a` have the value of `b` instead"
update(db::DB, a::T, b::T) where T = begin
  statements = []=>[]
  values = []=>[]
  for (c,val) in zip(fieldnames(T), sqlrow(b))
    i = (getfield(a, c) == getfield(b, c)) + 1
    if isnothing(val)
      i > 1 && push!(statements[i], "'$c' IS Null")
    else
      push!(statements[i], "'$c' = ?")
      push!(values[i], val)
    end
  end
  @assert !isempty(statements[2])
  sql = "UPDATE \"$(table_name(T))\" SET $(join(statements[1], ',')) WHERE $(join(statements[2], " AND "))"
  !isempty(DBInterface.execute(db, sql, vcat(values[1], values[2])))
end

"Serialise the `sql` query and create a Vector of the values that should be assigned to its parameters"
prepare(sql::SQLQuery) = begin
  @dynamic! let variables = []
    sprint(write, sql), variables[]
  end
end

"Run a query against the `db`. Any keyword arguments will be added to the query as WHERE clauses"
query(db::DB, sql::SQLQuery; kv...) = begin
  sql = reduce(pairs(kv), init=sql) do out, (k,v)
    out |> SQLFunction{:(=)}([SQLReference(out.table, string(k)), v])
  end
  str, vars = prepare(sql)
  DBInterface.execute(db, str, vars)
end
