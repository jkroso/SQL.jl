@use Dates: Date, DateTime, @dateformat_str, format
@use "." SQLFunction SQLReference SQLNode SQLQuery variables @sql
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

addwhere(q::SQLQuery, (k,v)::Pair) = q |> SQLFunction{:(=)}([SQLReference(q.table, string(k)), sqlvalue(v)])

"Update the `db` row associated with `a` have the value of `b` instead"
update(db::DB, a::T, b::T) where T = begin
  table = table_name(T)
  pk = primary_key(db, table)
  q = reduce(addwhere, pairs(a), init=@sql From(table))
  r = query(db, q)
  @assert !isempty(r)
  id = getproperty(first(r), Symbol(pk))
  statements = []
  values = []
  for (c,val) in pairs(b)
    if getfield(a, c) != val
      push!(statements, "\"$c\" = ?")
      push!(values, sqlvalue(val))
    end
  end
  @assert !isempty(statements)
  DBInterface.execute(db, "UPDATE \"$table\" SET $(join(statements, ',')) WHERE $pk=$id", values)
  nothing
end

"Serialise the `sql` query and create a Vector of the values that should be assigned to its parameters"
prepare(sql::SQLQuery) = begin
  @dynamic! let variables = []
    sprint(write, sql), variables[]
  end
end

"Run a query against the `db`. Any keyword arguments will be added to the query as WHERE clauses"
query(db::DB, sql::SQLQuery; kv...) = begin
  sql = reduce(addwhere, pairs(kv), init=sql)
  str, vars = prepare(sql)
  DBInterface.execute(db, str, vars)
end

struct TableMetadata
  columns::Vector{String}
  primarykey::String
end

const meta = WeakKeyDict{DB,Dict{String,TableMetadata}}()

generate_metadata(db) = Dict{String, TableMetadata}((t.name=>generate_metadata(db, t.name) for t in tables(db)))
generate_metadata(db, table) = begin
  meta = columns(db, table)
  pki = findfirst(!iszero, meta.pk)
  pk = isnothing(pki) ? "rowid" : meta.name[pki]
  TableMetadata(meta.name, pk)
end

primary_key(db, table) = begin
  data = get!(()->generate_metadata(db), meta, db)
  data[table].primarykey
end
