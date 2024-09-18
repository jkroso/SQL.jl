@use Dates: Date, DateTime, @dateformat_str, format
@use "." SQLFunction SQLReference SQLNode SQLQuery variables
@use "github.com/jkroso/DynamicVar.jl" @dynamic!
@use SQLite: DBInterface, DB, columns, tables

sqlvalue(m::Date) = format(m, dateformat"yyyy-mm-dd")
sqlvalue(m::DateTime) = format(m, dateformat"yyyy-mm-ddTHH:MM:SS.sssZ")
sqlvalue(::Missing) = missing
sqlvalue(n::Unsigned) = convert(Int, n)
sqlvalue(n::Integer) = n
sqlvalue(n::AbstractFloat) = n
sqlvalue(s::AbstractString) = s

save(db::DB, obj) = begin
  table = Symbol(table_name(typeof(obj)))
  names = columns(db, table)
  sql = "INSERT INTO \"$table\" ($(join(names, ','))) VALUES ($(join(fill('?', length(names)), ',')))"
  DBInterface.execute(db, sql, sqlrow(obj))
  nothing
end

table_name(::Type{T}) where T = String(T.name.name)
sqlrow(obj) = map(p->sqlvalue(getproperty(obj, p)), propertynames(obj))

update(db::DB, a::T, b::T) where T = begin
  wheres = []
  updates = []
  for (c,val) in zip(fieldnames(T), sqlrow(b))
    push!(getfield(a, c) == getfield(b, c) ? wheres : updates, c=>val)
  end
  @assert !isempty(updates)
  sql = "UPDATE \"$(table_name(T))\" SET $(eqwhat(updates)) WHERE $(eqwhat(wheres, " AND "))"
  r = DBInterface.execute(db, sql, (map(last, updates)..., map(last, wheres)...))
  !isempty(r)
end

eqwhat(pairs, sep=',') = join(("$(p[1])=?" for p in pairs), sep)

prepare(sql::SQLQuery) = begin
  @dynamic! let variables = []
    sprint(write, sql), variables[]
  end
end

query(db::DB, sql::SQLQuery; kv...) = begin
  sql = reduce(pairs(kv), init=sql) do out, (k,v)
    out |> SQLFunction{:(=)}([SQLReference(out.table, string(k)), v])
  end
  str, vars = prepare(sql)
  DBInterface.execute(db, str, vars)
end
