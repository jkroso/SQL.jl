@use "github.com/jkroso/Prospects.jl" assoc append @struct
@use "github.com/jkroso/DynamicVar.jl" @dynamic!
@use MacroTools: @capture, MacroTools
@use Dates: Date

@dynamic! variables = []

abstract type SQLNode end
abstract type SQLOption <: SQLNode end

mapjoin(fn, io, itr, (pre, sep, post)=('(', ',', ')')) = begin
  first = true
  write(io, pre)
  for value in itr
    first ? (first = false) : write(io, sep)
    fn(io, value)
  end
  write(io, post)
end

@struct SQLFunction{name}(args::Vector) <: SQLNode
@struct SQLReference(table::String, column::String) <: SQLNode
@struct Join(table::String) <: SQLNode
@struct Select(refs::Vector{SQLReference}) <: SQLNode

@struct Limit(n::Integer) <: SQLOption
@struct Offset(n::Integer) <: SQLOption
@struct Desc() <: SQLOption
@struct Asc() <: SQLOption
@struct Order(ref::SQLReference) <: SQLOption

@struct struct SQLQuery <: SQLNode
  table::String=""
  select::Vector{SQLReference}=[]
  joins::Vector{Join}=[]
  wheres::Vector{SQLFunction}=[]
  options::Vector{SQLOption}=[]
end

SQLReference(ref::AbstractString) = occursin('.', ref) ? SQLReference(split(ref, '.')...) : SQLReference("", ref)
SQLReference(ref::Symbol) = SQLReference("", string(ref))
Base.convert(::Type{SQLReference}, x) = SQLReference(x)
Base.convert(::Type{SQLReference}, x::SQLReference) = x

write_reference(io, ref) = isempty(ref.table) ? write(io, ref.column) : write(io, ref.table, '.', ref.column)
write_join(io, join::Join) = write(io, join.table)
write_value(io, x) = begin
  push!(variables[], x)
  write(io, '?')
end
write_value(io, d::Date) = write(io, '\'', string(d), '\'')
write_value(io, r::SQLReference) = write_reference(io, r)

write_where(io, f::SQLFunction{name}) where name = begin
  mapjoin(write_value, io, f.args, ("", " $name ", ""))
end

write_where(io, f::SQLFunction{:(=)}) = begin
  if isnothing(f.args[2])
    write_value(io, f.args[1])
    write(io, " IS Null")
  else
    mapjoin(write_value, io, f.args, ("", " = ", ""))
  end
end

write_where(io, f::SQLFunction{:between}) = begin
  write_value(io, f.args[1])
  write(io, " BETWEEN ")
  write_value(io, f.args[2])
  write(io, " AND ")
  write_value(io, f.args[3])
end

write_option(io, o::Asc) = write(io, "ASC")
write_option(io, o::Desc) = write(io, "DESC")
write_option(io, o::Limit) = print(io, "LIMIT(", o.n, ')')
write_option(io, o::Offset) = print(io, "OFFSET(", o.n, ')')
write_option(io, o::Order) = begin
  write(io, "ORDER BY ")
  write_reference(io, o.ref)
end

write_query(io::IO, sql::SQLQuery) = begin
  if isempty(sql.select)
    write(io, "SELECT * FROM ")
  else
    mapjoin(write_reference, io, sql.select, ("SELECT ", ',', " FROM "))
  end
  write(io, sql.table)
  isempty(sql.joins) || mapjoin(write_join, io, sql.joins, (" JOIN ", " JOIN ", ""))
  isempty(sql.wheres) || mapjoin(write_where, io, sql.wheres, (" WHERE ", " AND ", ""))
  isempty(sql.options) || mapjoin(write_option, io, sql.options, (" ", " ", ""))
end

Base.write(io::IO, sql::SQLQuery) = write_query(io, sql)
Base.:|>(a::SQLNode, b::SQLNode) = combine(a, b)

combine(a::SQLQuery, b::Join) = assoc(a, :joins, append(a.joins, b))
combine(a::SQLQuery, b::SQLFunction) = assoc(a, :wheres, append(a.wheres, b))
combine(a::SQLQuery, b::SQLReference) = assoc(a, :select, append(a.select, namespace(a, b)))
namespace(a, b::SQLReference) = isempty(b.table) ? assoc(b, :table, a.table) : b
combine(a::SQLQuery, b::Select) = assoc(a, :select, append(a.select, (namespace(a, br) for br in b.refs)...))
combine(a::SQLQuery, b::SQLOption) = assoc(a, :options, append(a.options, b))
combine(a::SQLQuery, b::SQLQuery) = begin
  assoc(a, :table, isempty(a.table) ? b.table : a.table,
           :select, vcat(a.select, b.select),
           :joins, vcat(a.joins, b.joins),
           :wheres, vcat(a.wheres, b.wheres),
           :options, vcat(a.options, b.options))
end
combine(a::SQLNode, b::SQLNode) = combine(convert(SQLQuery, a), convert(SQLQuery, b))

Base.convert(::Type{SQLQuery}, b::Join) = SQLQuery(joins=[b])
Base.convert(::Type{SQLQuery}, b::SQLOption) = SQLQuery(options=[b])
Base.convert(::Type{SQLQuery}, b::SQLFunction) = SQLQuery(wheres=[b])
Base.convert(::Type{SQLQuery}, b::Select) = SQLQuery(select=[b])

macro sql(exprs...)
  @capture quote $(exprs...) end begin lines__ end
  foldl((a,b)->:($a|>$b), map(sql_expr, lines))
end

const keywords = (:Where, :From, :Select, :Join, :Order, :Limit, :Desc, :Asc, :Sort)
const functions = (:<, :>, :<=, :>=, :(==))
const function_map = Base.ImmutableDict(:(==) => :(=))

sql_expr(e) = esc(e)
sql_expr(e::Expr) = begin
  if @capture e a_ <= b_ <= c_
    :(SQLFunction{:between}([$(sql_expr(b)), $(sql_expr(a)), $(sql_expr(c))]))
  elseif @capture e a_ = b_
    :(SQLFunction{:(=)}([$(sql_expr(a)), $(sql_expr(b))]))
  elseif @capture e a_ ≈ b_
    :(SQLFunction{:like}([$(sql_expr(a)), $(sql_expr(b))]))
  elseif @capture(e, f_(a_, b_)) && f in functions
    :(SQLFunction{$(QuoteNode(get(function_map, f, f)))}([$(sql_expr(a)), $(sql_expr(b))]))
  elseif @capture(e, Join(table_, on_))
    :(Join($(esc(table))) |> $(sql_expr(on)))
  elseif @capture e Select(args__)
    :(Select([$(map(sql_expr, args)...)]))
  elseif @capture(e, From(table_))
    if @capture table ref_macrocall
      SQLQuery(ref.args[3])
    else
      :(SQLQuery($(esc(table))))
    end
  elseif @capture(e, f_(args__)) && f in keywords
    :($f($(map(sql_arg, args)...)))
  elseif @capture(e, ref_macrocall) && ref.args[1].name == Symbol("@cmd")
    ref = ref.args[3]
    m = match(r"\$\(?(\w+)\)?\.(\w+)?", ref)
    isnothing(m) && return SQLReference(ref)
    :(SQLReference($(esc(Meta.parse(m[1]))), $(m[2])))
  else
    esc(e)
  end
end

sql_arg(e) = begin
  if Meta.isexpr(e, :kw)
    Expr(:kw, map(sql_expr, e.args)...)
  elseif @capture e a_ => b_
    :($a => $(sql_expr(b)))
  else
    sql_expr(e)
  end
end
