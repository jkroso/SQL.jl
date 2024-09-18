@use "github.com/jkroso/Prospects.jl" assoc append @struct
@use "github.com/jkroso/DynamicVar.jl" @dynamic!
@use MacroTools: @capture, MacroTools
@use Dates: Date

@dynamic! variables = []

abstract type SQLNode end

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
@struct struct SQLQuery <: SQLNode
  table::String=""
  select::Vector{SQLReference}=[]
  joins::Vector{Join}=[]
  wheres::Vector{SQLFunction}=[]
end
SQLReference(ref::AbstractString) = occursin('.', ref) ? SQLReference(split(ref, '.')...) : SQLReference("", ref)

write_reference(io, ref) = write(io, ref.table, '.', ref.column)
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

write_where(io, f::SQLFunction{:between}) = begin
  write_value(io, f.args[1])
  write(io, " BETWEEN ")
  write_value(io, f.args[2])
  write(io, " AND ")
  write_value(io, f.args[3])
end

write_query(io::IO, sql::SQLQuery) = begin
  mapjoin(write_reference, io, sql.select, ("SELECT ", ',', " FROM "))
  write(io, sql.table, ' ')
  isempty(sql.joins) || mapjoin(write_join, io, sql.joins, ("JOIN ", " JOIN ", " "))
  isempty(sql.joins) || mapjoin(write_where, io, sql.wheres, ("WHERE ", " AND ", ""))
end

Base.write(io::IO, sql::SQLQuery) = write_query(io, sql)
Base.:|>(a::SQLNode, b::SQLNode) = combine(a, b)

combine(a::SQLQuery, b::Join) = assoc(a, :joins, append(a.joins, b))
combine(a::SQLQuery, b::SQLFunction) = assoc(a, :wheres, append(a.wheres, b))
combine(a::SQLQuery, b::SQLReference) = assoc(a, :select, append(a.select, b))
combine(a::SQLQuery, b::Select) = assoc(a, :select, append(a.select, b.refs...))
combine(a::Join, b::SQLNode) = combine(convert(SQLQuery, a), b)
combine(a::SQLQuery, b::SQLQuery) = begin
  assoc(a, :table, isempty(a.table) ? b.table : a.table,
           :select, vcat(a.select, b.select),
           :joins, vcat(a.joins, b.joins),
           :wheres, vcat(a.wheres, b.wheres))
end

Base.convert(::Type{SQLQuery}, b::Join) = SQLQuery(joins=[b])

macro sql(exprs...)
  @capture quote $(exprs...) end begin lines__ end
  foldl((a,b)->:($a|>$b), map(sql_expr, lines))
end

const keywords = (:Where, :From, :Select, :Join, :As, :Order, :Limit, :Desc, :Asc, :Sort)
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
