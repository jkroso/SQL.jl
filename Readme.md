# SQL.jl

A simple query builder. Julia already has something [like this](https://github.com/MechanicalRabbit/FunSQL.jl) and I used it until I started having trouble with namespaces. This library makes namespaces simpler by just allowing you to use fully qualified names

### API

The syntax is essentially just plain SQL except all references to tables or table columns are wrapped in backticks

```julia
@use "github.com/jkroso/SQL.jl" @sql ["db" query] ["test/test" db]
import Dates.Date

table = "Invoice" # using a variable so we can demonstrate interpolation
sql = @sql begin
  From(`InvoiceLine`)
  Join(table, `InvoiceLine.invoiceId` == `$table.invoiceId`)
  Date(2009) <= `$table.InvoiceDate` <= Date(2011)
  Select(`InvoiceLine.UnitPrice`, `InvoiceLine.Quantity`)
end

sum(r->r.UnitPrice*r.Quantity, query(db, sql)) # total sales from 2009-2011
```

To compose two queries together just use `|>`

```julia
sum(r->r.Quantity, query(db, sql |> @sql `Invoice.customerID` == 2)) # total sales to a specific customer
```
