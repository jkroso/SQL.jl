@use "github.com/jkroso/Rutherford.jl/test" @test
@use ".." SQLQuery SQLReference Join SQLFunction Select @sql
@use "../db" DB query prepare
@use Dates: Date
@use SQLite: DBInterface

const db = DB("$(@dirname)/chinook.db")

@test first(query(db, @sql From(`Invoice`))).InvoiceId == 1

@test sprint(write, @sql From(`Invoice`) Select(`rowid`) Order(`rowid`)) == "SELECT Invoice.rowid FROM Invoice ORDER BY rowid"

a = SQLQuery("InvoiceLine",
             [SQLReference("InvoiceLine.UnitPrice"), SQLReference("InvoiceLine.Quantity")],
             [Join("Invoice")],
             [SQLFunction{:(=)}([SQLReference("InvoiceLine.invoiceId"),
                                 SQLReference("Invoice.invoiceId")]),
              SQLFunction{:between}([SQLReference("Invoice.InvoiceDate"), Date(2009), Date(2011)])])

@test a.table == "InvoiceLine"

b = SQLQuery("InvoiceLine") |>
             Select([SQLReference("InvoiceLine.UnitPrice"), SQLReference("InvoiceLine.Quantity")])|>
             Join("Invoice")|>
             SQLFunction{:(=)}([SQLReference("InvoiceLine.invoiceId"),
                                SQLReference("Invoice.invoiceId")])|>
             SQLFunction{:between}([SQLReference("Invoice.InvoiceDate"), Date(2009), Date(2011)])

@test a == b

table = "Invoice"

c = @sql begin
  From(`InvoiceLine`)
  Join(table, `InvoiceLine.invoiceId` == `$table.invoiceId`)
  Date(2009) <= `$table.InvoiceDate` <= Date(2011)
  Select(`InvoiceLine.UnitPrice`, `InvoiceLine.Quantity`)
end

@test a == c

@test sprint(write, c) == "SELECT InvoiceLine.UnitPrice,InvoiceLine.Quantity FROM InvoiceLine JOIN Invoice WHERE InvoiceLine.invoiceId = Invoice.invoiceId AND Invoice.InvoiceDate BETWEEN '2009-01-01' AND '2011-01-01'"

@test sum([r.Quantity for r in query(db, c)]) == 909
@test [r.Quantity for r in query(db, c; TrackID=2)] == [1]

d = c |> @sql `Invoice.customerID` == 2

@test sum([r.Quantity for r in query(db, d)]) == 25

e = c |> @sql Order(`InvoiceLine.Quantity`) Asc() Limit(1)
@test sum([r.Quantity for r in query(db, e)]) == 1
