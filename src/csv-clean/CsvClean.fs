module CsvClean

open System
open FSharp.Data

type CsvOrder = CsvProvider<"../../example/sample.csv", 
    Schema="CUSTOMER#=string, \
            INVOICE#=string, \
            INV DATE=date, \
            SHIP DATE=date, \
            SHIP-TO NAME=string, \
            SHIP-TO ADDR1=string, \
            SHIP-TO ADDR2=string, \
            SHIP-TO ADDR3=string, \
            SHIP-TO CITY=string, \
            SHIP-TO ST=string, \
            SHIP-TO ZIP=string, \
            SHIP-TO COUNTRY=string, \
            PRODUCT#=string, \
            QTY SHIP=int, \
            UNIT PRICE=string, \
            EXTENDED PRICE=string, \
            UNIT COST=string, \
            Vendor=string, \
            DEALER CUST ID=string, \
            DEALER PROD DESC=string">
type CsvProduct =  CsvProvider<
    Schema="Id (string),\
            Description(string),\
            Vendor (string),\
            UnitCost (decimal),\
            UnitPrice (decimal)", HasHeaders=false>

type Product = {
    Id: string
    Description: string
    Vendor: string
    UnitCost: decimal
    UnitPrice: decimal
}

let productToCsv (product: Product) =
    CsvProduct.Row(product.Id, product.Description, product.Vendor, product.UnitCost, product.UnitPrice)

let productsToCsv (products: Product seq) =
    let rows = 
        products 
        |> Seq.map productToCsv

    new CsvProduct(rows)

let writeProductCsv (products: Product seq) =
    let csvProducts = (productsToCsv products)

    csvProducts.Save("""C:\projects\csv-transform\data\products.csv""")

type Row = {
    InvoiceNumber: string
    InvoiceDate: DateTime
    ProductNumber: string
    Description: string
    Vendor: string
    UnitCost: decimal
    UnitPrice: decimal
    Quantity: int
}
let getProductFromRow (row: Row) =
    {
        Id = row.ProductNumber
        Description = row.Description
        Vendor = row.Vendor
        UnitCost = row.UnitCost
        UnitPrice = row.UnitPrice
    }: Product

let getProductsFromRows (rows: Row seq) =
    rows |> Seq.map getProductFromRow 

let getUniqueProductsFromRows (rows: Row seq) =
    let allProductFromRows = (getProductsFromRows rows)

    allProductFromRows
    |> Seq.sortBy (fun row -> row.Id)
    |> Seq.distinct


let priceFromFormatted (price: string) =
    try
        let rawPrice = 
            price
                .Replace("$", "")
                .Replace(",", "")
        (decimal rawPrice)
    with
    | :? System.Exception ->
        printfn "Bad Price: %A" price
        0m

let deserializeRow (row: CsvOrder.Row) =
    {
        InvoiceNumber = row.``INVOICE#``
        InvoiceDate = row.``INV DATE``
        ProductNumber = row.``PRODUCT#``
        Description = row.``DEALER PROD DESC``
        Vendor = row.Vendor
        UnitCost = (priceFromFormatted row.``UNIT PRICE``)
        UnitPrice = (priceFromFormatted row.``UNIT PRICE``)
        Quantity = row.``QTY SHIP``
    }: Row

let deserializeRows (rows: CsvOrder.Row seq) =
    rows |> Seq.map deserializeRow

let loadRows (path: string) = 
    let allRows = (CsvOrder.Load path).Rows 
    let deserializedRows = (deserializeRows allRows)
    let allProducts = (getUniqueProductsFromRows deserializedRows)
    (writeProductCsv allProducts)
    printfn "%A %d" allProducts (Seq.length allProducts)

[<EntryPoint>]
let main argv =
    let path = (Seq.head argv) 
    loadRows path
    printfn "%A" argv
    System.Console.WriteLine("Press Any Key to Continue...")
    System.Console.ReadKey() |> ignore
    0 // return an integer exit code
