using HTTP: HTTP, request, URI
using JSON3: JSON3
using LibPQ: LibPQ, Connection, execute, load!
using DataFrames: DataFrame

conn = Connection("dbname = sdad");
page = 7139
while true
    global page
    response = request("POST",
                       URI(scheme = "https",
                           host = "api.usaspending.gov",
                           path = "/api/v2/recipient/duns"),
                       ["Content-Type" => "application/json"],
                       JSON3.write(Dict("sort" => "name", "limit" => 100, "page" => page)))
    json = JSON3.read(response.body)
    data = DataFrame(json.results)
    data[!,:duns] = something.(data[!,:duns], missing)
    execute(conn, "BEGIN;")
    load!(data, 
          conn,
          string("INSERT INTO us_spending.recipients VALUES(",
                 join(("\$$i" for i in 1:size(data, 2)), ','),
                 ") ON CONFLICT DO NOTHING;"))
    execute(conn, "COMMIT;")
    json.page_metadata.hasNext || break
    page = json.page_metadata.next
    println(page)
    # sleep(0.5)
end


execute(conn, "CREATE TABLE ")
