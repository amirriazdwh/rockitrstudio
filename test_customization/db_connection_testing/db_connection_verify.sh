#!/usr/bin/env bash
# Verify DB connectors without needing any running DB servers

set -Eeuo pipefail
umask 022
trap 'echo "❌ Error on line $LINENO"; exit 1' ERR

command -v Rscript >/dev/null 2>&1 || { echo "Rscript not found."; exit 1; }

echo "🔎 Verifying DB connectors…"
Rscript --vanilla - <<'RS'
pkgs <- c("DBI","RSQLite","RPostgres","RMariaDB","duckdb","odbc","bigrquery")

# 1) Packages load
for (p in pkgs) {
  if (!requireNamespace(p, quietly = TRUE)) stop("Package not available: ", p)
}
cat("• Packages load: OK\n")

# 2) SQLite round-trip (in-memory)
con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
on.exit(try(DBI::dbDisconnect(con), silent=TRUE), add=TRUE)
DBI::dbWriteTable(con, "iris", iris[1:10,])
stopifnot(DBI::dbExistsTable(con, "iris"))
stopifnot(DBI::dbGetQuery(con, "select count(*) n from iris")$n == 10)
cat("• SQLite round-trip: OK\n")

# 3) DuckDB round-trip (in-memory)
con2 <- DBI::dbConnect(duckdb::duckdb(), dbdir=":memory:")
on.exit(try(DBI::dbDisconnect(con2, shutdown=TRUE), silent=TRUE), add=TRUE)
DBI::dbWriteTable(con2, "mtcars", mtcars[1:5,])
stopifnot(DBI::dbGetQuery(con2, "select count(*) n from mtcars")$n == 5)
cat("• DuckDB round-trip: OK\n")

# 4) ODBC manager presence (driver list may be empty unless you installed drivers)
drv <- tryCatch(odbc::odbcListDrivers(), error = function(e) e)
if (inherits(drv, "error")) stop("unixODBC not working: ", drv$message)
print(drv)
cat("• ODBC driver manager: OK (drivers above; may be empty if none installed)\n")

# 5) Driver objects exist for Postgres & MariaDB
stopifnot(is.list(RPostgres::Postgres()))
stopifnot(is.list(RMariaDB::MariaDB()))
cat("• Postgres/MariaDB driver objects: OK\n")

cat("\n✅ DB connectors verification SUCCESS.\n")
RS
