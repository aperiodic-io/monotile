#!/bin/sh
# Creates the live views of views.sql once brrrrr has made the tables from the topics' first rows.
until curl -sf http://brrrrr:4242/health >/dev/null; do sleep 1; done
until curl -s http://brrrrr:4242/tables | grep -q '"quotes"' && curl -s http://brrrrr:4242/tables | grep -q '"trades"'; do sleep 1; done
# one statement per request: views.sql's statements end with `;` on a line of its own
awk 'BEGIN { RS = ";" } NF { print > ("/tmp/view" ++n ".sql") }' /views.sql
for f in /tmp/view*.sql; do
  curl -s http://brrrrr:4242/query --data-binary @"$f"; echo
done
